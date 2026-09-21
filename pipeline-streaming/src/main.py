"""
Etapa RT-3 — o Flink le o topico em streaming e grava Parquet, com checkpoint.

Espelho da Etapa 5 do NRT: o `print` sai, entra escrita em arquivo no layout do
contrato. Ainda sem as colunas de medicao — elas entram na RT-4, nos dois
motores.

A etapa testa a hipotese central do planoRT.md secao 1: no Flink, o Parquet so
fica visivel quando um checkpoint completa. Se isso se confirmar, o intervalo de
checkpoint tem, na visibilidade do dado, o papel que o trigger tem no Spark.

Roda dentro do conteiner, pelo lancador:

    bash pipeline-streaming/run_local.sh
"""

import signal
import sys
import threading
import time
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config  # noqa: E402
import esquema  # noqa: E402

ESTADOS_FINAIS = ("FINISHED", "FAILED", "CANCELED")


def ultimo_checkpoint(raiz: str) -> Path | None:
    """O checkpoint completo mais recente, ou None se nao houver nenhum.

    DIFERENCA DE MECANISMO EM RELACAO AO SPARK, e ela e dado do criterio D1. O
    Spark retoma sozinho: basta apontar o checkpointLocation. O Flink nao. Cada
    execucao do job ganha um identificador novo, e os checkpoints ficam em
    <raiz>/<id-do-job>/chk-<n>/. Para retomar, e preciso ENCONTRAR o ultimo e
    informa-lo explicitamente. E so conta um checkpoint que terminou — o
    _metadata e gravado por ultimo, entao a presenca dele e a prova.
    """
    completos = [meta.parent for meta in Path(raiz).glob("*/chk-*/_metadata")]
    if not completos:
        return None
    return max(completos, key=lambda chk: (chk / "_metadata").stat().st_mtime)


def main() -> int:
    print("\n=== Etapa RT-3 — Flink gravando Parquet ===")
    print(config.resumo())
    print()

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    conf = t_env.get_config()

    conf.set("pipeline.jars", ";".join(f"file://{jar}" for jar in config.JARS))

    # CONTRATO.md secao 4.1: dt e hh derivam de event_ts em UTC. Fixado na
    # SESSAO, como o spark.sql.session.timeZone do lado do Spark.
    conf.set("table.local-time-zone", config.TIMEZONE)

    # --- Checkpoint -----------------------------------------------------
    # O intervalo e o equivalente do trigger do Spark. EXACTLY_ONCE e o padrao;
    # declarado para que ninguem precise adivinhar.
    conf.set("execution.checkpointing.interval", config.CHECKPOINT_INTERVAL)
    conf.set("execution.checkpointing.mode", "EXACTLY_ONCE")
    conf.set("state.checkpoints.dir", f"file://{config.CHECKPOINT_DIR}")

    # O PADRAO DO FLINK E APAGAR os checkpoints quando o job e cancelado. Sem
    # esta linha, o Ctrl+C destruiria justamente o que o teste de retomada
    # precisa. O Spark nunca apaga o dele.
    conf.set(
        "execution.checkpointing.externalized-checkpoint-retention",
        "RETAIN_ON_CANCELLATION",
    )

    # Retomada explicita — ver ultimo_checkpoint(). Mesmo aviso em destaque do
    # lado do Spark: o caminho do checkpoint e fixo, entao toda execucao herda
    # a anterior, e esquecer de limpa-lo antes de uma execucao medida NAO falha.
    retomar = ultimo_checkpoint(config.CHECKPOINT_DIR)
    if retomar is None:
        print(f">> checkpoint novo: {config.CHECKPOINT_DIR}")
    else:
        conf.set("execution.savepoint.path", f"file://{retomar}")
        barra = "!" * 72
        print(barra)
        print(f"!! CHECKPOINT EXISTENTE — retomando de {retomar}")
        print("!! O job NAO reprocessa o topico, e o startup mode deixa de valer.")
        print(f"!! Para comecar limpo:  rm -rf ~/tcc-data/checkpoints/rt")
        print(barra)
    print()

    # --- Fonte -----------------------------------------------------------
    # Agora o valor e INTERPRETADO: 'format' = 'json', com as colunas vindas da
    # fonte unica do schema (shared/schema/evento.json). Primeiro JSON
    # interpretado do lado do Flink — como o primeiro from_json foi na Etapa 5
    # do lado do Spark.
    #
    # Sem a coluna da chave: o Spark da Etapa 5 tambem a descartou. O destino
    # tem os dez campos do evento e mais nada.
    #
    # 'json.ignore-parse-errors': um evento malformado vira campos nulos em vez
    # de derrubar o job, como o from_json do Spark. A equivalencia exata dos
    # dois comportamentos e verificada na limpeza (RT-5).
    t_env.execute_sql(f"""
        CREATE TABLE eventos (
            {esquema.colunas_ddl()}
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{config.TOPIC}',
            'properties.bootstrap.servers' = '{config.BOOTSTRAP}',
            'scan.startup.mode'            = '{config.STARTUP_MODE}',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)

    # --- Destino ---------------------------------------------------------
    # Layout do CONTRATO.md secao 4: PARTITIONED BY gera
    # run_id=.../dt=.../hh=..., e as tres colunas saem do conteudo dos
    # arquivos para o caminho — o mesmo que o partitionBy do Spark faz.
    #
    # Sem repartition, como o Spark (decisao E3): cada subtarefa escreve os
    # proprios arquivos, e o numero de arquivos e resultado medido.
    #
    # 'parquet.compression': o codec do contrato, declarado. O formato Parquet
    # do Flink repassa as opcoes parquet.* para o escritor do parquet-hadoop.
    t_env.execute_sql(f"""
        CREATE TABLE destino (
            {esquema.colunas_ddl()},
            `dt` STRING,
            `hh` STRING
        ) PARTITIONED BY (`run_id`, `dt`, `hh`) WITH (
            'connector'           = 'filesystem',
            'path'                = 'file://{config.DIR_EVENTOS}',
            'format'              = 'parquet',
            'parquet.compression' = '{config.PARQUET_CODEC}'
        )
    """)

    # dt e hh a partir de event_ts (milissegundos). FROM_UNIXTIME recebe
    # segundos e formata no fuso da SESSAO — que e UTC, fixado acima.
    colunas = ", ".join(f"`{nome}`" for nome in esquema.nomes())
    resultado = t_env.execute_sql(f"""
        INSERT INTO destino
        SELECT {colunas},
               FROM_UNIXTIME(`event_ts` / 1000, 'yyyy-MM-dd') AS `dt`,
               FROM_UNIXTIME(`event_ts` / 1000, 'HH')         AS `hh`
        FROM eventos
    """)
    job = resultado.get_job_client()

    print(f">> job no ar. Checkpoint a cada {config.CHECKPOINT_INTERVAL}.")
    print(f">> destino: {config.DIR_EVENTOS}")
    print(">> Ctrl+C para parar.\n")

    # --- Encerramento ----------------------------------------------------
    # Mesmo desenho da RT-2: o tratador de sinal so marca um sinalizador, e o
    # cancelamento acontece no laco, em ponto seguro. Com
    # RETAIN_ON_CANCELLATION, o ultimo checkpoint completo sobrevive ao Ctrl+C.
    encerrar = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: encerrar.set())
    signal.signal(signal.SIGTERM, lambda *_: encerrar.set())

    estado = ""
    while not encerrar.is_set():
        estado = str(job.get_job_status().result()).upper()
        if any(final in estado for final in ESTADOS_FINAIS):
            break
        time.sleep(1)

    if encerrar.is_set():
        print("\n>> cancelando o job...")
        job.cancel().result()
        print(">> encerrado pelo usuario.")
        return 130

    print(f"\n>> o job terminou sozinho, com estado {estado}.")
    return 0 if "FINISHED" in estado else 1


if __name__ == "__main__":
    raise SystemExit(main())
