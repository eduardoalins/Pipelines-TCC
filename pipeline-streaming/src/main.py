"""
Etapa RT-4 — o pipeline Flink passa a ser MEDIVEL.

Sobre a RT-3 (Parquet + checkpoint), o job agora:

  1. grava processed_ts por EVENTO, no Parquet (CONTRATO.md 2.5);
  2. commita no Kafka, a cada checkpoint, os offsets processados, sob um
     group.id proprio — para o collector.py medir o lag (decisao F7);
  3. imprime uma linha por checkpoint completo e a registra num log de
     progresso — o sinal de vida que faltava desde a RT-3.

O committed_ts do Flink NAO vem do log: e o ctime de cada arquivo, o instante
em que ele deixa de ser oculto (CONTRATO.md 1.6, 2.2). O log e conferencia.

Nao ha batch_id no Flink (CONTRATO.md 2.3): a unidade de progresso e o arquivo.

Roda dentro do conteiner, pelo lancador:

    bash pipeline-streaming/run_local.sh
"""

import json
import signal
import sys
import threading
import time
import urllib.request
from pathlib import Path

from pyflink.common import Configuration
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


def checkpoints_novos(job_id: str, vistos: set[int]) -> list[dict]:
    """Checkpoints completos ainda nao registrados, pela REST API do MiniCluster.

    A API guarda so os ultimos N (web.checkpoints.history, ajustado abaixo);
    consultando a cada segundo nenhum se perde. Falha de consulta nao derruba o
    job: o log e conferencia, nao a fonte da medicao.
    """
    url = f"http://localhost:{config.REST_PORT}/jobs/{job_id}/checkpoints"
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            historico = json.loads(r.read()).get("history", [])
    except Exception:
        return []
    novos = [h for h in historico
             if h.get("status") == "COMPLETED" and h["id"] not in vistos]
    return sorted(novos, key=lambda h: h["id"])


def registrar_progresso(linha: dict) -> None:
    Path(config.METRICS_DIR).mkdir(parents=True, exist_ok=True)
    with open(config.LOG_PROGRESSO, "a", encoding="utf-8") as f:
        f.write(json.dumps(linha, separators=(",", ":")) + "\n")


def main() -> int:
    print("\n=== Etapa RT-4 — Flink gravando Parquet com medicao ===")
    print(config.resumo())
    print()

    # A configuracao e passada NA CRIACAO do ambiente, e nao depois: a porta da
    # REST API e do cluster, nao do job, e so vale se existir antes de o
    # MiniCluster subir. Foi assim que as sondas de 24-25/09 a verificaram.
    conf = Configuration()
    conf.set_string("pipeline.jars", ";".join(f"file://{jar}" for jar in config.JARS))

    # CONTRATO.md secao 4.1: dt e hh derivam de event_ts em UTC. Fixado na
    # SESSAO, como o spark.sql.session.timeZone do lado do Spark.
    conf.set_string("table.local-time-zone", config.TIMEZONE)

    # --- Checkpoint -----------------------------------------------------
    # O intervalo e o equivalente do trigger do Spark. EXACTLY_ONCE e o padrao;
    # declarado para que ninguem precise adivinhar.
    conf.set_string("execution.checkpointing.interval", config.CHECKPOINT_INTERVAL)
    conf.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    conf.set_string("state.checkpoints.dir", f"file://{config.CHECKPOINT_DIR}")

    # O PADRAO DO FLINK E APAGAR os checkpoints quando o job e cancelado. Sem
    # esta linha, o Ctrl+C destruiria justamente o que o teste de retomada
    # precisa. O Spark nunca apaga o dele.
    conf.set_string(
        "execution.checkpointing.externalized-checkpoint-retention",
        "RETAIN_ON_CANCELLATION",
    )

    # REST API do MiniCluster, so dentro do conteiner. O historico de
    # checkpoints guarda 10 por padrao; 100 da folga para intervalos curtos.
    conf.set_string("rest.port", config.REST_PORT)
    conf.set_string("web.checkpoints.history", "100")

    # Retomada explicita — ver ultimo_checkpoint(). Mesmo aviso em destaque do
    # lado do Spark: o caminho do checkpoint e fixo, entao toda execucao herda
    # a anterior, e esquecer de limpa-lo antes de uma execucao medida NAO falha.
    retomar = ultimo_checkpoint(config.CHECKPOINT_DIR)
    if retomar is None:
        print(f">> checkpoint novo: {config.CHECKPOINT_DIR}")
    else:
        conf.set_string("execution.savepoint.path", f"file://{retomar}")
        barra = "!" * 72
        print(barra)
        print(f"!! CHECKPOINT EXISTENTE — retomando de {retomar}")
        print("!! O job NAO reprocessa o topico, e o startup mode deixa de valer.")
        print(f"!! Para comecar limpo:  rm -rf ~/tcc-data/checkpoints/rt")
        print(barra)
    print()

    t_env = TableEnvironment.create(
        EnvironmentSettings.new_instance().in_streaming_mode()
        .with_configuration(conf).build()
    )

    # --- Fonte -----------------------------------------------------------
    # O valor e INTERPRETADO: 'format' = 'json', com as colunas vindas da fonte
    # unica do schema (shared/schema/evento.json).
    #
    # Sem a coluna da chave: o Spark tambem a descartou. O destino tem os dez
    # campos do evento e as colunas de medicao.
    #
    # 'json.ignore-parse-errors': um evento malformado vira campos nulos em vez
    # de derrubar o job, como o from_json do Spark. A equivalencia exata dos
    # dois comportamentos e verificada na limpeza (RT-5).
    #
    # group.id + commit no checkpoint (decisao F7): o Flink devolve ao Kafka os
    # offsets do ultimo checkpoint completo — o mesmo papel do commit que o
    # Spark faz por codigo nosso depois de cada micro-batch. O commit NAO decide
    # a retomada; quem decide e o checkpoint, como no Spark.
    t_env.execute_sql(f"""
        CREATE TABLE eventos (
            {esquema.colunas_ddl()}
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{config.TOPIC}',
            'properties.bootstrap.servers' = '{config.BOOTSTRAP}',
            'properties.group.id'          = '{config.GROUP_ID}',
            'properties.commit.offsets.on.checkpoint' = 'true',
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
    # processed_ts (CONTRATO.md 2.5): as tres opcoes parquet.* abaixo foram
    # verificadas por sonda em 25/09/2026 e cada uma evita um defeito
    # silencioso:
    #   write.int64.timestamp  sem ela, o formato legado INT96
    #   timestamp.time.unit    micros, o mesmo que o Spark grava sem marca de UTC
    #   utc-timezone           sem ela, o VALOR depende do fuso da JVM: com a
    #                          JVM em America/Recife, todos os carimbos sairam
    #                          3 h deslocados, sem erro
    t_env.execute_sql(f"""
        CREATE TABLE destino (
            {esquema.colunas_ddl()},
            `processed_ts` TIMESTAMP_LTZ(3),
            `dt` STRING,
            `hh` STRING
        ) PARTITIONED BY (`run_id`, `dt`, `hh`) WITH (
            'connector'           = 'filesystem',
            'path'                = 'file://{config.DIR_EVENTOS}',
            'format'              = 'parquet',
            'parquet.compression' = '{config.PARQUET_CODEC}',
            'parquet.write.int64.timestamp' = 'true',
            'parquet.timestamp.time.unit'   = 'micros',
            'parquet.utc-timezone'          = 'true'
        )
    """)

    # dt e hh a partir de event_ts (milissegundos). FROM_UNIXTIME recebe
    # segundos e formata no fuso da SESSAO — que e UTC, fixado acima.
    #
    # processed_ts: CURRENT_ROW_TIMESTAMP() e reavaliado A CADA EVENTO por
    # definicao — o instante em que o evento passa por esta projecao, logo
    # antes de seguir para o escritor. Sem funcao Python (decisao F2).
    colunas = ", ".join(f"`{nome}`" for nome in esquema.nomes())
    resultado = t_env.execute_sql(f"""
        INSERT INTO destino
        SELECT {colunas},
               CURRENT_ROW_TIMESTAMP()                       AS `processed_ts`,
               FROM_UNIXTIME(`event_ts` / 1000, 'yyyy-MM-dd') AS `dt`,
               FROM_UNIXTIME(`event_ts` / 1000, 'HH')         AS `hh`
        FROM eventos
    """)
    job = resultado.get_job_client()
    job_id = str(job.get_job_id())

    print(f">> job no ar ({job_id}). Checkpoint a cada {config.CHECKPOINT_INTERVAL}.")
    print(f">> destino: {config.DIR_EVENTOS}")
    print(">> Ctrl+C para parar.\n")

    # --- Encerramento e sinal de vida -----------------------------------
    # Mesmo desenho da RT-2: o tratador de sinal so marca um sinalizador, e o
    # cancelamento acontece no laco, em ponto seguro. Com
    # RETAIN_ON_CANCELLATION, o ultimo checkpoint completo sobrevive ao Ctrl+C.
    #
    # O laco que ja consultava o estado do job passa a consultar tambem os
    # checkpoints: uma linha por checkpoint completo, o equivalente do
    # "[batch N]" do Spark.
    encerrar = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: encerrar.set())
    signal.signal(signal.SIGTERM, lambda *_: encerrar.set())

    vistos: set[int] = set()
    estado = ""
    while not encerrar.is_set():
        estado = str(job.get_job_status().result()).upper()
        if any(final in estado for final in ESTADOS_FINAIS):
            break
        for ck in checkpoints_novos(job_id, vistos):
            vistos.add(ck["id"])
            registrar_progresso({
                "motor": "flink",
                "checkpoint_id": ck["id"],
                "inicio_ts": ck["trigger_timestamp"],
                "committed_ts": ck["latest_ack_timestamp"],
            })
            print(f"[checkpoint {ck['id']}] completo  "
                  f"duracao={ck['latest_ack_timestamp'] - ck['trigger_timestamp']} ms  "
                  f"estado={ck.get('state_size', 0)} bytes")
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
