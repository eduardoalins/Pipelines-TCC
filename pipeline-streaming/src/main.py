"""
Etapa RT-2 — o Flink le o topico em streaming e imprime na tela.

Espelho da Etapa 4 do NRT. Sem Parquet, sem transformacao, sem medicao. O
objetivo e um so: ver o modelo evento-a-evento acontecer.

A diferenca para o Spark aparece na tela: la o console ficava parado durante o
trigger e depois despejava o lote inteiro. Aqui NAO EXISTE trigger — cada evento
atravessa o job assim que chega, e as linhas aparecem continuamente.

Roda dentro do conteiner, pelo lancador:

    bash pipeline-streaming/run_local.sh
"""

import signal
import sys
import threading
import time
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

# O conteiner monta o repositorio em /app e executa este arquivo pelo caminho;
# a pasta dele nao entra sozinha no sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import config  # noqa: E402

# Estados em que o job nao volta mais a rodar.
ESTADOS_FINAIS = ("FINISHED", "FAILED", "CANCELED")


def main() -> int:
    print("\n=== Etapa RT-2 — Flink lendo o Kafka ===")
    print(config.resumo())
    print()

    # Modo STREAMING: a fonte nao tem fim e o job fica vivo processando o que
    # chegar. (A verificacao da RT-1 usou modo batch, com fonte limitada,
    # justamente para terminar sozinha.)
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # O equivalente do --packages do Spark: o jar ja esta no disco da imagem, e
    # o caminho e declarado aqui.
    t_env.get_config().set("pipeline.jars", f"file://{config.KAFKA_CONNECTOR_JAR}")

    # Paralelismo: deixado no padrao do MiniCluster, que e o numero de nucleos
    # visiveis. O valor definitivo e a pergunta E2 do perguntas2.txt, a
    # responder antes da RT-3. Na tela, cada linha sai prefixada com o numero da
    # subtarefa que a imprimiu (ex.: "3>").

    # --- Fonte -----------------------------------------------------------
    # 'raw' na chave e no valor: nada e interpretado. A Etapa 4 do NRT tambem
    # mostrou o value como texto, e o criterio de pronto foi ver o schema do
    # contrato chegar inteiro. Interpretar o JSON e trabalho da RT-3, como foi
    # da Etapa 5 do lado do Spark.
    #
    # A chave e o user_id em UTF-8 (CONTRATO.md secao 1.3). Para le-la junto com
    # o valor, a chave vira uma coluna propria ('key.fields') e o valor passa a
    # excluir essa coluna ('value.fields-include' = 'EXCEPT_KEY'). No Spark isso
    # era um cast de uma coluna que ja vinha pronta — mais um dado de
    # configuracao para o criterio D1.
    #
    # `partition` e `offset` sao METADADOS do conector: vem do Kafka, nao do
    # JSON. VIRTUAL significa que sao so de leitura.
    #
    # Sem 'properties.group.id': como o Spark, o job nao gerencia offsets por
    # consumer group. Sem checkpoint, o Flink tambem nao os commita no broker.
    t_env.execute_sql(f"""
        CREATE TABLE eventos (
            `chave`     STRING,
            `evento`    STRING,
            `partition` INT    METADATA VIRTUAL,
            `offset`    BIGINT METADATA VIRTUAL
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{config.TOPIC}',
            'properties.bootstrap.servers' = '{config.BOOTSTRAP}',
            'scan.startup.mode'            = '{config.STARTUP_MODE}',
            'key.format'                   = 'raw',
            'key.fields'                   = 'chave',
            'value.format'                 = 'raw',
            'value.fields-include'         = 'EXCEPT_KEY'
        )
    """)

    # --- Saida -----------------------------------------------------------
    # O conector 'print' escreve cada linha na saida padrao, no momento em que
    # ela chega. E o equivalente do console sink do Spark, sem o lote.
    #
    # Sem checkpoint, de proposito — como o Spark da Etapa 4. Ele entra na RT-3.
    t_env.execute_sql("""
        CREATE TABLE tela (
            `partition` INT,
            `offset`    BIGINT,
            `chave`     STRING,
            `evento`    STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # execute_sql de um INSERT submete o job e volta na hora: o job passa a
    # rodar em segundo plano, dentro do MiniCluster.
    resultado = t_env.execute_sql("""
        INSERT INTO tela
        SELECT `partition`, `offset`, `chave`, `evento` FROM eventos
    """)
    job = resultado.get_job_client()

    print(">> job no ar. Cada evento aparece assim que chega — nao ha trigger.")
    print(">> Ctrl+C para parar.\n")

    # --- Encerramento ----------------------------------------------------
    # Mesmo desenho do lado do Spark, e pelo mesmo motivo: o Python controla o
    # motor por uma ponte py4j, e chamar a ponte de dentro do tratador de sinal
    # pode quebrar uma leitura em andamento. O tratador so marca um
    # sinalizador; o cancelamento acontece no laco, em ponto seguro.
    #
    # A diferenca em relacao ao Spark: aqui a JVM e filha do processo Python, e
    # nao o contrario. Isso deve permitir um cancelamento limpo do job — o que
    # no Spark nao foi possivel (DecisaoEtapa4.md secao 5.1). A verificar na
    # primeira execucao.
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
        # 130 e a convencao de "interrompido por SIGINT" — a mesma semantica do
        # lado do Spark, mas aqui emitida de proposito, apos um cancelamento
        # limpo, em vez de herdada de um processo morto pelo sinal.
        return 130

    print(f"\n>> o job terminou sozinho, com estado {estado}.")
    return 0 if "FINISHED" in estado else 1


if __name__ == "__main__":
    raise SystemExit(main())
