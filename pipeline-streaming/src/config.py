"""
Configuracao do pipeline Streaming (Apache Flink).

Espelho do pipeline-nrt/src/config.py. Tudo que muda entre execucoes ou entre
ambientes fica aqui, nunca no meio do main.py — pelo mesmo motivo do lado do
Spark: a partir da Etapa RT-6 cada execucao grava um manifesto com a
configuracao usada, e ele so e confiavel se existir um unico lugar de onde ela
sai.

Toda variavel aceita override por variavel de ambiente. O run_local.sh as
repassa para dentro do conteiner.
"""

import os

from pyflink.version import __version__ as FLINK_VERSION


# --- Kafka ---------------------------------------------------------------
# kafka:29092 e o listener INTERNAL: este codigo roda DENTRO do conteiner, na
# rede do Docker. O Spark, que roda no host, usa localhost:9092 — assimetria
# declarada no docker-compose.yml e no planoRT.md secao 0.5.
BOOTSTRAP = os.getenv("BOOTSTRAP", "kafka:29092")

TOPIC = os.getenv("TOPIC", "ecommerce.events.v1")

# De onde comecar a ler. 'earliest-offset' e o equivalente exato do
# startingOffsets=earliest do Spark (decisao da Etapa 4, espelhada aqui pela
# regra de tuning simetrico): os eventos ja presentes no topico aparecem logo,
# provando a conexao antes de o gerador entrar.
#
# Mesma armadilha do lado do Spark, a partir da RT-3: com checkpoint, este valor
# so vale na primeira subida. Depois, quem manda e o checkpoint.
STARTUP_MODE = os.getenv("STARTUP_MODE", "earliest-offset")


# --- Destino (CONTRATO.md secao 4) ---------------------------------------
# /tcc-data e o ~/tcc-data do host, montado pelo docker-compose.yml.
#
# O <base> e POR MOTOR desde o CONTRATO.md 1.3: o Spark grava em
# ~/tcc-data/out/spark, o Flink aqui. Abaixo do <base>, o layout e identico:
#
#     <base>/events/run_id=<run_id>/dt=<YYYY-MM-DD>/hh=<HH>/part-*
BASE_OUT = os.getenv("BASE", "/tcc-data/out/flink")
DIR_EVENTOS = f"{BASE_OUT}/events"

# Codec do Parquet — CONTRATO.md secao 4.4. Declarado explicitamente, como no
# Spark: e o que impede os dois motores de divergirem por padroes diferentes.
PARQUET_CODEC = os.getenv("PARQUET_CODEC", "SNAPPY")

# Fuso de dt/hh — CONTRATO.md secao 4.1. Fixado na SESSAO, como no Spark
# (spark.sql.session.timeZone): nenhuma expressao de data escapa da regra.
TIMEZONE = "UTC"


# --- Checkpoint ------------------------------------------------------------
# Caminho FIXO por job, espelhando a decisao da Etapa 5 do lado do Spark
# (~/tcc-data/checkpoints/nrt). Mesma armadilha, mesma mitigacao: o job avisa
# em destaque na subida quando vai RETOMAR em vez de comecar limpo.
CHECKPOINT_DIR = os.getenv("CHECKPOINT", "/tcc-data/checkpoints/rt")

# Intervalo de checkpoint: no Flink, e o parametro equivalente ao trigger do
# Spark (planoRT.md secao 3.1). E ele que decide QUANDO o Parquet fica visivel
# no destino — a hipotese central do planoRT.md secao 1, testada nesta etapa.
#
# 5 s para desenvolvimento, o mesmo do trigger do Spark. Os valores do
# Experimento A sao a decisao F11 (provisoria ate a RT-6).
CHECKPOINT_INTERVAL = os.getenv("CHECKPOINT_INTERVAL", "5 s")


# --- Jars ----------------------------------------------------------------
# Todos definidos no Dockerfile, que e onde sao baixados e tem o SHA-1
# conferido. Lidos daqui para que cada versao exista em UM lugar so.
#
# DADO PARA O CRITERIO D1: o Spark resolve o conector Kafka sozinho do Maven, e
# escreve Parquet sem nada a mais. O Flink precisa de quatro jars obtidos e
# declarados a mao: o conector, o formato Parquet e dois do Hadoop.
KAFKA_CONNECTOR_JAR = os.environ["FLINK_KAFKA_CONNECTOR_JAR"]
JARS = [
    KAFKA_CONNECTOR_JAR,
    os.environ["FLINK_PARQUET_JAR"],
    os.environ["HADOOP_API_JAR"],
    os.environ["HADOOP_RUNTIME_JAR"],
]


def resumo() -> str:
    """Uma linha por parametro, impressa no inicio de toda execucao.

    Mesma funcao do resumo() do lado do Spark: a saida do console diz com o que
    ela foi produzida.
    """
    itens = [
        ("flink", FLINK_VERSION),
        ("jvm", os.getenv("JVM_ARGS", "(padrao da JVM)")),
        ("bootstrap", BOOTSTRAP),
        ("topico", TOPIC),
        ("startup", STARTUP_MODE),
        ("destino", DIR_EVENTOS),
        ("codec", PARQUET_CODEC),
        ("fuso", TIMEZONE),
        ("checkpoint", CHECKPOINT_DIR),
        ("intervalo", CHECKPOINT_INTERVAL),
        ("jars", ", ".join(os.path.basename(j) for j in JARS)),
    ]
    largura = max(len(k) for k, _ in itens)
    return "\n".join(f"  {k.ljust(largura)}  {v}" for k, v in itens)
