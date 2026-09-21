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


# --- Conector ------------------------------------------------------------
# Definido no Dockerfile, que e onde o jar e baixado e tem o SHA-1 conferido.
# Lido daqui para que a versao do conector exista em UM lugar so.
#
# Diferenca de mecanismo em relacao ao Spark, e ela e dado do criterio D1: o
# Spark resolve o conector sozinho do Maven; aqui o jar precisa estar no disco,
# e o caminho e declarado a mao.
KAFKA_CONNECTOR_JAR = os.environ["FLINK_KAFKA_CONNECTOR_JAR"]


def resumo() -> str:
    """Uma linha por parametro, impressa no inicio de toda execucao.

    Mesma funcao do resumo() do lado do Spark: a saida do console diz com o que
    ela foi produzida.
    """
    itens = [
        ("flink", FLINK_VERSION),
        ("bootstrap", BOOTSTRAP),
        ("topico", TOPIC),
        ("startup", STARTUP_MODE),
        ("conector", os.path.basename(KAFKA_CONNECTOR_JAR)),
    ]
    largura = max(len(k) for k, _ in itens)
    return "\n".join(f"  {k.ljust(largura)}  {v}" for k, v in itens)
