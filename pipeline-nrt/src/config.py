"""
Configuracao do pipeline NRT (Spark Structured Streaming).

Tudo que muda entre execucoes ou entre ambientes fica aqui, nunca no meio do
main.py. O motivo e experimental, nao estetico: a partir da Etapa 8 cada
execucao grava um manifesto com a configuracao que usou, e esse manifesto so e
confiavel se existir um unico lugar de onde a configuracao sai.

Toda variavel aceita override por variavel de ambiente, no mesmo idioma do
gerador (RATE=1000 DURATION=600 python generator.py). E o que permite ao arnes
da Etapa 8 varrer um fator por execucao sem editar arquivo nenhum.
"""

import os

import pyspark


# --- Kafka ---------------------------------------------------------------
# localhost:9092 e o listener EXTERNAL do broker, o mesmo que o gerador usa.
# Quem roda dentro da rede do Docker usaria kafka:29092; nada aqui roda la.
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")

TOPIC = os.getenv("TOPIC", "ecommerce.events.v1")

# De onde comecar a ler quando ainda NAO existe checkpoint (decisao da Etapa 4).
#
# Armadilha para lembrar a partir da Etapa 5: depois que o checkpoint existe,
# este valor deixa de ter efeito. Quem manda passa a ser o checkpoint, e mudar
# esta linha nao muda nada — sem erro, sem aviso.
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")


# --- Micro-batch ---------------------------------------------------------
# O trigger e o parametro que DEFINE o paradigma NRT deste trabalho: o Spark
# acumula eventos durante este intervalo e processa o lote inteiro de uma vez.
#
# 5 segundos e o trigger minimo do protocolo experimental e o que estabelece o
# piso pratico de latencia do Spark — o achado que o plano diz nunca cortar.
TRIGGER = os.getenv("TRIGGER", "5 seconds")

# maxOffsetsPerTrigger NAO e configurado, de proposito. Limitar offsets por
# trigger conteria o tamanho do lote, mas cria backlog artificial e
# descaracteriza a semantica do intervalo que esta sendo medido
# (planoNRT3 secao 2.2).


# --- Recursos ------------------------------------------------------------
# Em modo local o driver tambem executa o trabalho: este valor e o teto de
# memoria do pipeline inteiro.
#
# 1g e o numero em cima do qual o teto de 4 GB do WSL foi dimensionado na
# Etapa 0 (Kafka ~800 MB + Spark ~1,4 GB com overhead da JVM + gerador ~150 MB
# + kernel e daemon ~700 MB). Aumentar sem uma medida que justifique repete o
# erro diagnosticado la: os travamentos nao vinham do teto, vinham do Windows
# sem memoria para ceder.
#
# ATENCAO: este valor precisa chegar ao Spark pelo spark-submit, nao pelo
# SparkSession.builder. Ver o comentario no run_local.sh.
DRIVER_MEMORY = os.getenv("DRIVER_MEMORY", "1g")

MASTER = os.getenv("MASTER", "local[*]")


# --- Conector do Kafka ---------------------------------------------------
# O conector NAO e instalado por pip: o Spark o resolve do Maven em tempo de
# execucao e guarda em cache no ~/.ivy2.5.2 — o diretorio leva a versao do Ivy
# embutido no Spark (2.5.3 no Spark 4.0.4), e NAO e o ~/.ivy2 da convencao
# antiga. Conferido no log de 07/09/2026.
#
# O sufixo de Scala precisa casar com a versao do Spark — a linha 4.x usa
# 2.13, a 3.x usava 2.12 — e errar isso produz um ClassNotFoundException que
# nao diz qual e a causa real. O plano aponta esse erro como o motivo pelo
# qual projetos travam justamente nesta etapa.
#
# Por isso a versao e lida do PySpark instalado em vez de escrita a mao: se o
# requirements.txt mudar, esta linha acompanha sozinha e nao ha como as duas
# ficarem fora de sincronia.
SCALA_BINARY = os.getenv("SCALA_BINARY", "2.13")
SPARK_VERSION = pyspark.__version__
KAFKA_CONNECTOR = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{SPARK_VERSION}"


def resumo() -> str:
    """Uma linha por parametro, impressa no inicio de toda execucao.

    Existe para que a saida do console diga com o que ela foi produzida. E o
    embriao do manifesto da Etapa 8: um numero sem procedencia nao serve.
    """
    itens = [
        ("master", MASTER),
        ("driver.memory", DRIVER_MEMORY),
        ("bootstrap", BOOTSTRAP),
        ("topico", TOPIC),
        ("startingOffsets", STARTING_OFFSETS),
        ("trigger", TRIGGER),
        ("spark", SPARK_VERSION),
        ("conector", KAFKA_CONNECTOR),
    ]
    largura = max(len(k) for k, _ in itens)
    return "\n".join(f"  {k.ljust(largura)}  {v}" for k, v in itens)
