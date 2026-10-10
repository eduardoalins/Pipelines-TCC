"""Verifica a cadeia de compatibilidade inteira num comando so.

    Python -> PySpark -> JVM -> conector Kafka (Scala) -> broker -> topico

Rodar depois de qualquer mudanca de versao, ou quando algo parar de
funcionar sem motivo aparente. E este script que sustenta a decisao E7:
as versoes do requirements.txt nao foram escolhidas por suposicao, foram
verificadas aqui.

    docker compose up -d
    bash scripts/create_topic.sh

Roda no conteiner do Spark, que e onde o PySpark esta instalado desde
10/10/2026 (ate la rodava no host, pelo venv):

    docker compose run --rm spark python scripts/check_chain.py
"""
import glob
import os
import sys

from pyspark.sql import SparkSession

SPARK_VERSION = "4.0.4"
TOPIC = "ecommerce.events.v1"
PARTICOES_ESPERADAS = 24

# No host, localhost:9092 (listener EXTERNAL). No conteiner, o docker-compose.yml
# define BOOTSTRAP=kafka:29092 (listener INTERNAL).
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")

# Spark 4.x e compilado com Scala 2.13; a linha 3.x usava 2.12. Errar este
# sufixo produz ClassNotFoundException sem indicar a causa real.
KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"

construtor = (
    SparkSession.builder.appName("check-chain")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
)

# De onde vem o conector. Na imagem, os jars ja foram resolvidos no build e
# estao em SPARK_EXTRA_JARS_DIR: entram por spark.jars, sem tocar no Maven. No
# host, o Spark os resolve do Maven (cache em ~/.ivy2.5.2).
pasta_jars = os.getenv("SPARK_EXTRA_JARS_DIR")
if pasta_jars:
    jars = sorted(glob.glob(os.path.join(pasta_jars, "*.jar")))
    if not jars:
        sys.exit(f"FALHOU: nenhum jar em {pasta_jars} — a imagem foi construida?")
    construtor = construtor.config("spark.jars", ",".join(jars))
    origem = f"{len(jars)} jars da imagem ({pasta_jars})"
else:
    construtor = construtor.config("spark.jars.packages", KAFKA_PKG)
    origem = f"Maven ({KAFKA_PKG})"

spark = construtor.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

jvm = spark.sparkContext._jvm.java.lang.System
print("=" * 62)
print("Python   :", sys.version.split()[0])
print("Spark    :", spark.version)
print("Scala    :", spark.sparkContext._jvm.scala.util.Properties.versionString())
print("Java     :", jvm.getProperty("java.vendor"), jvm.getProperty("java.runtime.version"))
print("Conector :", origem)
print("Broker   :", BOOTSTRAP)
print("=" * 62)

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

mensagens = df.count()
particoes = df.rdd.getNumPartitions()

print(f"\nmensagens no topico : {mensagens}")
print(f"particoes vistas    : {particoes}")

df.selectExpr("CAST(value AS STRING) AS valor", "partition", "offset").show(
    5, truncate=False
)

spark.stop()

# Uma particao quando esperavamos 24 e o sintoma classico de topico criado
# por engano pelo auto-create do Kafka — que esta desligado no
# docker-compose.yml justamente para isso falhar alto.
if particoes != PARTICOES_ESPERADAS:
    print(
        f"\nFALHOU: o Spark viu {particoes} particoes, esperado "
        f"{PARTICOES_ESPERADAS}.\n"
        f"Confira o nome do topico e rode scripts/create_topic.sh."
    )
    sys.exit(1)

print("\nCADEIA COMPLETA OK")
