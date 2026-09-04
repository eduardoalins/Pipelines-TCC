"""Verifica a cadeia de compatibilidade inteira num comando so.

    Python -> PySpark -> JVM -> conector Kafka (Scala) -> broker -> topico

Rodar depois de qualquer mudanca de versao, ou quando algo parar de
funcionar sem motivo aparente. E este script que sustenta a decisao E7:
as versoes do requirements.txt nao foram escolhidas por suposicao, foram
verificadas aqui.

    docker compose up -d
    bash scripts/create_topic.sh
    ~/.venvs/tcc/bin/python scripts/check_chain.py
"""
import sys

from pyspark.sql import SparkSession

SPARK_VERSION = "4.0.4"
TOPIC = "ecommerce.events.v1"
BOOTSTRAP = "localhost:9092"
PARTICOES_ESPERADAS = 24

# Spark 4.x e compilado com Scala 2.13; a linha 3.x usava 2.12. Errar este
# sufixo produz ClassNotFoundException sem indicar a causa real.
KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"

spark = (
    SparkSession.builder.appName("check-chain")
    .master("local[2]")
    .config("spark.jars.packages", KAFKA_PKG)
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 62)
print("Python :", sys.version.split()[0])
print("Spark  :", spark.version)
print("Scala  :", spark.sparkContext._jvm.scala.util.Properties.versionString())
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
