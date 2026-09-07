"""
Etapa 4 — o Spark le o topico e imprime no console.

Sem Parquet, sem transformacao, sem medicao. O objetivo e um so: provar que a
cadeia PySpark -> conector Kafka -> broker funciona, e ver o micro-batch
acontecer na tela.

Rodar pelo lancador, nunca por `python main.py`:

    bash pipeline-nrt/run_local.sh

O motivo esta no run_local.sh: --driver-memory e o --packages precisam ser
resolvidos ANTES da JVM subir, e este arquivo ja roda depois disso.
"""

import signal
import sys
import threading
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# O spark-submit entrega este arquivo pelo caminho completo, entao a pasta dele
# nao entra automaticamente no sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import config  # noqa: E402


def main() -> int:
    print("\n=== Etapa 4 — Spark lendo o Kafka ===")
    print(config.resumo())
    print()

    spark = (
        SparkSession.builder
        .appName("nrt-etapa4-console")
        .getOrCreate()
    )

    # O Spark loga em INFO por padrao e afoga a saida do console sink em
    # centenas de linhas por micro-batch. Em WARN sobra o que interessa.
    spark.sparkContext.setLogLevel("WARN")

    # --- Leitura ---------------------------------------------------------
    # readStream, nao read: a diferenca e que o segundo leria o topico uma vez
    # e terminaria. Aqui a consulta fica viva e reexecuta a cada trigger.
    #
    # failOnDataLoss fica no padrao (true) DE PROPOSITO. Se o Kafka descartar
    # por retencao offsets que a consulta ainda nao leu, ela falha alto em vez
    # de pular em silencio. Desligar isso esconderia perda de dados — e perda
    # de dados e exatamente o que o Experimento C existe para medir.
    #
    # Nao se configura kafka.group.id: o Spark nao gerencia offsets por consumer
    # group, ele os guarda no proprio checkpoint. Consequencia a resolver na
    # Etapa 6 (ver o DecisaoEtapa4.md): o kafka-consumer-groups.sh nao enxerga
    # o job do Spark, e o consumer lag — metrica central do Experimento B —
    # precisa ser obtido de outra forma.
    bruto = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.BOOTSTRAP)
        .option("subscribe", config.TOPIC)
        .option("startingOffsets", config.STARTING_OFFSETS)
        .load()
    )

    # O que o conector entrega: key, value, topic, partition, offset,
    # timestamp e timestampType. key e value vem como binary.
    #
    # O cast para string e a unica coisa feita com o dado. NAO se usa
    # from_json aqui: interpretar o JSON ja seria transformacao, e ela e da
    # Etapa 5 em diante. Como texto, o value ja mostra que o schema do
    # CONTRATO.md chegou inteiro, que e o criterio de pronto desta etapa.
    #
    # partition e offset entram na saida por serem uteis de olhar: mostram que
    # o job esta lendo as 24 particoes, e nao so uma.
    eventos = bruto.select(
        col("partition"),
        col("offset"),
        col("key").cast("string").alias("chave"),
        col("value").cast("string").alias("evento"),
    )

    # --- Escrita ---------------------------------------------------------
    # outputMode append: cada micro-batch mostra apenas as linhas novas. E o
    # unico modo que faz sentido sem agregacao — o pipeline e stateless
    # (decisao B1).
    #
    # numRows=5 e truncate=False: cinco eventos por lote, inteiros. Mostrar os
    # 20 padrao truncados em 20 caracteres esconderia justamente o JSON que se
    # quer conferir.
    #
    # Sem checkpointLocation, de proposito. Sem ele a consulta recomeca do
    # startingOffsets a cada execucao, que e o que se quer enquanto se testa.
    # O checkpoint entra na Etapa 5, e a partir dali passa a ser ele quem
    # decide de onde a leitura continua.
    consulta = (
        eventos.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 5)
        .trigger(processingTime=config.TRIGGER)
        .start()
    )

    print(f">> consulta no ar. Um micro-batch a cada {config.TRIGGER}.")
    print(">> Spark UI: http://localhost:4040  (aba Structured Streaming)")
    print(">> Ctrl+C para parar.\n")

    # --- Encerramento limpo ----------------------------------------------
    # O PySpark instala o proprio handler de SIGINT, e ele chama
    # cancelAllJobs() DE DENTRO do sinal. Se o Ctrl+C chega enquanto o py4j
    # esta lendo a resposta de outra chamada, a leitura e reentrada e o
    # processo morre com uma pilha de Py4JNetworkError.
    #
    # Trocar o handler por um que apenas marca um sinalizador resolve: nada e
    # chamado de dentro do sinal, e a parada acontece no laco abaixo, em ponto
    # seguro. Precisa vir DEPOIS do start(), senao o PySpark sobrescreve.
    #
    # Isso importa a partir da Etapa 8: o arnes de benchmark para o pipeline de
    # forma controlada e le o codigo de saida para decidir se a execucao vale.
    # Um encerramento que morre por excecao nao tem codigo de saida com
    # significado.
    encerrar = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: encerrar.set())

    while consulta.isActive and not encerrar.is_set():
        consulta.awaitTermination(1)

    print("\n>> parando a consulta...")
    consulta.stop()
    spark.stop()
    print(">> encerrado.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
