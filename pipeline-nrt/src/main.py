"""
Etapa 5 — o Spark le o topico e grava Parquet, com checkpoint.

O console sai, entra escrita em arquivo. E a primeira vez que o pipeline produz
um ARTEFATO em vez de mostrar dados na tela.

Ainda sem as colunas de medicao (batch_id, processed_ts, watermark_ts,
is_late) — elas entram na Etapa 6, dentro do mesmo foreachBatch.

Rodar pelo lancador, nunca por `python main.py`:

    bash pipeline-nrt/run_local.sh

O motivo esta no run_local.sh: --driver-memory e --packages precisam ser
resolvidos ANTES da JVM subir, e este arquivo ja roda depois disso.
"""

import signal
import sys
import threading
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_json, timestamp_millis

# O spark-submit entrega este arquivo pelo caminho completo, entao a pasta dele
# nao entra automaticamente no sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import config  # noqa: E402
import esquema  # noqa: E402


def inspecionar_checkpoint(caminho: str) -> None:
    """Avisa em destaque quando o job vai RETOMAR em vez de comecar limpo.

    O checkpoint fica num caminho fixo por job (decisao da Etapa 5), o que
    significa que toda execucao herda os offsets da anterior. Esquecer de
    limpa-lo antes de uma execucao medida NAO falha: o job sobe, processa so o
    que chegou depois e reporta numeros menores sem nenhum aviso.

    Esta funcao nao impede o erro — torna impossivel nao ve-lo. Mesmo principio
    do auto.create.topics.enable=false.
    """
    pasta = Path(caminho)
    if not pasta.exists():
        print(f">> checkpoint novo: {caminho}")
        return

    ultimo = None
    offsets = pasta / "offsets"
    if offsets.is_dir():
        lotes = [int(f.name) for f in offsets.iterdir() if f.name.isdigit()]
        if lotes:
            ultimo = max(lotes)

    barra = "!" * 72
    print(barra)
    print(f"!! CHECKPOINT EXISTENTE em {caminho}")
    if ultimo is not None:
        print(f"!! Ultimo micro-batch registrado: {ultimo}")
    print("!! O job vai RETOMAR de onde parou. Ele NAO reprocessa o topico,")
    print("!! e startingOffsets deixa de ter qualquer efeito.")
    print(f"!! Para comecar limpo:  rm -rf {caminho}")
    print(barra)


def escrever_lote(lote_df, batch_id: int) -> None:
    """Grava um micro-batch em Parquet.

    Escrita em foreachBatch, e nao pelo sink nativo de arquivo, porque a partir
    da Etapa 6 e aqui dentro que entram batch_id e watermark_ts — o batch_id nao
    existe como expressao de coluna, e o watermark acumulado precisa de estado no
    driver. Adotar o mecanismo definitivo agora evita trocar de sink com um
    checkpoint ja existente, o que nao funciona.

    CONSEQUENCIA REGISTRADA: o escritor de lote NAO gera o diretorio
    `_spark_metadata`, que so o sink nativo produz. A decisao B3 do planoNRT3
    manda o reconcile.py filtrar pela lista de arquivos commitados extraida dele
    — e ela precisa ser reescrita para reconciliar por (gen_id, seq), que e para
    o que o campo seq foi criado.
    """
    # O count() forcaria uma segunda leitura do Kafka sem o persist. A 100 evt/s
    # o custo e irrelevante e o retorno visual vale; na Etapa 6 isso e
    # substituido pelo numInputRows do StreamingQueryProgress, que sai de graca.
    lote_df.persist()
    try:
        n = lote_df.count()
        if n == 0:
            print(f"[batch {batch_id}] vazio")
            return

        (
            lote_df.write.mode("append")
            # Gera <destino>/run_id=.../dt=.../hh=... — o layout do
            # CONTRATO.md secao 4. As tres colunas saem do conteudo dos
            # arquivos e passam a viver no caminho; o DuckDB as le de volta.
            .partitionBy("run_id", "dt", "hh")
            .option("compression", config.PARQUET_CODEC)
            .parquet(config.DIR_EVENTOS)
        )
        print(f"[batch {batch_id}] {n} eventos gravados")
    finally:
        lote_df.unpersist()


def main() -> int:
    print("\n=== Etapa 5 — Spark gravando Parquet ===")
    print(config.resumo())
    print()
    inspecionar_checkpoint(config.CHECKPOINT_DIR)
    print()

    spark = (
        SparkSession.builder.appName("nrt-etapa5-parquet")
        # CONTRATO.md secao 4.1: dt e hh derivam de event_ts em UTC. Fixar o
        # fuso da SESSAO e o que garante isso — sem esta linha, a mesma entrada
        # geraria diretorios diferentes na maquina local (America/Recife) e na
        # nuvem (us-east-1).
        .config("spark.sql.session.timeZone", config.TIMEZONE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Leitura ---------------------------------------------------------
    # failOnDataLoss fica no padrao (true) de proposito: se o Kafka descartar
    # por retencao offsets que a consulta ainda nao leu, ela falha alto em vez
    # de pular em silencio. Perda de dados e o que o Experimento C mede.
    bruto = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.BOOTSTRAP)
        .option("subscribe", config.TOPIC)
        .option("startingOffsets", config.STARTING_OFFSETS)
        .load()
    )

    # --- Interpretacao ---------------------------------------------------
    # Primeiro from_json do projeto. Ate a Etapa 4 o value era texto.
    eventos = (
        bruto.select(
            from_json(col("value").cast("string"), esquema.EVENTO).alias("e")
        )
        .select("e.*")
        # event_ts e epoch em milissegundos; timestamp_millis converte sem
        # passar por divisao em ponto flutuante.
        .withColumn("_ts", timestamp_millis(col("event_ts")))
        .withColumn("dt", date_format(col("_ts"), "yyyy-MM-dd"))
        .withColumn("hh", date_format(col("_ts"), "HH"))
        .drop("_ts")
    )

    # --- Escrita ---------------------------------------------------------
    # checkpointLocation e o que torna o pipeline reiniciavel: e nele que ficam
    # os offsets ja processados. Sem ele nao existe "retomar", e o Experimento C
    # fica sem objeto.
    #
    # Sem repartition nem coalesce (decisao E3, provisoria ate esta etapa).
    # Escreve-se com o paralelismo natural, e o numero de arquivos por execucao
    # e registrado como METRICA — ele governa o custo de PUT no S3 e sustenta o
    # trade-off latencia-versus-custo.
    consulta = (
        eventos.writeStream.foreachBatch(escrever_lote)
        .outputMode("append")
        .option("checkpointLocation", config.CHECKPOINT_DIR)
        .trigger(processingTime=config.TRIGGER)
        .start()
    )

    print(f">> consulta no ar. Um micro-batch a cada {config.TRIGGER}.")
    print(f">> destino: {config.DIR_EVENTOS}")
    print(">> Spark UI: http://localhost:4040  (aba Structured Streaming)")
    print(">> Ctrl+C para parar (sai com 130, ver DecisaoEtapa4.md 5.1).\n")

    # --- Encerramento ----------------------------------------------------
    # O PySpark instala o proprio handler de SIGINT, e ele chama cancelAllJobs()
    # DE DENTRO do sinal, o que quebra a leitura do py4j. Trocar por um handler
    # que apenas marca um sinalizador elimina a pilha de erro.
    #
    # Nao resolve o codigo de saida: o Ctrl+C vai para o grupo de processos e a
    # JVM em primeiro plano encerra por ele, entao o processo sai com 130. Um
    # codigo com significado exige o job ter condicao de parada propria —
    # pendencia da Etapa 8.
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
