"""
Etapa 6 — o pipeline passa a ser MEDIVEL.

Sobre a Etapa 5 (Parquet + checkpoint), cada micro-batch agora:

  1. grava batch_id e processed_ts como colunas do Parquet;
  2. registra committed_ts, linhas e offsets num log JSON, fora do Parquet;
  3. commita os offsets gravados no Kafka, para o collector.py medir o lag.

watermark_ts e is_late NAO sao calculados aqui: saem da analise, pelo
reconcile.py (CONTRATO.md 2.1, versao 1.4).

Rodar pelo lancador, nunca por `python main.py`:

    bash pipeline-nrt/run_local.sh

O motivo esta no run_local.sh: --driver-memory e --packages precisam ser
resolvidos ANTES da JVM subir, e este arquivo ja roda depois disso.
"""

import json
import signal
import sys
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    date_format,
    from_json,
    lit,
    timestamp_millis,
)
from pyspark.sql.functions import max as max_

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


# Colunas de controle do Kafka que viajam ate o foreachBatch so para o commit
# de offsets. Saem antes da escrita: nao fazem parte do contrato.
KAFKA_COLS = ["_kafka_partition", "_kafka_offset"]

# Cliente do Kafka usado APENAS para commitar offsets sob config.GROUP_ID. Nunca
# se inscreve em topico nem consome nada. Criado uma vez, no driver.
_commitador = None


def commitar_offsets(proximos: dict[int, int]) -> bool:
    """Commita no Kafka, sob config.GROUP_ID, o progresso de um micro-batch.

    `proximos` mapeia particao -> PROXIMO offset a ler (ultimo gravado + 1). E a
    semantica padrao do Kafka, e e a mesma que o Flink usa ao commitar no
    checkpoint — com ela, lag = latest - committed vale igual para os dois.

    Falha NAO derruba o batch: a escrita ja aconteceu, e relancar a excecao
    faria o Spark refazer o batch e gravar duplicatas por causa de um problema
    do INSTRUMENTO, nao do pipeline. Em vez disso a falha e impressa em destaque
    e registrada no log de progresso (commit_kafka_ok=false), onde o
    collector.py e a analise a enxergam.

    Erros TRANSITORIOS sao repetidos. Visto em 24/09/2026: o primeiro commit de
    um grupo que ainda nao existe recebe NOT_COORDINATOR, porque o broker ainda
    esta elegendo o coordenador do grupo. O Kafka marca esse erro como
    retriable. Sem repetir, o primeiro batch de TODA execucao num cluster novo
    (toda execucao na AWS) ficaria sem lag medido.
    """
    global _commitador
    if _commitador is None:
        _commitador = Consumer({
            "bootstrap.servers": config.BOOTSTRAP,
            "group.id": config.GROUP_ID,
            "enable.auto.commit": False,
        })
    particoes = [TopicPartition(config.TOPIC, p, o) for p, o in proximos.items()]

    tentativas = 5
    for tentativa in range(1, tentativas + 1):
        try:
            _commitador.commit(offsets=particoes, asynchronous=False)
            if tentativa > 1:
                print(f">> commit de offsets aceito na tentativa {tentativa}")
            return True
        except KafkaException as e:
            erro = e.args[0]
            if erro.retriable() and tentativa < tentativas:
                time.sleep(0.2 * tentativa)
                continue
            print(f"!! commit de offsets FALHOU apos {tentativa} tentativa(s) "
                  f"(lag deste batch nao medido): {erro}", file=sys.stderr)
            return False
    return False


def registrar_progresso(linha: dict) -> None:
    """Acrescenta uma linha ao log de progresso (config.LOG_PROGRESSO).

    JSON Lines, uma linha por micro-batch, com flush a cada escrita: se o job
    morrer no meio da execucao (Experimento C), tudo que ja foi commitado
    continua registrado.
    """
    Path(config.METRICS_DIR).mkdir(parents=True, exist_ok=True)
    with open(config.LOG_PROGRESSO, "a", encoding="utf-8") as f:
        f.write(json.dumps(linha, separators=(",", ":")) + "\n")


def escrever_lote(lote_df, batch_id: int) -> None:
    """Grava um micro-batch em Parquet e registra os instantes de medicao.

    Escrita em foreachBatch, e nao pelo sink nativo de arquivo, porque o
    batch_id nao existe como expressao de coluna — ele so chega aqui, como
    parametro.

    CONSEQUENCIA REGISTRADA: o escritor de lote NAO gera o diretorio
    `_spark_metadata`, que so o sink nativo produz. Por isso o reconcile.py
    reconcilia por (gen_id, seq), e nao pela lista de arquivos commitados
    (decisao B3 do planoNRT3, revogada).

    GARANTIA: at-least-once. Se o job cair depois do write e antes de o Spark
    registrar o batch como concluido, o batch e refeito na retomada com o MESMO
    batch_id e gravado de novo. O reconcile.py reporta essas duplicatas; nao as
    remove.
    """
    # inicio_ts: o primeiro dos tres instantes do batch, tomado ao entrar aqui.
    # Com ele a latencia se decompoe nas tres parcelas do planoNRT3 §3.1:
    #   espera no trigger = inicio_ts    - event_ts
    #   processamento     = processed_ts - inicio_ts   (leitura do Kafka inclusa)
    #   escrita           = committed_ts - processed_ts
    # e as duas latencias da decisao B6 saem das somas: processed_ts - event_ts
    # (processamento) e committed_ts - event_ts (visibilidade).
    inicio_ts = int(time.time() * 1000)

    # persist: o lote e lido duas vezes (agregacao de offsets + escrita). Sem
    # ele, cada acao releria o Kafka.
    lote_df.persist()
    try:
        # Uma unica acao entrega as tres coisas que o batch precisa saber sobre
        # si: quantas linhas, de quais particoes, ate qual offset. Substitui o
        # count() da Etapa 5.
        por_particao = (
            lote_df.groupBy("_kafka_partition")
            .agg(max_("_kafka_offset").alias("ultimo"), count("*").alias("n"))
            .collect()
        )
        n = sum(r["n"] for r in por_particao)
        if n == 0:
            print(f"[batch {batch_id}] vazio")
            return

        # processed_ts: UM valor por batch, tomado imediatamente antes da escrita
        # (CONTRATO.md 2). E um lit(), nao current_timestamp(): o valor sai do
        # relogio do driver, o mesmo que produziu o committed_ts abaixo, e as
        # duas latencias ficam na mesma base.
        #
        # Gravado como TIMESTAMP em ms, nao como inteiro (CONTRATO.md 2.5, v1.5):
        # e o tipo que o Flink consegue produzir por evento sem funcao Python, e
        # a coluna precisa ter o mesmo tipo nos dois motores. No log de progresso
        # (JSON) ele continua inteiro; a analise converte com epoch_ms.
        #
        # SEM marca de UTC (timestamp_ntz), por decisao de 25/09/2026: o Flink
        # 1.20 nao consegue marcar (isAdjustedToUTC=false fixo no escritor dele),
        # e os arquivos dos dois motores ficam identicos. O VALOR continua sendo
        # o instante em UTC: a conversao para ntz usa o fuso da sessao, que e UTC.
        processed_ts = int(time.time() * 1000)
        saida = (
            lote_df.drop(*KAFKA_COLS)
            .withColumn("batch_id", lit(batch_id).cast("long"))
            .withColumn("processed_ts",
                        timestamp_millis(lit(processed_ts)).cast("timestamp_ntz"))
        )
        (
            saida.write.mode("append")
            # Gera <destino>/run_id=.../dt=.../hh=... — o layout do
            # CONTRATO.md secao 4. As tres colunas saem do conteudo dos
            # arquivos e passam a viver no caminho; o DuckDB as le de volta.
            .partitionBy("run_id", "dt", "hh")
            .option("compression", config.PARQUET_CODEC)
            .parquet(config.DIR_EVENTOS)
        )
        # committed_ts: DEPOIS que o write retornou — os arquivos ja estao no
        # lugar final e visiveis para quem le a pasta (CONTRATO.md 2.2).
        committed_ts = int(time.time() * 1000)

        proximos = {int(r["_kafka_partition"]): int(r["ultimo"]) + 1 for r in por_particao}
        commit_ok = commitar_offsets(proximos)

        registrar_progresso({
            "motor": "spark",
            "batch_id": batch_id,
            "inicio_ts": inicio_ts,
            "processed_ts": processed_ts,
            "committed_ts": committed_ts,
            "linhas": n,
            "offsets": {str(p): o for p, o in sorted(proximos.items())},
            "commit_kafka_ok": commit_ok,
        })
        print(f"[batch {batch_id}] {n} eventos gravados  "
              f"processamento={processed_ts - inicio_ts} ms  "
              f"escrita={committed_ts - processed_ts} ms  "
              f"particoes={len(proximos)}")
    finally:
        lote_df.unpersist()


def main() -> int:
    print("\n=== Etapa 6 — Spark gravando Parquet com medicao ===")
    print(config.resumo())
    print()
    inspecionar_checkpoint(config.CHECKPOINT_DIR)
    print()

    spark = (
        SparkSession.builder.appName("nrt-etapa6-medicao")
        # CONTRATO.md secao 4.1: dt e hh derivam de event_ts em UTC. Fixar o
        # fuso da SESSAO e o que garante isso — sem esta linha, a mesma entrada
        # geraria diretorios diferentes na maquina local (America/Recife) e na
        # nuvem (us-east-1).
        .config("spark.sql.session.timeZone", config.TIMEZONE)
        # Tira do legado INT96 qualquer coluna de timestamp COM marca de UTC. Nao
        # afeta o processed_ts: ele e timestamp_ntz, que o Spark grava sempre
        # como INT64 em microssegundos, ignorando esta opcao (verificado em
        # 25/09/2026; CONTRATO.md 2.5). Fica para que uma coluna futura desse
        # tipo nao caia no formato legado sem ninguem perceber.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
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
    #
    # partition e offset do Kafka seguem junto ate o foreachBatch (Etapa 6): e
    # deles que sai o progresso commitado para medir o lag. Saem antes da
    # escrita.
    eventos = (
        bruto.select(
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            from_json(col("value").cast("string"), esquema.EVENTO).alias("e"),
        )
        .select(*KAFKA_COLS, "e.*")
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
