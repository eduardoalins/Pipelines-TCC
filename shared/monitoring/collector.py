"""
Coletor de consumer lag — Etapa 6 (= RT-4). O MESMO codigo para os dois motores.

    GROUP_ID=tcc-nrt-spark DURATION=90 ~/.venvs/tcc/bin/python shared/monitoring/collector.py

A cada INTERVAL segundos, para cada uma das 24 particoes, pergunta ao broker:

    ultimo offset da particao   (a ultima "senha entregue" pelo gerador)
    offset commitado pelo grupo (a ultima "senha atendida" pelo motor)

e grava a diferenca — o lag — num CSV em ~/tcc-data/metrics/.

--- Por que fora dos motores (decisao F7, planoRT.md) ----------------------
O coletor nao conversa com o Spark nem com o Flink: so com o Kafka. Os dois
motores commitam o progresso no broker, cada um sob o proprio group.id (o Flink
nativamente, no checkpoint; o Spark por codigo nosso, apos cada micro-batch), e
este arquivo mede os dois com a mesma regua. Muda so o GROUP_ID.

--- O que ele NAO mede ------------------------------------------------------
Latencia. Lag e QUANTOS eventos esperam; latencia e QUANTO TEMPO cada um esperou,
e sai dos carimbos de tempo, no reconcile.py. Um pipeline pode ter latencia alta
e lag saudavel (trigger longo, vales em zero) — por isso as duas existem.

--- Para que o CSV serve ---------------------------------------------------
O SLO de escalabilidade (planoNRT3 §4) observa os VALES do lag: o valor logo
apos cada micro-batch. Vales estaveis perto de zero: o pipeline aguentou a carga.
Vales subindo de um ciclo para o outro: nao aguentou. Na fase local e so isso que
se mede — CPU e memoria nao (decisao B4.1, 24/09/2026).
"""

import csv
import os
import signal
import sys
import threading
import time
from pathlib import Path

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "ecommerce.events.v1")
GROUP_ID = os.getenv("GROUP_ID", "")

# 1 s, e NAO os 5 s do planoNRT3 (Etapa 6) — correcao de 24/09/2026.
#
# Amostrar no MESMO periodo do trigger (5 s) fotografa a fila sempre no mesmo
# ponto do ciclo: o serrilhado some e o lag parece constante, sem vales. E os
# vales sao exatamente o que o SLO observa. E o efeito da roda que parece parada
# quando filmada na propria frequencia (aliasing).
#
# A regra: o intervalo precisa ser bem menor que o menor ciclo medido. Com 1 s
# ha 5 amostras por ciclo no trigger de 5 s, e 30 no de 30 s. No Flink o ciclo e
# o intervalo de checkpoint, e vale o mesmo raciocinio. Custo: duas perguntas por
# segundo ao broker.
INTERVAL = float(os.getenv("INTERVAL", "1"))

# Condicao de parada propria, no mesmo idioma do gerador. 0 = ate o Ctrl+C.
DURATION = float(os.getenv("DURATION", "0"))

DATA_DIR = Path(os.getenv("DATA_DIR", Path.home() / "tcc-data"))

# Limite de espera de cada pergunta ao broker. Uma amostra que nao responde neste
# tempo e registrada como falha, nunca como lag zero.
TIMEOUT_S = 5.0


def particoes(admin: AdminClient) -> list[int]:
    """As particoes do topico, perguntadas ao broker e nao supostas.

    Se o topico nao tiver 24, o coletor avisa em vez de medir a coisa errada — o
    mesmo principio do auto.create.topics.enable=false.
    """
    meta = admin.list_topics(topic=TOPIC, timeout=TIMEOUT_S)
    t = meta.topics.get(TOPIC)
    if t is None or t.error is not None:
        raise SystemExit(f"ERRO: topico {TOPIC} nao encontrado em {BOOTSTRAP}")
    return sorted(t.partitions)


def amostrar(admin: AdminClient, parts: list[int]) -> dict[int, tuple[int, int | None]]:
    """Uma amostra: {particao: (ultimo_offset, commitado ou None)}.

    ORDEM IMPORTA: primeiro o commitado, depois o ultimo. Entre as duas perguntas
    o motor pode commitar mais; perguntando nessa ordem, o commitado lido nunca
    passa do ultimo lido, e o lag nunca sai negativo. Na ordem inversa sairia —
    um lag negativo e um defeito do instrumento, nao um fenomeno.

    `None` = o grupo ainda nao commitou nada nessa particao. Nao vira lag = ultimo
    offset: o motor pode ter comecado do fim do topico (startingOffsets=latest),
    e o historico anterior nao e fila dele.
    """
    pedido = ConsumerGroupTopicPartitions(
        GROUP_ID, [TopicPartition(TOPIC, p) for p in parts]
    )
    fut = admin.list_consumer_group_offsets([pedido], request_timeout=TIMEOUT_S)
    commitados = {}
    for tp in fut[GROUP_ID].result().topic_partitions:
        commitados[tp.partition] = tp.offset if tp.offset >= 0 else None

    futs = admin.list_offsets(
        {TopicPartition(TOPIC, p): OffsetSpec.latest() for p in parts},
        request_timeout=TIMEOUT_S,
    )
    ultimos = {tp.partition: f.result().offset for tp, f in futs.items()}

    return {p: (ultimos[p], commitados.get(p)) for p in parts}


def main() -> int:
    if not GROUP_ID:
        print("ERRO: defina GROUP_ID (tcc-nrt-spark para o Spark).", file=sys.stderr)
        return 2

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    parts = particoes(admin)

    inicio_ms = int(time.time() * 1000)
    destino = DATA_DIR / "metrics"
    destino.mkdir(parents=True, exist_ok=True)
    arquivo = destino / f"lag.{GROUP_ID}.{inicio_ms}.csv"

    print(f"grupo ........... {GROUP_ID}")
    print(f"topico .......... {TOPIC} @ {BOOTSTRAP} ({len(parts)} particoes)")
    print(f"intervalo ....... {INTERVAL:g} s")
    print(f"duracao ......... {f'{DURATION:g} s' if DURATION else 'ate Ctrl+C'}")
    print(f"csv ............. {arquivo}")
    if len(parts) != 24:
        print(f"!! ATENCAO: o CONTRATO.md fixa 24 particoes; o topico tem {len(parts)}")
    print("-" * 68)

    # Mesmo desenho de encerramento dos pipelines: o sinal so marca, o laco para
    # em ponto seguro. Aqui nao ha JVM no caminho, entao o encerramento e limpo.
    parar = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: parar.set())

    amostras = falhas = 0
    maior_lag = 0

    with open(arquivo, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["amostra_ts", "particao", "ultimo_offset", "commitado", "lag"])

        t0 = time.monotonic()
        i = 0
        while not parar.is_set():
            # Cronograma ABSOLUTO (t0 + i * INTERVAL), como no gerador: dormir o
            # intervalo a cada volta acumularia o tempo das proprias perguntas, e
            # as amostras iriam escorregando.
            alvo = t0 + i * INTERVAL
            if DURATION and alvo - t0 > DURATION:
                break
            espera = alvo - time.monotonic()
            if espera > 0 and parar.wait(espera):
                break
            i += 1

            ts = int(time.time() * 1000)
            try:
                dados = amostrar(admin, parts)
            except KafkaException as e:
                falhas += 1
                print(f"  [{ts}] amostra FALHOU: {e.args[0]}", file=sys.stderr)
                continue

            total = 0
            sem_commit = 0
            pior = (None, 0)
            for p, (ultimo, commitado) in sorted(dados.items()):
                if commitado is None:
                    sem_commit += 1
                    w.writerow([ts, p, ultimo, "", ""])
                    continue
                lag = ultimo - commitado
                total += lag
                if lag > pior[1]:
                    pior = (p, lag)
                w.writerow([ts, p, ultimo, commitado, lag])
            f.flush()

            amostras += 1
            if sem_commit == len(parts):
                # Grupo sem nenhum commit ainda: nao ha lag a medir, e "0" seria
                # lido como fila vazia.
                print(f"  {time.monotonic() - t0:6.1f}s  lag total=      -  "
                      f"(o grupo ainda nao commitou nada)")
                continue
            maior_lag = max(maior_lag, total)
            extra = f"  sem commit={sem_commit}" if sem_commit else ""
            maior = f"  maior=p{pior[0]}:{pior[1]}" if pior[0] is not None else ""
            print(f"  {time.monotonic() - t0:6.1f}s  lag total={total:>7}{maior}{extra}")

    print("-" * 68)
    print(f"amostras ........ {amostras}  (falhas: {falhas})")
    print(f"maior lag total . {maior_lag}")
    print(f"csv ............. {arquivo}")
    return 0 if falhas == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
