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
Vales subindo de um ciclo para o outro: nao aguentou.

--- CPU e memoria (Etapa 8) -------------------------------------------------
    CONTAINERS=spark-run,kafka GROUP_ID=tcc-nrt-spark ... collector.py

Com CONTAINERS, a cada amostra o coletor le tambem o consumo de cada conteiner
cujo nome contem um dos trechos pedidos, e grava num segundo CSV. A decisao de
24/09/2026 era NAO medir CPU nem memoria, porque cada motor expunha um numero
diferente pela propria API. Ela foi reaberta em 11/10/2026: com os dois motores
em conteiner, quem mede e o Docker — o mesmo instrumento para os dois, sem
depender de nenhum deles.

Le os CONTADORES BRUTOS da API do Docker (tempo de CPU acumulado, em ns; memoria,
em bytes), e nao a saida do `docker stats`. Verificado em 11/10/2026 com um
conteiner de consumo conhecido: 100,0% e 200,0% para um e dois lacos (o
`docker stats` deu 98,8% e 195,9%), 301 MiB para 300 MiB reservados, e o total de
CPU a 0,03 s do cpu.stat do kernel. Cada leitura custa ~12 ms; o `docker stats`
leva ~2 s e arredonda a memoria acima de 1 GiB.

  CPU      em % de UM nucleo: 100 = um nucleo inteiro; com 8 nucleos, ate 800.
  memoria  SEM o cache de arquivos (inactive_file), como o `docker stats` mostra:
           o kernel usa memoria livre para acelerar a escrita e a devolve quando
           alguem precisa. O total bruto fica na coluna ao lado.
"""

import csv
import json
import os
import signal
import socket
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

# Trechos de nome dos conteineres a medir, separados por virgula. Trecho, e nao
# nome inteiro: o conteiner do motor ganha um nome novo a cada execucao
# (pipelines-tcc-spark-run-<hash>). Vazio = so o lag, como ate a Etapa 7.
CONTAINERS = [c.strip() for c in os.getenv("CONTAINERS", "").split(",") if c.strip()]
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
MIB = 1024 * 1024


def docker_api(caminho: str):
    """Uma pergunta a API do Docker, pelo socket local. So biblioteca padrao."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT_S)
    try:
        s.connect(DOCKER_SOCK)
        # HTTP/1.0: a resposta vem inteira e a conexao fecha, sem "chunked".
        s.sendall(f"GET {caminho} HTTP/1.0\r\nHost: docker\r\n\r\n".encode())
        dados = b""
        while True:
            pedaco = s.recv(65536)
            if not pedaco:
                break
            dados += pedaco
    finally:
        s.close()
    cabecalho, _, corpo = dados.partition(b"\r\n\r\n")
    status = cabecalho.split(b"\r\n", 1)[0].decode(errors="replace")
    if " 200 " not in status:
        raise OSError(f"API do Docker respondeu: {status}")
    return json.loads(corpo)


def ler_recursos() -> dict[str, tuple[int, int, int, int]]:
    """{nome: (instante_ns, cpu_ns, mem_bytes, cache_bytes)} dos conteineres pedidos.

    cpu_ns e ACUMULADO desde que o conteiner subiu: a porcentagem sai da
    diferenca entre duas leituras, dividida pelo tempo entre elas — por isso o
    instante (relogio monotonico) e tomado junto de cada leitura.
    """
    saida = {}
    for c in docker_api("/containers/json"):
        nome = c["Names"][0].lstrip("/")
        if not any(trecho in nome for trecho in CONTAINERS):
            continue
        antes = time.monotonic_ns()
        d = docker_api(f"/containers/{c['Id']}/stats?stream=false&one-shot=true")
        instante = (antes + time.monotonic_ns()) // 2
        mem = d.get("memory_stats") or {}
        est = mem.get("stats") or {}
        saida[nome] = (
            instante,
            d["cpu_stats"]["cpu_usage"]["total_usage"],
            mem.get("usage", 0),
            est.get("inactive_file", est.get("total_inactive_file", 0)),
        )
    return saida


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
    arquivo_rec = destino / f"recursos.{GROUP_ID}.{inicio_ms}.csv"
    if CONTAINERS:
        print(f"conteineres ..... {', '.join(CONTAINERS)}  (CPU e memoria)")
        print(f"csv recursos .... {arquivo_rec}")
    if len(parts) != 24:
        print(f"!! ATENCAO: o CONTRATO.md fixa 24 particoes; o topico tem {len(parts)}")
    print("-" * 68)

    # Mesmo desenho de encerramento dos pipelines: o sinal so marca, o laco para
    # em ponto seguro. Aqui nao ha JVM no caminho, entao o encerramento e limpo.
    parar = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: parar.set())

    amostras = falhas = 0
    maior_lag = 0

    # Recursos: a ultima leitura de cada conteiner (para a diferenca) e o que o
    # resumo final precisa. Falha da API do Docker NAO para o coletor: o lag
    # continua sendo medido, e a falha e contada.
    f_rec = w_rec = None
    if CONTAINERS:
        f_rec = open(arquivo_rec, "w", newline="", encoding="utf-8")
        w_rec = csv.writer(f_rec)
        w_rec.writerow(["amostra_ts", "conteiner", "cpu_ns", "cpu_pct",
                        "mem_bytes", "cache_bytes", "mem_sem_cache_bytes"])
    anterior: dict[str, tuple[int, int]] = {}
    resumo: dict[str, dict] = {}
    falhas_rec = 0

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

            if w_rec is not None:
                try:
                    lidos = ler_recursos()
                except (OSError, ValueError, KeyError) as e:
                    lidos = {}
                    falhas_rec += 1
                    if falhas_rec == 1:
                        print(f"  !! leitura de CPU/memoria falhou: {e} — o lag continua "
                              "sendo medido", file=sys.stderr)
                for nome, (instante, cpu_ns, mem, cache) in sorted(lidos.items()):
                    pct = ""
                    if nome in anterior and instante > anterior[nome][0]:
                        pct = round((cpu_ns - anterior[nome][1])
                                    / (instante - anterior[nome][0]) * 100, 2)
                    anterior[nome] = (instante, cpu_ns)
                    util = mem - cache
                    w_rec.writerow([ts, nome, cpu_ns, pct, mem, cache, util])
                    r = resumo.setdefault(nome, {"t0": instante, "cpu0": cpu_ns, "n": 0,
                                                 "mem_soma": 0, "mem_pico": 0, "cpu_pico": 0.0})
                    r.update(t1=instante, cpu1=cpu_ns, n=r["n"] + 1,
                             mem_soma=r["mem_soma"] + util, mem_pico=max(r["mem_pico"], util))
                    if pct != "":
                        r["cpu_pico"] = max(r["cpu_pico"], pct)
                f_rec.flush()

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

    if f_rec is not None:
        f_rec.close()
        print(f"csv recursos .... {arquivo_rec}  (falhas de leitura: {falhas_rec})")
        for nome, r in sorted(resumo.items()):
            # CPU media = tempo de CPU consumido / tempo decorrido, entre a
            # primeira e a ultima leitura — exata, e nao a media das porcentagens.
            decorrido = r["t1"] - r["t0"]
            media = (r["cpu1"] - r["cpu0"]) / decorrido * 100 if decorrido > 0 else 0.0
            print(f"  {nome}")
            print(f"    CPU      media {media:7.1f} %   pico {r['cpu_pico']:7.1f} %")
            print(f"    memoria  media {r['mem_soma'] / r['n'] / MIB:7.1f} MiB  "
                  f"pico {r['mem_pico'] / MIB:7.1f} MiB   ({r['n']} leituras)")
    return 0 if falhas == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
