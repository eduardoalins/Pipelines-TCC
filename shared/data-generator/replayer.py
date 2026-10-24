"""
Replayer de eventos REAIS para o Kafka — CONTRATO.md 3.0 (23/10/2026).

Desde a versao 3.0 do contrato os eventos do benchmark sao reais: vem da base
da REES46 (outubro de 2019), preparada por scripts/preparar_base.sh. Este
programa substitui o generator.py NAS RODADAS. O generator.py continua no
repositorio, sem uso nas rodadas, para a bateria feita com ele seguir
reproduzivel (decisao de 23/10/2026). O que os dois compartilham — a
configuracao (config.yaml, lida pela mesma funcao), os contadores e a distincao
entre carga oferecida e atingida — vem do proprio generator.py, para nao
divergir.

    ~/.venvs/tcc/bin/python shared/data-generator/replayer.py
    RATE=1000 DURATION=60 SEED=42 ~/.venvs/tcc/bin/python shared/data-generator/replayer.py

O que faz (CONTRATO.md 1.5 e 5.1):

    JANELA   a rodada recebe uma faixa CONTINUA de linhas da base, na ordem
             original, com RATE x DURATION eventos. Onde ela comeca e sorteado
             pela SEED da rodada: a rodada N do Spark e a do Flink recebem a
             mesma janela. Janelas de rodadas diferentes podem se sobrepor.

    ANTES DO RELOGIO   so avanca no arquivo ate a primeira linha da janela.

    DURANTE A MEDICAO  le uma linha, monta a mensagem e envia — uma de cada
             vez, na taxa pedida, como um produtor real (decisao de 23/10/2026).
             Essa leitura disputa a CPU com o motor, e isso faz parte do que se
             mede.

Da linha a mensagem: event_type, user_id, product_id, event_time e
user_session (como session_id) vao como vieram; price vai como amount, em
TEXTO, exatamente como na base ("543.10"), para nenhum motor reescreve-lo;
category_id, category_code e brand nao entram. Carimbados aqui, para a medicao:
event_id (o endereco da linha, ex. 2019-10:00000005), seq, gen_id, run_id e
event_ts (o instante do envio).

A base NUNCA e alterada: este programa so le o .csv.
"""

from __future__ import annotations

import collections
import itertools
import json
import os
import pathlib
import random
import sys
import time

from confluent_kafka import KafkaException, Producer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# A mesma configuracao e os mesmos contadores do gerador sintetico.
from generator import DATA_DIR, Contadores, carregar_config, montar_run_id  # noqa: E402

# --- A base (CONTRATO.md 1.5) --------------------------------------------------
BASE = pathlib.Path(os.environ.get("BASE", pathlib.Path.home() / "tcc-data" / "base" / "2019-Oct.csv"))
BASE_BYTES = 5_668_612_855          # o .csv descompactado; o mesmo tamanho que o Kaggle informa
BASE_EVENTOS = 42_448_764           # linhas de dados, sem o cabecalho
BASE_ORIGEM = {
    "nome": "eCommerce behavior data from multi category store (REES46), 2019-Oct",
    "kaggle": "https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store",
    "arquivo": "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz",
    "sha256_gz": "8ebca1ad741295297368f2cf0315e3f36853a1a11768fb16babf8c9b83838147",
}
MES = "2019-10"                     # prefixo do event_id
CABECALHO = "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session"


def janela(seed: int, tamanho: int) -> tuple[int, int]:
    """(primeira, ultima) linha de dados da janela, contadas a partir de 1.

    Usa SO random(): a documentacao do Python garante a mesma sequencia de
    random() para a mesma seed entre versoes — garantia que randint() nao tem.
    Nenhum relogio, nenhuma entropia do sistema (CONTRATO.md 5.1).
    """
    if not 0 < tamanho <= BASE_EVENTOS:
        raise SystemExit(f"janela de {tamanho} eventos nao cabe na base ({BASE_EVENTOS}).")
    ultimo_inicio = BASE_EVENTOS - tamanho            # contando de 0
    inicio = int(random.Random(seed).random() * (ultimo_inicio + 1))
    return inicio + 1, inicio + tamanho


def _texto(valor: str) -> str:
    """Um campo de texto da base em JSON; vazio vira null."""
    return json.dumps(valor) if valor else "null"


def _inteiro(valor: str, nome: str, linha: int) -> str:
    """Um campo inteiro da base, com o MESMO texto que veio; vazio vira null."""
    if not valor:
        return "null"
    if not valor.isdigit():
        raise SystemExit(f"linha {linha}: {nome}={valor!r} nao e inteiro. A base mudou?")
    return valor


def mensagem(campos: list[str], linha: int, seq: int, gen_id: int, run_id_json: str,
             agora_ms: int) -> bytes:
    """A mensagem de uma linha da base, na ordem dos campos do evento.json."""
    event_time, event_type, product_id, _cat_id, _cat_code, _marca, price, user_id, sessao = campos
    return (
        f'{{"event_id":"{MES}:{linha:08d}","seq":{seq},"gen_id":{gen_id},'
        f'"run_id":{run_id_json},"event_type":{_texto(event_type)},'
        f'"user_id":{_inteiro(user_id, "user_id", linha)},'
        f'"product_id":{_inteiro(product_id, "product_id", linha)},'
        f'"amount":{_texto(price)},"session_id":{_texto(sessao)},'
        f'"event_ts":{agora_ms},"event_time":{_texto(event_time)}}}'
    ).encode("utf-8")


def abrir_base():
    """A base, conferida pelo tamanho e pelo cabecalho. O sha256 e conferido pelo
    preparar_base.sh; aqui o tamanho basta para pegar um arquivo errado ou cortado."""
    if not BASE.is_file():
        raise SystemExit(f"base nao encontrada: {BASE}\n   Rode: bash scripts/preparar_base.sh")
    if BASE.stat().st_size != BASE_BYTES:
        raise SystemExit(f"a base tem {BASE.stat().st_size} bytes, esperado {BASE_BYTES}.\n"
                         "   Apague o .csv e rode: bash scripts/preparar_base.sh")
    f = open(BASE, "r", encoding="utf-8", newline="", buffering=1 << 20)
    if f.readline().rstrip("\n") != CABECALHO:
        raise SystemExit(f"cabecalho inesperado em {BASE}")
    return f


def main() -> int:
    cfg = carregar_config()
    run_id = montar_run_id(cfg)
    gen_id = int(cfg["gen_id"])
    taxa = int(cfg["rate_per_sec"])
    duracao = int(cfg["duration_sec"])
    seed = int(cfg["seed"])
    topico = cfg["topic"]

    total_previsto = taxa * duracao
    primeira, ultima = janela(seed, total_previsto)
    cont = Contadores()

    def entregue(err, msg):
        """Callback de entrega. E AQUI que um evento vira 'confirmado'."""
        if err is not None:
            cont.erros_entrega += 1
        else:
            cont.confirmados += 1

    conf = {"bootstrap.servers": cfg["broker"]}
    conf.update({str(k): v for k, v in cfg["producer"].items()})
    produtor = Producer(conf)

    print(f"run_id .......... {run_id}")
    print(f"gen_id .......... {gen_id}")
    print(f"topico .......... {topico} @ {cfg['broker']}")
    print(f"carga ........... {taxa} evt/s por {duracao}s = {total_previsto} eventos")
    print(f"base ............ {BASE}")
    print(f"janela .......... linhas {primeira} a {ultima} (seed {seed})")
    print(f"produtor ........ {conf.get('acks')=} {conf.get('compression.type')=} "
          f"{conf.get('partitioner')=} {conf.get('linger.ms')=}")

    # --- Antes do relogio: so avancar ate a primeira linha da janela ---------------
    t_pos = time.perf_counter()
    base = abrir_base()
    collections.deque(itertools.islice(base, primeira - 1), maxlen=0)
    posicionamento = time.perf_counter() - t_pos
    print(f"posicionamento .. {posicionamento:.1f}s ate a linha {primeira}")
    print("-" * 68)

    run_id_json = json.dumps(run_id)
    # O instante em que o envio comeca, depois do posicionamento: e dele que o
    # run_round.py conta a CPU do motor, para nao misturar segundos ociosos.
    envio_inicio_ms = int(time.time() * 1000)
    t0 = time.perf_counter()
    proximo_aviso = 10.0

    for i in range(total_previsto):
        # Cronograma ABSOLUTO, como no generator.py: o alvo do evento i e
        # t0 + i/taxa. Dormir 1/taxa a cada volta acumularia o erro do sleep.
        alvo = t0 + i / taxa
        espera = alvo - time.perf_counter()
        if espera > 0.0005:
            time.sleep(espera)

        # Ler e montar AGORA, durante a medicao (CONTRATO.md 5.1).
        linha = primeira + i
        campos = base.readline().rstrip("\n").split(",")
        if len(campos) != 9:
            raise SystemExit(f"linha {linha} da base com {len(campos)} campos, esperado 9.")
        payload = mensagem(campos, linha, i, gen_id, run_id_json, int(time.time() * 1000))
        chave = campos[7].encode("utf-8")       # user_id: a chave da mensagem (CONTRATO 1.3)

        cont.tentados += 1
        try:
            produtor.produce(topico, key=chave, value=payload, callback=entregue)
            cont.enfileirados += 1
            cont.bytes_payload += len(payload)
        except BufferError:
            # Fila local do produtor cheia: a carga oferecida deixa de virar
            # carga atingida. Contado e reportado, como no generator.py.
            cont.nao_enfileirados += 1
        except KafkaException as e:
            cont.nao_enfileirados += 1
            print(f"[erro] produce falhou: {e}", file=sys.stderr)

        produtor.poll(0)

        decorrido = time.perf_counter() - t0
        if decorrido >= proximo_aviso:
            print(f"  {decorrido:5.1f}s  tentados={cont.tentados}  "
                  f"confirmados={cont.confirmados}")
            proximo_aviso += 10.0

    base.close()
    cont.ultimo_seq = cont.tentados - 1

    pendentes = produtor.flush(timeout=30)
    duracao_real = time.perf_counter() - t0

    oferecida = cont.tentados / duracao_real
    atingida = cont.confirmados / duracao_real
    payload_medio = (cont.bytes_payload / cont.enfileirados) if cont.enfileirados else 0

    print("-" * 68)
    print(f"duracao real ............ {duracao_real:.2f}s")
    print(f"tentados ................ {cont.tentados}")
    print(f"enfileirados ............ {cont.enfileirados}")
    print(f"nao enfileirados ........ {cont.nao_enfileirados}")
    print(f"confirmados pelo broker . {cont.confirmados}")
    print(f"erros de entrega ........ {cont.erros_entrega}")
    print(f"pendentes no flush ...... {pendentes}")
    print(f"ultimo seq (gen {gen_id}) ..... {cont.ultimo_seq}")
    print(f"carga oferecida ......... {oferecida:.1f} evt/s")
    print(f"carga atingida .......... {atingida:.1f} evt/s")
    print(f"payload medio ........... {payload_medio:.1f} bytes")

    # O mesmo manifesto do generator.py, que o reconcile.py le, mais a origem
    # dos dados: a base e a janela da rodada.
    destino = DATA_DIR / "metrics"
    destino.mkdir(parents=True, exist_ok=True)
    manifesto = destino / f"{run_id}.gen{gen_id}.json"
    with open(manifesto, "w", encoding="utf-8") as f:
        json.dump(
            {
                "run_id": run_id,
                "gen_id": gen_id,
                "topico": topico,
                "fonte": "replayer",
                "seed": seed,
                "base": {**BASE_ORIGEM, "csv": str(BASE), "eventos": BASE_EVENTOS},
                "janela": {"primeira_linha": primeira, "ultima_linha": ultima,
                           "eventos": total_previsto},
                "posicionamento_s": round(posicionamento, 3),
                "envio_inicio_ms": envio_inicio_ms,
                "taxa_pedida": taxa,
                "duracao_pedida_s": duracao,
                "duracao_real_s": round(duracao_real, 3),
                "tentados": cont.tentados,
                "enfileirados": cont.enfileirados,
                "nao_enfileirados": cont.nao_enfileirados,
                "confirmados": cont.confirmados,
                "erros_entrega": cont.erros_entrega,
                "pendentes_no_flush": pendentes,
                "ultimo_seq": cont.ultimo_seq,
                "carga_oferecida_evt_s": round(oferecida, 2),
                "carga_atingida_evt_s": round(atingida, 2),
                "payload_medio_bytes": round(payload_medio, 1),
                "producer": {str(k): v for k, v in cfg["producer"].items()},
                # Sem malformados desde o contrato 3.0 (secao 2.6).
                "malformed_rate": 0.0,
                "mensagens_confirmadas": cont.confirmados,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
    print(f"manifesto ............... {manifesto}")

    ok = (
        cont.confirmados == cont.tentados
        and cont.erros_entrega == 0
        and cont.nao_enfileirados == 0
        and pendentes == 0
    )
    if not ok:
        print("\nATENCAO: confirmados != tentados. Ver os contadores acima.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
