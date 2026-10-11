"""
Injetor de eventos malformados — Etapa 7 (= RT-5).

    ~/.venvs/tcc/bin/python shared/data-generator/inject_malformed.py

O que cada motor faz com um evento que nao consegue ler? Ninguem configurou isso
de proposito, e o gerador nunca emite um evento quebrado — entao a resposta so
aparece provocando. Este arquivo provoca.

Emite 420 eventos VALIDOS (gen_id 0, seq 0..419, pela mesma FabricaDeEventos do
gerador) e, a cada 10 deles, um evento quebrado: 14 tipos, 3 de cada, 42 no
total. Os quebrados que ainda sao JSON levam gen_id 99 e um event_id
"malf-<tipo>-<n>", para serem achados no destino.

--- O que foi medido com ele (11/10/2026, 462 mensagens por motor) -------------

                                        Spark        Flink
    linhas gravadas                       462          447
    INVALIDAS (campo obrigatorio nulo)     21           15
    NULAS (todos os campos nulos)          15            0
    DESCARTADAS (sumiram)                   0           15

    nao e JSON, JSON cortado, lista,    grava linha   descarta,
      mensagem vazia, mensagem nula     toda nula     sem aviso
    numero entre aspas ("17")           campo nulo    converte para 17
    fracao em campo inteiro (1.5)       campo nulo    trunca para 1
    texto em campo numerico             campo nulo    campo nulo
    numero grande demais (int32)        campo nulo    campo nulo
    campo ausente                       campo nulo    campo nulo
    campo a mais                        passa         passa

Nenhum dos dois derruba o job nem emite erro ou aviso. Uma linha sem run_id vai
para a particao nula do destino, cujo nome difere por motor:
run_id=__HIVE_DEFAULT_PARTITION__ (Spark) e run_id=__DEFAULT_PARTITION__ (Flink).

--- Por que os dois NAO foram igualados ----------------------------------------
O pipeline e ELT estrito (decisao de 11/10/2026): carrega, nao limpa. Limpeza e
transformacao, e fica para o destino. A diferenca acima e declarada no
CONTRATO.md e CONTADA pelo reconcile.py em toda execucao — num benchmark, as tres
contagens tem de dar zero.

--- ATENCAO ---------------------------------------------------------------------
As 42 mensagens quebradas FICAM no topico ate a retencao apaga-las ou o topico
ser recriado (docker compose down -v). Um pipeline que leia desde o inicio
(earliest) vai encontra-las. Nunca rodar isto no meio de uma bateria medida.

Grava, em ~/tcc-data/metrics:
    <run_id>.gen0.json           manifesto dos validos, para o reconcile.py
    <run_id>.topico.json         quantas mensagens o topico recebeu (validos +
                                 quebrados) — e o que permite contar as DESCARTADAS
    malformados.<run_id>.json    o que foi injetado, e em que particao/offset caiu
"""

from __future__ import annotations

import json
import os
import pathlib
import random
import sys
import time

import yaml
from confluent_kafka import Producer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schema import FabricaDeEventos  # noqa: E402

AQUI = pathlib.Path(__file__).resolve().parent
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", pathlib.Path.home() / "tcc-data"))

VALIDOS = 420
A_CADA = 10      # um quebrado depois de cada 10 validos
POR_TIPO = 3


def texto(evento: dict) -> bytes:
    return json.dumps(evento, separators=(",", ":")).encode("utf-8")


def com(evento: dict, **troca) -> bytes:
    return texto({**evento, **troca})


def sem(evento: dict, campo: str) -> bytes:
    return texto({k: v for k, v in evento.items() if k != campo})


def numero(marcador: str) -> int:
    return int(marcador.rsplit("-", 1)[1])


# tipo -> funcao(evento base, marcador) -> valor da mensagem (bytes, ou None)
TIPOS = {
    # --- nao e um evento ------------------------------------------------------
    "nao_json":           lambda e, m: f"isto nao e json: {m}".encode(),
    "json_truncado":      lambda e, m: texto(e)[: len(texto(e)) // 2],
    "json_array":         lambda e, m: b"[1, 2, 3]",
    "vazio":              lambda e, m: b"",
    "nulo":               lambda e, m: None,
    # --- tipo errado num campo --------------------------------------------------
    "texto_em_seq":       lambda e, m: com(e, seq="abc"),
    "numero_como_texto":  lambda e, m: com(e, user_id="17"),
    # Valor DIFERENTE em cada um dos tres: o Flink trunca a fracao, e tres
    # eventos com o mesmo seq apareceriam como duplicata no reconcile.py.
    "fracao_em_inteiro":  lambda e, m: com(e, seq=900_000 + numero(m) + 0.5),
    "estouro_int32":      lambda e, m: com(e, user_id=9_999_999_999),
    "texto_em_amount":    lambda e, m: com(e, amount="caro"),
    "texto_em_event_ts":  lambda e, m: com(e, event_ts="ontem"),
    # --- campo ausente ----------------------------------------------------------
    "sem_seq":            lambda e, m: sem(e, "seq"),
    "sem_run_id":         lambda e, m: sem(e, "run_id"),
    # --- controle: este e VALIDO e tem de passar ----------------------------------
    "campo_extra":        lambda e, m: com(e, extra="x"),
}


def main() -> int:
    with open(AQUI / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # UTC, como no gerador. O prefixo separa estas execucoes das medidas.
    run_id = os.environ.get("RUN_ID") or (
        "malformados-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))

    conf = {"bootstrap.servers": cfg["broker"]}
    conf.update({str(k): v for k, v in cfg["producer"].items()})
    produtor = Producer(conf)

    # Duas fabricas, com seeds diferentes: os quebrados nao consomem a
    # sequencia dos validos, que fica identica a do gerador.
    validos = FabricaDeEventos(random.Random(int(cfg["seed"])), cfg, 0, run_id)
    quebrados = FabricaDeEventos(random.Random(int(cfg["seed"]) + 1), cfg, 99, run_id)

    confirmados = {"ok": 0, "erro": 0}
    injetados = []

    def entregue_valido(err, msg):
        confirmados["erro" if err else "ok"] += 1

    def entregue_quebrado(registro):
        def cb(err, msg):
            registro["erro"] = str(err) if err else None
            if not err:
                registro["particao"], registro["offset"] = msg.partition(), msg.offset()
        return cb

    fila = [(tipo, n) for n in range(POR_TIPO) for tipo in TIPOS]
    for i in range(VALIDOS):
        agora = int(time.time() * 1000)
        ev = validos.proximo(agora)
        produtor.produce(cfg["topic"], key=str(ev["user_id"]).encode("utf-8"),
                         value=texto(ev), callback=entregue_valido)
        if (i + 1) % A_CADA == 0 and fila:
            tipo, n = fila.pop(0)
            marcador = f"malf-{tipo}-{n}"
            base = quebrados.proximo(agora)
            base["event_id"] = marcador
            valor = TIPOS[tipo](base, marcador)
            registro = {"tipo": tipo, "n": n, "event_id": marcador,
                        "valor": None if valor is None else valor.decode("utf-8", "replace")}
            injetados.append(registro)
            produtor.produce(cfg["topic"], key=str(base["user_id"]).encode("utf-8"),
                             value=valor, callback=entregue_quebrado(registro))
        produtor.poll(0)
        time.sleep(0.005)
    pendentes = produtor.flush(timeout=30)

    recusados = [r for r in injetados if r.get("erro")]
    no_topico = confirmados["ok"] + len(injetados) - len(recusados)

    destino = DATA_DIR / "metrics"
    destino.mkdir(parents=True, exist_ok=True)
    (destino / f"{run_id}.gen0.json").write_text(json.dumps({
        "run_id": run_id, "gen_id": 0, "topico": cfg["topic"], "seed": cfg["seed"],
        "tentados": VALIDOS, "confirmados": confirmados["ok"],
        "erros_entrega": confirmados["erro"], "pendentes_no_flush": pendentes,
        "ultimo_seq": validos.ultimo_seq, "malformados": True,
    }, indent=2), encoding="utf-8")
    (destino / f"{run_id}.topico.json").write_text(json.dumps({
        "run_id": run_id, "mensagens": no_topico, "origem": "inject_malformed.py",
    }, indent=2), encoding="utf-8")
    (destino / f"malformados.{run_id}.json").write_text(json.dumps({
        "run_id": run_id, "validos": VALIDOS, "tipos": list(TIPOS), "por_tipo": POR_TIPO,
        "injetados": injetados,
    }, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"run_id ............ {run_id}")
    print(f"topico ............ {cfg['topic']} @ {cfg['broker']}")
    print(f"validos ........... {confirmados['ok']} confirmados de {VALIDOS}"
          f" (erros {confirmados['erro']})")
    print(f"quebrados ......... {len(injetados) - len(recusados)} entregues de {len(injetados)}"
          f" ({len(TIPOS)} tipos x {POR_TIPO})")
    for r in recusados:
        print(f"   !! o broker recusou {r['event_id']}: {r['erro']}")
    print(f"no topico ......... {no_topico} mensagens")
    print(f"pendentes ......... {pendentes}")
    print("\nATENCAO: as mensagens quebradas ficam no topico ate a retencao apaga-las.")

    ok = (confirmados["ok"] == VALIDOS and not recusados and pendentes == 0)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
