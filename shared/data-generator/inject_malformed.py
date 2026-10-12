"""
Injetor de eventos malformados — Etapa 7 (= RT-5), revisto no contrato 2.0.

    ~/.venvs/tcc/bin/python shared/data-generator/inject_malformed.py

O que cada motor faz com um evento com defeito? Este arquivo provoca, para a
resposta ser vista, e nao suposta.

Emite 420 eventos VALIDOS (gen_id 0, seq 0..419, pela mesma FabricaDeEventos do
gerador) e, a cada 10 deles, um evento com defeito: 9 tipos, 3 de cada, 27 no
total — os 8 tipos de malformado do malformed.py e o controle, um evento valido
com um campo a mais. Todos sao JSON valido. Os 27 levam gen_id 99 e um event_id
"malf-<tipo>-<n>", para serem achados no destino.

--- O que foi medido com ele (12/10/2026, 447 mensagens por motor) -------------
Os pipelines carregam os dez campos como TEXTO (CONTRATO.md 2.0): gravam o que
veio, e quem converte e conta e o reconcile.py, depois do Load. Os dois motores
dao o MESMO resultado:

                                                    Spark        Flink
    linhas gravadas                                   447          447
    INVALIDAS (obrigatorio nulo ou que nao converte)   18           18
    NULAS (todos os campos nulos)                       0            0
    DESCARTADAS (sumiram)                               0            0

    "seq": "abc"                 chega como  abc           nao converte: invalida
    "user_id": "17"              chega como  17            converte: valida
    "seq": 900000.5              chega como  900000.5      nao converte: invalida
    "user_id": 9999999999        chega como  9999999999    nao cabe: invalida
    "amount": "caro"             chega como  caro          amount nao e obrigatorio
    "event_ts": "ontem"          chega como  ontem         invalida; pasta sem data
    sem seq, sem run_id          chega nulo                invalida
    campo a mais                 passa

Uma linha sem run_id vai para a particao nula do destino, cujo nome difere por
motor: run_id=__HIVE_DEFAULT_PARTITION__ (Spark) e run_id=__DEFAULT_PARTITION__
(Flink).

--- O que este arquivo fazia antes, e por que mudou ------------------------------
Ate 11/10/2026 os pipelines carregavam os campos ja tipados, e este injetor
mostrava os dois motores divergindo: 462 mensagens, com 21 invalidas e 15 nulas
no Spark contra 15 invalidas e 15 DESCARTADAS no Flink. O Flink convertia "17" e
truncava 1.5, que o Spark anulava. Havia tambem cinco tipos que nao eram JSON
(texto solto, JSON cortado, lista, mensagem vazia e nula), que o Spark gravava
como linha nula e o Flink descartava. Com a carga em texto os campos de tipo
errado sairam iguais; e os cinco tipos que nao sao JSON deixaram de ser enviados
(decisao de 12/10/2026) — neles a diferenca entre os motores continua existindo,
declarada no CONTRATO.md 2.6.

As tres contagens sao informacao e nunca reprovam: no benchmark, 4 das 10 rodadas
de cada combinacao levam 1% de malformados de proposito, emitidos pelo proprio
gerador (MALFORMED_RATE).

--- ATENCAO ---------------------------------------------------------------------
As 27 mensagens com defeito FICAM no topico ate a retencao apaga-las ou o topico
ser recriado. Um pipeline que leia desde o inicio (earliest) vai encontra-las. O
script de rodada recria o topico, entao uma rodada feita depois nao e afetada.

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
from malformed import (  # noqa: E402
    CONTROLE, GEN_ID_MALFORMADOS, MALFORMADOS, marcador as marcador_de, texto,
)
from schema import FabricaDeEventos  # noqa: E402

AQUI = pathlib.Path(__file__).resolve().parent
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", pathlib.Path.home() / "tcc-data"))

VALIDOS = 420
A_CADA = 10      # um quebrado depois de cada 10 validos
POR_TIPO = 3

# Os 8 tipos malformados (os mesmos que o gerador usa nas rodadas com
# malformados) mais o controle, que e valido e tem de passar. A lista vive no
# malformed.py: um lugar so, para as duas nao divergirem.
TIPOS = {**MALFORMADOS, **CONTROLE}


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
    quebrados = FabricaDeEventos(random.Random(int(cfg["seed"]) + 1), cfg,
                                 GEN_ID_MALFORMADOS, run_id)

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
            marcador = marcador_de(tipo, n)
            base = quebrados.proximo(agora)
            base["event_id"] = marcador
            valor = TIPOS[tipo](base, n)
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
