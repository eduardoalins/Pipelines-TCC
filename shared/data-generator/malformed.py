"""
Os tipos de evento malformado — a lista unica, usada pelo gerador e pelo injetor.

Sao 8 formas de um evento chegar com defeito ao Kafka, em dois grupos. Todas sao
JSON VALIDO: um objeto com um campo de tipo errado, ou sem um campo.

    tipo errado num campo    seq, user_id, amount ou event_ts com texto, fracao,
                             numero entre aspas ou numero grande demais
    campo ausente            sem seq, ou sem run_id

O que os pipelines fazem com elas (CONTRATO.md 2.0, secao 2.6): CARREGAM. Os dez
campos vao para o destino como texto, do jeito que vieram — `"seq": "abc"` chega
como abc, `"user_id": "17"` como 17 —, e o campo ausente chega nulo. Converter
para o tipo do contrato, e decidir o que fazer com o que nao converte, e trabalho
do Transform, depois do Load. Os dois motores gravam o MESMO para os oito tipos
(verificado por sonda).

O QUE SAIU DAQUI (decisao de 12/10/2026): as cinco formas que NAO sao JSON —
texto solto, JSON cortado, lista, mensagem vazia e mensagem nula. O gerador nao
as envia mais. Nelas os motores continuam diferentes: o Spark grava uma linha
toda nula e o Flink descarta a mensagem (medido em 11/10/2026).

Quem usa:
    generator.py          1% da carga nas rodadas com malformados, em rodizio
    inject_malformed.py   3 de cada tipo, para o passo 21 do README

A ORDEM do dicionario e a ordem do rodizio. Mudar a ordem, acrescentar ou
retirar um tipo muda a carga das rodadas com malformados — e invalida a
comparacao com as rodadas ja feitas.
"""

from __future__ import annotations

import json


def texto(evento: dict) -> bytes:
    """O evento serializado como o gerador serializa: JSON compacto, UTF-8."""
    return json.dumps(evento, separators=(",", ":")).encode("utf-8")


def _com(evento: dict, **troca) -> bytes:
    return texto({**evento, **troca})


def _sem(evento: dict, campo: str) -> bytes:
    return texto({k: v for k, v in evento.items() if k != campo})


# tipo -> funcao(evento base, n) -> valor da mensagem (bytes).
# `n` e o numero de ordem do malformado DAQUELE tipo (0, 1, 2...).
MALFORMADOS = {
    # --- tipo errado num campo --------------------------------------------------
    "texto_em_seq":       lambda e, n: _com(e, seq="abc"),
    "numero_como_texto":  lambda e, n: _com(e, user_id="17"),
    # Valor DIFERENTE a cada vez: carregando tipado, o Flink truncava a fracao, e
    # eventos com o mesmo seq apareciam como duplicata (visto em 11/10/2026).
    "fracao_em_inteiro":  lambda e, n: _com(e, seq=900_000 + n + 0.5),
    "estouro_int32":      lambda e, n: _com(e, user_id=9_999_999_999),
    "texto_em_amount":    lambda e, n: _com(e, amount="caro"),
    "texto_em_event_ts":  lambda e, n: _com(e, event_ts="ontem"),
    # --- campo ausente ----------------------------------------------------------
    "sem_seq":            lambda e, n: _sem(e, "seq"),
    "sem_run_id":         lambda e, n: _sem(e, "run_id"),
}

# Um evento VALIDO com um campo a mais. Nao e malformado: e o controle do
# injetor, que tem de passar nos dois motores. Fica fora do rodizio do gerador.
CONTROLE = {
    "campo_extra":        lambda e, n: _com(e, extra="x"),
}

# gen_id dos eventos-base dos malformados. Separado do gen_id dos validos para
# que a sequencia (gen_id, seq) dos validos fique sem buracos — e a conferencia
# de perdas do reconcile.py continue valendo nas rodadas com malformados.
GEN_ID_MALFORMADOS = 99


def marcador(tipo: str, n: int) -> str:
    """O event_id de um malformado: e por ele que a linha e achada no destino."""
    return f"malf-{tipo}-{n}"
