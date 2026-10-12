"""
Os tipos de evento malformado — a lista unica, usada pelo gerador e pelo injetor.

Sao 13 formas de um evento chegar quebrado ao Kafka, em tres grupos. O que cada
motor faz com cada uma foi medido em 11/10/2026 (CONTRATO.md 2.6):

    nao e um evento          o Spark grava linha toda nula; o Flink descarta
    tipo errado num campo    os dois anulam o campo — mas o Flink converte o
                             numero entre aspas e trunca a fracao
    campo ausente            os dois anulam o campo

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


# tipo -> funcao(evento base, n) -> valor da mensagem (bytes, ou None).
# `n` e o numero de ordem do malformado DAQUELE tipo (0, 1, 2...).
MALFORMADOS = {
    # --- nao e um evento ------------------------------------------------------
    "nao_json":           lambda e, n: f"isto nao e json: {e['event_id']}".encode(),
    "json_truncado":      lambda e, n: texto(e)[: len(texto(e)) // 2],
    "json_array":         lambda e, n: b"[1, 2, 3]",
    "vazio":              lambda e, n: b"",
    "nulo":               lambda e, n: None,
    # --- tipo errado num campo --------------------------------------------------
    "texto_em_seq":       lambda e, n: _com(e, seq="abc"),
    "numero_como_texto":  lambda e, n: _com(e, user_id="17"),
    # Valor DIFERENTE a cada vez: o Flink trunca a fracao, e eventos com o mesmo
    # seq apareceriam como duplicata no reconcile.py (visto em 11/10/2026).
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
