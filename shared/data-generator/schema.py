"""
Montagem do evento definido no CONTRATO.md, secao 1.1.

Uma regra governa este arquivo inteiro: ele NAO le o relogio e NAO usa
entropia do sistema. Todo sorteio sai do objeto `rng` semeado que recebe no
construtor, e o `event_ts` chega pronto de fora.

O motivo esta no CONTRATO.md 5.1: a carga e reproduzida por seed, sem replay.
Se qualquer campo dependesse de `time.time()` ou de `uuid.uuid4()` (que usa
os.urandom por baixo), a sequencia de eventos deixaria de ser identica entre
a execucao do Spark e a do Flink, e a comparacao perderia a base.

O `event_ts` e a unica excecao, e por isso ele e parametro em vez de sorteio:
ele DEVE mudar entre execucoes. Toda latencia e calculada em relacao ao
proprio event_ts de cada evento, entao o instante absoluto e irrelevante — o
que precisa ser identico e a sequencia.
"""

from __future__ import annotations

import uuid

# Nestes dois tipos o campo `amount` e nulo (CONTRATO.md 1.1).
TIPOS_SEM_AMOUNT = ("pageview", "add_to_cart")

# Ordem dos campos no dicionario = ordem no JSON. Fixa, para que o payload
# serializado seja byte a byte o mesmo entre execucoes.
CAMPOS = (
    "event_id",
    "seq",
    "gen_id",
    "run_id",
    "event_type",
    "user_id",
    "product_id",
    "amount",
    "session_id",
    "event_ts",
)


def _uuid_do_rng(rng) -> str:
    """UUID v4 derivado do rng semeado, nao de os.urandom."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


class FabricaDeEventos:
    """Produz eventos sucessivos, mantendo `seq` e as sessoes por usuario."""

    def __init__(self, rng, cfg: dict, gen_id: int, run_id: str):
        self._rng = rng
        self._gen_id = gen_id
        self._run_id = run_id

        self._users = int(cfg["users"])
        self._products = int(cfg["products"])
        self._amount_min = float(cfg["amount_min"])
        self._amount_max = float(cfg["amount_max"])
        self._prob_nova_sessao = float(cfg["session_rotate_prob"])

        # Ordenado por nome do tipo, e nao pela ordem em que aparecem no
        # YAML: assim reordenar o config nao muda a sequencia sorteada.
        pesos = cfg["event_type_weights"]
        self._tipos = sorted(pesos)
        self._pesos = [float(pesos[t]) for t in self._tipos]

        # Sessao corrente de cada usuario. Uma sessao agrupa varios eventos —
        # e o que faz "ordem dentro da sessao" significar alguma coisa.
        self._sessoes: dict[int, str] = {}

        # Sequencial monotonico por processo gerador, base 0. Buracos nesta
        # sequencia sao perda; repeticoes sao duplicata (CONTRATO.md 1.1).
        self._seq = 0

    @property
    def ultimo_seq(self) -> int:
        return self._seq - 1

    def proximo(self, event_ts_ms: int) -> dict:
        rng = self._rng

        user_id = rng.randint(1, self._users)

        sessao = self._sessoes.get(user_id)
        if sessao is None or rng.random() < self._prob_nova_sessao:
            sessao = _uuid_do_rng(rng)
            self._sessoes[user_id] = sessao

        tipo = rng.choices(self._tipos, weights=self._pesos, k=1)[0]

        if tipo in TIPOS_SEM_AMOUNT:
            amount = None
        else:
            amount = round(rng.uniform(self._amount_min, self._amount_max), 2)

        evento = {
            "event_id": _uuid_do_rng(rng),
            "seq": self._seq,
            "gen_id": self._gen_id,
            "run_id": self._run_id,
            "event_type": tipo,
            "user_id": user_id,
            "product_id": rng.randint(1, self._products),
            "amount": amount,
            "session_id": sessao,
            "event_ts": event_ts_ms,
        }

        self._seq += 1
        return evento
