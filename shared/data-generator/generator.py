"""
Gerador de eventos para o Kafka — Etapa 3.

A distincao que este arquivo existe para sustentar:

    CARGA OFERECIDA  = o que o gerador TENTOU emitir
    CARGA ATINGIDA   = o que o broker CONFIRMOU ter gravado

Sem separar as duas, corre-se o risco de medir o limite do proprio gerador e
reportar isso como limite do Spark — um erro que nao aparece em lugar nenhum,
porque o pipeline continua rodando normalmente.

A armadilha concreta: `produce()` do confluent-kafka e ASSINCRONO. Ele
enfileira e retorna na hora. Contar chamadas de produce() como eventos
entregues e contar intencoes, nao entregas. Por isso a decisao E2 exige o
callback de entrega — e so o que passa por ele conta como confirmado.

Uso:
    ~/.venvs/tcc/bin/python shared/data-generator/generator.py
    RATE=1000 DURATION=600 ~/.venvs/tcc/bin/python shared/data-generator/generator.py

Desde a Etapa 8, dois parametros a mais, para as rodadas do benchmark:

    SEED=43               a semente da rodada. Cada rodada usa uma, a MESMA para os
                          dois motores: a rodada N do Spark e a do Flink recebem os
                          mesmos eventos; rodadas diferentes, dados diferentes.
    MALFORMED_RATE=0.01   1 mensagem malformada a cada 100, NO LUGAR de um evento
                          valido — o volume que chega ao Kafka nao muda. Os 13
                          tipos do malformed.py, em rodizio. Desligado por padrao.

Sem os dois, o gerador faz exatamente o que fazia: seed 42, so eventos validos.
"""

from __future__ import annotations

import json
import os
import pathlib
import random
import sys
import time

import yaml
from confluent_kafka import KafkaException, Producer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from malformed import GEN_ID_MALFORMADOS, MALFORMADOS, marcador, texto  # noqa: E402
from schema import FabricaDeEventos  # noqa: E402

AQUI = pathlib.Path(__file__).resolve().parent
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", pathlib.Path.home() / "tcc-data"))


class Contadores:
    """Tudo que separa carga oferecida de carga atingida."""

    def __init__(self) -> None:
        self.tentados = 0          # iteracoes do laco: a carga OFERECIDA
        self.enfileirados = 0      # produce() aceitou
        self.nao_enfileirados = 0  # fila local cheia — ver nota abaixo
        self.confirmados = 0       # callback de entrega sem erro: a ATINGIDA
        self.erros_entrega = 0     # callback de entrega com erro
        self.bytes_payload = 0     # CONTRATO.md 1.2 exige medir o payload
        self.ultimo_seq = -1

        # Malformados (so nas rodadas com MALFORMED_RATE). Contados A PARTE: o
        # reconcile.py compara o destino com os CONFIRMADOS acima, que continuam
        # sendo so os eventos validos.
        self.malf_tentados = 0
        self.malf_enfileirados = 0
        self.malf_confirmados = 0
        self.malf_erros = 0


class Montador:
    """Decide, para cada posicao da carga, que mensagem vai para o Kafka.

    Nao le o relogio nem usa entropia do sistema: o que decide os DADOS e a
    seed, e o que decide ONDE entram os malformados e uma regra fixa (a posicao
    na carga). Por isso duas execucoes com a mesma seed produzem a mesma
    sequencia de mensagens — a garantia que a comparacao entre os motores exige.

    Os malformados ENTRAM NO LUGAR de eventos validos (decisao de 11/10/2026):
    com 1%, sao 990 validos e 10 malformados a cada 1000 mensagens. O volume que
    chega ao Kafka e o mesmo com e sem malformados.

    Duas fabricas, com seeds diferentes: os eventos-base dos malformados nao
    consomem a sequencia dos validos, cujo seq continua sem buracos.
    """

    def __init__(self, cfg: dict, gen_id: int, run_id: str) -> None:
        seed = int(cfg["seed"])
        self._validos = FabricaDeEventos(random.Random(seed), cfg, gen_id, run_id)

        # A proporcao e 1/N com N inteiro: 0.01 -> 1 a cada 100.
        proporcao = float(cfg.get("malformed_rate") or 0)
        self.a_cada = round(1 / proporcao) if proporcao > 0 else 0
        self._quebrados = (
            FabricaDeEventos(random.Random(seed + 1), cfg, GEN_ID_MALFORMADOS, run_id)
            if self.a_cada else None
        )
        self._tipos = list(MALFORMADOS)
        self._emitidos = 0
        self.por_tipo = {tipo: 0 for tipo in self._tipos}

    @property
    def ultimo_seq(self) -> int:
        return self._validos.ultimo_seq

    def proxima(self, i: int, agora_ms: int) -> tuple[bytes, bytes | None, str | None]:
        """(chave, valor, tipo) da mensagem da posicao i. tipo None = evento valido."""
        if self.a_cada and (i + 1) % self.a_cada == 0:
            # Rodizio: o k-esimo malformado e do tipo k mod 13.
            tipo = self._tipos[self._emitidos % len(self._tipos)]
            n = self._emitidos // len(self._tipos)
            base = self._quebrados.proximo(agora_ms)
            base["event_id"] = marcador(tipo, n)
            self._emitidos += 1
            self.por_tipo[tipo] += 1
            return str(base["user_id"]).encode("utf-8"), MALFORMADOS[tipo](base, n), tipo

        evento = self._validos.proximo(agora_ms)
        return str(evento["user_id"]).encode("utf-8"), texto(evento), None


def carregar_config() -> dict:
    with open(AQUI / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Sobrescritas por ambiente, para varrer cargas sem editar o arquivo.
    if os.environ.get("RATE"):
        cfg["rate_per_sec"] = int(os.environ["RATE"])
    if os.environ.get("DURATION"):
        cfg["duration_sec"] = int(os.environ["DURATION"])
    if os.environ.get("GEN_ID"):
        cfg["gen_id"] = int(os.environ["GEN_ID"])
    if os.environ.get("RUN_ID"):
        cfg["run_id"] = os.environ["RUN_ID"]
    if os.environ.get("SEED"):
        cfg["seed"] = int(os.environ["SEED"])
    if os.environ.get("MALFORMED_RATE"):
        cfg["malformed_rate"] = float(os.environ["MALFORMED_RATE"])
    return cfg


def montar_run_id(cfg: dict) -> str:
    if cfg.get("run_id"):
        return str(cfg["run_id"])
    # UTC, para nao depender do fuso da maquina (mesmo motivo do CONTRATO 4.1).
    carimbo = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"{carimbo}-r{cfg['rate_per_sec']}"


def main() -> int:
    cfg = carregar_config()
    run_id = montar_run_id(cfg)
    gen_id = int(cfg["gen_id"])
    taxa = int(cfg["rate_per_sec"])
    duracao = int(cfg["duration_sec"])
    topico = cfg["topic"]

    total_previsto = taxa * duracao
    cont = Contadores()

    def entregue(err, msg):
        """Callback de entrega. E AQUI que um evento vira 'confirmado'."""
        if err is not None:
            cont.erros_entrega += 1
        else:
            cont.confirmados += 1

    def malformado_entregue(err, msg):
        if err is not None:
            cont.malf_erros += 1
        else:
            cont.malf_confirmados += 1

    conf = {"bootstrap.servers": cfg["broker"]}
    conf.update({str(k): v for k, v in cfg["producer"].items()})
    produtor = Producer(conf)

    montador = Montador(cfg, gen_id, run_id)

    print(f"run_id .......... {run_id}")
    print(f"gen_id .......... {gen_id}")
    print(f"topico .......... {topico} @ {cfg['broker']}")
    print(f"carga ........... {taxa} evt/s por {duracao}s = {total_previsto} eventos")
    print(f"seed ............ {cfg['seed']}")
    if montador.a_cada:
        print(f"malformados ..... 1 a cada {montador.a_cada} mensagens, no lugar de um "
              f"evento valido ({len(MALFORMADOS)} tipos em rodizio)")
    print(f"produtor ........ {conf.get('acks')=} {conf.get('compression.type')=} "
          f"{conf.get('partitioner')=} {conf.get('linger.ms')=}")
    print("-" * 68)

    t0 = time.perf_counter()
    proximo_aviso = 10.0

    for i in range(total_previsto):
        # Cronograma ABSOLUTO: o alvo do evento i e t0 + i/taxa. Dormir
        # 1/taxa a cada volta acumularia o erro de granularidade do sleep
        # (~1 a 15 ms) e a taxa real ficaria abaixo da pedida — o gerador
        # viraria o gargalo sem avisar.
        alvo = t0 + i / taxa
        espera = alvo - time.perf_counter()
        if espera > 0.0005:
            time.sleep(espera)

        chave, payload, tipo = montador.proxima(i, int(time.time() * 1000))

        if tipo is None:
            cont.tentados += 1
        else:
            cont.malf_tentados += 1
        try:
            produtor.produce(topico, key=chave, value=payload,
                             callback=entregue if tipo is None else malformado_entregue)
            if tipo is None:
                cont.enfileirados += 1
                cont.bytes_payload += len(payload)
            else:
                cont.malf_enfileirados += 1
        except BufferError:
            # Fila local do produtor cheia. NAO e erro a esconder: e
            # exatamente o ponto em que a carga oferecida deixa de virar
            # carga atingida. Contado e reportado.
            cont.nao_enfileirados += 1
        except KafkaException as e:
            cont.nao_enfileirados += 1
            print(f"[erro] produce falhou: {e}", file=sys.stderr)

        # Serve os callbacks de entrega ja prontos. Sem isso eles so seriam
        # processados no flush() final e o progresso nao apareceria.
        produtor.poll(0)

        decorrido = time.perf_counter() - t0
        if decorrido >= proximo_aviso:
            print(f"  {decorrido:5.1f}s  tentados={cont.tentados}  "
                  f"confirmados={cont.confirmados}")
            proximo_aviso += 10.0

    cont.ultimo_seq = montador.ultimo_seq

    # flush() espera as entregas pendentes. O retorno e quantas ficaram na
    # fila sem confirmacao — se nao for zero, houve perda no lado do gerador.
    pendentes = produtor.flush(timeout=30)
    duracao_real = time.perf_counter() - t0

    # A carga e a de MENSAGENS: nas rodadas com malformados eles ocupam o lugar
    # de eventos validos, e o volume que chega ao Kafka e o mesmo.
    oferecida = (cont.tentados + cont.malf_tentados) / duracao_real
    atingida = (cont.confirmados + cont.malf_confirmados) / duracao_real
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
    if montador.a_cada:
        print(f"malformados ............. {cont.malf_confirmados} confirmados de "
              f"{cont.malf_tentados} (erros {cont.malf_erros})")
    print(f"carga oferecida ......... {oferecida:.1f} evt/s")
    print(f"carga atingida .......... {atingida:.1f} evt/s")
    print(f"payload medio ........... {payload_medio:.1f} bytes")

    # Manifesto do gerador (decisao E2). Na Etapa 8 ele passa a ser um dos
    # blocos do manifesto completo da execucao; aqui ja fica gravado para o
    # reconcile.py da Etapa 6 ter contra o que comparar.
    destino = DATA_DIR / "metrics"
    destino.mkdir(parents=True, exist_ok=True)
    manifesto = destino / f"{run_id}.gen{gen_id}.json"
    with open(manifesto, "w", encoding="utf-8") as f:
        json.dump(
            {
                "run_id": run_id,
                "gen_id": gen_id,
                "topico": topico,
                "seed": cfg["seed"],
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
                # Etapa 8. "tentados" e "confirmados" acima sao so os eventos
                # VALIDOS, que e contra o que o reconcile.py confere perdas.
                "malformed_rate": float(cfg.get("malformed_rate") or 0),
                "malformados": {
                    "tentados": cont.malf_tentados,
                    "confirmados": cont.malf_confirmados,
                    "erros_entrega": cont.malf_erros,
                    "por_tipo": {t: n for t, n in montador.por_tipo.items() if n},
                },
                "mensagens_confirmadas": cont.confirmados + cont.malf_confirmados,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
    print(f"manifesto ............... {manifesto}")

    # Codigo de saida diferente de zero quando algo nao fechou, para que um
    # script de benchmark perceba sem ninguem precisar ler a tela.
    ok = (
        cont.confirmados == cont.tentados
        and cont.erros_entrega == 0
        and cont.nao_enfileirados == 0
        and pendentes == 0
        # Um malformado que o broker recusou muda a carga da rodada.
        and cont.malf_confirmados == cont.malf_tentados
    )
    if not ok:
        print("\nATENCAO: confirmados != tentados. Ver os contadores acima.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
