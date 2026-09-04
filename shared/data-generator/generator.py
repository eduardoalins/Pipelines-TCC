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

    conf = {"bootstrap.servers": cfg["broker"]}
    conf.update({str(k): v for k, v in cfg["producer"].items()})
    produtor = Producer(conf)

    rng = random.Random(int(cfg["seed"]))
    fabrica = FabricaDeEventos(rng, cfg, gen_id, run_id)

    print(f"run_id .......... {run_id}")
    print(f"gen_id .......... {gen_id}")
    print(f"topico .......... {topico} @ {cfg['broker']}")
    print(f"carga ........... {taxa} evt/s por {duracao}s = {total_previsto} eventos")
    print(f"seed ............ {cfg['seed']}")
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

        evento = fabrica.proximo(int(time.time() * 1000))
        payload = json.dumps(evento, separators=(",", ":")).encode("utf-8")
        chave = str(evento["user_id"]).encode("utf-8")

        cont.tentados += 1
        try:
            produtor.produce(topico, key=chave, value=payload, callback=entregue)
            cont.enfileirados += 1
            cont.bytes_payload += len(payload)
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

    cont.ultimo_seq = fabrica.ultimo_seq

    # flush() espera as entregas pendentes. O retorno e quantas ficaram na
    # fila sem confirmacao — se nao for zero, houve perda no lado do gerador.
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
    )
    if not ok:
        print("\nATENCAO: confirmados != tentados. Ver os contadores acima.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
