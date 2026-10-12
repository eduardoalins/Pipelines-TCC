"""
Sequencia de uma combinacao — Etapa 8 (= RT-6). As 10 rodadas de cada motor para
UM intervalo e UMA carga: 20 chamadas ao run_round.py, na ordem certa.

    INTERVALO=5 CARGA=1000 DURACAO=600 ~/.venvs/tcc/bin/python benchmarks/run_combination.py

    INTERVALO  segundos — trigger no Spark, intervalo de checkpoint no Flink
    CARGA      mensagens por segundo
    DURACAO    segundos de geracao em cada rodada
    RODADAS    opcional: quais rodadas fazer, ex. RODADAS=1,2 (padrao: 1 a 10)

--- A ordem (decisao de 11/10/2026) -------------------------------------------
Os motores ALTERNAM, e quem comeca inverte a cada rodada:

    rodada 1: Spark, Flink        rodada 2: Flink, Spark        rodada 3: Spark, Flink ...

Assim os dois ficam espalhados por igual ao longo da sessao. O que muda com o
tempo — a maquina esquentar, outro programa abrir — pesa igual sobre os dois, e
nenhum roda sempre "depois do outro". Rodar um motor inteiro e depois o outro
poria uma hora ou mais entre eles, e essa distancia apareceria como diferenca
entre os motores.

--- Quando uma rodada falha (decisao de 11/10/2026) -----------------------------
  nem comecou (codigo 2: relogio, disco, Kafka)   a sequencia PARA. E problema do
                                                  ambiente, e a rodada seguinte
                                                  esbarraria no mesmo defeito.
  comecou e nao valeu (codigo 1)                  tenta UMA vez de novo, guardando
                                                  a tentativa anterior. Se falhar
                                                  de novo, registra e segue.

--- Retomada ---------------------------------------------------------------------
Rodar o mesmo comando de novo pula as rodadas que ja estao validas em results/ e
faz so as que faltam. Uma sequencia interrompida nao se perde.

--- Fechamento (decisao de 12/10/2026) --------------------------------------------
Quando as 20 rodadas da combinacao estao validas, a sequencia FECHA a combinacao:

  1. chama o write_results.py, que escreve o .txt e o .md dela;
  2. confere que os dois arquivos sairam, e completos;
  3. APAGA a pasta da combinacao em results/.

O material bruto nao e guardado nem vai para o git: o que fica da combinacao sao
os dois arquivos. Por isso o passo 3 so acontece se o 2 passou — se os arquivos
nao sairem, a pasta fica e a sequencia avisa. MANTER_BRUTO=1 escreve os arquivos
e nao apaga nada.

Uma combinacao fechada nao e refeita por engano: se a pasta dela nao existe em
results/ e o .txt dela existe, a sequencia nao comeca.

Sai com 0 se as rodadas pedidas estao todas validas; 1 se alguma ficou faltando,
ou se a combinacao estava completa e os arquivos de resultado nao sairam; 2 se
parou por problema do ambiente.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from write_results import SAIDA

REPO = Path(__file__).resolve().parent.parent
PREPARO_S = 90      # relogio, topico e subida do pipeline, medido em 11/10/2026
TODAS = [(n, motor) for n in range(1, 11) for motor in ("spark", "flink")]


def manifesto(pasta: Path) -> dict | None:
    arquivo = pasta / "manifesto.json"
    return json.loads(arquivo.read_text("utf-8")) if arquivo.is_file() else None


def fechar(base: Path, intervalo: int, carga: int) -> bool:
    """Escreve o .txt e o .md da combinacao e, se sairam, apaga o material bruto."""
    print("\n=== fechamento: as 20 rodadas estao validas " + "=" * 24, flush=True)
    inicio = time.time()
    codigo = subprocess.run(
        [sys.executable, "-u", "benchmarks/write_results.py"], cwd=REPO,
        env={**os.environ, "INTERVALO": str(intervalo), "CARGA": str(carga)}).returncode
    txt, md = (SAIDA / f"Resultado{base.name}{ext}" for ext in (".txt", ".md"))
    problema = None
    if codigo != 0:
        problema = f"o write_results.py saiu com codigo {codigo}"
    elif not all(a.is_file() and a.stat().st_size and a.stat().st_mtime >= inicio - 2
                 for a in (txt, md)):
        problema = f"os arquivos nao foram escritos agora em {SAIDA}"
    elif "INCOMPLETO" in txt.read_text("utf-8"):
        problema = f"o {txt.name} saiu marcado como INCOMPLETO"
    if problema:
        print(f"\nNAO FECHEI: {problema}.\nO material bruto CONTINUA em {base}. "
              "Corrija e rode o mesmo comando — ele so refaz o fechamento.")
        return False
    if os.environ.get("MANTER_BRUTO") == "1":
        print(f"\nMANTER_BRUTO=1: o material bruto continua em {base}")
        return True
    shutil.rmtree(base)
    print(f"\nmaterial bruto apagado: {base}\nA combinacao esta fechada. O que fica dela:"
          f"\n   {txt}\n   {md}")
    return True


def main() -> int:
    try:
        intervalo, carga, duracao = (int(os.environ[v]) for v in ("INTERVALO", "CARGA", "DURACAO"))
    except (KeyError, ValueError) as e:
        print(f"parametro ausente ou invalido: {e}. Ver o cabecalho deste arquivo.", file=sys.stderr)
        return 2
    rodadas = [int(n) for n in os.environ.get("RODADAS", "1,2,3,4,5,6,7,8,9,10").split(",")]
    base = REPO / "results" / f"Intervalo{intervalo}Carga{carga}"
    fechada = SAIDA / f"Resultado{base.name}.txt"
    if not base.exists() and fechada.is_file():
        print(f"NAO COMECEI: esta combinacao ja foi fechada — {fechada}\n"
              "Para refaze-la, mova ou apague o .txt e o .md dela antes.", file=sys.stderr)
        return 2

    ordem = []
    for n in rodadas:
        for motor in (("spark", "flink") if n % 2 == 1 else ("flink", "spark")):
            ordem.append((n, motor))
    pasta = lambda n, motor: base / motor / f"rodada-{n:02d}"
    valida = lambda n, motor: bool((manifesto(pasta(n, motor)) or {}).get("valida"))

    pendentes = [(n, m) for n, m in ordem if not valida(n, m)]
    minutos = len(pendentes) * (duracao + PREPARO_S) / 60
    print(f"=== combinacao: intervalo {intervalo} s, carga {carga} msg/s, {duracao} s por rodada ===")
    print(f"    rodadas pedidas: {len(ordem)}   ja validas: {len(ordem) - len(pendentes)}   "
          f"a fazer: {len(pendentes)}")
    print(f"    tempo estimado: {minutos:.0f} min ({minutos / 60:.1f} h)   "
          f"termina por volta de {time.strftime('%H:%M', time.localtime(time.time() + minutos * 60))}")
    print()

    faltaram = []
    for i, (n, motor) in enumerate(pendentes, 1):
        print(f"--- [{i}/{len(pendentes)}] rodada {n:02d}, {motor} " + "-" * 40, flush=True)
        for tentativa in (1, 2):
            env = {**os.environ, "MOTOR": motor, "INTERVALO": str(intervalo), "CARGA": str(carga),
                   "DURACAO": str(duracao), "RODADA": str(n)}
            # Uma tentativa anterior que nao valeu e guardada ao lado, nunca apagada.
            if pasta(n, motor).exists():
                env["REFAZER"] = "1"
            codigo = subprocess.run([sys.executable, "-u", "benchmarks/run_round.py"],
                                    cwd=REPO, env=env).returncode
            if codigo == 0:
                break
            if codigo == 2:
                print(f"\nSEQUENCIA INTERROMPIDA: a rodada {n:02d} de {motor} nem comecou. "
                      "Corrija o ambiente e rode o mesmo comando — ele retoma daqui.")
                return 2
            if tentativa == 1:
                print(f"\n   a rodada nao valeu; tentando mais uma vez\n", flush=True)
        else:
            faltaram.append((n, motor))
            print(f"\n   a rodada {n:02d} de {motor} falhou duas vezes; seguindo\n", flush=True)
        print(flush=True)

    print("=== resumo " + "=" * 57)
    print(f"{'rodada':<8}{'motor':<7}{'vale':<6}{'proc P50':>9}{'vis P50':>9}{'thr':>8}"
          f"{'CPU %':>8}{'mem MiB':>9}{'inval':>7}{'nulas':>7}{'desc':>6}")
    for n, motor in ordem:
        m = manifesto(pasta(n, motor))
        r = (m or {}).get("resultado") or {}
        p50 = lambda chave: (r.get(chave) or {}).get("p50", "-")
        celula = lambda chave: "-" if r.get(chave) is None else r[chave]
        print(f"{n:<8}{motor:<7}{('sim' if m and m.get('valida') else 'NAO'):<6}"
              f"{p50('latencia_processamento_ms'):>9}{p50('latencia_visibilidade_ms'):>9}"
              f"{celula('throughput_medio_evt_s'):>8}{celula('cpu_media_pct'):>8}"
              f"{celula('mem_pico_mib'):>9}{celula('invalidas'):>7}{celula('nulas'):>7}"
              f"{celula('descartadas'):>6}")
    if faltaram:
        print("\nFICARAM FALTANDO: " + ", ".join(f"rodada {n:02d} de {m}" for n, m in faltaram))
        print("As tentativas falhas estao guardadas ao lado, em results/. "
              "Rodar o mesmo comando tenta essas de novo.")
        return 1
    print("\nTodas as rodadas pedidas estao validas.")
    if all(valida(n, motor) for n, motor in TODAS):
        return 0 if fechar(base, intervalo, carga) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
