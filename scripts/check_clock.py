"""Confere o relogio da maquina ANTES de uma execucao medida.

    ~/.venvs/tcc/bin/python scripts/check_clock.py
    DURATION=90 ~/.venvs/tcc/bin/python scripts/check_clock.py

Toda latencia do trabalho e uma subtracao entre dois carimbos do relogio de
parede (event_ts, processed_ts, committed_ts). Um relogio com defeito falsifica
todas elas SEM ERRO NENHUM.

Aconteceu em 10/10/2026, no WSL2: dois sincronizadores brigavam pelo mesmo
relogio — o WSL seguindo a hora do Windows e o systemd-timesyncd seguindo a
internet, com 3,76 s de diferenca entre as duas. O resultado eram saltos de
-1,05 s e +3,78 s a cada ~30 s, e o relogio andando a 91,7% da velocidade. So
foi notado porque um salto caiu no meio de um micro-batch e a tela mostrou
`processamento=-1123 ms`. A saida adotada: parar o systemd-timesyncd durante as
medicoes (ele volta sozinho a cada boot do WSL) e conferir aqui antes de medir.

--- O que este script confere ---------------------------------------------
1. systemd-timesyncd parado. Exigido so no WSL: num Linux comum ha um unico
   sincronizador, e ele ligado e o estado correto.
2. `tick` do kernel perto de 10000. E a "velocidade" que o kernel aplica ao
   relogio; com o defeito, estava em 9166.
3. Nenhum salto: o relogio de parede e comparado, por DURATION segundos, com o
   monotonico BRUTO (CLOCK_MONOTONIC_RAW), que nenhum sincronizador ajusta.
4. Ritmo: no fim da janela, quanto o relogio de parede andou a mais ou a menos
   que o bruto.

Depois de parar o servico o relogio leva ~2 min para assentar. Se este script
reprovar logo em seguida, esperar e rodar de novo.

Sai com 0 se pode medir, 1 se NAO pode — para o arnes da Etapa 8 chamar antes
de cada execucao.
"""
import ctypes
import ctypes.util
import os
import platform
import subprocess
import sys
import time

DURACAO_S = float(os.getenv("DURATION", "40"))

# Limites, tirados do relogio sao medido em 10/10/2026: tick em 10000-10001 e um
# ajuste do proprio WSL de -2 a -4 ms a cada 30 s (normal, nao e defeito).
TICK_NORMAL = 10_000
TICK_TOLERANCIA = 10
SALTO_MS = 10.0
RITMO_TOLERANCIA = 0.001  # 0,1%: 5 ms de erro numa latencia de 5 s

INTERVALO_S = 0.02


class Timex(ctypes.Structure):
    """struct timex do adjtimex(2), ate o campo `tick`."""
    _fields_ = [("modes", ctypes.c_uint), ("offset", ctypes.c_long), ("freq", ctypes.c_long),
                ("maxerror", ctypes.c_long), ("esterror", ctypes.c_long), ("status", ctypes.c_int),
                ("constant", ctypes.c_long), ("precision", ctypes.c_long),
                ("tolerance", ctypes.c_long), ("time_sec", ctypes.c_long),
                ("time_usec", ctypes.c_long), ("tick", ctypes.c_long),
                ("resto", ctypes.c_byte * 128)]


def tick_do_kernel() -> int:
    """Le o tick sem alterar nada (modes = 0 e so leitura)."""
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    t = Timex()
    t.modes = 0
    if libc.adjtimex(ctypes.byref(t)) < 0:
        raise OSError(ctypes.get_errno(), "adjtimex falhou")
    return t.tick


def estado_timesyncd() -> str:
    try:
        r = subprocess.run(["systemctl", "is-active", "systemd-timesyncd"],
                           capture_output=True, text=True, timeout=10)
        return r.stdout.strip() or "desconhecido"
    except (OSError, subprocess.SubprocessError):
        return "desconhecido"


def parede_menos_bruto() -> float:
    """Diferenca entre o relogio de parede e o monotonico bruto, em segundos.

    Le tres vezes e fica com a leitura mais "apertada", para que uma troca de
    contexto entre as duas chamadas nao pareca um salto.
    """
    melhor = None
    for _ in range(3):
        a = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
        parede = time.time()
        b = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
        if melhor is None or (b - a) < melhor[0]:
            melhor = (b - a, parede - (a + b) / 2)
    return melhor[1]


def linha(nome: str, valor: str, ok: bool | None = None) -> None:
    marca = "" if ok is None else ("OK" if ok else "FALHA")
    print(f"  {nome + ' ':.<26} {valor:<44} {marca}")


def main() -> int:
    if not sys.platform.startswith("linux"):
        print("ERRO: este script le o relogio do kernel Linux (rodar dentro do WSL).",
              file=sys.stderr)
        return 1

    kernel = platform.release()
    wsl = "microsoft" in kernel.lower()
    falhas = []

    print("=== check_clock — o relogio antes de medir ===\n")
    linha("ambiente", ("WSL2" if wsl else "Linux") + f" ({kernel})")

    # --- 1. Sincronizador do Ubuntu -----------------------------------------
    servico = estado_timesyncd()
    if wsl:
        ok = servico != "active"
        linha("systemd-timesyncd", servico, ok)
        if not ok:
            falhas.append("systemd-timesyncd ligado: sudo systemctl stop systemd-timesyncd")
    else:
        linha("systemd-timesyncd", f"{servico}  (fora do WSL nao e exigido parado)")

    # --- 2. Tick do kernel, no inicio ----------------------------------------
    tick_inicio = tick_do_kernel()
    ok = abs(tick_inicio - TICK_NORMAL) <= TICK_TOLERANCIA
    linha("tick do kernel", f"{tick_inicio}  (normal {TICK_NORMAL})", ok)
    if not ok:
        falhas.append(f"tick={tick_inicio}: o kernel esta acelerando ou freando o relogio")

    # --- 3 e 4. Saltos e ritmo ------------------------------------------------
    print(f"\n  observando por {DURACAO_S:.0f} s (parede x monotonico bruto)...", flush=True)
    base = anterior = parede_menos_bruto()
    t0 = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
    saltos, maior = [], 0.0
    while (decorrido := time.clock_gettime(time.CLOCK_MONOTONIC_RAW) - t0) < DURACAO_S:
        time.sleep(INTERVALO_S)
        d = parede_menos_bruto()
        passo_ms = (d - anterior) * 1000
        if abs(passo_ms) > abs(maior):
            maior = passo_ms
        if abs(passo_ms) > SALTO_MS:
            saltos.append((decorrido, passo_ms))
        anterior = d
    deriva_ms = (anterior - base) * 1000
    ritmo = (anterior - base) / decorrido

    print()
    ok = not saltos
    linha(f"saltos acima de {SALTO_MS:.0f} ms", str(len(saltos)), ok)
    for quando, ms in saltos[:10]:
        print(f"      aos {quando:5.1f} s: {ms:+9.1f} ms")
    if not ok:
        falhas.append(f"{len(saltos)} salto(s) no relogio de parede")
    linha("maior passo", f"{maior:+.1f} ms  (entre duas leituras)")

    ok = abs(ritmo) <= RITMO_TOLERANCIA
    linha("ritmo", f"{(1 + ritmo) * 100:.3f} %  (deriva de {deriva_ms:+.1f} ms na janela)", ok)
    if not ok:
        falhas.append(f"ritmo {(1 + ritmo) * 100:.2f}%: o relogio nao anda na velocidade certa")

    tick_fim = tick_do_kernel()
    ok = abs(tick_fim - TICK_NORMAL) <= TICK_TOLERANCIA
    linha("tick no fim", str(tick_fim), ok)
    if not ok and tick_fim != tick_inicio:
        falhas.append(f"tick mudou para {tick_fim} durante a observacao")

    # --- Veredito -------------------------------------------------------------
    print("\n" + "=" * 68)
    if not falhas:
        print("OK: relogio sao. Pode medir.")
        return 0
    print("NAO MEDIR: as latencias sairiam falsificadas, sem erro nenhum.")
    for f in falhas:
        print(f"  - {f}")
    if wsl:
        print("Depois de parar o systemd-timesyncd, esperar ~2 min e rodar de novo.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
