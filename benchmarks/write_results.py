"""
Arquivos de resultado — Etapa 8 (= RT-6). Le o material bruto de results/ e escreve,
para cada combinacao de intervalo e carga, os dois arquivos que se LEEM:

    ResultadoIntervalo<I>Carga<C>.txt   uma linha por rodada e por motor, depois a
                                        media e o desvio padrao de cada motor
    ResultadoIntervalo<I>Carga<C>.md    as mesmas rodadas em tabelas, mais o que o
                                        .txt nao mostra

    ~/.venvs/tcc/bin/python benchmarks/write_results.py

    INTERVALO, CARGA  opcionais, os dois juntos: so essa combinacao
                      (padrao: todas as que existem em results/)
    AMBIENTE          local | aws (padrao: local). Escolhe o titulo ("Resultados
                      Local:" ou "Resultados AWS:"), a pasta padrao e, na aws, os
                      campos de custo (decisoes de 24/10/2026)
    SAIDA             pasta onde escrever (padrao: ../../Informações/Resultados Local
                      ou .../Resultados AWS, contado a partir do repositorio)
    ORIGEM            pasta de onde ler (padrao: results/)

Nenhum numero e copiado a mao: tudo sai do manifesto.json de cada rodada, gravado
pelo run_round.py. Rodar de novo reescreve os arquivos — e, com as mesmas rodadas,
escreve exatamente o mesmo.

--- As decisoes (11/10/2026) -----------------------------------------------------
  nomes         NRT e o Spark, RT e o Flink. Em cada linha do .txt o nome completo
                da variavel se repete em todos os campos.
  media         sobre as rodadas VALIDAS do motor, sem separar as que levaram
                eventos malformados; a linha diz sobre quantas foi calculada.
  desvio padrao AMOSTRAL (divide por n - 1): as rodadas sao uma amostra do que o
                pipeline faria se rodasse muitas vezes. Com menos de 2 rodadas
                validas nao existe.
  rodada que    aparece no lugar dela, com o motivo, e fica fora da media. O titulo
  nao valeu     avisa que a combinacao esta INCOMPLETA.
  falha         as rodadas com eventos malformados sao sinalizadas no titulo, e so
  induzida      nele.
  Leitura       a ultima secao do .md e interpretacao, escrita a mao. O script NAO
                a reescreve: preserva tudo o que estiver abaixo da linha de
                marcacao, e avisa quando os numeros mudaram com ela ja escrita.

--- Estresse (decisoes de 01/11/2026) ----------------------------------------------
Uma combinacao cujas rodadas tem "estresse" no manifesto (run_round.py com
ESTRESSE=1) e a carga posta de proposito acima do limite. Nela:
  - a rodada que nao valeu mostra o motivo E os numeros que existirem;
  - a media e o desvio padrao continuam so das rodadas validas;
  - o cabecalho (periodo, commit, imagens, duracao) sai de todas as rodadas;
  - o .md ganha, depois das Ocorrencias, o resumo de cada rodada (quando o motor
    caiu, quanto enviou e gravou, a maior fila, os picos da maquina) e a serie do
    coletor de 5 em 5 s — e pode passar do limite de linhas, sem aviso.

Sai com 0 se escreveu; 2 se nao havia o que ler ou onde escrever.
"""

from __future__ import annotations

import csv
import json
import os
import re
import statistics
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
AMBIENTE = os.environ.get("AMBIENTE", "local").lower()
if AMBIENTE not in ("local", "aws"):
    sys.exit(f"AMBIENTE={AMBIENTE!r}: use local ou aws.")
ROTULO = {"local": "Local", "aws": "AWS"}[AMBIENTE]
ORIGEM = Path(os.environ.get("ORIGEM", REPO / "results"))
SAIDA = Path(os.environ.get("SAIDA", REPO.parent.parent / "Informações" / f"Resultados {ROTULO}"))

# --- Custo na nuvem (variavel 6; decisoes de 24/10/2026) ------------------------
# O custo da FATURA: o preco da maquina e do disco vezes o tempo. A EC2 cobra pelo
# tempo ligado, nao pelo uso de CPU. Dois tempos por rodada: a RODADA INTEIRA (o
# principal — inclui o motor subir, que tambem difere entre os modelos) e so a
# INGESTAO (do primeiro envio ate a fila zerar). Para comparar os motores, o
# custo da rodada por milhao de eventos.
# Precos da tabela publica da AWS (pagina de precos do EC2/EBS), us-east-1,
# publicada em 23/10/2026: m5.large Linux On-Demand e o gp3 (3.000 IOPS e 125 MB/s
# de base, sem custo extra). O disco e cobrado por GB-mes, proporcional ao tempo
# (a AWS conta 730 h por mes). O IPv4 publico da maquina (so para o SSH) tambem
# entra na fatura: pagina de precos da VPC, conferida em 24/10/2026.
PRECO_MAQUINA_HORA = 0.096
PRECO_DISCO_GB_MES = 0.08
DISCO_GB = 80
HORAS_MES = 730
PRECO_IPV4_HORA = 0.005
PRECO_HORA = PRECO_MAQUINA_HORA + PRECO_DISCO_GB_MES * DISCO_GB / HORAS_MES + PRECO_IPV4_HORA
PRECOS_TEXTO = ("m5.large US$ 0,096/h + gp3 US$ 0,08/GB-mês × 80 GB + IPv4 público "
                "US$ 0,005/h = US$ " + f"{PRECO_HORA:.4f}".replace(".", ",")
                + "/h (tabelas públicas da AWS, us-east-1: EC2/EBS publicada em "
                "23/10/2026, VPC conferida em 24/10/2026)")

RODADAS = range(1, 11)
MOTORES = (("spark", "NRT"), ("flink", "RT"))
NOME = re.compile(r"Intervalo(\d+)Carga(\d+)")
LIMITE_LINHAS_MD = 300
MARCA = ("<!-- LEITURA: daqui para baixo o texto e escrito a mao; "
         "benchmarks/write_results.py preserva o que estiver abaixo desta linha -->")
LEITURA_VAZIA = "## 7. Leitura\n\n_Ainda não escrita._\n"
# Estresse: o nome do conteiner de cada motor nas leituras do coletor, e o passo da serie.
CONTEINER = {"spark": "spark-run", "flink": "flink-run"}
PASSO_SERIE_S = 5
MIB = 1048576

# chave, nome completo, onde esta no bloco "resultado" do manifesto, unidade, casas
# decimais na linha da rodada. A media e o desvio padrao levam uma casa a mais.
#
# CPU (decisao de 12/10/2026): nos arquivos ela sai em porcentagem da MAQUINA
# INTEIRA — 100% e todos os nucleos ocupados. O manifesto guarda a medicao como
# foi feita, em porcentagem de UM nucleo (112% = um nucleo e um pouco), e o
# numero de nucleos; a divisao e feita aqui, em valor().
CAMPOS = (
    ("proc_p50", "Latência de processamento P50", ("latencia_processamento_ms", "p50"), " ms", 0),
    ("proc_p95", "Latência de processamento P95", ("latencia_processamento_ms", "p95"), " ms", 0),
    ("proc_p99", "Latência de processamento P99", ("latencia_processamento_ms", "p99"), " ms", 0),
    ("proc_med", "Latência de processamento Média", ("latencia_processamento_ms", "media"), " ms", 0),
    ("vis_p50", "Latência de visibilidade P50", ("latencia_visibilidade_ms", "p50"), " ms", 0),
    ("vis_p95", "Latência de visibilidade P95", ("latencia_visibilidade_ms", "p95"), " ms", 0),
    ("vis_p99", "Latência de visibilidade P99", ("latencia_visibilidade_ms", "p99"), " ms", 0),
    ("vis_med", "Latência de visibilidade Média", ("latencia_visibilidade_ms", "media"), " ms", 0),
    ("thr", "Throughput médio", ("throughput_medio_evt_s",), " evt/s", 1),
    ("cpu_med", "CPU média", ("cpu_media_pct",), "%", 1),
    ("cpu_pico", "CPU máximo", ("cpu_pico_pct",), "%", 1),
    ("mem_med", "Memória média", ("mem_media_mib",), " MiB", 0),
    ("mem_pico", "Memória pico", ("mem_pico_mib",), " MiB", 0),
    ("perdas", "Perdas", ("perdas",), "", 0),
    ("duplicatas", "Duplicatas", ("duplicatas",), "", 0),
    ("atrasados", "Atrasados", ("atrasados",), "", 0),
    ("invalidas", "Inválidas", ("invalidas",), "", 0),
    ("nulas", "Nulas", ("nulas",), "", 0),
    ("descartadas", "Descartadas", ("descartadas",), "", 0),
    # A maquina inteira, contando o que roda fora do Docker (decisao de 24/10/2026).
    ("cpu_maq_med", "CPU da máquina média", ("maquina_cpu_media_pct",), "%", 1),
    ("cpu_maq_pico", "CPU da máquina máximo", ("maquina_cpu_pico_pct",), "%", 1),
)
# So na nuvem. Nao estao no manifesto: saem de custo(), abaixo.
CAMPOS_CUSTO = (
    ("custo_rodada", "Custo da rodada", (), " US$", 4),
    ("custo_ingestao", "Custo da ingestão", (), " US$", 4),
    ("custo_milhao", "Custo por milhão de eventos", (), " US$", 4),
)
if AMBIENTE == "aws":
    CAMPOS = CAMPOS + CAMPOS_CUSTO
POR_CHAVE = {c[0]: c for c in CAMPOS}
EM_PCT_DA_MAQUINA = ("cpu_med", "cpu_pico", "cpu_maq_med", "cpu_maq_pico")


class Parar(Exception):
    """Nao da para escrever os arquivos."""


# --- leitura ---------------------------------------------------------------------
def ler(arquivo: Path | None) -> dict | None:
    return json.loads(arquivo.read_text("utf-8")) if arquivo and arquivo.is_file() else None


def carregar(base: Path) -> dict:
    """As 20 rodadas esperadas de uma combinacao, existam ou nao."""
    rodadas = {}
    for motor, _ in MOTORES:
        for n in RODADAS:
            pasta = base / motor / f"rodada-{n:02d}"
            relatorios = sorted(pasta.glob(f"reconcile.{motor}.*.json")) if pasta.is_dir() else []
            rodadas[motor, n] = {
                "pasta": pasta,
                "existe": pasta.is_dir(),
                "manifesto": ler(pasta / "manifesto.json"),
                "relatorio": ler(relatorios[-1]) if relatorios else None,
                # Tentativas que nao valeram e foram refeitas: o run_round.py as guarda ao lado.
                "anteriores": [ler(p / "manifesto.json") or {} for p in
                               sorted((base / motor).glob(f"rodada-{n:02d}.anterior-*"))],
            }
    return rodadas


def vale(r: dict) -> bool:
    return bool((r["manifesto"] or {}).get("valida"))


def situacao(r: dict) -> str | None:
    """Por que a rodada nao tem numeros — ou None, se ela vale."""
    m = r["manifesto"]
    if m is None:
        return "sem manifesto (em andamento ou interrompida)" if r["existe"] else "não realizada"
    if m.get("valida"):
        return None
    return "não valeu — " + "; ".join(m.get("motivos") or ["motivo não registrado"])


def nucleos(manifesto: dict | None) -> int:
    """Quantos nucleos a maquina tinha na rodada. O run_round.py grava; as rodadas
    feitas antes de ele gravar sao da mesma maquina que roda este programa."""
    return int((manifesto or {}).get("nucleos") or os.cpu_count())


def custo(manifesto: dict, chave: str):
    """O custo da fatura de uma rodada, em US$ (so na nuvem).

    rodada     do primeiro passo da rodada (as conferencias) ao ultimo (o manifesto)
    ingestao   do primeiro envio do replayer ate a fila zerar
    milhao     o custo da rodada por milhao de eventos enviados
    """
    passos = [p["quando_ms"] for p in manifesto.get("passos") or []]
    rodada_s = (max(passos) - min(passos)) / 1000 if len(passos) > 1 else None
    if chave == "custo_rodada":
        return rodada_s / 3600 * PRECO_HORA if rodada_s else None
    if chave == "custo_ingestao":
        ini, fim = manifesto.get("gerador_inicio_ms"), manifesto.get("fila_zerou_ms")
        return (fim - ini) / 1000 / 3600 * PRECO_HORA if ini and fim and fim > ini else None
    eventos = (manifesto.get("carga_evt_s") or 0) * (manifesto.get("duracao_s") or 0)
    total = custo(manifesto, "custo_rodada")
    return total / eventos * 1e6 if total is not None and eventos else None


def valor(r: dict, chave: str):
    if chave in ("custo_rodada", "custo_ingestao", "custo_milhao"):
        return custo(r["manifesto"], chave) if r["manifesto"] else None
    v = (r["manifesto"] or {}).get("resultado") or {}
    for parte in POR_CHAVE[chave][2]:
        v = (v or {}).get(parte)
    if v is not None and chave in EM_PCT_DA_MAQUINA:
        v = v / nucleos(r["manifesto"])
    return v


# --- numeros ---------------------------------------------------------------------
def num(v, casas: int) -> str:
    return f"{v:.{casas}f}".replace(".", ",")


def celula(v, chave: str, mais_casas: int = 0, vazio: str = "não medido") -> str:
    _, _, _, unidade, casas = POR_CHAVE[chave]
    return vazio if v is None else num(v, casas + mais_casas) + unidade


def lista(itens) -> str:
    itens = [str(i) for i in itens]
    return itens[0] if len(itens) == 1 else ", ".join(itens[:-1]) + " e " + itens[-1]


def n_rodadas(k: int) -> str:
    return f"{k} rodada" + ("" if k == 1 else "s")


class Combinacao:
    def __init__(self, base: Path) -> None:
        self.base = base
        self.intervalo, self.carga = (int(x) for x in NOME.fullmatch(base.name).groups())
        self.nome = f"Resultado{base.name}"
        self.rodadas = carregar(base)
        self.validas = {motor: [n for n in RODADAS if vale(self.rodadas[motor, n])]
                        for motor, _ in MOTORES}
        self.total_validas = sum(len(v) for v in self.validas.values())
        self.estresse = any((r["manifesto"] or {}).get("estresse") for r in self.rodadas.values())
        # As rodadas que descrevem a combinacao (cabecalho, intervalos): as validas — ou,
        # no estresse, todas as que tem manifesto.
        self.descritas = {motor: [n for n in RODADAS if self.rodadas[motor, n]["manifesto"]
                                  and (self.estresse or vale(self.rodadas[motor, n]))]
                          for motor, _ in MOTORES}
        self.manifestos = [self.rodadas[motor, n]["manifesto"]
                           for motor, _ in MOTORES for n in self.descritas[motor]]
        self.duracoes = sorted({m["duracao_s"] for m in self.manifestos})
        self.nucleos = sorted({nucleos(m) for m in self.manifestos})
        # O intervalo que cada motor usou de fato. O da combinacao e o do Spark; o
        # checkpoint do Flink e fixo (run_round.py). Manifesto antigo nao tem o
        # campo: valia o mesmo intervalo para os dois.
        self.intervalos = {
            motor: sorted({self.rodadas[motor, n]["manifesto"].get(
                "intervalo_do_motor_s", self.rodadas[motor, n]["manifesto"]["intervalo_s"])
                for n in self.descritas[motor]})
            for motor, _ in MOTORES}
        # {rodada: proporcao} das rodadas que levaram eventos malformados, como foi gravado.
        self.malformadas = {}
        for r in self.rodadas.values():
            m = r["manifesto"] or {}
            if m.get("malformed_rate"):
                self.malformadas[m["rodada"]] = m["malformed_rate"]
        # {motor: {chave: (media, desvio padrao amostral)}}
        self.estat = {}
        for motor, _ in MOTORES:
            self.estat[motor] = {}
            for chave, *_ in CAMPOS:
                vs = [v for n in self.validas[motor]
                      if (v := valor(self.rodadas[motor, n], chave)) is not None]
                self.estat[motor][chave] = (statistics.fmean(vs) if vs else None,
                                            statistics.stdev(vs) if len(vs) > 1 else None)

    def titulo(self) -> str:
        return f"Intervalo de {self.intervalo} s, Carga de {self.carga} eventos por segundo"

    def intervalo_dos_motores(self) -> str:
        papel = {"spark": "trigger do NRT", "flink": "checkpoint do RT"}
        partes = [f"{papel[motor]} de {lista(f'{s} s' for s in self.intervalos[motor])}"
                  for motor, _ in MOTORES if self.intervalos[motor]]
        return "; ".join(partes) or f"{self.intervalo} s"

    def cpu(self) -> str:
        quantos = lista(self.nucleos) if self.nucleos else "?"
        return (f"em porcentagem da máquina inteira ({quantos} núcleos): "
                "100% são todos os núcleos ocupados")

    def duracao(self) -> str:
        if not self.duracoes:
            return "sem rodada válida"
        if len(self.duracoes) == 1:
            return f"{self.duracoes[0]} s"
        return "DIFERENTE entre as rodadas: " + lista(f"{d} s" for d in self.duracoes)

    def eventos(self) -> str:
        """De onde vieram os eventos. Os manifestos sao apagados quando a combinacao
        fecha: e aqui que fica registrado (decisao de 24/10/2026)."""
        shas = sorted({m.get("base_sha256_gz") for m in self.manifestos if m.get("janela")})
        if not shas:
            return "gerador sintético (até o contrato 2.2)"
        return ("base real da REES46, outubro de 2019 (sha256 do .gz "
                + lista(f"{s[:12]}…" for s in shas if s)
                + "), uma janela contínua de linhas por rodada")

    def falha_induzida(self) -> str:
        if not self.malformadas:
            return "nenhuma rodada com eventos malformados"
        proporcoes = lista(f"{p:.0%}" for p in sorted(set(self.malformadas.values())))
        quais = sorted(self.malformadas)
        return (f"{'rodada' if len(quais) == 1 else 'rodadas'} {lista(quais)}: "
                f"{proporcoes} de eventos malformados")

    # --- o .txt --------------------------------------------------------------------
    def txt(self) -> str:
        linhas = [f"Resultados {ROTULO}: {self.titulo()}",
                  f"Intervalo de cada motor: {self.intervalo_dos_motores()}.",
                  f"Duração de cada rodada: {self.duracao()}.",
                  f"Eventos: {self.eventos()}.",
                  f"CPU {self.cpu()}."]
        if AMBIENTE == "aws":
            linhas.append(f"Preços: {PRECOS_TEXTO}. Custo principal: a rodada inteira; "
                          "o da ingestão vai do primeiro envio até a fila zerar.")
        if self.malformadas:
            linhas.append(f"Com falha induzida — {self.falha_induzida()}.")
        if self.total_validas < 2 * len(RODADAS):
            linhas.append(f"INCOMPLETO: {self.total_validas} de {2 * len(RODADAS)} rodadas válidas.")
        linhas.append("")
        for motor, sigla in MOTORES:
            for n in RODADAS:
                r = self.rodadas[motor, n]
                campos = ", ".join(f"{nome} - {celula(valor(r, chave), chave)}"
                                   for chave, nome, *_ in CAMPOS)
                if self.estresse and r["manifesto"] and situacao(r):
                    linhas.append(f"Rodada {n} {sigla}: {situacao(r)} | {campos}")
                else:
                    linhas.append(f"Rodada {n} {sigla}: {situacao(r) or campos}")
            linhas.append("")
        for rotulo, indice in (("Media", 0), ("Desvio padrão", 1)):
            for motor, sigla in MOTORES:
                k = len(self.validas[motor])
                if k <= indice:
                    falta = ("nenhuma rodada válida" if k == 0 else
                             "não calculado (precisa de pelo menos 2 rodadas válidas)")
                    linhas.append(f"{rotulo} {sigla}: {falta}")
                    continue
                campos = ", ".join(f"{nome} - {celula(self.estat[motor][chave][indice], chave, 1)}"
                                   for chave, nome, *_ in CAMPOS)
                linhas.append(f"{rotulo} {sigla} (sobre {n_rodadas(k)}): {campos}")
            linhas.append("")
        return "\n".join(linhas)

    # --- o .md ----------------------------------------------------------------------
    def md(self) -> str:
        L = [f"# Resultado {ROTULO} — {self.titulo()}", "",
             f"> Gerado por `benchmarks/write_results.py` a partir de `results/{self.base.name}/`. "
             "As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.", ""]
        L += self.cabecalho() + self.resumo() + self.por_rodada() + self.integridade()
        L += self.apoio() + self.ocorrencias()
        if self.estresse:
            L += self.estresse_resumo() + self.estresse_serie()
        return "\n".join(L) + "\n"

    def cabecalho(self) -> list[str]:
        quando = [p["quando_ms"] for m in self.manifestos for p in m["passos"]]
        dia = lambda ms: time.strftime("%d/%m/%Y %H:%M", time.localtime(ms / 1000))
        commits = sorted({((m.get("git") or {}).get("commit") or "")[:7] or "desconhecido"
                          for m in self.manifestos})
        imagens = {}
        for m in self.manifestos:
            for nome, ident in (m.get("imagens") or {}).items():
                imagens.setdefault(nome, set()).add(ident.removeprefix("sha256:")[:12] or "ausente")
        validas = " · ".join(f"{sigla} {len(self.validas[motor])} de {len(RODADAS)}"
                             for motor, sigla in MOTORES)
        if self.total_validas < 2 * len(RODADAS):
            validas += " — **INCOMPLETO**"
        return (["## 1. Cabeçalho", "", "| | |", "|---|---|",
                f"| Intervalo | {self.intervalo_dos_motores()} |",
                f"| Carga | {self.carga} eventos por segundo |",
                f"| Duração de cada rodada | {self.duracao()} |",
                f"| Eventos | {self.eventos()} |",
                f"| CPU | {self.cpu()} |",
                f"| Rodadas válidas | {validas} |"]
                + (["| Estresse | carga posta acima do limite: uma tentativa por rodada; a rodada "
                    "que não valeu aparece com o motivo e os números que existirem; média e "
                    "desvio padrão só das válidas (seções 6.1 e 6.2) |"] if self.estresse else [])
                + [f"| Falha induzida | {self.falha_induzida()} |",
                f"| Período | {dia(min(quando))} a {dia(max(quando))} |" if quando else "| Período | — |",
                f"| Commit | {lista(f'`{c}`' for c in commits) if commits else '—'} |",
                "| Imagens | " + (" · ".join(f"`{nome}` {lista(sorted(ids))}"
                                             for nome, ids in imagens.items()) or "—") + " |",
                "| Desvio padrão | amostral (divide por n − 1) |"]
                + ([f"| Preços | {PRECOS_TEXTO} |"] if AMBIENTE == "aws" else []) + [""])

    def resumo(self) -> list[str]:
        L = ["## 2. Resumo", "",
             "Média e desvio padrão entre as rodadas válidas de cada motor, "
             "com e sem eventos malformados juntas.", "",
             "| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |",
             "|---|---:|---:|---:|---:|"]
        for chave, nome, *_ in CAMPOS:
            L.append(f"| {nome} | " + " | ".join(
                celula(self.estat[motor][chave][i], chave, 1, "—")
                for motor, _ in MOTORES for i in (0, 1)) + " |")
        return L + [""]

    def tabela_rodadas(self, cabecalho: list[str], chaves: tuple, extra) -> list[str]:
        """Uma linha por rodada e por motor; as rodadas com malformados em negrito.
        No estresse, uma coluna a mais diz se a rodada valeu, e a que nao valeu mostra
        os numeros que tiver (o motivo esta nas Ocorrencias)."""
        if self.estresse:
            cabecalho = ["Situação"] + cabecalho
        L = ["| Rodada | Motor | " + " | ".join(cabecalho) + " |",
             "|---|---|" + "---:|" * len(cabecalho)]
        for n in RODADAS:
            rotulo = f"**{n}**" if n in self.malformadas else str(n)
            for motor, sigla in MOTORES:
                r = self.rodadas[motor, n]
                numeros = lambda: extra(r) + [celula(valor(r, c), c, 0, "—") for c in chaves]
                if self.estresse and r["manifesto"]:
                    celulas = ["válida" if vale(r) else "não valeu"] + numeros()
                elif situacao(r):
                    celulas = [situacao(r)]
                else:
                    celulas = numeros()
                L.append(f"| {rotulo} | {sigla} | " + " | ".join(celulas) + " |")
        return L

    def por_rodada(self) -> list[str]:
        negrito = (["", "Em negrito, as rodadas com eventos malformados."]
                   if self.malformadas else [])

        real = any(m.get("janela") for m in self.manifestos)

        def origem(r: dict) -> list[str]:
            """A janela da base (linhas), ou a semente nas rodadas do gerador sintetico.
            Sem janela numa combinacao da base real: a rodada caiu antes de o replayer
            terminar, e ele nao chegou a registra-la."""
            j = r["manifesto"].get("janela")
            if j:
                return [f"{j['primeira_linha']:,}–{j['ultima_linha']:,}".replace(",", ".")]
            return ["—"] if real else [str(r["manifesto"]["seed"])]
        return (["## 3. Por rodada", ""]
                + self.tabela_rodadas(
                    ["Janela (linhas)" if real else "Semente", "Proc. P50", "Proc. P99", "Vis. P50", "Vis. P99", "Throughput",
                     "CPU média", "CPU máx.", "CPU máquina", "Mem. pico"]
                    + (["Custo rodada"] if AMBIENTE == "aws" else []),
                    ("proc_p50", "proc_p99", "vis_p50", "vis_p99", "thr", "cpu_med", "cpu_pico",
                     "cpu_maq_med", "mem_pico") + (("custo_rodada",) if AMBIENTE == "aws" else ()),
                    origem)
                + negrito + [""])

    def integridade(self) -> list[str]:
        if self.malformadas:
            guarda = "informação: é o que cada motor fez com os eventos malformados."
        elif any(m.get("janela") for m in self.manifestos):
            guarda = ("guarda: sem eventos malformados, o esperado é zero — salvo os 2 eventos "
                      "da base sem sessão, que saem como inválidos se caírem na janela.")
        else:
            guarda = "guarda: sem eventos malformados, o esperado é zero."
        return (["## 4. Integridade", "",
                 f"Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são {guarda}",
                 ""]
                + self.tabela_rodadas(
                    ["Mensagens no tópico", "Perdas", "Duplicatas", "Atrasados", "Inválidas",
                     "Nulas", "Descartadas"],
                    ("perdas", "duplicatas", "atrasados", "invalidas", "nulas", "descartadas"),
                    lambda r: [str(r["manifesto"].get("mensagens_no_topico", "—"))])
                + [""])

    def apoio(self) -> list[str]:
        media = lambda vs, casas: num(statistics.fmean(vs), casas) if vs else "—"
        gravado, parcelas = {}, []
        for motor, sigla in MOTORES:
            destinos = [d for n in self.validas[motor]
                        if (d := (self.rodadas[motor, n]["relatorio"] or {}).get("destino"))]
            gravado[motor] = (
                media([d["arquivos"] for d in destinos], 1),
                media([d["bytes"] / 1048576 for d in destinos], 2),
                media([d["bytes"] / d["linhas"] for d in destinos if d.get("linhas")], 1))
            # As parcelas da latencia: tudo o que o reconciliador mede e nao e um total.
            por_parcela = {}
            for n in self.validas[motor]:
                latencias = (self.rodadas[motor, n]["relatorio"] or {}).get("latencia_ms") or {}
                for nome, medida in latencias.items():
                    if not nome.startswith("LAT."):
                        por_parcela.setdefault(nome, []).append(medida)
            for nome, medidas in por_parcela.items():
                parcelas.append(f"| {sigla} | {nome} | {media([m['p50'] for m in medidas], 1)} ms "
                                f"| {media([m['media'] for m in medidas], 1)} ms |")
        L = ["## 5. Apoio", "", "O que foi gravado no destino, em média por rodada válida:", "",
             "| | NRT | RT |", "|---|---:|---:|"]
        for i, rotulo in enumerate(("Arquivos", "Tamanho (MiB)", "Bytes por linha")):
            L.append(f"| {rotulo} | " + " | ".join(gravado[motor][i] for motor, _ in MOTORES) + " |")
        L += ["", "De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média "
              "de cada parcela:", "",
              "| Motor | Parcela | P50 | Média |", "|---|---|---:|---:|"] + parcelas
        L += ["", "No NRT a latência de visibilidade é, aproximadamente, espera no trigger + "
              "processamento + escrita. No RT é a latência de processamento + a espera pelo "
              "checkpoint.", ""]
        return L

    def ocorrencias(self) -> list[str]:
        itens = []
        for motor, sigla in MOTORES:
            for n in RODADAS:
                r = self.rodadas[motor, n]
                if r["existe"] and situacao(r):
                    itens.append(f"Rodada {n} {sigla}: {situacao(r)}.")
                for anterior in r["anteriores"]:
                    motivo = "; ".join(anterior.get("motivos") or ["motivo não registrado"])
                    itens.append(f"Rodada {n} {sigla}: tentativa anterior guardada "
                                 f"(`{anterior.get('run_id', 'sem manifesto')}`) — {motivo}.")
        faltam = [f"{sigla} {lista(ns)}" for motor, sigla in MOTORES
                  if (ns := [n for n in RODADAS if not self.rodadas[motor, n]["existe"]])]
        if faltam:
            itens.append("Rodadas não realizadas: " + "; ".join(faltam) + ".")
        if len(self.duracoes) > 1:
            itens.append("As rodadas válidas não têm todas a mesma duração: "
                         + lista(f"{d} s" for d in self.duracoes) + ".")
        diferiam = sum(1 for m in self.manifestos
                       if not (m.get("git") or {}).get("pasta_local_igual_ao_commit", True))
        if diferiam:
            itens.append(f"Em {diferiam} de {n_rodadas(len(self.manifestos))} "
                         f"válida{'' if len(self.manifestos) == 1 else 's'} a pasta local tinha "
                         "mudança não commitada (a lista está no manifesto de cada uma).")
        if len({(m.get("git") or {}).get("commit") for m in self.manifestos}) > 1:
            itens.append("As rodadas válidas não saíram todas do mesmo commit.")
        if len({json.dumps(m.get("imagens"), sort_keys=True) for m in self.manifestos}) > 1:
            itens.append("As rodadas válidas não usaram todas as mesmas imagens.")
        return ["## 6. Ocorrências", ""] + [f"- {i}" for i in itens or ["Nenhuma."]] + [""]

    # --- estresse (decisoes de 01/11/2026) ---------------------------------------------
    def leituras(self, motor: str, r: dict) -> dict | None:
        """As leituras do coletor de uma rodada, do inicio do envio em diante.

        {"t0": ms, "recursos": {categoria: [(ts, cpu_ns, cpu_pct, mem_mib)]}, "fila": [(ts, total)]}
        com as categorias maquina, kafka e motor. Le os CSVs que o coletor grava na
        pasta da rodada; None se eles nao existem."""
        m, pasta = r["manifesto"], r["pasta"]
        rec = sorted((pasta / "metrics").glob("recursos.*.csv"))
        lag = sorted((pasta / "metrics").glob("lag.*.csv"))
        t0 = (m or {}).get("gerador_inicio_ms")
        if not rec or t0 is None:
            return None
        recursos = {"maquina": [], "kafka": [], "motor": []}
        for l in csv.DictReader(open(rec[-1], encoding="utf-8")):
            ts, c = int(l["amostra_ts"]), l["conteiner"]
            cat = ("maquina" if c == "maquina" else "kafka" if c == "kafka"
                   else "motor" if CONTEINER[motor] in c else None)
            if cat and ts >= t0:
                recursos[cat].append((ts, int(l["cpu_ns"]), float(l["cpu_pct"]) if l["cpu_pct"] else None,
                                      int(l["mem_sem_cache_bytes"]) / MIB))
        por_ts = {}
        for l in csv.DictReader(open(lag[-1], encoding="utf-8")) if lag else []:
            ts = int(l["amostra_ts"])
            if ts >= t0 and l["lag"]:
                por_ts[ts] = por_ts.get(ts, 0) + int(l["lag"])
        return {"t0": t0, "recursos": recursos, "fila": sorted(por_ts.items())}

    @staticmethod
    def cpu_media(linhas: list) -> float | None:
        """Tempo de CPU consumido / tempo decorrido, como o run_round.py — em % de UM nucleo."""
        if len(linhas) < 2:
            return None
        segundos = (linhas[-1][0] - linhas[0][0]) / 1000
        return (linhas[-1][1] - linhas[0][1]) / 1e9 / segundos * 100 if segundos > 0 else None

    def queda(self, r: dict, s: dict | None) -> str:
        """Quando o motor caiu, contado do inicio do envio: a ultima leitura do conteiner dele."""
        quebrou = (r["manifesto"] or {}).get("quebrou")
        if not quebrou:
            return "não caiu"
        motor = (s or {}).get("recursos", {}).get("motor")
        quando = (f"aos {(motor[-1][0] - s['t0']) / 1000:.0f} s" if motor else "antes da 1ª leitura")
        return quando + (" (durante o envio)" if "DURANTE" in quebrou else " (esvaziando a fila)")

    def estresse_resumo(self) -> list[str]:
        L = ["## 6.1 Estresse: o que aconteceu em cada rodada", "",
             "Contado do início do envio. CPU em porcentagem da máquina inteira; memória sem "
             "o cache de arquivos, como no resto do arquivo. Picos e médias do início do envio "
             "até a última leitura do coletor.", "",
             "| Rodada | Motor | Situação | Queda | Envio real | Eventos gravados | Maior fila "
             "| CPU máquina média | CPU máquina máx. | Mem. máquina pico | Mem. Kafka pico "
             "| Mem. motor pico |",
             "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
        inteiro = lambda v: "—" if v is None else f"{v:,.0f}".replace(",", ".")
        pct = lambda v, k: "—" if v is None else num(v / k, 1) + "%"
        for n in RODADAS:
            for motor, sigla in MOTORES:
                r = self.rodadas[motor, n]
                m = r["manifesto"]
                if not m:
                    L.append(f"| {n} | {sigla} | {situacao(r)} |" + " — |" * 9)
                    continue
                s, k = self.leituras(motor, r), nucleos(m)
                rec = (s or {}).get("recursos", {})
                pico = lambda cat, i: max((l[i] for l in rec.get(cat, []) if l[i] is not None),
                                          default=None)
                gravados = ((r["relatorio"] or {}).get("destino") or {}).get("linhas")
                ritmo = m.get("ritmo_envio_evt_s")
                L.append(" | ".join([
                    f"| {n}", sigla, "válida" if vale(r) else "não valeu", self.queda(r, s),
                    (num(ritmo, 1) + " evt/s") if ritmo is not None else "interrompido",
                    inteiro(gravados),
                    inteiro(max((f for _, f in (s or {}).get("fila", [])), default=None)),
                    pct(self.cpu_media(rec.get("maquina", [])), k), pct(pico("maquina", 2), k),
                    inteiro(pico("maquina", 3)) + " MiB", inteiro(pico("kafka", 3)) + " MiB",
                    inteiro(pico("motor", 3)) + " MiB"]) + " |")
        return L + [""]

    def estresse_serie(self) -> list[str]:
        L = ["## 6.2 Estresse: a máquina ao longo da rodada", "",
             f"Leituras do coletor de {PASSO_SERIE_S} em {PASSO_SERIE_S} s, contadas do início "
             "do envio: em cada instante, a última leitura até ele. CPU em porcentagem da "
             "máquina inteira; memória em MiB, sem o cache; fila em eventos. Depois da queda, "
             "as colunas do motor ficam vazias.", ""]
        for n in RODADAS:
            for motor, sigla in MOTORES:
                r = self.rodadas[motor, n]
                if not r["manifesto"]:
                    continue
                s, k = self.leituras(motor, r), nucleos(r["manifesto"])
                L += [f"### Rodada {n} {sigla} — "
                      + ("válida" if vale(r) else "não valeu") + f", {self.queda(r, s)}", ""]
                if not s:
                    L += ["Sem leituras do coletor.", ""]
                    continue
                rec = s["recursos"]
                fim = max([l[0] for ls in rec.values() for l in ls] + [t for t, _ in s["fila"]],
                          default=s["t0"])

                def ate(lista: list, ts: int):
                    """A ultima leitura ate o instante ts, ou None."""
                    antes = [l for l in lista if l[0] <= ts]
                    return antes[-1] if antes else None

                L += ["| t (s) | CPU máquina | Mem. máquina | Mem. Kafka | CPU motor | Mem. motor | Fila |",
                      "|---:|---:|---:|---:|---:|---:|---:|"]
                for t in range(0, int((fim - s["t0"]) / 1000) + 1, PASSO_SERIE_S):
                    ts = s["t0"] + t * 1000
                    maq, kaf, mot = (ate(rec[c], ts) for c in ("maquina", "kafka", "motor"))
                    # O motor so conta enquanto ainda aparece nas leituras: depois da queda, vazio.
                    if mot and rec["motor"] and ts > rec["motor"][-1][0] + 1000:
                        mot = None
                    fila = ate(s["fila"], ts)
                    if not (maq or kaf or mot or fila):
                        continue        # ainda nenhuma leitura neste instante
                    cpu = lambda l: "—" if not l or l[2] is None else num(l[2] / k, 1) + "%"
                    mem = lambda l: "—" if not l else f"{l[3]:,.0f}".replace(",", ".")
                    L.append(f"| {t} | {cpu(maq)} | {mem(maq)} | {mem(kaf)} | {cpu(mot)} | {mem(mot)} | "
                             + ("—" if not fila else f"{fila[1]:,}".replace(",", ".")) + " |")
                L.append("")
        return L


# --- escrita -----------------------------------------------------------------------
def escrever_md(destino: Path, gerado: str, com_limite: bool = True) -> list[str]:
    """Reescreve as secoes geradas e preserva o que estiver abaixo da linha de marcacao.
    com_limite=False (estresse): o .md leva a serie do coletor e passa do limite de linhas."""
    leitura, avisos = LEITURA_VAZIA, []
    if destino.is_file():
        antigo = destino.read_text("utf-8")
        if MARCA not in antigo:
            raise Parar(f"{destino} existe e nao tem a linha de marcacao: nao sei o que nele "
                        "foi escrito a mao. Nao sobrescrevi. Mova ou apague o arquivo.")
        antes, depois = antigo.split(MARCA, 1)
        leitura = depois.strip("\n") + "\n"
        if leitura.strip() != LEITURA_VAZIA.strip() and antes != gerado:
            avisos.append("os numeros MUDARAM e a secao Leitura ja estava escrita: releia-a.")
    texto = gerado + MARCA + "\n\n" + leitura
    if com_limite and texto.count("\n") > LIMITE_LINHAS_MD:
        avisos.append(f"o .md ficou com {texto.count(chr(10))} linhas; o limite e {LIMITE_LINHAS_MD}.")
    destino.write_text(texto, encoding="utf-8")
    return avisos


def main() -> int:
    intervalo, carga = os.environ.get("INTERVALO"), os.environ.get("CARGA")
    try:
        if bool(intervalo) != bool(carga):
            raise Parar("INTERVALO e CARGA vao juntos: ou os dois, ou nenhum.")
        if intervalo:
            bases = [ORIGEM / f"Intervalo{int(intervalo)}Carga{int(carga)}"]
            if not bases[0].is_dir():
                raise Parar(f"nao ha rodadas dessa combinacao: {bases[0]}")
        else:
            bases = sorted((p for p in ORIGEM.glob("Intervalo*Carga*")
                            if p.is_dir() and NOME.fullmatch(p.name)),
                           key=lambda p: tuple(int(x) for x in NOME.fullmatch(p.name).groups()))
            if not bases:
                raise Parar(f"nenhuma combinacao em {ORIGEM}")
        if not SAIDA.is_dir():
            raise Parar(f"a pasta de saida nao existe: {SAIDA}\n   Crie-a, ou aponte outra com SAIDA=.")

        print(f"lendo de     {ORIGEM}\nescrevendo em {SAIDA}\n")
        for base in bases:
            c = Combinacao(base)
            # O .md primeiro: e ele que pode se recusar, e ai o .txt tambem nao muda.
            avisos = escrever_md(SAIDA / f"{c.nome}.md", c.md(), com_limite=not c.estresse)
            (SAIDA / f"{c.nome}.txt").write_text(c.txt(), encoding="utf-8")
            print(f"{c.nome}  (.txt e .md)")
            print("   rodadas validas: " + ", ".join(
                f"{sigla} {len(c.validas[motor])} de {len(RODADAS)}" for motor, sigla in MOTORES)
                + ("" if c.total_validas == 2 * len(RODADAS) else "   <- INCOMPLETO"))
            for aviso in avisos:
                print(f"   AVISO: {aviso}")
    except Parar as e:
        print(f"NAO ESCREVI: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
