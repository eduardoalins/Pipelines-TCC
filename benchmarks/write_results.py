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
    SAIDA             pasta onde escrever (padrao: ../../Informações/Resultados Local,
                      contado a partir do repositorio)
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

Sai com 0 se escreveu; 2 se nao havia o que ler ou onde escrever.
"""

from __future__ import annotations

import json
import os
import re
import statistics
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ORIGEM = Path(os.environ.get("ORIGEM", REPO / "results"))
SAIDA = Path(os.environ.get("SAIDA", REPO.parent.parent / "Informações" / "Resultados Local"))

RODADAS = range(1, 11)
MOTORES = (("spark", "NRT"), ("flink", "RT"))
NOME = re.compile(r"Intervalo(\d+)Carga(\d+)")
LIMITE_LINHAS_MD = 300
MARCA = ("<!-- LEITURA: daqui para baixo o texto e escrito a mao; "
         "benchmarks/write_results.py preserva o que estiver abaixo desta linha -->")
LEITURA_VAZIA = "## 7. Leitura\n\n_Ainda não escrita._\n"

# chave, nome completo, onde esta no bloco "resultado" do manifesto, unidade, casas
# decimais na linha da rodada. A media e o desvio padrao levam uma casa a mais.
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
    ("cpu_med", "CPU média", ("cpu_media_pct",), "%", 0),
    ("cpu_pico", "CPU pico", ("cpu_pico_pct",), "%", 0),
    ("mem_med", "Memória média", ("mem_media_mib",), " MiB", 0),
    ("mem_pico", "Memória pico", ("mem_pico_mib",), " MiB", 0),
    ("perdas", "Perdas", ("perdas",), "", 0),
    ("duplicatas", "Duplicatas", ("duplicatas",), "", 0),
    ("atrasados", "Atrasados", ("atrasados",), "", 0),
    ("invalidas", "Inválidas", ("invalidas",), "", 0),
    ("nulas", "Nulas", ("nulas",), "", 0),
    ("descartadas", "Descartadas", ("descartadas",), "", 0),
)
POR_CHAVE = {c[0]: c for c in CAMPOS}


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


def valor(r: dict, chave: str):
    v = (r["manifesto"] or {}).get("resultado") or {}
    for parte in POR_CHAVE[chave][2]:
        v = (v or {}).get(parte)
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
        self.manifestos = [self.rodadas[motor, n]["manifesto"]
                           for motor, _ in MOTORES for n in self.validas[motor]]
        self.duracoes = sorted({m["duracao_s"] for m in self.manifestos})
        # O intervalo que cada motor usou de fato. O da combinacao e o do Spark; o
        # checkpoint do Flink e fixo (run_round.py). Manifesto antigo nao tem o
        # campo: valia o mesmo intervalo para os dois.
        self.intervalos = {
            motor: sorted({self.rodadas[motor, n]["manifesto"].get(
                "intervalo_do_motor_s", self.rodadas[motor, n]["manifesto"]["intervalo_s"])
                for n in self.validas[motor]})
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

    def duracao(self) -> str:
        if not self.duracoes:
            return "sem rodada válida"
        if len(self.duracoes) == 1:
            return f"{self.duracoes[0]} s"
        return "DIFERENTE entre as rodadas: " + lista(f"{d} s" for d in self.duracoes)

    def falha_induzida(self) -> str:
        if not self.malformadas:
            return "nenhuma rodada com eventos malformados"
        proporcoes = lista(f"{p:.0%}" for p in sorted(set(self.malformadas.values())))
        quais = sorted(self.malformadas)
        return (f"{'rodada' if len(quais) == 1 else 'rodadas'} {lista(quais)}: "
                f"{proporcoes} de eventos malformados")

    # --- o .txt --------------------------------------------------------------------
    def txt(self) -> str:
        linhas = [f"Resultados Local: {self.titulo()}",
                  f"Intervalo de cada motor: {self.intervalo_dos_motores()}.",
                  f"Duração de cada rodada: {self.duracao()}."]
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
        L = [f"# Resultado — {self.titulo()}", "",
             f"> Gerado por `benchmarks/write_results.py` a partir de `results/{self.base.name}/`. "
             "As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.", ""]
        L += self.cabecalho() + self.resumo() + self.por_rodada() + self.integridade()
        L += self.apoio() + self.ocorrencias()
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
        return ["## 1. Cabeçalho", "", "| | |", "|---|---|",
                f"| Intervalo | {self.intervalo_dos_motores()} |",
                f"| Carga | {self.carga} eventos por segundo |",
                f"| Duração de cada rodada | {self.duracao()} |",
                f"| Rodadas válidas | {validas} |",
                f"| Falha induzida | {self.falha_induzida()} |",
                f"| Período | {dia(min(quando))} a {dia(max(quando))} |" if quando else "| Período | — |",
                f"| Commit | {lista(f'`{c}`' for c in commits) if commits else '—'} |",
                "| Imagens | " + (" · ".join(f"`{nome}` {lista(sorted(ids))}"
                                             for nome, ids in imagens.items()) or "—") + " |",
                "| Desvio padrão | amostral (divide por n − 1) |", ""]

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
        """Uma linha por rodada e por motor; as rodadas com malformados em negrito."""
        L = ["| Rodada | Motor | " + " | ".join(cabecalho) + " |",
             "|---|---|" + "---:|" * len(cabecalho)]
        for n in RODADAS:
            rotulo = f"**{n}**" if n in self.malformadas else str(n)
            for motor, sigla in MOTORES:
                r = self.rodadas[motor, n]
                celulas = ([situacao(r)] if situacao(r) else
                           extra(r) + [celula(valor(r, c), c, 0, "—") for c in chaves])
                L.append(f"| {rotulo} | {sigla} | " + " | ".join(celulas) + " |")
        return L

    def por_rodada(self) -> list[str]:
        negrito = (["", "Em negrito, as rodadas com eventos malformados."]
                   if self.malformadas else [])
        return (["## 3. Por rodada", ""]
                + self.tabela_rodadas(
                    ["Semente", "Proc. P50", "Proc. P99", "Vis. P50", "Vis. P99", "Throughput",
                     "CPU média", "CPU pico", "Mem. pico"],
                    ("proc_p50", "proc_p99", "vis_p50", "vis_p99", "thr", "cpu_med", "cpu_pico",
                     "mem_pico"),
                    lambda r: [str(r["manifesto"]["seed"])])
                + negrito + [""])

    def integridade(self) -> list[str]:
        return (["## 4. Integridade", "",
                 "Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são "
                 "informação: é o que cada motor fez com os eventos malformados.", ""]
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


# --- escrita -----------------------------------------------------------------------
def escrever_md(destino: Path, gerado: str) -> list[str]:
    """Reescreve as secoes geradas e preserva o que estiver abaixo da linha de marcacao."""
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
    if texto.count("\n") > LIMITE_LINHAS_MD:
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
            avisos = escrever_md(SAIDA / f"{c.nome}.md", c.md())
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
