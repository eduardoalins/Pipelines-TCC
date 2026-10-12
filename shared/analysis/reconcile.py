"""
Reconciliacao — Etapa 6 (= RT-4). A conferencia DEPOIS da execucao.

    RUN_ID=<run_id> ~/.venvs/tcc/bin/python shared/analysis/reconcile.py
    MOTOR=flink RUN_ID=<run_id> ~/.venvs/tcc/bin/python shared/analysis/reconcile.py
    ~/.venvs/tcc/bin/python shared/analysis/reconcile.py          # lista os run_id

Confere a "prateleira" (o Parquet gravado pelo motor) contra a "nota fiscal" (o
manifesto do gerador) e responde tres perguntas:

  1. Chegou tudo, sem sobra?   perdas e duplicatas por (gen_id, seq)
  2. Quanto tempo levou?       latencia decomposta e as duas latencias da B6
  3. Algum evento atrasado?    watermark e is_late, calculados AQUI (contrato 2.1)

Le tudo com o DuckDB: o mesmo leitor para os dois motores, que nao pertence a
nenhum deles.

--- Decisoes que este arquivo aplica ---------------------------------------
- Reconcilia por (gen_id, seq), nao pela lista de arquivos commitados: o
  _spark_metadata nao existe com foreachBatch, e o Flink nao tem equivalente
  (decisao B3 revogada, planoRT D2).
- Compara contra os CONFIRMADOS pelo broker, nunca contra os tentados (E2): uma
  perda do gerador nao pode ser contada como perda do pipeline.
- Duplicatas sao REPORTADAS, nunca removidas. No Spark (foreachBatch,
  at-least-once) elas podem existir de verdade apos falha, e conta-las e
  resultado do Experimento C.
- Arquivos descobertos PELA PASTA, nunca pela extensao: os do Flink nao tem
  `.parquet`. Ignora o que comeca com "." ou "_": arquivos em andamento do Flink,
  `.crc` e `_SUCCESS` do Spark, pastas `_temporary` — a mesma convencao de
  "arquivo oculto" que o Hadoop usa.
- O pipeline NAO limpa eventos (ELT estrito, Etapa 7) e NAO interpreta tipos
  (CONTRATO.md 2.0): os dez campos chegam ao destino como TEXTO. A conversao
  para o tipo do contrato e feita AQUI, na leitura, com a mesma regra para os
  dois motores — ver selecao_tipada(). E uma regra provisoria e sem conserto: o
  que converte e usado, o que nao converte fica nulo. O lugar definitivo dela e
  o modulo de Transform, a escrever.
- Tres contagens, pelo mesmo metodo nos dois motores: INVALIDAS, NULAS e
  DESCARTADAS. Para isso este arquivo le tambem a particao nula do destino, onde
  vao as linhas sem run_id. As duas ultimas so aparecem com mensagens que nao
  sao JSON, que o gerador deixou de enviar: o Spark grava linha nula, o Flink
  descarta.

--- Saida ------------------------------------------------------------------
Relatorio na tela e um JSON em ~/tcc-data/metrics/reconcile.<motor>.<run_id>.json,
para o arnes da Etapa 8. Sai com 0 se nao houver perda nem duplicata entre os
eventos validos; 1 caso contrario — para o arnes perceber sem ler a tela.
Invalidas, nulas e descartadas sao reportadas, mas NAO reprovam: no benchmark, 4
das 10 rodadas de cada combinacao levam eventos malformados de proposito.
"""

import json
import os
import sys
from pathlib import Path

import duckdb

DATA_DIR = Path(os.getenv("DATA_DIR", Path.home() / "tcc-data"))
MOTOR = os.getenv("MOTOR", "spark")
RUN_ID = os.getenv("RUN_ID", "")
BASE = Path(os.getenv("BASE", DATA_DIR / "out" / MOTOR))
EVENTOS = BASE / "events"
METRICS = DATA_DIR / "metrics"

# CONTRATO.md 2.1: watermark = maximo ACUMULADO de event_ts menos 60 s.
ATRASO_WATERMARK_MS = 60_000

# --- Aquecimento (decisao de 12/10/2026) --------------------------------------
# Os primeiros eventos de uma execucao sao processados com o motor frio. As
# latencias "em regime" deixam de fora os eventos GERADOS nos primeiros
# AQUECIMENTO_S segundos — contados do primeiro evento da execucao, pelo
# event_ts. A regra e a MESMA nos dois motores: sai o mesmo TRECHO da carga. A
# rodada N dos dois recebe os mesmos eventos (mesma seed), mas o event_ts e o
# instante real da emissao, entao na borda do corte poucos eventos diferem
# (medido em 12/10/2026: 4999 e 5002 fora, de 60000).
#
# No benchmark quem passa o valor e o run_round.py: o intervalo da combinacao —
# 5 s nas rodadas de 60 s, 30 s nas de 5 min. Sem a variavel, 5 s.
#
# Ate 12/10/2026 descartava-se a primeira unidade de progresso (o primeiro lote
# do Spark, o primeiro checkpoint do Flink). Nao era a mesma coisa nos dois: o
# primeiro checkpoint do Flink as vezes tinha pouquissimos eventos.
AQUECIMENTO_S = float(os.getenv("AQUECIMENTO_S", "5"))

# --- Do texto ao tipo do contrato (CONTRATO.md 2.0) -------------------------
# Os pipelines carregam os dez campos como TEXTO. Os tipos estao na fonte unica
# do schema, a mesma que os dois pipelines leem.
ARQUIVO_SCHEMA = Path(__file__).resolve().parents[1] / "schema" / "evento.json"

# tipo logico -> (tipo no DuckDB, a forma que o texto precisa ter, INTEIRO).
# A forma e conferida ANTES do cast porque o cast de texto do DuckDB e
# complacente: '1.5' viraria o inteiro 2. Aqui '1.5' num campo inteiro nao
# converte. O TRY_CAST cuida do que tem a forma certa e nao cabe no tipo.
_INTEIRO = r"-?[0-9]+"
_NUMERO = r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?"
CONVERSAO = {
    "int32": ("INTEGER", _INTEIRO),
    "int64": ("BIGINT", _INTEIRO),
    "float64": ("DOUBLE", _NUMERO),
}

# --- Eventos com defeito ------------------------------------------------------
# O pipeline NAO limpa nada (ELT estrito, decisao de 11/10/2026) e nao decide o
# que e valido: isso e contado AQUI, depois do Load.
#
# Campos do evento como chegam ao Parquet (CONTRATO.md 1.1). As colunas de
# particao (run_id, dt, hh) ficam no caminho, nao no arquivo.
CAMPOS_EVENTO = ("event_id", "seq", "gen_id", "event_type", "user_id", "product_id",
                 "amount", "session_id", "event_ts")

# Campos que um evento valido NUNCA traz nulos: todos, menos o amount, que o
# contrato permite nulo (pageview e add_to_cart). O run_id tambem e obrigatorio,
# mas e conferido pela pasta: quem esta na particao nula nao o tem.
OBRIGATORIOS = tuple(c for c in CAMPOS_EVENTO if c != "amount")

# Onde cada motor grava a linha SEM run_id. O nome difere: o primeiro e o do
# Spark, o segundo o do Flink (verificado em 11/10/2026).
PARTICAO_NULA = ("__HIVE_DEFAULT_PARTITION__", "__DEFAULT_PARTITION__")

# Uma linha sem run_id nao diz a que execucao pertence. E atribuida a esta pelo
# instante em que foi processada, com esta folga nas duas pontas.
MARGEM_PARTICAO_NULA_MS = 5_000

# Flink: os arquivos de um mesmo checkpoint ficam visiveis quase juntos, mas nem
# sempre no mesmo milissegundo (visto em 10/10/2026: 8 arquivos divididos em
# .236 e .246). Arquivos cujo ctime dista ate este limite do anterior sao do
# MESMO checkpoint. Tem de ficar bem abaixo do intervalo de checkpoint (5 s).
AGRUPAR_CTIME_MS = 1_000


# --- Descoberta de arquivos ------------------------------------------------

def oculto(nome: str) -> bool:
    return nome.startswith(".") or nome.startswith("_")


def arquivos_de_dados(pasta: Path) -> list[Path]:
    """Todos os arquivos de dados sob `pasta`, pela pasta e nao pela extensao."""
    achados = []
    for raiz, dirs, arqs in os.walk(pasta):
        dirs[:] = [d for d in dirs if not oculto(d)]  # nao desce em _temporary
        achados += [Path(raiz) / a for a in arqs if not oculto(a)]
    return sorted(achados)


def lista_sql(caminhos) -> str:
    """Lista de caminhos como literal SQL, com aspas escapadas."""
    return "[" + ", ".join("'" + str(c).replace("'", "''") + "'" for c in caminhos) + "]"


def colunas_de(con, fonte: str) -> set:
    return {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM {fonte}").fetchall()}


def listar_runs() -> None:
    print(f"run_id disponiveis em {EVENTOS}:\n")
    for d in sorted(EVENTOS.glob("run_id=*")):
        arqs = arquivos_de_dados(d)
        mb = sum(a.stat().st_size for a in arqs) / 1e6
        print(f"  {d.name.removeprefix('run_id='):<32} {len(arqs):>6} arquivos  {mb:8.2f} MB")
    print("\nUse:  RUN_ID=<run_id> python shared/analysis/reconcile.py")


def manifestos(run_id: str) -> dict[int, dict]:
    """A 'nota fiscal': um manifesto por processo gerador (gen_id)."""
    saida = {}
    for m in sorted(METRICS.glob(f"{run_id}.gen*.json")):
        dado = json.loads(m.read_text(encoding="utf-8"))
        saida[int(dado["gen_id"])] = dado
    return saida


# --- Do texto ao tipo do contrato ---------------------------------------------

def selecao_tipada(con, fonte: str) -> tuple[str, list[str]]:
    """O comeco de um SELECT que le `fonte` com os campos nos tipos do contrato.

    Devolve ("SELECT * REPLACE (...), <campo> AS <campo>__txt ...", convertidos).
    Quem chama completa com o FROM. Cada campo numerico que esta em TEXTO no
    destino e substituido pelo valor convertido, e o texto original fica ao
    lado, em <campo>__txt — e por ele que se conta o que nao converteu.

    A regra, igual para os dois motores porque e aplicada aqui e nao neles:

        inteiro   so digitos, com sinal opcional, e que caiba no tipo
        decimal   um numero escrito por extenso ou em notacao cientifica
        texto     como veio

    O que nao converte fica NULO. Nada e consertado, arredondado ou adivinhado.
    Um campo que ja esta tipado no arquivo (destino gravado antes do contrato
    2.0) passa direto.

    processed_ts e timestamp desde o CONTRATO.md 1.5 (2.5); antes era inteiro
    (epoch ms). Tudo aqui trabalha em epoch ms, que e tambem como os logs de
    progresso o guardam — entao ele e convertido na entrada.
    """
    tipos = {c[0]: c[1].upper() for c in con.execute(f"DESCRIBE SELECT * FROM {fonte}").fetchall()}
    trocas, originais, convertidos = [], [], []
    if "TIMESTAMP" in tipos.get("processed_ts", ""):
        trocas.append("epoch_ms(processed_ts) AS processed_ts")
    for campo in json.loads(ARQUIVO_SCHEMA.read_text(encoding="utf-8"))["campos"]:
        nome, tipo = campo["nome"], campo["tipo"]
        if tipo == "string":
            continue
        if tipo not in CONVERSAO:
            raise ValueError(f"tipo '{tipo}' do campo '{nome}' em {ARQUIVO_SCHEMA} nao tem "
                             f"conversao. Tipos conhecidos: string, {', '.join(CONVERSAO)}")
        if tipos.get(nome) != "VARCHAR":
            continue                    # ausente, ou ja tipado no arquivo
        destino, forma = CONVERSAO[tipo]
        trocas.append(f"CASE WHEN regexp_full_match({nome}, '{forma}') "
                      f"THEN TRY_CAST({nome} AS {destino}) END AS {nome}")
        originais.append(f"{nome} AS {nome}__txt")
        convertidos.append(nome)
    selecao = "SELECT *" + (f" REPLACE ({', '.join(trocas)})" if trocas else "")
    return selecao + "".join(f", {o}" for o in originais), convertidos


# --- Estatistica ------------------------------------------------------------

def resumo(con, expr: str, origem: str, filtro: str = "") -> dict | None:
    """P50, P95, P99, media, min e max de uma expressao em milissegundos."""
    onde = f"WHERE x IS NOT NULL {('AND ' + filtro) if filtro else ''}"
    r = con.execute(f"""
        SELECT count(x), quantile_cont(x, 0.5), quantile_cont(x, 0.95),
               quantile_cont(x, 0.99), avg(x), min(x), max(x)
        FROM (SELECT {expr} AS x, * FROM {origem}) {onde}
    """).fetchone()
    if not r[0]:
        return None
    chaves = ["n", "p50", "p95", "p99", "media", "min", "max"]
    return {k: (round(v) if k != "n" else v) for k, v in zip(chaves, r)}


def corte_do_aquecimento(con) -> tuple[str, dict]:
    """O filtro SQL do "regime" sobre a visao `lat`, e o registro do que saiu."""
    inicio, total = con.execute("SELECT min(event_ts), count(*) FROM lat").fetchone()
    if inicio is None:
        return "true", {"segundos": AQUECIMENTO_S, "eventos_descartados": None}
    corte = int(inicio + AQUECIMENTO_S * 1000)
    fora = con.execute(f"SELECT count(*) FROM lat WHERE event_ts < {corte}").fetchone()[0]
    print(f"  aquecimento ............ os eventos gerados nos primeiros {AQUECIMENTO_S:g} s "
          f"ficam fora do regime: {fora} de {total}")
    return f"event_ts >= {corte}", {"segundos": AQUECIMENTO_S, "eventos_descartados": fora,
                                    "primeiro_event_ts": int(inicio)}


def linha_ms(nome: str, s: dict | None) -> str:
    if s is None:
        return f"  {nome:<26} indisponivel"
    return (f"  {nome:<26} P50 {s['p50']:>6}  P95 {s['p95']:>6}  P99 {s['p99']:>6}  "
            f"media {s['media']:>6}  min {s['min']:>6}  max {s['max']:>6}   (ms, n={s['n']})")


# --- Eventos com defeito: invalidas, nulas e descartadas --------------------

def contar_ilegiveis(con, colunas: set, tem_particao_nula: bool, duplicatas: int,
                     convertidos: list[str]) -> dict:
    """As tres contagens da Etapa 7, pelo mesmo metodo nos dois motores.

        INVALIDAS    linha gravada com ALGUM campo obrigatorio nulo DEPOIS da
                     conversao — porque nao veio, ou porque veio um texto que
                     nao converte para o tipo do contrato. Obrigatorio e
                     qualquer campo do evento menos o amount; e o run_id,
                     quando a linha esta na particao nula
        NULAS        linha gravada com TODOS os campos do evento nulos
        DESCARTADAS  mensagem que estava no topico e nao virou linha nenhuma

    Limite da INVALIDAS: o amount pode ser nulo num evento valido (pageview e
    add_to_cart), entao um amount que nao converte NAO entra nela. Ele aparece
    na linha "texto que nao converte", que e so informacao.

    `"user_id": "17"` NAO e invalida: o texto 17 converte.

    As duas primeiras se veem no destino. A terceira e uma ausencia: exige saber
    quantas mensagens o topico recebeu na execucao, o que vem de uma anotacao
    <run_id>.topico.json (gravada pelo run_round.py). Sem ela, fica
    indisponivel — nunca e estimada.
    """
    campos = [c for c in CAMPOS_EVENTO if c in colunas]
    tudo_nulo = " AND ".join(f"{c} IS NULL" for c in campos)
    obrigatorios = [c for c in OBRIGATORIOS if c in colunas]
    obrigatorio_nulo = " OR ".join(f"{c} IS NULL" for c in obrigatorios)

    na_pasta, nulas, invalidas = con.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE {tudo_nulo}),
               count(*) FILTER (WHERE NOT ({tudo_nulo}) AND ({obrigatorio_nulo}))
        FROM ev
    """).fetchone()
    por_campo = {c: con.execute(
        f"SELECT count(*) FROM ev WHERE NOT ({tudo_nulo}) AND {c} IS NULL").fetchone()[0]
        for c in obrigatorios}

    fora = 0
    if tem_particao_nula:
        fora, nulas_fora = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE {tudo_nulo}) FROM sem_run_id").fetchone()
        nulas += nulas_fora
        invalidas += fora - nulas_fora          # tem dado, mas nao tem run_id
        por_campo["run_id"] = fora - nulas_fora
        for c in obrigatorios:
            por_campo[c] += con.execute(
                f"SELECT count(*) FROM sem_run_id WHERE NOT ({tudo_nulo}) AND {c} IS NULL"
            ).fetchone()[0]
    por_campo = {c: n for c, n in por_campo.items() if n}
    linhas = na_pasta + fora

    # Texto que chegou ao destino e nao converte para o tipo do contrato. Ja
    # esta dentro dos "nulos por campo" acima; aqui e separado do campo ausente.
    nao_convertem = {}
    for c in convertidos:
        n = con.execute(
            f"SELECT count(*) FROM ev WHERE {c}__txt IS NOT NULL AND {c} IS NULL").fetchone()[0]
        if tem_particao_nula and f"{c}__txt" in colunas_de(con, "sem_run_id"):
            n += con.execute(f"SELECT count(*) FROM sem_run_id "
                             f"WHERE {c}__txt IS NOT NULL AND {c} IS NULL").fetchone()[0]
        if n:
            nao_convertem[c] = n

    print("\n  Eventos com defeito:")
    print(f"    linhas no destino ............ {linhas}   ({na_pasta} na pasta do run_id"
          f" + {fora} em particao nula)")
    print(f"    INVALIDAS (obrigatorio nulo ou que nao converte) {invalidas}")
    if por_campo:
        print("        nulos por campo: " + ", ".join(f"{c} {n}" for c, n in por_campo.items()))
    if nao_convertem:
        print("        texto que nao converte: "
              + ", ".join(f"{c} {n}" for c, n in nao_convertem.items()))
    print(f"    NULAS (todos os campos nulos) .. {nulas}")

    mensagens = descartadas = None
    anotacao = METRICS / f"{RUN_ID}.topico.json"
    if anotacao.is_file():
        mensagens = int(json.loads(anotacao.read_text(encoding="utf-8"))["mensagens"])
        # Uma duplicata e uma linha a mais para a MESMA mensagem: nao pode
        # esconder um descarte.
        descartadas = mensagens - (linhas - duplicatas)
        print(f"    mensagens no topico .......... {mensagens}   ({anotacao.name})")
        print(f"    DESCARTADAS (mensagens - linhas) {descartadas}")
        if descartadas < 0:
            print("    !! mais linhas que mensagens: ha linhas de outra execucao na janela?")
    else:
        print(f"    DESCARTADAS .................. indisponivel (sem {anotacao.name})")

    return {"invalidas": invalidas, "nulas": nulas, "descartadas": descartadas,
            "linhas_no_destino": linhas, "linhas_em_particao_nula": fora,
            "mensagens_no_topico": mensagens, "nulos_por_campo": por_campo,
            "nao_convertem": nao_convertem}


# --- Throughput medio -------------------------------------------------------

def throughput_medio(con, modo: str, aquecimento: dict) -> dict | None:
    """Eventos gravados por segundo na execucao, sem o aquecimento.

        eventos  as linhas dos eventos GERADOS depois do aquecimento
        tempo    do fim do aquecimento ate a ultima gravacao

    O aquecimento e o mesmo das latencias (decisao de 12/10/2026; ver
    AQUECIMENTO_S): os eventos gerados nos primeiros segundos da execucao, pelo
    event_ts. Igual nos dois motores, que descartam o mesmo trecho da carga. Ate
    entao o throughput descartava a primeira unidade de progresso.

    Consequencia declarada: o tempo vai ate a ULTIMA gravacao, que acontece
    depois de o gerador parar — a fila ainda esta sendo esvaziada. Por isso,
    quando o pipeline acompanha a carga, este numero fica um pouco ABAIXO dela,
    e tanto mais abaixo quanto mais o motor demora a gravar o fim da carga.
    Nessa situacao ele nao distingue capacidade; so passa a mostra-la quando o
    pipeline nao acompanha.

    Uma linha sem event_ts valido nao tem instante de geracao e fica de fora.

    A ultima gravacao vem do mesmo lugar que na latencia de visibilidade: log de
    progresso no Spark, ctime dos arquivos no Flink (CONTRATO.md 2.2).
    """
    inicio = (aquecimento or {}).get("primeiro_event_ts")
    if modo == "arquivo":
        fim = con.execute("SELECT max(committed_ts) FROM ev").fetchone()[0]
    else:
        fim = con.execute("""
            SELECT max(g.committed_ts)
            FROM (SELECT DISTINCT unidade FROM ev) u JOIN prog g ON g.processed_ts = u.unidade
        """).fetchone()[0]
    if inicio is None or fim is None:
        print("\n  THROUGHPUT MEDIO .......... indisponivel (sem instante de inicio ou de gravacao)")
        return None
    corte = int(inicio + AQUECIMENTO_S * 1000)
    eventos = con.execute(f"SELECT count(*) FROM ev WHERE event_ts >= {corte}").fetchone()[0]
    segundos = (fim - corte) / 1000
    if segundos <= 0 or not eventos:
        print("\n  THROUGHPUT MEDIO .......... indisponivel (nada gravado depois do aquecimento)")
        return None
    evt_s = eventos / segundos
    print(f"\n  THROUGHPUT MEDIO .......... {evt_s:.1f} evt/s   ({eventos} eventos gerados depois "
          f"dos primeiros {AQUECIMENTO_S:g} s, em {segundos:.1f} s ate a ultima gravacao)")
    return {"evt_s": round(evt_s, 2), "eventos": eventos, "segundos": round(segundos, 3)}


# --- Latencia no Flink (committed_ts = ctime do arquivo) ---------------------

def agrupar_por_checkpoint(ctimes: dict[Path, int]) -> dict[Path, int]:
    """A unidade de progresso de cada arquivo do Flink: o checkpoint que o fechou.

    Sem batch_id nao ha numero de checkpoint no dado; o que existe e o ctime.
    Percorre os arquivos em ordem de ctime e abre um grupo novo sempre que o
    salto para o anterior passa de AGRUPAR_CTIME_MS. O grupo e identificado
    pelo seu primeiro ctime.

    So AGRUPA: o committed_ts de cada arquivo continua sendo o ctime dele.
    """
    unidade, inicio, anterior = {}, None, None
    for arq, ct in sorted(ctimes.items(), key=lambda par: par[1]):
        if anterior is None or ct - anterior > AGRUPAR_CTIME_MS:
            inicio = ct
        unidade[arq] = inicio
        anterior = ct
    return unidade


def latencia_por_arquivo(con, logs: list[Path]) -> dict:
    """Latencias do Flink, com o committed_ts vindo do ctime de cada arquivo.

    Sem batch nao ha "espera no trigger" nem "processamento do lote": o evento
    e carimbado ao passar pelo job. A decomposicao fica em duas parcelas:

        processamento           processed_ts - event_ts
        espera pelo checkpoint  committed_ts - processed_ts
        --------------------------------------------------
        visibilidade            committed_ts - event_ts

    O log de checkpoints NAO entra na conta: e so conferido contra o ctime.
    """
    con.execute("""
        CREATE VIEW lat AS
        SELECT * FROM (
            SELECT gen_id, seq, event_ts, processed_ts, committed_ts, unidade,
                   row_number() OVER (PARTITION BY gen_id, seq ORDER BY processed_ts) AS rn
            FROM ev WHERE gen_id IS NOT NULL AND seq IS NOT NULL
        ) WHERE rn = 1
    """)
    n_unidades, n_instantes = con.execute(
        "SELECT count(DISTINCT unidade), count(DISTINCT committed_ts) FROM lat"
    ).fetchone()
    metricas = {
        "espera pelo checkpoint": "committed_ts - processed_ts",
        "LAT. PROCESSAMENTO": "processed_ts - event_ts",
        "LAT. VISIBILIDADE": "committed_ts - event_ts",
    }
    print(f"  checkpoints com dados .. {n_unidades}")
    print(f"  instantes de ctime ..... {n_instantes}   (arquivos a ate {AGRUPAR_CTIME_MS} ms "
          "um do outro = mesmo checkpoint)")
    print("  committed_ts ........... ctime de cada arquivo (CONTRATO.md 1.6, 2.2)")
    # Aquecimento: a mesma regra do Spark, pelo instante em que o evento foi gerado.
    regime, aquecimento = corte_do_aquecimento(con)
    print(f"\n  em regime (sem os primeiros {AQUECIMENTO_S:g} s de eventos):")
    lat = {}
    for nome, expr in metricas.items():
        s = resumo(con, expr, "lat", regime)
        lat[nome] = s
        print(linha_ms(nome, s))
    print("\n  execucao inteira (com o aquecimento):")
    for nome in ("LAT. PROCESSAMENTO", "LAT. VISIBILIDADE"):
        s = resumo(con, metricas[nome], "lat")
        lat[nome + " (com aquecimento)"] = s
        print(linha_ms(nome, s))

    # Conferencia: cada ctime deve cair a poucos ms do fim de um checkpoint no
    # log (verificado a -7..+8 ms em 25/09/2026). Um desvio grande indica que
    # algo tocou nos arquivos depois de gravados (chmod, copia...) — e a
    # latencia de visibilidade estaria falsificada.
    conferencia = None
    if logs:
        con.execute(f"""
            CREATE VIEW ck AS
            SELECT committed_ts::BIGINT AS fim
            FROM read_json({lista_sql(logs)}, format = 'newline_delimited',
                           union_by_name = true)
            WHERE checkpoint_id IS NOT NULL
        """)
        # Por INSTANTE de ctime, nao por grupo: todo arquivo e conferido.
        d_min, d_max, n = con.execute("""
            WITH u AS (SELECT DISTINCT committed_ts AS ct FROM lat),
            par AS (
                SELECT u.ct, arg_min(u.ct - ck.fim, abs(u.ct - ck.fim)) AS d
                FROM u, ck GROUP BY u.ct
            )
            SELECT min(d), max(d), count(*) FROM par
        """).fetchone()
        conferencia = {"ctime_menos_fim_checkpoint_ms": [d_min, d_max],
                       "unidades": n_unidades, "instantes": n}
        print(f"\n  conferencia ctime x log de checkpoints: {d_min}..{d_max} ms "
              f"em {n} instantes de ctime")
        if max(abs(d_min), abs(d_max)) > 1000:
            print("  !! desvio acima de 1 s: os arquivos foram tocados depois de gravados?")
    else:
        print("\n  (sem log de checkpoints para conferir o ctime)")

    return {"latencia_ms": lat, "aquecimento": aquecimento, "conferencia_ctime": conferencia,
            "agrupamento_ctime": {"checkpoints": n_unidades, "instantes": n_instantes,
                                  "limite_ms": AGRUPAR_CTIME_MS}}


# --- Principal --------------------------------------------------------------

def main() -> int:
    if not EVENTOS.is_dir():
        print(f"ERRO: destino inexistente: {EVENTOS}", file=sys.stderr)
        return 2
    if not RUN_ID:
        listar_runs()
        return 2

    pasta = EVENTOS / f"run_id={RUN_ID}"
    arqs = arquivos_de_dados(pasta)
    if not arqs:
        print(f"ERRO: nenhum arquivo de dados em {pasta}", file=sys.stderr)
        return 2

    con = duckdb.connect()
    # filename = true: cada linha traz o arquivo de onde veio. No Flink e ele
    # que liga o evento ao instante de visibilidade (CONTRATO.md 1.6, 2.2).
    con.execute(f"""
        CREATE VIEW ev_bruto AS
        SELECT * FROM read_parquet({lista_sql(arqs)}, hive_partitioning = true,
                                   filename = true)
    """)
    tipos = {c[0]: c[1] for c in con.execute("DESCRIBE ev_bruto").fetchall()}
    colunas = set(tipos)

    # Do texto ao tipo do contrato, e o processed_ts em epoch ms: tudo o que vem
    # depois le as visoes `ev` e `sem_run_id`, ja convertidas.
    selecao, convertidos = selecao_tipada(con, "ev_bruto")
    em_ms = "TIMESTAMP" in tipos.get("processed_ts", "").upper()
    pts_ms = "epoch_ms(processed_ts)" if em_ms else "processed_ts"

    # UNIDADE DE PROGRESSO — o que agrupa os eventos que ficaram visiveis juntos
    # (CONTRATO.md 1.6, 2.2-2.3):
    #   Spark  -> o micro-batch, identificado pelo processed_ts (um por batch);
    #             o committed_ts vem do log de progresso.
    #   Flink  -> o checkpoint, reconhecido pelos arquivos: sem batch_id, cada
    #             arquivo e finalizado por exatamente um checkpoint, e o
    #             committed_ts e o ctime dele — o instante em que deixou de ser
    #             oculto. Os arquivos que ficaram visiveis juntos formam a
    #             unidade (agrupar_por_checkpoint).
    modo = "batch" if "batch_id" in colunas else "arquivo"
    if modo == "arquivo":
        ctimes = {a: int(a.stat().st_ctime * 1000) for a in arqs}
        unidades = agrupar_por_checkpoint(ctimes)
        con.execute("CREATE TABLE arq_ctime "
                    "(filename VARCHAR, committed_ts BIGINT, unidade BIGINT)")
        con.executemany("INSERT INTO arq_ctime VALUES (?, ?, ?)",
                        [(str(a), ctimes[a], unidades[a]) for a in arqs])
        # O `*` da selecao ja traz committed_ts e unidade, vindos de arq_ctime.
        con.execute(f"""
            CREATE VIEW ev AS
            {selecao}
            FROM ev_bruto JOIN arq_ctime USING (filename)
        """)
    else:
        con.execute(f"CREATE VIEW ev AS {selecao}, {pts_ms} AS unidade FROM ev_bruto")
    relatorio: dict = {"motor": MOTOR, "run_id": RUN_ID, "unidade_de_progresso": modo}

    # LINHAS SEM run_id — ficam fora da pasta desta execucao, na particao nula
    # do motor. Entram na conta se foram processadas na janela desta execucao.
    arqs_nulos = [a for nome in PARTICAO_NULA if (EVENTOS / f"run_id={nome}").is_dir()
                  for a in arquivos_de_dados(EVENTOS / f"run_id={nome}")]
    tem_particao_nula = False
    if arqs_nulos and "processed_ts" in colunas:
        t0, t1 = con.execute("SELECT min(processed_ts), max(processed_ts) FROM ev").fetchone()
        if t0 is not None:
            # So os arquivos gravados desde o comeco desta execucao: um arquivo
            # mais antigo nao pode ter linha dela. Tira do caminho, tambem, sobra
            # de execucoes anteriores ao contrato 2.0, cujas colunas tem outro
            # tipo e nao se misturam com as de agora na mesma leitura.
            desde = (int(t0) - MARGEM_PARTICAO_NULA_MS) / 1000
            arqs_nulos = [a for a in arqs_nulos if a.stat().st_mtime >= desde]
        if t0 is not None and arqs_nulos:
            leitura = (f"read_parquet({lista_sql(arqs_nulos)}, hive_partitioning = true, "
                       "hive_types_autocast = false, union_by_name = true)")
            selecao_nula, _ = selecao_tipada(con, leitura)
            con.execute(f"""
                CREATE VIEW sem_run_id AS
                SELECT * FROM ({selecao_nula} FROM {leitura})
                WHERE processed_ts BETWEEN {int(t0) - MARGEM_PARTICAO_NULA_MS}
                                       AND {int(t1) + MARGEM_PARTICAO_NULA_MS}
            """)
            tem_particao_nula = True

    print(f"=== reconcile — motor {MOTOR}, run_id {RUN_ID} ===\n")

    # --- 0. O que ha na prateleira -----------------------------------------
    n_linhas = con.execute("SELECT count(*) FROM ev").fetchone()[0]
    n_bytes = sum(a.stat().st_size for a in arqs)
    relatorio["destino"] = {"arquivos": len(arqs), "bytes": n_bytes, "linhas": n_linhas}
    print(f"destino ................. {pasta}")
    print(f"arquivos ................ {len(arqs)}   ({n_bytes / 1e6:.2f} MB, "
          f"{n_bytes / max(n_linhas, 1):.0f} bytes/evento)")

    # --- 1. Integridade: perdas e duplicatas por (gen_id, seq) -------------
    nota = manifestos(RUN_ID)
    if not nota:
        print(f"!! ATENCAO: sem manifesto do gerador em {METRICS}/{RUN_ID}.gen*.json — "
              "sem a nota fiscal, perda nao e mensuravel")

    print("\n--- 1. Integridade (por gen_id, contra os CONFIRMADOS pelo broker) ---")
    total_perdas = total_dup = 0
    integridade = []
    grupos = con.execute("""
        SELECT gen_id, count(*), count(DISTINCT seq), min(seq), max(seq)
        FROM ev WHERE gen_id IS NOT NULL AND seq IS NOT NULL
        GROUP BY gen_id ORDER BY gen_id
    """).fetchall()
    gens = {g[0] for g in grupos} | set(nota)

    for gen in sorted(gens):
        linha = next((g for g in grupos if g[0] == gen), (gen, 0, 0, None, None))
        _, gravados, distintos, menor, maior = linha
        dup = gravados - distintos
        m = nota.get(gen)
        confirmados = m["confirmados"] if m else None
        ultimo = m["ultimo_seq"] if m else maior

        # Lacunas: numeros esperados (0..ultimo_seq do manifesto) que nao estao
        # na prateleira. Se o gerador teve erro de entrega, parte delas pode ser
        # perda DO GERADOR — por isso a perda do pipeline e medida contra os
        # confirmados, e as lacunas sao listadas a parte.
        lacunas, exemplos = 0, []
        if ultimo is not None:
            lacunas = con.execute(f"""
                SELECT count(*) FROM range(0, {int(ultimo) + 1}) t(s)
                WHERE s NOT IN (SELECT seq FROM ev WHERE gen_id = {int(gen)} AND seq IS NOT NULL)
            """).fetchone()[0]
            if lacunas:
                exemplos = [r[0] for r in con.execute(f"""
                    SELECT s FROM range(0, {int(ultimo) + 1}) t(s)
                    WHERE s NOT IN (SELECT seq FROM ev WHERE gen_id = {int(gen)} AND seq IS NOT NULL)
                    ORDER BY s LIMIT 10
                """).fetchall()]
        fora = con.execute(f"""
            SELECT count(*) FROM ev
            WHERE gen_id = {int(gen)} AND (seq < 0 OR seq > {int(ultimo) if ultimo is not None else -1})
        """).fetchone()[0] if ultimo is not None else 0

        perdas = max(confirmados - distintos, 0) if confirmados is not None else None
        total_perdas += perdas or 0
        total_dup += dup

        print(f"  gen_id {gen}")
        if m:
            print(f"    tentados / confirmados ....... {m['tentados']} / {confirmados}"
                  f"   (erros de entrega: {m['erros_entrega']})")
        print(f"    gravados no destino .......... {gravados}")
        print(f"    seq distintos ................ {distintos}   (de {menor} a {maior})")
        print(f"    PERDAS (confirmados - distintos) {perdas if perdas is not None else '?'}")
        print(f"    DUPLICATAS (gravados - distintos) {dup}")
        print(f"    lacunas no seq 0..{ultimo} ...... {lacunas}"
              + (f"   primeiras: {exemplos}" if exemplos else ""))
        if fora:
            print(f"    !! seq fora do intervalo do manifesto: {fora}")
        integridade.append({"gen_id": gen, "confirmados": confirmados, "gravados": gravados,
                            "distintos": distintos, "perdas": perdas, "duplicatas": dup,
                            "lacunas": lacunas, "fora_do_intervalo": fora})

    # Linhas sem gen_id ou seq nao entram na conta de seq acima; sao contadas aqui.
    ilegiveis = contar_ilegiveis(con, colunas, tem_particao_nula, total_dup, convertidos)
    invalidas, nulas, descartadas = (ilegiveis["invalidas"], ilegiveis["nulas"],
                                     ilegiveis["descartadas"])
    relatorio["integridade"] = {"por_gen": integridade, "perdas": total_perdas,
                                "duplicatas": total_dup, **ilegiveis}

    # --- 2. Latencia ---------------------------------------------------------
    print("\n--- 2. Latencia ---")
    logs = sorted(METRICS.glob(f"{MOTOR}.progresso.*.jsonl"))
    if "processed_ts" not in colunas:
        print("  indisponivel: o destino nao tem processed_ts")
    elif modo == "arquivo":
        relatorio.update(latencia_por_arquivo(con, logs))
    elif not logs:
        print(f"  indisponivel: sem log de progresso ({METRICS}/{MOTOR}.progresso.*.jsonl)")
    else:
        con.execute(f"""
            CREATE VIEW prog AS
            SELECT batch_id::BIGINT AS batch_id, processed_ts::BIGINT AS processed_ts,
                   inicio_ts::BIGINT AS inicio_ts, committed_ts::BIGINT AS committed_ts
            FROM read_json({lista_sql(logs)}, format = 'newline_delimited',
                           union_by_name = true)
        """)
        # Uma linha por evento: a PRIMEIRA gravacao dele. Uma duplicata e
        # contada na secao 1; aqui ela nao pode inflar os percentis.
        #
        # A ligacao com o log e o par (batch_id, processed_ts): o batch_id
        # sozinho repete quando o checkpoint e apagado ou um batch e refeito.
        con.execute("""
            CREATE VIEW lat AS
            WITH primeiro AS (
                SELECT gen_id, seq, event_ts, batch_id, processed_ts,
                       row_number() OVER (PARTITION BY gen_id, seq ORDER BY processed_ts) AS rn
                FROM ev WHERE gen_id IS NOT NULL AND seq IS NOT NULL
            )
            SELECT p.*, g.inicio_ts, g.committed_ts
            FROM primeiro p LEFT JOIN prog g USING (batch_id, processed_ts)
            WHERE rn = 1
        """)
        sem_log = con.execute(
            "SELECT count(*) FROM lat WHERE committed_ts IS NULL").fetchone()[0]
        n_batches = con.execute("SELECT count(DISTINCT processed_ts) FROM lat").fetchone()[0]
        metricas = {
            "espera no trigger": "inicio_ts - event_ts",
            "processamento": "processed_ts - inicio_ts",
            "escrita": "committed_ts - processed_ts",
            "LAT. PROCESSAMENTO": "processed_ts - event_ts",
            "LAT. VISIBILIDADE": "committed_ts - event_ts",
        }
        print(f"  batches ................ {n_batches}")
        if sem_log:
            print(f"  !! eventos sem linha no log de progresso: {sem_log} — "
                  "sem committed_ts, fora da latencia de visibilidade")
        # Aquecimento: o primeiro lote processa com a JVM fria. As parcelas sao
        # mostradas sem os primeiros segundos de eventos; as latencias totais,
        # com e sem. A mesma regra do Flink.
        regime, aquecimento = corte_do_aquecimento(con)
        print(f"\n  em regime (sem os primeiros {AQUECIMENTO_S:g} s de eventos):")
        lat = {}
        for nome, expr in metricas.items():
            s = resumo(con, expr, "lat", regime)
            lat[nome] = s
            print(linha_ms(nome, s))
        print("\n  execucao inteira (com o aquecimento):")
        for nome in ("LAT. PROCESSAMENTO", "LAT. VISIBILIDADE"):
            s = resumo(con, metricas[nome], "lat")
            lat[nome + " (com aquecimento)"] = s
            print(linha_ms(nome, s))
        relatorio["latencia_ms"] = lat
        relatorio["aquecimento"] = aquecimento
        relatorio["latencia_eventos_sem_log"] = sem_log

    # --- 2b. Throughput medio (Etapa 8) ---------------------------------------
    if "processed_ts" in colunas and (modo == "arquivo" or logs):
        relatorio["throughput_medio"] = throughput_medio(con, modo, relatorio.get("aquecimento"))

    # --- 3. Watermark e is_late (CONTRATO.md 2.1) ---------------------------
    print("\n--- 3. Atraso (watermark = max acumulado de event_ts - 60 s) ---")
    if "processed_ts" not in colunas:
        print("  indisponivel sem processed_ts (ordem de processamento)")
    else:
        # Por unidade de progresso, na ordem em que ficaram visiveis: o
        # watermark de uma unidade e o maior event_ts visto nas unidades
        # ANTERIORES, menos 60 s. Acumulado, nunca so da unidade corrente —
        # senao ele retrocederia e um evento deixaria de ser atrasado conforme a
        # unidade em que caiu. A unidade e o micro-batch no Spark e o
        # checkpoint (os arquivos visiveis juntos) no Flink: a mesma regra nos dois.
        atrasados, total = con.execute(f"""
            WITH por_unidade AS (
                SELECT unidade, max(event_ts) AS mx FROM ev GROUP BY unidade
            ), wm AS (
                SELECT unidade,
                       max(mx) OVER (ORDER BY unidade
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                       - {ATRASO_WATERMARK_MS} AS watermark_ts
                FROM por_unidade
            )
            SELECT count(*) FILTER (WHERE e.event_ts < w.watermark_ts), count(*)
            FROM ev e JOIN wm w USING (unidade)
        """).fetchone()
        print(f"  is_late ................ {atrasados} de {total}")
        relatorio["is_late"] = atrasados

    # --- Veredito ---------------------------------------------------------------
    # O que reprova: perda ou duplicata entre os eventos VALIDOS, ou a falta do
    # manifesto. Invalidas, nulas e descartadas sao INFORMACAO, nunca motivo de
    # reprovacao (decisao de 11/10/2026): 4 das 10 rodadas de cada combinacao
    # levam malformados de proposito, e reprovar por eles seria descartar
    # justamente o que se quer medir.
    ok = total_perdas == 0 and total_dup == 0 and bool(nota)
    relatorio["ok"] = ok
    METRICS.mkdir(parents=True, exist_ok=True)
    saida = METRICS / f"reconcile.{MOTOR}.{RUN_ID}.json"
    saida.write_text(json.dumps(relatorio, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n" + "=" * 68)
    print(f"{'OK' if ok else 'DIVERGENCIA'}: perdas={total_perdas}  duplicatas={total_dup}  "
          f"invalidas={invalidas}  nulas={nulas}  "
          f"descartadas={'?' if descartadas is None else descartadas}"
          + ("" if nota else "  (sem manifesto)"))
    print(f"relatorio: {saida}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
