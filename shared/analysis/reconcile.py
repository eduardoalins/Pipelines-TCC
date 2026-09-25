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

--- Saida ------------------------------------------------------------------
Relatorio na tela e um JSON em ~/tcc-data/metrics/reconcile.<motor>.<run_id>.json,
para o arnes da Etapa 8. Sai com 0 se nao houver perda, duplicata nem linha
invalida; 1 caso contrario — para o arnes perceber sem ler a tela.
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


def linha_ms(nome: str, s: dict | None) -> str:
    if s is None:
        return f"  {nome:<26} indisponivel"
    return (f"  {nome:<26} P50 {s['p50']:>6}  P95 {s['p95']:>6}  P99 {s['p99']:>6}  "
            f"media {s['media']:>6}  min {s['min']:>6}  max {s['max']:>6}   (ms, n={s['n']})")


# --- Latencia no Flink (unidade de progresso = arquivo) ----------------------

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
    n_unidades, primeira = con.execute(
        "SELECT count(DISTINCT unidade), min(unidade) FROM lat").fetchone()

    # Aquecimento, pela mesma regra do Spark: descarta a primeira unidade de
    # progresso (o primeiro checkpoint com dados).
    regime = f"unidade > {primeira}"
    metricas = {
        "espera pelo checkpoint": "committed_ts - processed_ts",
        "LAT. PROCESSAMENTO": "processed_ts - event_ts",
        "LAT. VISIBILIDADE": "committed_ts - event_ts",
    }
    print(f"  checkpoints com dados .. {n_unidades}   (o 1o e descartado como aquecimento)")
    print("  committed_ts ........... ctime de cada arquivo (CONTRATO.md 1.6, 2.2)")
    print("\n  em regime (sem o 1o checkpoint):")
    lat = {}
    for nome, expr in metricas.items():
        s = resumo(con, expr, "lat", regime)
        lat[nome] = s
        print(linha_ms(nome, s))
    print("\n  execucao inteira (com o 1o checkpoint):")
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
        d_min, d_max, n = con.execute("""
            WITH u AS (SELECT DISTINCT unidade FROM lat),
            par AS (
                SELECT u.unidade, arg_min(u.unidade - ck.fim, abs(u.unidade - ck.fim)) AS d
                FROM u, ck GROUP BY u.unidade
            )
            SELECT min(d), max(d), count(*) FROM par
        """).fetchone()
        conferencia = {"ctime_menos_fim_checkpoint_ms": [d_min, d_max], "unidades": n}
        print(f"\n  conferencia ctime x log de checkpoints: {d_min}..{d_max} ms "
              f"em {n} checkpoints")
        if max(abs(d_min), abs(d_max)) > 1000:
            print("  !! desvio acima de 1 s: os arquivos foram tocados depois de gravados?")
    else:
        print("\n  (sem log de checkpoints para conferir o ctime)")

    return {"latencia_ms": lat, "conferencia_ctime": conferencia}


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

    # processed_ts e timestamp desde o CONTRATO.md 1.5 (2.5); antes era inteiro
    # (epoch ms). Tudo aqui trabalha em epoch ms, que e tambem como os logs de
    # progresso o guardam — entao o timestamp e convertido na entrada, e as
    # execucoes gravadas antes da 1.5 continuam legiveis.
    em_ms = "TIMESTAMP" in tipos.get("processed_ts", "").upper()
    selecao = ("SELECT * REPLACE (epoch_ms(processed_ts) AS processed_ts)"
               if em_ms else "SELECT *")
    pts_ms = "epoch_ms(processed_ts)" if em_ms else "processed_ts"

    # UNIDADE DE PROGRESSO — o que agrupa os eventos que ficaram visiveis juntos
    # (CONTRATO.md 1.6, 2.2-2.3):
    #   Spark  -> o micro-batch, identificado pelo processed_ts (um por batch);
    #             o committed_ts vem do log de progresso.
    #   Flink  -> o arquivo: sem batch_id, cada arquivo e finalizado por
    #             exatamente um checkpoint, e o committed_ts e o ctime dele — o
    #             instante em que deixou de ser oculto.
    modo = "batch" if "batch_id" in colunas else "arquivo"
    if modo == "arquivo":
        con.execute("CREATE TABLE arq_ctime (filename VARCHAR, committed_ts BIGINT)")
        con.executemany("INSERT INTO arq_ctime VALUES (?, ?)",
                        [(str(a), int(a.stat().st_ctime * 1000)) for a in arqs])
        con.execute(f"""
            CREATE VIEW ev AS
            {selecao}, committed_ts AS unidade
            FROM ev_bruto JOIN arq_ctime USING (filename)
        """)
    else:
        con.execute(f"CREATE VIEW ev AS {selecao}, {pts_ms} AS unidade FROM ev_bruto")
    relatorio: dict = {"motor": MOTOR, "run_id": RUN_ID, "unidade_de_progresso": modo}

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

    # Linhas sem gen_id ou seq: JSON malformado vira nulos no from_json do
    # Spark. Nao entram na conta de seq, mas sao contadas e reportadas.
    invalidas = con.execute(
        "SELECT count(*) FROM ev WHERE gen_id IS NULL OR seq IS NULL").fetchone()[0]

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

    print(f"  linhas invalidas (gen_id ou seq nulos) ... {invalidas}")
    relatorio["integridade"] = {"por_gen": integridade, "perdas": total_perdas,
                                "duplicatas": total_dup, "invalidas": invalidas}

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
        n_batches, primeiro_batch = con.execute(
            "SELECT count(DISTINCT processed_ts), min(processed_ts) FROM lat").fetchone()

        # Aquecimento (planoNRT3 §5.5): o primeiro batch da execucao processa
        # com a JVM fria e costuma estourar o trigger. As parcelas sao mostradas
        # sem ele; as latencias totais, com e sem.
        regime = f"processed_ts > {primeiro_batch}"
        metricas = {
            "espera no trigger": "inicio_ts - event_ts",
            "processamento": "processed_ts - inicio_ts",
            "escrita": "committed_ts - processed_ts",
            "LAT. PROCESSAMENTO": "processed_ts - event_ts",
            "LAT. VISIBILIDADE": "committed_ts - event_ts",
        }
        print(f"  batches ................ {n_batches}   (o 1o e descartado como aquecimento)")
        if sem_log:
            print(f"  !! eventos sem linha no log de progresso: {sem_log} — "
                  "sem committed_ts, fora da latencia de visibilidade")
        print("\n  em regime (sem o 1o batch):")
        lat = {}
        for nome, expr in metricas.items():
            s = resumo(con, expr, "lat", regime)
            lat[nome] = s
            print(linha_ms(nome, s))
        print("\n  execucao inteira (com o 1o batch):")
        for nome in ("LAT. PROCESSAMENTO", "LAT. VISIBILIDADE"):
            s = resumo(con, metricas[nome], "lat")
            lat[nome + " (com aquecimento)"] = s
            print(linha_ms(nome, s))
        relatorio["latencia_ms"] = lat
        relatorio["latencia_eventos_sem_log"] = sem_log

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
        # checkpoint (o ctime do arquivo) no Flink: a mesma regra nos dois.
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
    ok = total_perdas == 0 and total_dup == 0 and invalidas == 0 and bool(nota)
    relatorio["ok"] = ok
    METRICS.mkdir(parents=True, exist_ok=True)
    saida = METRICS / f"reconcile.{MOTOR}.{RUN_ID}.json"
    saida.write_text(json.dumps(relatorio, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n" + "=" * 68)
    print(f"{'OK' if ok else 'DIVERGENCIA'}: perdas={total_perdas}  duplicatas={total_dup}  "
          f"invalidas={invalidas}" + ("" if nota else "  (sem manifesto)"))
    print(f"relatorio: {saida}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
