"""
Transform — o "T" do ELT: do destino bruto a tabela tipada (decisoes de 24/10/2026).

    MOTOR=spark RUN_ID=<run_id> ~/.venvs/tcc/bin/python shared/transform/transform.py
    MOTOR=flink RUN_ID=<run_id> ~/.venvs/tcc/bin/python shared/transform/transform.py

Os pipelines so carregam: gravam os onze campos do evento como TEXTO, exatamente
como vieram (ELT estrito, CONTRATO.md 2.6 e 2.7). Este programa roda DEPOIS, fora
da medicao, e le o destino de uma rodada:

    ~/tcc-data/out/<motor>/events/run_id=<run_id>/     o destino bruto, intocado

e grava ao lado uma camada nova, a tabela tipada:

    ~/tcc-data/transformado/<motor>/run_id=<run_id>/eventos.parquet

com os onze campos do contrato nos tipos do contrato — o amount em decimal exato
de duas casas, o event_time em data e hora UTC —, pela regra de
shared/transform/conversao.py, a mesma que o reconcile.py usa para contar as
invalidas; e SEM DUPLICATAS.

--- As duplicatas (decisao de 24/10/2026) ----------------------------------------
Saem em duas etapas, nesta ordem, cada uma contada a parte:

  1. TECNICA    o mesmo event_id mais de uma vez: a mesma linha da base gravada
                de novo pelo motor (o Spark entrega "pelo menos uma vez" depois
                de uma falha). Fica a copia de menor seq.
  2. DA ORIGEM  os seis campos que vem da loja identicos (event_time, event_type,
                product_id, user_id, amount, session_id): a base tem linhas
                repetidas, que chegam como eventos diferentes, com event_id
                diferente. Fica a de menor event_id — a primeira na base.

Uma linha incompleta (um dos seis campos nulo, porque nao veio ou nao converteu)
nao entra na etapa 2, e uma sem event_id nao entra na etapa 1: dois nulos nao
provam que duas linhas sao iguais. A repetida da origem so e achada se as duas
copias cairam na mesma rodada.

A etapa 2 compara os seis campos que o evento leva. A contagem de 23/10/2026
(30.220 linhas repetidas) comparava as nove colunas da base, inclusive marca e
categoria, que nao viajam.

--- O que ele NAO faz ----------------------------------------------------------
- Nao mexe no destino bruto: a tabela tipada e outra camada, ao lado. Toda linha
  removida continua la.
- Nao conserta: um valor que nao converte fica NULO, e a linha fica.
- Nao leva as colunas de medicao (processed_ts, batch_id): sao instrumentos do
  benchmark, diferentes em cada motor, e ficam no destino bruto. So os onze campos
  — por isso a mesma rodada, transformada a partir do Spark e do Flink, tem de dar
  tabelas identicas.
- Nao le a particao nula: uma linha sem run_id nao diz de que rodada e. O
  reconcile.py a conta.
- Nao e chamado pelo run_round.py: roda-se a mao, depois da rodada.

A tabela sai ordenada por (gen_id, seq), para dois Transforms da mesma rodada
darem o mesmo arquivo. Rodar de novo sobrescreve.

Sai com 0 quando grava a tabela; 2 quando nao ha o que ler.
"""

import os
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conversao import CAMPOS, TIPOS, selecao_tipada  # noqa: E402

DATA_DIR = Path(os.getenv("DATA_DIR", Path.home() / "tcc-data"))
MOTOR = os.getenv("MOTOR", "spark")
RUN_ID = os.getenv("RUN_ID", "")
EVENTOS = DATA_DIR / "out" / MOTOR / "events"
SAIDA = DATA_DIR / "transformado" / MOTOR / f"run_id={RUN_ID}"

# Os campos que vem da loja (CONTRATO.md 1.5): os que dizem se duas linhas sao o
# mesmo evento repetido na origem.
DA_LOJA = ("event_time", "event_type", "product_id", "user_id", "amount", "session_id")


def arquivos_de_dados(pasta: Path) -> list[Path]:
    """Os arquivos de dados sob `pasta`, pela pasta e nao pela extensao.

    A mesma convencao do reconcile.py: o que comeca com "." ou "_" nao e dado
    (arquivos em andamento do Flink, .crc e _SUCCESS do Spark, _temporary).
    """
    oculto = lambda nome: nome.startswith((".", "_"))
    achados = []
    for raiz, dirs, arqs in os.walk(pasta):
        dirs[:] = [d for d in dirs if not oculto(d)]
        achados += [Path(raiz) / a for a in arqs if not oculto(a)]
    return sorted(achados)


def main() -> int:
    if not RUN_ID:
        print("defina RUN_ID (e MOTOR, padrao spark). Ver o cabecalho deste arquivo.",
              file=sys.stderr)
        return 2
    pasta = EVENTOS / f"run_id={RUN_ID}"
    arqs = arquivos_de_dados(pasta)
    if not arqs:
        print(f"ERRO: nenhum arquivo de dados em {pasta}", file=sys.stderr)
        return 2

    con = duckdb.connect()
    lista = "[" + ", ".join("'" + str(a).replace("'", "''") + "'" for a in arqs) + "]"
    con.execute(f"CREATE VIEW bruto AS SELECT * FROM read_parquet({lista}, hive_partitioning = true)")
    colunas = {c[0] for c in con.execute("DESCRIBE bruto").fetchall()}
    faltam = [c for c in CAMPOS if c not in colunas]
    if faltam:
        print(f"ERRO: o destino nao tem {faltam}: e anterior ao contrato 3.0?", file=sys.stderr)
        return 2

    selecao, convertidos = selecao_tipada(con, "bruto")
    con.execute(f"CREATE VIEW tipado AS {selecao} FROM bruto")

    print(f"=== transform — motor {MOTOR}, run_id {RUN_ID} ===\n")
    print(f"destino bruto ..... {pasta}   ({len(arqs)} arquivos)")
    linhas = con.execute("SELECT count(*) FROM tipado").fetchone()[0]
    print(f"linhas ............ {linhas}")

    # O que ficou nulo, por campo: o que nao veio, e o texto que nao converteu.
    print("\ncampo         tipo            nulos   (nao vieram / texto que nao converte)")
    nao_convertem = 0
    for c in CAMPOS:
        if c in convertidos:
            nulos, texto = con.execute(f"SELECT count(*) FILTER (WHERE {c} IS NULL), "
                                       f"count(*) FILTER (WHERE {c} IS NULL AND {c}__txt IS NOT NULL) "
                                       "FROM tipado").fetchone()
        else:
            nulos, texto = con.execute(f"SELECT count(*) FILTER (WHERE {c} IS NULL), 0 "
                                       "FROM tipado").fetchone()
        nao_convertem += texto
        print(f"{c:<13} {TIPOS[c]:<15} {nulos:>6}   ({nulos - texto} / {texto})")

    # As duplicatas, em duas etapas (ver o cabecalho).
    con.execute("""
        CREATE TABLE sem_tecnicas AS SELECT * FROM tipado
        QUALIFY event_id IS NULL
             OR row_number() OVER (PARTITION BY event_id ORDER BY seq NULLS LAST) = 1
    """)
    completa = " AND ".join(f"{c} IS NOT NULL" for c in DA_LOJA)
    con.execute(f"""
        CREATE TABLE limpa AS SELECT * FROM sem_tecnicas
        QUALIFY NOT ({completa})
             OR row_number() OVER (PARTITION BY {', '.join(DA_LOJA)} ORDER BY event_id) = 1
    """)
    depois_1 = con.execute("SELECT count(*) FROM sem_tecnicas").fetchone()[0]
    depois_2 = con.execute("SELECT count(*) FROM limpa").fetchone()[0]
    print(f"\nduplicatas tecnicas removidas ...... {linhas - depois_1}   (mesmo event_id)")
    print(f"repetidas da origem removidas ...... {depois_1 - depois_2}   (os seis campos da loja iguais)")

    SAIDA.mkdir(parents=True, exist_ok=True)
    destino = SAIDA / "eventos.parquet"
    parcial = SAIDA / ".eventos.parquet.parcial"
    con.execute(f"""
        COPY (SELECT {', '.join(CAMPOS)} FROM limpa ORDER BY gen_id NULLS LAST, seq NULLS LAST,
                     event_id NULLS LAST)
        TO '{parcial}' (FORMAT parquet, COMPRESSION snappy)
    """)
    parcial.replace(destino)       # quem le nunca ve um arquivo pela metade

    print(f"\ntabela tipada ..... {destino}   ({destino.stat().st_size / 1e6:.2f} MB)")
    for nome, tipo, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{destino}')").fetchall():
        print(f"    {nome:<13} {tipo}")
    print(f"\n{depois_2} linhas gravadas, de {linhas} lidas; {nao_convertem} valores nao converteram"
          + ("" if not nao_convertem else " (ficaram nulos, ver acima)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
