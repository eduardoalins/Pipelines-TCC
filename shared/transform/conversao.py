"""
Conversao — do texto do destino ao tipo do contrato. O "T" do ELT, parte 1.

Os pipelines carregam os onze campos do evento como TEXTO (CONTRATO.md 2.7): nenhum
motor decide o que e um valor valido. Quem decide e este arquivo, DEPOIS do Load,
com uma regra so para os dois motores. Usam a regra:

  - shared/analysis/reconcile.py, para contar as invalidas de cada rodada;
  - o programa do Transform, para gravar a tabela tipada (a escrever).

Tudo vem de shared/schema/evento.json, a fonte unica do schema: os nomes, a ordem
e o tipo de cada campo. Nada de lista de campos escrita a mao aqui — foi assim que
o event_time, o 11o campo, ficou de fora da conferencia ate o contrato 3.1.

--- A regra (CONTRATO.md 2.7) --------------------------------------------------

    tipo do contrato   converte quando o texto e                   senao
    int32, int64       so digitos, com sinal opcional, e cabe       nulo
    float64            um numero, por extenso ou notacao cientifica nulo
    decimal(10,2)      ate 8 digitos inteiros e ate 2 casas         nulo
    timestamp          exatamente AAAA-MM-DD HH:MM:SS UTC, e a      nulo
                       data existe
    string             fica como veio                               —

O que nao converte fica NULO. Nada e consertado, arredondado, truncado ou
adivinhado. A forma do texto e conferida ANTES da conversao porque a conversao do
DuckDB e complacente: sozinha, ela transformaria '1.5' no inteiro 2 e '1.555' no
decimal 1.56. A conversao com TRY_ cuida do que tem a forma certa e nao cabe no
tipo (um inteiro grande demais, o dia 30 de fevereiro).

O timestamp sai sem fuso, e e UTC por contrato: a base so tem UTC.

--- Obrigatorios (decisao de 24/10/2026, CONTRATO.md 2.6) ---------------------
Os onze campos. Um evento da base sempre traz todos — inclusive o preco (o preco 0
e valor, nao falta) e o instante original. Ate o contrato 3.0 o amount ficava de
fora, porque o gerador sintetico o deixava nulo em parte dos eventos.
"""

import json
from pathlib import Path

ARQUIVO_SCHEMA = Path(__file__).resolve().parents[1] / "schema" / "evento.json"
_schema = json.loads(ARQUIVO_SCHEMA.read_text(encoding="utf-8"))

# Os campos do evento, na ordem do contrato.
CAMPOS = tuple(c["nome"] for c in _schema["campos"])
TIPOS = {c["nome"]: c["tipo"] for c in _schema["campos"]}

# Todos obrigatorios desde o contrato 3.1.
OBRIGATORIOS = CAMPOS

# O run_id e campo do evento, mas o destino o guarda no CAMINHO (a pasta
# run_id=), nao no arquivo. Lido com hive_partitioning, ele vira uma coluna
# preenchida pela pasta — entao nao diz nada sobre a linha, e quem confere
# linha a linha (nulas, invalidas) o deixa de fora e o confere pela pasta.
NO_CAMINHO = ("run_id",)

# tipo logico -> (a forma que o texto precisa ter, a conversao no DuckDB).
_INTEIRO = r"-?[0-9]+"
_NUMERO = r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?"
_DECIMAL_10_2 = r"-?[0-9]{1,8}(\.[0-9]{1,2})?"
_INSTANTE_UTC = r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} UTC"
CONVERSAO = {
    "int32": (_INTEIRO, "TRY_CAST({c} AS INTEGER)"),
    "int64": (_INTEIRO, "TRY_CAST({c} AS BIGINT)"),
    "float64": (_NUMERO, "TRY_CAST({c} AS DOUBLE)"),
    "decimal(10,2)": (_DECIMAL_10_2, "TRY_CAST({c} AS DECIMAL(10,2))"),
    "timestamp": (_INSTANTE_UTC, "TRY_STRPTIME({c}, '%Y-%m-%d %H:%M:%S UTC')"),
}

# Um tipo desconhecido FALHA ja na importacao, como nos dois esquema.py: a regra
# nunca e adivinhada.
for _nome, _tipo in TIPOS.items():
    if _tipo != "string" and _tipo not in CONVERSAO:
        raise ValueError(f"tipo '{_tipo}' do campo '{_nome}' em {ARQUIVO_SCHEMA} nao tem "
                         f"conversao. Tipos conhecidos: string, {', '.join(CONVERSAO)}")


def expressao(campo: str) -> str:
    """A expressao SQL que converte o texto de `campo` para o tipo do contrato."""
    forma, conversao = CONVERSAO[TIPOS[campo]]
    return f"CASE WHEN regexp_full_match({campo}, '{forma}') THEN {conversao.format(c=campo)} END"


def selecao_tipada(con, fonte: str) -> tuple[str, list[str]]:
    """O comeco de um SELECT que le `fonte` com os campos nos tipos do contrato.

    Devolve ("SELECT * REPLACE (...), <campo> AS <campo>__txt ...", convertidos).
    Quem chama completa com o FROM. Cada campo nao-texto que esta em TEXTO no
    destino e substituido pelo valor convertido, e o texto original fica ao
    lado, em <campo>__txt — e por ele que se conta o que nao converteu.

    Um campo que ja esta tipado no arquivo (destino gravado antes do contrato
    2.0) passa direto, e um que nao existe (o event_time antes do 3.0) e
    ignorado.

    processed_ts e timestamp desde o CONTRATO.md 1.5 (2.5); antes era inteiro
    (epoch ms). A analise trabalha em epoch ms, que e tambem como os logs de
    progresso o guardam — entao ele e convertido na entrada.
    """
    tipos = {c[0]: c[1].upper() for c in con.execute(f"DESCRIBE SELECT * FROM {fonte}").fetchall()}
    trocas, originais, convertidos = [], [], []
    if "TIMESTAMP" in tipos.get("processed_ts", ""):
        trocas.append("epoch_ms(processed_ts) AS processed_ts")
    for nome in CAMPOS:
        if TIPOS[nome] == "string" or tipos.get(nome) != "VARCHAR":
            continue                    # texto, ausente, ou ja tipado no arquivo
        trocas.append(f"{expressao(nome)} AS {nome}")
        originais.append(f"{nome} AS {nome}__txt")
        convertidos.append(nome)
    selecao = "SELECT *" + (f" REPLACE ({', '.join(trocas)})" if trocas else "")
    return selecao + "".join(f", {o}" for o in originais), convertidos
