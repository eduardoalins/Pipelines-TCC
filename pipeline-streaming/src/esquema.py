"""
O schema do evento, na forma que a Table API do Flink entende.

Os campos NAO estao escritos aqui: vem de shared/schema/evento.json, a mesma
fonte unica que o pipeline-nrt/src/esquema.py le (decisao da Etapa RT-3). Este
arquivo so traduz os tipos logicos de la para os tipos SQL do Flink. Assim o
schema do Flink e o do Spark nao tem como divergir.

Onde o Spark monta um StructType, o Flink recebe as colunas como texto, dentro
do CREATE TABLE — dai colunas_ddl() devolver uma string.

Declarado e nao inferido, pelo mesmo motivo do lado do Spark: o schema e
invariante de comparabilidade (CONTRATO.md secao 5), e inferido ele dependeria
da amostra.
"""

import json
from pathlib import Path

# src/ -> pipeline-streaming/ -> raiz do repositorio (montada em /app no
# conteiner, entao o caminho funciona dentro e fora dele)
ARQUIVO_SCHEMA = Path(__file__).resolve().parents[2] / "shared" / "schema" / "evento.json"

# Tipos logicos do evento.json -> tipos SQL do Flink. Equivalencia com o Spark:
#
#   string  -> STRING   (Spark: StringType)
#   int32   -> INT      (Spark: IntegerType)
#   int64   -> BIGINT   (Spark: LongType)
#   float64 -> DOUBLE   (Spark: DoubleType)
TIPOS = {
    "string": "STRING",
    "int32": "INT",
    "int64": "BIGINT",
    "float64": "DOUBLE",
}


def _campos() -> list[dict]:
    return json.loads(ARQUIVO_SCHEMA.read_text(encoding="utf-8"))["campos"]


def nomes() -> list[str]:
    """Os nomes dos campos, na ordem do contrato."""
    return [campo["nome"] for campo in _campos()]


def colunas_ddl() -> str:
    """As colunas para dentro de um CREATE TABLE, uma por linha.

    Sem NOT NULL: todas anulaveis, como no Spark. Um campo malformado vira NULL
    em vez de derrubar o job.
    """
    linhas = []
    for campo in _campos():
        tipo = campo["tipo"]
        # Tipo desconhecido FALHA em vez de virar um padrao qualquer: um palpite
        # aqui produziria um schema diferente do Spark, em silencio.
        if tipo not in TIPOS:
            raise ValueError(
                f"tipo '{tipo}' do campo '{campo['nome']}' em {ARQUIVO_SCHEMA} "
                f"nao tem traducao para o Flink. Tipos conhecidos: {sorted(TIPOS)}"
            )
        linhas.append(f"`{campo['nome']}` {TIPOS[tipo]}")
    return ",\n".join(linhas)
