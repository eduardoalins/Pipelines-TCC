"""
O schema do evento, na forma que a Table API do Flink entende.

Os campos NAO estao escritos aqui: vem de shared/schema/evento.json, a mesma
fonte unica que o pipeline-nrt/src/esquema.py le (decisao da Etapa RT-3). Assim
o schema do Flink e o do Spark nao tem como divergir.

OS ONZE CAMPOS SAO CARREGADOS COMO TEXTO (CONTRATO.md 2.0; onze desde o 3.0). O pipeline nao
interpreta tipo: `"user_id": 17` e `"user_id": "17"` chegam ao destino como o
texto 17; `"seq": "abc"` chega como abc. Quem converte para o tipo do contrato —
e decide o que fazer com o que nao converte — e o Transform, DEPOIS do Load. Os
tipos do evento.json continuam sendo o contrato do evento; este arquivo so os
confere, nao os aplica.

Por que: carregando tipado, o leitor de JSON do Flink convertia `"17"` em 17 e
truncava 1.5 para 1, e o Spark anulava os dois (medido em 11/10/2026). Como
texto, os dois gravam o MESMO para tudo o que o gerador produz (verificado por
sonda, 3000 eventos e os 8 tipos de malformado).

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

# Os tipos logicos que o contrato conhece. Nenhum e aplicado aqui — toda coluna
# e STRING —, mas um tipo desconhecido no evento.json FALHA, como no lado do
# Spark: quem converte depois do Load nao pode encontrar surpresa.
TIPOS_CONHECIDOS = ("string", "int32", "int64", "float64", "decimal(10,2)", "timestamp")

_schema = json.loads(ARQUIVO_SCHEMA.read_text(encoding="utf-8"))

# De qual campo saem as pastas dt= e hh=, e que texto e aceito como instante. A
# mesma expressao regular vale no Spark — ver o evento.json.
CAMPO_DE_PARTICAO = _schema["particionamento"]["campo"]
PARTICAO_VALIDA = _schema["particionamento"]["valido"]


def _campos() -> list[dict]:
    return _schema["campos"]


def nomes() -> list[str]:
    """Os nomes dos campos, na ordem do contrato."""
    return [campo["nome"] for campo in _campos()]


def colunas_ddl() -> str:
    """As colunas para dentro de um CREATE TABLE, uma por linha.

    Todas STRING e sem NOT NULL: anulaveis, como no Spark. Um campo ausente vira
    NULL.
    """
    linhas = []
    for campo in _campos():
        if campo["tipo"] not in TIPOS_CONHECIDOS:
            raise ValueError(
                f"tipo '{campo['tipo']}' do campo '{campo['nome']}' em {ARQUIVO_SCHEMA} "
                f"nao e conhecido. Tipos conhecidos: {sorted(TIPOS_CONHECIDOS)}"
            )
        linhas.append(f"`{campo['nome']}` STRING")
    return ",\n".join(linhas)
