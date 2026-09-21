"""
O schema do evento, na forma que o Spark entende.

Os campos NAO estao escritos aqui: vem de shared/schema/evento.json, a fonte
unica do schema, que o pipeline Flink tambem le (decisao da Etapa RT-3). Este
arquivo so traduz os tipos logicos de la para os tipos do Spark. Assim o schema
do Spark e o do Flink nao tem como divergir — uma mudanca so pode ser feita num
lugar, e chega aos dois.

Ate a Etapa RT-3 os campos eram escritos a mao aqui. A troca nao mudou o schema
produzido: verificado comparando o simpleString() antes e depois.

POR QUE DECLARADO E NAO INFERIDO. O Spark consegue inferir schema de JSON, mas
so lendo os dados antes — o que uma consulta de streaming nao pode fazer, porque
os dados ainda nao existem quando o plano e montado. Mesmo se pudesse, inferir
seria errado aqui: o schema e uma INVARIANTE DE COMPARABILIDADE (CONTRATO.md
secao 5). Inferido, ele passaria a depender do que apareceu na amostra — um lote
sem nenhum `purchase` faria o `amount` ser inferido como nulo, e os dois motores
poderiam inferir coisas diferentes a partir da mesma entrada.

Campos malformados viram NULL em vez de derrubar a consulta. A contagem de
descartes entra na Etapa 7, junto com a limpeza.
"""

import json
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# src/ -> pipeline-nrt/ -> raiz do repositorio
ARQUIVO_SCHEMA = Path(__file__).resolve().parents[2] / "shared" / "schema" / "evento.json"

# Tipos logicos do evento.json -> tipos do Spark. O lado do Flink tem a tabela
# equivalente no pipeline-streaming/src/esquema.py.
TIPOS = {
    "string": StringType(),
    "int32": IntegerType(),
    "int64": LongType(),
    "float64": DoubleType(),
}


def _montar() -> StructType:
    campos = json.loads(ARQUIVO_SCHEMA.read_text(encoding="utf-8"))["campos"]
    estrutura = []
    for campo in campos:
        tipo = campo["tipo"]
        # Tipo desconhecido FALHA em vez de virar um padrao qualquer: um palpite
        # aqui produziria um schema diferente do Flink, em silencio.
        if tipo not in TIPOS:
            raise ValueError(
                f"tipo '{tipo}' do campo '{campo['nome']}' em {ARQUIVO_SCHEMA} "
                f"nao tem traducao para o Spark. Tipos conhecidos: {sorted(TIPOS)}"
            )
        # Todos anulaveis: um campo malformado vira NULL em vez de derrubar a
        # consulta. O Flink tambem declara todos anulaveis.
        estrutura.append(StructField(campo["nome"], TIPOS[tipo], True))
    return StructType(estrutura)


EVENTO = _montar()
