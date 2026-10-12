"""
O schema do evento, na forma que o Spark entende.

Os campos NAO estao escritos aqui: vem de shared/schema/evento.json, a fonte
unica do schema, que o pipeline Flink tambem le (decisao da Etapa RT-3). Assim o
schema do Spark e o do Flink nao tem como divergir — uma mudanca so pode ser
feita num lugar, e chega aos dois.

OS DEZ CAMPOS SAO CARREGADOS COMO TEXTO (CONTRATO.md 2.0). O pipeline nao
interpreta tipo: `"user_id": 17` e `"user_id": "17"` chegam ao destino como o
texto 17; `"seq": "abc"` chega como abc. Quem converte para o tipo do contrato —
e decide o que fazer com o que nao converte — e o Transform, DEPOIS do Load. Os
tipos do evento.json continuam sendo o contrato do evento; este arquivo so os
confere, nao os aplica.

Por que: carregando tipado, cada motor decidia sozinho o que era valido, e os
dois decidiam diferente (medido em 11/10/2026: o Spark anulava `"17"`, o Flink
convertia; o Spark anulava 1.5, o Flink truncava). Como texto, os dois gravam o
MESMO para tudo o que o gerador produz (verificado por sonda, 3000 eventos e os
8 tipos de malformado).

POR QUE DECLARADO E NAO INFERIDO. O Spark consegue inferir schema de JSON, mas
so lendo os dados antes — o que uma consulta de streaming nao pode fazer. E o
schema e uma INVARIANTE DE COMPARABILIDADE (CONTRATO.md secao 5): inferido, ele
dependeria do que apareceu na amostra.

Um campo ausente vira NULL. Uma mensagem que nao e JSON vira uma linha com os dez
campos nulos — o gerador nao envia mensagens assim (CONTRATO.md 2.6).
"""

import json
from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

# src/ -> pipeline-nrt/ -> raiz do repositorio
ARQUIVO_SCHEMA = Path(__file__).resolve().parents[2] / "shared" / "schema" / "evento.json"

# Os tipos logicos que o contrato conhece. Nenhum e aplicado aqui — todo campo e
# carregado como texto —, mas um tipo desconhecido no evento.json FALHA, como no
# lado do Flink: quem converte depois do Load nao pode encontrar surpresa.
TIPOS_CONHECIDOS = ("string", "int32", "int64", "float64")

_schema = json.loads(ARQUIVO_SCHEMA.read_text(encoding="utf-8"))


def _montar() -> StructType:
    estrutura = []
    for campo in _schema["campos"]:
        if campo["tipo"] not in TIPOS_CONHECIDOS:
            raise ValueError(
                f"tipo '{campo['tipo']}' do campo '{campo['nome']}' em {ARQUIVO_SCHEMA} "
                f"nao e conhecido. Tipos conhecidos: {sorted(TIPOS_CONHECIDOS)}"
            )
        # Texto, e anulavel: o Flink declara igual.
        estrutura.append(StructField(campo["nome"], StringType(), True))
    return StructType(estrutura)


EVENTO = _montar()

# De qual campo saem as pastas dt= e hh=, e que texto e aceito como instante. A
# mesma expressao regular vale no Flink — ver o evento.json.
CAMPO_DE_PARTICAO = _schema["particionamento"]["campo"]
PARTICAO_VALIDA = _schema["particionamento"]["valido"]
