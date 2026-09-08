"""
O schema do evento, transcrito do CONTRATO.md secao 1.1.

Este arquivo e a fronteira entre o contrato e o codigo. Ate a Etapa 4 o `value`
vindo do Kafka era tratado como texto; a partir daqui ele e interpretado, e para
isso o schema precisa estar declarado.

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

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Tres destes campos nao tem relacao com e-commerce e existem apenas para a
# medicao (CONTRATO.md secao 1.1):
#
#   seq     sequencial monotonico por gerador. Sem ele so e possivel contar
#           totais, o que esconde perda compensada por duplicacao. Buracos na
#           sequencia sao perda; repeticoes sao duplicata.
#   gen_id  identifica o processo gerador, para varias instancias produzirem em
#           paralelo sem colidir a sequencia.
#   run_id  etiqueta a execucao, e e por ele que o destino e particionado.
#
# event_ts e epoch em MILISSEGUNDOS, nao texto ISO: havera aritmetica temporal
# sobre milhoes de linhas, e o parse de string viraria gargalo do proprio
# instrumento de medicao.
EVENTO = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("seq", LongType(), True),
        StructField("gen_id", IntegerType(), True),
        StructField("run_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        # Nulo para pageview e add_to_cart, por definicao do contrato.
        StructField("amount", DoubleType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_ts", LongType(), True),
    ]
)
