"""Verifica a cadeia de compatibilidade do Flink inteira num comando so.

    Python -> PyFlink -> JVM -> conector Kafka (jar) -> broker -> topico

Espelho do scripts/check_chain.py, que fez o mesmo pelo Spark em 03/09/2026. E
este script que sustenta a decisao F1: as versoes do pipeline-streaming nao
foram escolhidas por suposicao, foram verificadas aqui.

Roda DENTRO do conteiner do Flink, nunca no venv do host:

    docker compose up -d
    bash scripts/create_topic.sh
    ~/.venvs/tcc/bin/python shared/data-generator/generator.py
    docker compose build flink
    docker compose run --rm flink python scripts/check_chain_flink.py

DIFERENCA DE METODO EM RELACAO AO check_chain.py, e ela e deliberada: aquele
conta as particoes ATRIBUIDAS ao Spark, com ou sem dado. Este conta as particoes
DE ONDE CHEGOU DADO. E uma verificacao mais forte — prova que o Flink leu de cada
uma das 24 —, mas exige o topico povoado. Por isso o gerador vem antes.
"""
import os
import sys

from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.version import __version__ as PYFLINK_VERSION

TOPIC = "ecommerce.events.v1"
PARTICOES_ESPERADAS = 24

# kafka:29092 e o listener INTERNAL: o conteiner esta na rede do Docker.
# Ver a assimetria declarada no docker-compose.yml.
BOOTSTRAP = os.getenv("BOOTSTRAP", "kafka:29092")

# Definido no Dockerfile, que e onde o jar e baixado e verificado. Lido daqui
# para que a versao do conector exista em UM lugar so.
CONECTOR = os.environ["FLINK_KAFKA_CONNECTOR_JAR"]


# Modo BATCH, e nao streaming: com 'scan.bounded.mode' a fonte tem fim, e o
# COUNT devolve um resultado final em vez de uma serie de atualizacoes. A
# verificacao precisa terminar sozinha, como o check_chain.py termina.
t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

# O equivalente do --packages do Spark, com uma diferenca que e dado do criterio
# D1: aqui o jar ja precisa estar no disco, e o caminho e declarado a mao.
t_env.get_config().set("pipeline.jars", f"file://{CONECTOR}")

jvm = get_gateway().jvm
print("=" * 62)
print("Python   :", sys.version.split()[0])
print("PyFlink  :", PYFLINK_VERSION)
print("Java     :", jvm.java.lang.System.getProperty("java.version"))
print("Conector :", os.path.basename(CONECTOR))
print("Broker   :", BOOTSTRAP)
print("=" * 62)

# `partition` e `offset` sao colunas de METADADO do conector: nao estao no
# JSON, vem do Kafka. VIRTUAL significa que sao so de leitura.
#
# 'format' = 'raw': o value entra como texto cru. Interpretar o JSON e trabalho
# da Etapa RT-2 em diante; aqui so importa que o dado chega.
t_env.execute_sql(f"""
    CREATE TABLE eventos (
        `value`     STRING,
        `partition` INT    METADATA VIRTUAL,
        `offset`    BIGINT METADATA VIRTUAL
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = '{TOPIC}',
        'properties.bootstrap.servers' = '{BOOTSTRAP}',
        'scan.startup.mode'            = 'earliest-offset',
        'scan.bounded.mode'            = 'latest-offset',
        'format'                       = 'raw'
    )
""")

resultado = t_env.execute_sql(
    "SELECT COUNT(*) AS mensagens, COUNT(DISTINCT `partition`) AS particoes "
    "FROM eventos"
)
with resultado.collect() as linhas:
    linha = next(linhas)
mensagens, particoes = linha[0], linha[1]

print(f"\nmensagens no topico      : {mensagens}")
print(f"particoes com dado lido  : {particoes}")

if mensagens > 0:
    print("\namostra:")
    t_env.execute_sql(
        "SELECT `partition`, `offset`, `value` FROM eventos LIMIT 5"
    ).print()

if mensagens == 0:
    print(
        "\nFALHOU: o topico esta vazio. Este verificador conta as particoes de "
        "onde chegou dado,\nentao precisa do topico povoado. Rode o gerador "
        "antes:\n"
        "    ~/.venvs/tcc/bin/python shared/data-generator/generator.py"
    )
    sys.exit(1)

# Menos de 24 com o topico povoado tem duas causas possiveis: topico criado com
# o numero errado de particoes, ou carga pequena demais para cobrir todas. Com
# os 6000 eventos do gerador cada particao recebe entre ~186 e ~317, entao a
# segunda so aparece se o gerador rodou com uma carga muito menor.
if particoes != PARTICOES_ESPERADAS:
    print(
        f"\nFALHOU: o Flink leu dado de {particoes} particoes, esperado "
        f"{PARTICOES_ESPERADAS}.\n"
        f"Confira o topico com scripts/create_topic.sh e a carga do gerador."
    )
    sys.exit(1)

print("\nCADEIA COMPLETA OK")
