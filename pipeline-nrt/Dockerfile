# Ambiente de execucao do pipeline NRT (Apache Spark) — conteiner desde 10/10/2026.
#
# Espelho do pipeline-streaming/Dockerfile. A imagem contem o RUNTIME (JVM,
# Python, PySpark, conector); o codigo e montado do repositorio em tempo de
# execucao pelo docker-compose.yml.
#
# Construir:   docker compose build spark
#
# --- Por que conteiner ----------------------------------------------------
# Ate a Etapa 6 o Spark rodava no host, pelo venv, e so o Flink em conteiner. A
# diferenca era aceita porque a fase local nao produzia os numeros comparados.
# Em outubro de 2026 as metricas LOCAIS passaram a entrar no trabalho, e os dois
# motores precisam rodar no mesmo tipo de ambiente: mesma imagem base, mesmo
# caminho ate o Kafka (kafka:29092), mesma forma de gravar em ~/tcc-data.

# --- Imagem base --------------------------------------------------------
# EXATAMENTE a mesma do Flink, travada pelo mesmo digest. Os dois motores passam
# a rodar sobre a mesma JVM (Temurin 17.0.20+8, o mesmo build) e o mesmo sistema
# (Ubuntu 22.04). So Java, e nao a imagem oficial apache/spark: a oficial ja traz
# um Spark, e o `pip install pyspark` traz OUTRO — a mesma armadilha de duas
# instalacoes evitada no Flink.
#
# Consequencia declarada: o Python do Spark passa de 3.12 (WSL) para 3.10
# (jammy). O Flink 1.20 nao roda em 3.12, entao quem pode se mover e o Spark.
FROM eclipse-temurin:17.0.20_8-jre-jammy@sha256:e85989f3e4d136b3d7dde921e157fddb9c7016805a225c1ec483326b825b3ca5

# --- Python ---------------------------------------------------------------
# procps: os scripts do Spark chamam `ps`; sem ele cada execucao emite um aviso
# "ps: command not found".
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/bin/python3 /usr/bin/python

# --- PySpark ------------------------------------------------------------
# requirements.txt diz O QUE instalar; requirements.lock diz EM QUAL VERSAO,
# inclusive das dependencias transitivas.
COPY requirements.txt requirements.lock /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt -c /tmp/requirements.lock

# --- Conector Kafka -----------------------------------------------------
# No host, o Spark resolvia o conector do Maven na primeira execucao e guardava
# em ~/.ivy2.5.2. Aqui a resolucao acontece UMA vez, na construcao da imagem,
# pelo mesmo mecanismo (--packages): a imagem fica autocontida e o job roda sem
# internet. Sao 11 jars — o conector e suas dependencias transitivas, entre elas
# o kafka-clients e o Hadoop.
#
# Em tempo de execucao o job NAO usa --packages: recebe os jars desta pasta por
# --jars. Assim nenhuma execucao depende do Maven nem de escrever no cache.
#
# O sufixo de Scala precisa casar com a versao do Spark (4.x usa 2.13). A
# primeira linha do RUN FALHA o build se a versao do pyspark instalado nao for a
# declarada aqui — os dois numeros nao podem ficar fora de sincronia.
ARG SPARK_VERSION=4.0.4
ARG SCALA_BINARY=2.13

ENV SPARK_IVY_DIR=/opt/spark-ivy \
    SPARK_EXTRA_JARS_DIR=/opt/spark-ivy/jars \
    PYSPARK_PYTHON=python3

RUN set -e \
 && test "$(python3 -c 'import pyspark; print(pyspark.__version__)')" = "${SPARK_VERSION}" \
 && printf '' > /tmp/noop.py \
 && spark-submit --master 'local[1]' \
      --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_BINARY}:${SPARK_VERSION}" \
      --conf "spark.jars.ivy=${SPARK_IVY_DIR}" \
      /tmp/noop.py \
 && echo "jars do conector: $(ls "${SPARK_EXTRA_JARS_DIR}"/*.jar | wc -l)" \
 && (cd "${SPARK_EXTRA_JARS_DIR}" && sha256sum *.jar > "${SPARK_IVY_DIR}/jars.sha256") \
 && rm -rf /tmp/noop.py /tmp/spark-* /tmp/blockmgr-*

# --- Usuario 1000 ---------------------------------------------------------
# Mesmo motivo do lado do Flink: o conteiner roda como uid 1000, o usuario do
# WSL, para os arquivos em ~/tcc-data sairem com o dono certo. O Linux so
# conhece usuarios por NUMERO, e o Hadoop — que o Spark usa para gravar — falha
# com um uid sem nome ("invalid null input: name").
RUN groupadd --gid 1000 spark \
 && useradd --uid 1000 --gid 1000 --no-create-home --home-dir /tmp \
      --shell /usr/sbin/nologin spark

# --- Codigo -------------------------------------------------------------
# Nada e copiado: o docker-compose.yml monta o repositorio em /app.
WORKDIR /app
