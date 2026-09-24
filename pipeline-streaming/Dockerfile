# Ambiente de execucao do pipeline Streaming (Apache Flink) — Etapa RT-1.
#
# A imagem e o equivalente, para o Flink, do venv do host para o Spark: ela
# contem o RUNTIME (JVM, Python, motor, conector), nao o codigo. O codigo e
# montado do repositorio em tempo de execucao pelo docker-compose.yml, assim como
# o Spark le o codigo direto do repo.
#
# Construir:   docker compose build flink
#
# --- Por que conteiner (decisao F3) --------------------------------------
# O Flink 1.20 (LTS) nao tem wheel para Python 3.12, e o venv do projeto e
# 3.12.3, com todo o NRT verificado sobre ele. O conteiner traz o proprio
# Python: o LTS fica possivel sem PPA de terceiros, sem segundo ambiente no host
# e sem refazer nenhuma verificacao. Ver planoRT.md secao 0.5.

# --- Imagem base --------------------------------------------------------
# So Java — e NAO a imagem oficial `flink`. A oficial ja traz uma distribuicao
# do Flink em /opt/flink, e o `pip install apache-flink` traz OUTRA, embutida no
# pacote Python. Duas instalacoes do mesmo motor na mesma imagem e armadilha:
# qual delas roda passa a depender de FLINK_HOME, sem aviso, e se as versoes
# divergirem o job roda sobre a errada. Aqui existe UM Flink, e ele chega do
# mesmo jeito que o Spark chega no host: dentro do pacote Python.
#
# Travada pelo DIGEST, nao so pela tag. A decisao E7 trata versao como parte do
# metodo, e tag nenhuma e imutavel: a `17.0.20_8-jre-jammy` foi REPUBLICADA em
# 18/09/2026 (atualizacao do Ubuntu por baixo, mesma JVM). Com a tag sozinha, um
# rebuild futuro traria outra imagem sem aviso. O digest abaixo e o da base que
# produziu as medicoes da RT-1 e da RT-3, tirado do historico do BuildKit
# (`docker buildx history inspect`) em 24/09/2026. A tag fica so para leitura.
#
# JVM: Temurin 17.0.20+8 (conferido com `java -version` dentro da imagem). O
# host do Spark roda o OpenJDK do Ubuntu, que o unattended-upgrades levou a
# 17.0.20.1 em 24/09/2026. Mesma versao de feature e mesmo nivel de atualizacao
# (17.0.20), builds de fornecedores diferentes, e um patch pontual a mais no
# host. Nao existe Temurin 17.0.20.1 publicada para alinhar. Ver
# PontosSensiveis.md §5.5.
#
# jammy = Ubuntu 22.04, cujo Python do sistema e o 3.10. O PyFlink 1.20.5
# publica wheel para 3.8 a 3.11; o 3.10 esta dentro.
FROM eclipse-temurin:17.0.20_8-jre-jammy@sha256:e85989f3e4d136b3d7dde921e157fddb9c7016805a225c1ec483326b825b3ca5

# --- Python ---------------------------------------------------------------
# curl entra so para baixar o conector, logo abaixo.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/bin/python3 /usr/bin/python

# O PyFlink invoca `python` em alguns caminhos internos; o link acima garante que
# ele encontra o mesmo interpretador que instalou os pacotes.

# --- PyFlink ------------------------------------------------------------
# requirements.txt diz O QUE instalar; requirements.lock diz EM QUAL VERSAO,
# inclusive das dependencias transitivas (numpy, pandas, pyarrow, apache-beam),
# que o requirements.txt sozinho deixaria o pip escolher a cada build. Ver o
# cabecalho do requirements.lock.
COPY requirements.txt requirements.lock /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt -c /tmp/requirements.lock

# --- Conector Kafka -----------------------------------------------------
# Diferenca de mecanismo em relacao ao Spark, e ela e um dado do criterio D1:
# o Spark resolve o conector do Maven sozinho, em tempo de execucao, via
# --packages. O Flink nao tem equivalente — o jar precisa ser obtido e o caminho
# declarado no job.
#
# Baixado AQUI, na construcao da imagem, e nao por um script no host: a imagem
# fica autocontida, e nao existe a armadilha de rodar o build sem ter baixado o
# jar antes. O principio da decisao E5 da Etapa 0 se mantem — a receita vai para
# o git, o jar nao.
#
# flink-SQL-connector-kafka, e nao flink-connector-kafka: o segundo espera o
# kafka-clients ja no classpath; o primeiro e um jar "gordo", com tudo embutido.
# Para a Table API, o gordo e o certo.
#
# O nome carrega as DUAS versoes: 3.4.0 e a do conector, 1.20 a do Flink alvo.
# Conferido no Maven Central em 21/09/2026 (5.602.516 bytes, publicado em
# 14/11/2024), e nao escrito de memoria.
#
# O SHA-1 e o que o Maven Central publica para este artefato (nao ha SHA-512).
# Serve ao que importa aqui: garantir que o arquivo baixado e o arquivo certo e
# que nao veio corrompido. Se o checksum nao bater, o build FALHA.
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG KAFKA_CONNECTOR_SHA1=266330f8f26fec4957339b5ff31aa67b81c31462

ENV FLINK_KAFKA_CONNECTOR_JAR=/opt/flink-connectors/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

RUN mkdir -p /opt/flink-connectors \
 && curl -fsSL -o "${FLINK_KAFKA_CONNECTOR_JAR}" \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
 && echo "${KAFKA_CONNECTOR_SHA1}  ${FLINK_KAFKA_CONNECTOR_JAR}" | sha1sum -c -

# --- Formato Parquet e Hadoop (Etapa RT-3) -------------------------------
# O PyFlink traz o conector de sistema de arquivos (flink-connector-files) e o
# formato JSON, mas NAO o formato Parquet — verificado listando pyflink/lib em
# 21/09/2026. Ele entra aqui, como o conector Kafka.
#
# E o flink-sql-parquet embute o parquet-hadoop, mas NAO o Hadoop em si, que o
# Parquet usa para configuracao e caminhos de arquivo — o POM confirma. O Flink
# espera o Hadoop ja no classpath; no PyFlink ele nao esta.
#
# Hadoop 3.4.1, e nao outra versao: e EXATAMENTE o que o Spark carrega, como
# dependencia transitiva do conector Kafka dele (hadoop-client-api e
# hadoop-client-runtime 3.4.1, visiveis no bloco do Ivy de toda execucao). Os
# dois motores gravam Parquet sobre o mesmo Hadoop.
#
# DADO PARA O CRITERIO D1: o Spark escreve Parquet sem nada a mais. O Flink
# precisa destes tres jars, obtidos e declarados a mao.
#
# Todos com o SHA-1 publicado no Maven Central, conferido em 21/09/2026.
ARG PARQUET_VERSION=1.20.5
ARG PARQUET_SHA1=2d9ae9e0a49808f33761425ed65271f834154a4e
ARG HADOOP_VERSION=3.4.1
ARG HADOOP_API_SHA1=7e4cfae7f5c85cffdbc21fdf749262fc78d4463a
ARG HADOOP_RUNTIME_SHA1=30ec07ceccb224b2ef17af34fbb593bff3e9e071

ENV FLINK_PARQUET_JAR=/opt/flink-connectors/flink-sql-parquet-${PARQUET_VERSION}.jar \
    HADOOP_API_JAR=/opt/flink-connectors/hadoop-client-api-${HADOOP_VERSION}.jar \
    HADOOP_RUNTIME_JAR=/opt/flink-connectors/hadoop-client-runtime-${HADOOP_VERSION}.jar

RUN set -e \
 && MAVEN=https://repo1.maven.org/maven2 \
 && curl -fsSL -o "${FLINK_PARQUET_JAR}" \
    "${MAVEN}/org/apache/flink/flink-sql-parquet/${PARQUET_VERSION}/flink-sql-parquet-${PARQUET_VERSION}.jar" \
 && curl -fsSL -o "${HADOOP_API_JAR}" \
    "${MAVEN}/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" \
 && curl -fsSL -o "${HADOOP_RUNTIME_JAR}" \
    "${MAVEN}/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" \
 && printf '%s  %s\n%s  %s\n%s  %s\n' \
      "${PARQUET_SHA1}" "${FLINK_PARQUET_JAR}" \
      "${HADOOP_API_SHA1}" "${HADOOP_API_JAR}" \
      "${HADOOP_RUNTIME_SHA1}" "${HADOOP_RUNTIME_JAR}" \
    | sha1sum -c -

# --- Usuario 1000 (Etapa RT-3) -------------------------------------------
# O docker-compose.yml roda o conteiner como uid 1000 — o mesmo do usuario do
# WSL —, para que os arquivos gravados em ~/tcc-data saiam com o dono certo no
# host. Mas o Linux so conhece usuarios por NUMERO; o NOME fica no /etc/passwd,
# e o /etc/passwd do conteiner e o da imagem, que nao tinha ninguem com uid 1000.
#
# Sem esta linha, a primeira execucao da RT-3 falhou com
#     KeyError: 'getpwuid(): uid not found: 1000'
# porque o PyFlink pergunta o nome do usuario atual para montar o nome do log.
# E o Hadoop tem uma falha conhecida pelo mesmo motivo ("invalid null input:
# name") — registrar o usuario resolve os dois de uma vez, em vez de contornar
# so o primeiro.
#
# Criado por ultimo de proposito: uma camada pequena no fim nao invalida o cache
# do pip install, que e a parte demorada do build.
RUN groupadd --gid 1000 flink \
 && useradd --uid 1000 --gid 1000 --no-create-home --home-dir /tmp \
      --shell /usr/sbin/nologin flink

# --- Codigo -------------------------------------------------------------
# Nada e copiado: o docker-compose.yml monta o repositorio em /app.
WORKDIR /app
