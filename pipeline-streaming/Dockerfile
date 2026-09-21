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
# Tag especifica, nao a flutuante `17-jre-jammy`: a flutuante muda a cada
# atualizacao, e a decisao E7 trata versao como parte do metodo. A 17.0.20 e a
# MESMA versao da JVM que o Spark usa no host (OpenJDK 17.0.20) — os dois
# motores sobre a mesma JVM.
#
# jammy = Ubuntu 22.04, cujo Python do sistema e o 3.10. O PyFlink 1.20.5
# publica wheel para 3.8 a 3.11; o 3.10 esta dentro.
FROM eclipse-temurin:17.0.20_8-jre-jammy

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

# --- Codigo -------------------------------------------------------------
# Nada e copiado: o docker-compose.yml monta o repositorio em /app.
WORKDIR /app
