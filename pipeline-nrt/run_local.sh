#!/usr/bin/env bash
#
# run_local.sh — lanca o pipeline NRT (Spark) no ambiente local.
#
#     bash pipeline-nrt/run_local.sh
#
# Espelho do pipeline-streaming/run_local.sh: chama o conteiner, e o job roda
# la dentro (pipeline-nrt/submit.sh).
#
# Ate 10/10/2026 este arquivo chamava o spark-submit direto no host, pelo venv.
# O Spark passou para conteiner quando as metricas locais entraram no trabalho
# (DecisaoEtapa6.2.md). Durante a verificacao da mudanca este lancador se chamou
# run_conteiner.sh.
#
# Overrides sem editar arquivo, no mesmo idioma do gerador:
#
#     TRIGGER="30 seconds" bash pipeline-nrt/run_local.sh
#     STARTING_OFFSETS=latest bash pipeline-nrt/run_local.sh
#
# Pre-requisitos: Kafka no ar (docker compose up -d) e a imagem construida
# (docker compose build spark).
#
# --- Por que existe um lancador, e nao so `python main.py` ----------------
#
# Duas coisas precisam existir ANTES da JVM do driver subir, e o codigo Python
# roda depois disso:
#
#   --driver-memory  quando o SparkSession.builder executa, a JVM ja subiu e
#                    sua memoria ja esta definida. Configurar
#                    spark.driver.memory ali e aceito sem erro e simplesmente
#                    ignorado: voce acharia que esta com 1g e estaria com o
#                    padrao. Nao ha aviso.
#
#   o conector       os jars do Kafka precisam estar no classpath da JVM. No
#                    host eles vinham do Maven por --packages, na primeira
#                    execucao; na imagem ja estao resolvidos e entram por --jars.
#
# O spark-submit e quem lanca a JVM, entao e por ele que os dois passam — e
# quem o chama, dentro do conteiner, e o submit.sh.
#
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RAIZ"

# Variaveis do host nao entram no conteiner sozinhas. Repassa apenas as que o
# config.py conhece, e apenas se estiverem definidas.
#
# BASE, CHECKPOINT, DATA_DIR e BOOTSTRAP ficam DE FORA de proposito: dentro do
# conteiner eles tem valores proprios (/tcc-data/..., kafka:29092), definidos no
# docker-compose.yml. Repassar os do host apontaria o job para caminhos que nao
# existem la dentro.
#
# STOP_FILE e o caminho de DENTRO do conteiner (/tcc-data/...): a pasta
# ~/tcc-data do host e montada la com esse nome.
ARGS=()
for var in TOPIC STARTING_OFFSETS TRIGGER DRIVER_MEMORY MASTER PARQUET_CODEC GROUP_ID \
           STOP_FILE INACTIVITY_TIMEOUT; do
  if [ -n "${!var:-}" ]; then
    ARGS+=(-e "$var=${!var}")
  fi
done

# -p 4040:4040 publica a Spark UI no host, como quando o Spark rodava nele:
# http://localhost:4040, aba Structured Streaming.
#
# `exec` substitui este shell pelo docker compose, para que o Ctrl+C chegue
# direto a ele — e dele ao processo dentro do conteiner.
exec docker compose run --rm -p 4040:4040 "${ARGS[@]}" spark bash pipeline-nrt/submit.sh
