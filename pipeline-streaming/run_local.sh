#!/usr/bin/env bash
#
# run_local.sh — lanca o pipeline Streaming (Flink) no ambiente local.
#
#     bash pipeline-streaming/run_local.sh
#
# Espelho do pipeline-nrt/run_local.sh: os dois chamam um conteiner, e o job
# roda DENTRO dele (decisao F3, planoRT.md secao 0.5). Ate 10/10/2026 o do Spark
# chamava o spark-submit no host.
#
# Overrides sem editar arquivo, no mesmo idioma do lado do Spark:
#
#     STARTUP_MODE=latest-offset bash pipeline-streaming/run_local.sh
#     CHECKPOINT_INTERVAL="30 s" bash pipeline-streaming/run_local.sh
#
# Pre-requisitos: Kafka no ar (docker compose up -d) e a imagem construida
# (docker compose build flink).
#
set -euo pipefail

# O docker-compose.yml esta na raiz do repositorio, e o `compose run` precisa
# ser chamado de la.
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RAIZ"

# Variaveis do host nao entram no conteiner sozinhas. Repassa apenas as que o
# config.py conhece, e apenas se estiverem definidas — o resto fica no padrao.
#
# STOP_FILE e o caminho de DENTRO do conteiner (/tcc-data/...): a pasta
# ~/tcc-data do host e montada la com esse nome.
ARGS=()
for var in BOOTSTRAP TOPIC STARTUP_MODE BASE CHECKPOINT CHECKPOINT_INTERVAL PARQUET_CODEC GROUP_ID \
           STOP_FILE INACTIVITY_TIMEOUT; do
  if [ -n "${!var:-}" ]; then
    ARGS+=(-e "$var=${!var}")
  fi
done

# `exec` substitui este shell pelo docker compose, para que o Ctrl+C chegue
# direto a ele — e dele ao processo Python dentro do conteiner.
exec docker compose run --rm "${ARGS[@]}" flink python pipeline-streaming/src/main.py
