#!/usr/bin/env bash
#
# run_local.sh — lanca o pipeline Streaming (Flink) no ambiente local.
#
#     bash pipeline-streaming/run_local.sh
#
# Espelho do pipeline-nrt/run_local.sh, com uma diferenca de natureza: o do
# Spark chama o spark-submit no host; este chama o conteiner do Flink. O job
# roda DENTRO dele (decisao F3, planoRT.md secao 0.5).
#
# Overrides sem editar arquivo, no mesmo idioma do lado do Spark:
#
#     STARTUP_MODE=latest-offset bash pipeline-streaming/run_local.sh
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
ARGS=()
for var in BOOTSTRAP TOPIC STARTUP_MODE; do
  if [ -n "${!var:-}" ]; then
    ARGS+=(-e "$var=${!var}")
  fi
done

# `exec` substitui este shell pelo docker compose, para que o Ctrl+C chegue
# direto a ele — e dele ao processo Python dentro do conteiner.
exec docker compose run --rm "${ARGS[@]}" flink python pipeline-streaming/src/main.py
