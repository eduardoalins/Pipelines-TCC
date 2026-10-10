#!/usr/bin/env bash
#
# submit.sh — roda DENTRO do conteiner do Spark. Nao chamar direto do host:
#
#     bash pipeline-nrt/run_local.sh
#
# E a metade do lancador que roda no conteiner: le master e memoria do config.py
# (um unico lugar de onde a configuracao sai) e entrega ao spark-submit — que e
# quem lanca a JVM, entao e por ele que --driver-memory precisa passar (ver o
# cabecalho do run_local.sh).
#
# O conector: ate 10/10/2026, no host, o spark-submit recebia --packages e
# resolvia os jars do Maven. Aqui eles ja estao na imagem, resolvidos no build
# (pipeline-nrt/Dockerfile), e entram por --jars. Nenhuma execucao toca na
# internet.
#
set -euo pipefail

SRC=/app/pipeline-nrt/src

ler_config() {
  python -c "import sys; sys.path.insert(0, '$SRC'); import config; print(config.$1)"
}

JARS="$(ls "${SPARK_EXTRA_JARS_DIR}"/*.jar | paste -sd, -)"
if [ -z "$JARS" ]; then
  echo "ERRO: nenhum jar em ${SPARK_EXTRA_JARS_DIR}. Reconstrua: docker compose build spark"
  exit 1
fi

exec spark-submit \
  --master "$(ler_config MASTER)" \
  --driver-memory "$(ler_config DRIVER_MEMORY)" \
  --jars "$JARS" \
  "$SRC/main.py"
