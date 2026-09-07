#!/usr/bin/env bash
#
# run_local.sh — lanca o pipeline NRT no ambiente local (WSL + venv).
#
#     bash pipeline-nrt/run_local.sh
#
# Overrides sem editar arquivo, no mesmo idioma do gerador:
#
#     TRIGGER="30 seconds" bash pipeline-nrt/run_local.sh
#     STARTING_OFFSETS=latest bash pipeline-nrt/run_local.sh
#
# --- Por que existe um lancador, e nao so `python main.py` ----------------
#
# Duas configuracoes precisam existir ANTES da JVM do driver subir, e o codigo
# Python roda depois disso:
#
#   --driver-memory  quando o SparkSession.builder executa, a JVM ja subiu e
#                    sua memoria ja esta definida. Configurar
#                    spark.driver.memory ali e aceito sem erro e simplesmente
#                    ignorado: voce acharia que esta com 1g e estaria com o
#                    padrao. Nao ha aviso.
#
#   --packages       o conector do Kafka e baixado do Maven na inicializacao.
#
# O spark-submit e quem lanca a JVM, entao e por ele que os dois passam.
#
set -euo pipefail

PY="${PY:-$HOME/.venvs/tcc/bin/python}"
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$AQUI/src"

if [ ! -x "$PY" ]; then
  echo "ERRO: interpretador nao encontrado em $PY"
  echo "O venv vive dentro do WSL. Recrie com: bash scripts/setup_env.sh"
  exit 1
fi

# Os executores precisam do mesmo interpretador do driver. Sem isso o Spark
# usa o `python3` do sistema e a versao pode divergir da do venv.
export PYSPARK_PYTHON="$PY"

# Le do config.py em vez de repetir os valores aqui: um unico lugar de onde a
# configuracao sai (ver o cabecalho do config.py).
ler_config() {
  "$PY" -c "import sys; sys.path.insert(0, '$SRC'); import config; print(config.$1)"
}

PACKAGE="$(ler_config KAFKA_CONNECTOR)"
DRIVER_MEMORY="$(ler_config DRIVER_MEMORY)"
MASTER="$(ler_config MASTER)"

# O `pip install pyspark` costuma colocar o spark-submit no bin do venv, mas
# nem sempre. O fallback usa o que vem dentro do proprio pacote, que existe
# em qualquer instalacao.
SPARK_SUBMIT="$(dirname "$PY")/spark-submit"
if [ ! -x "$SPARK_SUBMIT" ]; then
  SPARK_HOME="$("$PY" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
fi

exec "$SPARK_SUBMIT" \
  --master "$MASTER" \
  --driver-memory "$DRIVER_MEMORY" \
  --packages "$PACKAGE" \
  "$SRC/main.py"
