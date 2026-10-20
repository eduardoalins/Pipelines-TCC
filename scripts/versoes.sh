#!/usr/bin/env bash
#
# versoes.sh — o que esta dentro das duas imagens e no host, para comparar
# ambientes (o WSL de referencia e a maquina da AWS).
#
#     bash scripts/versoes.sh
#
# Por que existe: na AWS as imagens sao construidas de novo a cada sessao
# (decisao de 19/10/2026). A base e travada por digest e os jars por checksum,
# mas o python3 e o pip vem do apt do Ubuntu SEM versao fixa: uma imagem
# construida em outro dia pode trazer outra atualizacao. Este script mostra
# cada versao que entra na medicao; rodado nos dois ambientes, a diferenca
# aparece na comparacao, e nao como surpresa nos numeros.
#
# So le. Precisa do Docker no ar e das imagens construidas.
#
set -uo pipefail

SPARK_IMG="${SPARK_IMG:-tcc-spark:4.0.4}"
FLINK_IMG="${FLINK_IMG:-tcc-flink:1.20.5}"

say() { printf '\n\033[1m>> %s\033[0m\n' "$*"; }

say "Host"
echo "sistema ........ $(lsb_release -ds 2>/dev/null) / kernel $(uname -r)"
echo "nucleos ........ $(nproc)"
echo "docker ......... $(docker version --format '{{.Server.Version}}' 2>&1)"
echo "compose ........ $(docker compose version --short 2>&1)"
echo "python do venv . $("$HOME/.venvs/tcc/bin/python" -V 2>&1)"

say "Imagem do Spark ($SPARK_IMG)"
echo "id ............. $(docker image inspect --format '{{.Id}}' "$SPARK_IMG" 2>&1)"
docker run --rm -i --entrypoint sh "$SPARK_IMG" -s <<'DENTRO'
echo "python ......... $(python3 -V 2>&1)"
dpkg-query -W -f='apt ............ ${Package} ${Version}\n' python3 python3-pip
echo "java ........... $(java -version 2>&1 | head -1)"
echo "pyspark ........ $(python3 -c 'import pyspark; print(pyspark.__version__)' 2>&1)"
echo "jars do conector $(wc -l < /opt/spark-ivy/jars.sha256) jars; impressao da lista: $(sha256sum /opt/spark-ivy/jars.sha256 | cut -c1-16)"
DENTRO

say "Imagem do Flink ($FLINK_IMG)"
echo "id ............. $(docker image inspect --format '{{.Id}}' "$FLINK_IMG" 2>&1)"
docker run --rm -i --entrypoint sh "$FLINK_IMG" -s <<'DENTRO'
echo "python ......... $(python3 -V 2>&1)"
dpkg-query -W -f='apt ............ ${Package} ${Version}\n' python3 python3-pip
echo "java ........... $(java -version 2>&1 | head -1)"
echo "pyflink ........ $(python3 -c 'from pyflink.version import __version__ as v; print(v)' 2>&1)"
for jar in /opt/flink-connectors/*.jar; do
  echo "jar ............ $(sha1sum "$jar" | cut -c1-40)  $(basename "$jar")"
done
DENTRO
