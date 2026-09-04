#!/usr/bin/env bash
#
# create_topic.sh — cria o topico do experimento.
#
# Rode depois de `docker compose up -d`:
#     bash scripts/create_topic.sh
#
# E idempotente: se o topico ja existe, nao faz nada e apenas mostra como
# ele esta.
#
set -euo pipefail

TOPIC="${TOPIC:-ecommerce.events.v1}"
PARTITIONS="${PARTITIONS:-24}"
REPLICATION="${REPLICATION:-1}"
BROKER="${BROKER:-localhost:9092}"

K="docker exec kafka /opt/kafka/bin"

say() { printf '\n\033[1m>> %s\033[0m\n' "$*"; }

# --- 1. O broker esta aceitando conexao? ----------------------------------
# "running" no docker ps significa que o processo subiu, nao que o Kafka
# responde. Perguntar ao proprio broker evita criar o topico cedo demais.
say "Esperando o broker responder em $BROKER"
for i in $(seq 1 30); do
  if $K/kafka-broker-api-versions.sh --bootstrap-server "$BROKER" >/dev/null 2>&1; then
    echo "broker pronto (tentativa $i)"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERRO: broker nao respondeu em 60s."
    echo "Veja o que houve com: docker compose logs kafka"
    exit 1
  fi
  sleep 2
done

# --- 2. Criar o topico ----------------------------------------------------
# 24 particoes, e este numero NAO muda depois.
#
# Particao e a unidade de paralelismo do Kafka: cada particao e lida por no
# maximo um consumidor do grupo por vez. Com 4 particoes, o quinto executor
# do Spark fica ocioso por mais que voce adicione. 24 permite varrer de 1 a
# 24 unidades de paralelismo no Experimento B sem nunca tocar no topico.
#
# Mudar este numero no meio dos experimentos invalida tudo que ja rodou:
# a distribuicao das chaves entre particoes muda, e as execucoes deixam de
# ser comparaveis entre si.
say "Criando $TOPIC com $PARTITIONS particoes"
$K/kafka-topics.sh \
  --bootstrap-server "$BROKER" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION" \
  --config min.insync.replicas=1 \
  --config compression.type=producer

# --- 3. Conferir ----------------------------------------------------------
say "Como o topico ficou"
$K/kafka-topics.sh --bootstrap-server "$BROKER" --describe --topic "$TOPIC"

REAL=$($K/kafka-topics.sh --bootstrap-server "$BROKER" --describe --topic "$TOPIC" \
       | grep -oP 'PartitionCount:\s*\K[0-9]+' | head -1)

echo
if [ "$REAL" = "$PARTITIONS" ]; then
  echo "OK: $TOPIC com $REAL particoes."
else
  echo "ATENCAO: o topico tem $REAL particoes, esperado $PARTITIONS."
  echo "Provavelmente ele foi criado antes com outro valor. Para recriar:"
  echo "    docker compose down -v && docker compose up -d"
  echo "    bash scripts/create_topic.sh"
  exit 1
fi
