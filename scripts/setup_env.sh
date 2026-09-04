#!/usr/bin/env bash
#
# setup_env.sh — reconstroi o ambiente de experimentos do zero.
#
# Por que este arquivo existe: o venv e as pastas de dados NAO vao para o git
# (sao binarios de Linux e gigabytes de Parquet). O que vai para o git e a
# receita que os recria. Se a maquina morrer no meio do TCC, isto aqui e o que
# devolve o ambiente identico.
#
# Rode DE DENTRO do WSL:
#     bash scripts/setup_env.sh
#
# E idempotente: rodar duas vezes nao quebra nada.
#
set -euo pipefail

# --- Onde as coisas moram (decisao E6, ver planoNRT3 secao 0) --------------
# Codigo fica no /mnt/c para voce editar no VS Code e commitar normalmente.
# Venv e dados ficam em ext4 nativo porque o /mnt/c escreve a 77 MB/s contra
# 1.2 GB/s do ext4 — 15,8x. Com a saida no /mnt/c o gargalo do pipeline seria
# o sistema de arquivos do Windows, e o Experimento A mediria a coisa errada.
VENV_DIR="${VENV_DIR:-$HOME/.venvs/tcc}"
DATA_DIR="${DATA_DIR:-$HOME/tcc-data}"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

say() { printf '\n\033[1m>> %s\033[0m\n' "$*"; }

# --- 1. Pacotes do sistema -------------------------------------------------
# python3-venv: sem ele o Python nao cria ambientes isolados (o modulo venv
#   existe, mas o ensurepip vem em pacote separado no Ubuntu).
# openjdk-17: o Spark roda na JVM; o PySpark e so uma casca em Python que
#   conversa com ela. A 17 serve tanto para Spark 3.5 quanto para 4.0.
say "Verificando pacotes do sistema"
FALTANDO=()
python3 -c 'import ensurepip' 2>/dev/null || FALTANDO+=("python3.12-venv")
command -v java >/dev/null 2>&1        || FALTANDO+=("openjdk-17-jdk-headless")

if [ ${#FALTANDO[@]} -gt 0 ]; then
  echo "Faltam: ${FALTANDO[*]}"
  if [ "$(id -u)" -eq 0 ]; then
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${FALTANDO[@]}"
  else
    echo
    echo "Instale rodando, a partir do PowerShell do Windows:"
    echo "    wsl -d Ubuntu -u root -- apt-get update"
    echo "    wsl -d Ubuntu -u root -- apt-get install -y ${FALTANDO[*]}"
    echo
    echo "(o -u root evita o prompt de senha do sudo, que nao funciona em"
    echo " execucao nao interativa)"
    exit 1
  fi
else
  echo "OK: $(java -version 2>&1 | head -1)"
fi

# --- 2. Pastas de dados ----------------------------------------------------
say "Criando pastas de dados em $DATA_DIR"
mkdir -p "$DATA_DIR/out" "$DATA_DIR/checkpoints" "$DATA_DIR/metrics"
ls -1 "$DATA_DIR"

# --- 3. Ambiente virtual ---------------------------------------------------
# O isolamento nao e frescura: as versoes exatas de pyspark e confluent-kafka
# fazem parte do metodo experimental. Se mudarem no meio das execucoes, os
# resultados deixam de ser comparaveis entre si.
say "Ambiente virtual em $VENV_DIR"
if [ ! -x "$VENV_DIR/bin/python" ]; then
  python3 -m venv "$VENV_DIR"
  echo "criado"
else
  echo "ja existe"
fi
"$VENV_DIR/bin/python" -m pip install --quiet --upgrade pip

# --- 4. Dependencias -------------------------------------------------------
# requirements.txt ainda nao existe: as versoes de Spark e Kafka sao travadas
# na Etapa 1 (decisao E7) e escritas no CONTRATO.md. Ate la, so o venv vazio.
if [ -f "$REPO_DIR/requirements.txt" ]; then
  say "Instalando dependencias"
  "$VENV_DIR/bin/pip" install -r "$REPO_DIR/requirements.txt"
else
  say "Sem requirements.txt ainda (versoes sao travadas na Etapa 1)"
fi

# --- 5. Resumo -------------------------------------------------------------
say "Ambiente pronto"
printf '  repositorio : %s\n' "$REPO_DIR"
printf '  venv        : %s\n' "$VENV_DIR"
printf '  dados       : %s\n' "$DATA_DIR"
printf '  python      : %s\n' "$("$VENV_DIR/bin/python" -V)"
printf '  java        : %s\n' "$(java -version 2>&1 | head -1)"
printf '  memoria WSL : %s\n' "$(free -h | awk '/^Mem:/{print $2}')"
printf '  cpus WSL    : %s\n' "$(nproc)"
echo
echo "Para usar:  source $VENV_DIR/bin/activate"
