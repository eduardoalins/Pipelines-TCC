#!/usr/bin/env bash
#
# preparar_base.sh — deixa a base REAL de eventos pronta em ~/tcc-data/base.
#
# A base (decisao de 23/10/2026, pedido da orientacao): "eCommerce behavior data
# from multi category store", eventos reais de uma loja online multicategoria,
# coletados pela REES46 e publicados no Kaggle por Michael Kechinov. Usamos o mes
# de OUTUBRO DE 2019. O arquivo e o mesmo do Kaggle, baixado pelo link direto da
# REES46, que nao pede login.
#
#   Kaggle: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
#   REES46: https://rees46.com
#   (uso livre, com a condicao de citar as duas fontes)
#
# Rode DE DENTRO do WSL, ou na maquina da AWS:
#     bash scripts/preparar_base.sh
#
# O que faz, em ordem — e idempotente, pula o que ja esta feito:
#   1. baixa o .gz, so se ele ainda nao existir
#   2. confere o sha256: se nao bater, PARA, sem descompactar nada
#   3. descompacta para .csv, mantendo o .gz ao lado
#   4. confere a contagem de eventos
#
# A base NUNCA e alterada: o replayer so le o .csv.
#
set -euo pipefail

URL="https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"
SHA256="8ebca1ad741295297368f2cf0315e3f36853a1a11768fb16babf8c9b83838147"
EVENTOS=42448764          # linhas de dados, sem o cabecalho (contadas em 23/10/2026)
PASTA="${BASE_DIR:-$HOME/tcc-data/base}"
GZ="$PASTA/2019-Oct.csv.gz"
CSV="$PASTA/2019-Oct.csv"

say() { printf '\n\033[1m>> %s\033[0m\n' "$*"; }
mkdir -p "$PASTA"

# --- 1. Baixar ---------------------------------------------------------------
say "1. Arquivo compactado"
if [ -f "$GZ" ]; then
  echo "ja existe: $GZ"
else
  echo "baixando de $URL (1,7 GB)"
  curl -fL --retry 3 -o "$GZ.parcial" "$URL"
  mv "$GZ.parcial" "$GZ"
fi

# --- 2. Conferir -------------------------------------------------------------
# A impressao digital da base. Um arquivo diferente — outro mes, um download
# cortado, uma versao republicada — para aqui.
say "2. Checksum"
REAL="$(sha256sum "$GZ" | cut -d' ' -f1)"
if [ "$REAL" != "$SHA256" ]; then
  echo "ERRO: o sha256 nao confere."
  echo "   esperado $SHA256"
  echo "   obtido   $REAL"
  echo "Nada foi descompactado. Apague $GZ e rode de novo para baixar outra vez."
  exit 1
fi
echo "OK: $SHA256"

# --- 3. Descompactar ---------------------------------------------------------
# O replayer le o .csv: sem descompactar a cada rodada (decisao de 23/10/2026).
say "3. Descompactar"
if [ -f "$CSV" ]; then
  echo "ja existe: $CSV"
else
  gunzip -c "$GZ" > "$CSV.parcial"
  mv "$CSV.parcial" "$CSV"
  echo "descompactado: $CSV"
fi

# --- 4. Contar -----------------------------------------------------------------
say "4. Contagem"
LINHAS=$(( $(wc -l < "$CSV") - 1 ))
if [ "$LINHAS" -ne "$EVENTOS" ]; then
  echo "ERRO: $LINHAS eventos no .csv, esperado $EVENTOS. Apague $CSV e rode de novo."
  exit 1
fi
echo "OK: $LINHAS eventos"

say "Base pronta"
printf '  arquivo ... %s\n' "$CSV"
printf '  tamanho ... %s bytes\n' "$(stat -c %s "$CSV")"
printf '  eventos ... %s\n' "$LINHAS"
printf '  origem .... %s (sha256 %s)\n' "$GZ" "$SHA256"
