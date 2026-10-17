#!/usr/bin/env bash
#
# setup_aws_tools.sh — instala as ferramentas da nuvem no WSL (Etapa 10).
#
# Instala, na pasta do usuario e SEM sudo:
#   - AWS CLI v2, pelo instalador oficial da Amazon  -> ~/.local/aws-cli
#   - Terraform, na versao travada abaixo             -> ~/.local/bin/terraform
# e escreve ~/.aws/config com a regiao do estudo, se ele ainda nao existir.
#
# NAO toca nas credenciais. Elas mudam a cada sessao do AWS Academy Learner Lab
# e quem as cola em ~/.aws/credentials e o autor, a mao — nunca um script,
# nunca o repositorio.
#
# Rode DE DENTRO do WSL:
#     bash scripts/setup_aws_tools.sh
#
# E idempotente: rodar duas vezes nao quebra nada; a AWS CLI e atualizada.
#
set -euo pipefail

# --- Versoes (decisao E7: versao travada faz parte do metodo) ---------------
# O Terraform e o mesmo 1.16.4 que estava instalado no Windows: uma versao so.
# A AWS CLI so le e cria recursos; nao entra em nenhuma medida. Sem
# AWSCLI_VERSION, instala a mais recente e o resumo diz qual foi.
TERRAFORM_VERSION="${TERRAFORM_VERSION:-1.16.4}"
AWSCLI_VERSION="${AWSCLI_VERSION:-}"
REGIAO="${REGIAO:-us-east-1}"   # a unica com o par de chaves vockey (decisao de 16/10/2026)

BIN_DIR="$HOME/.local/bin"
AWSCLI_DIR="$HOME/.local/aws-cli"

say() { printf '\n\033[1m>> %s\033[0m\n' "$*"; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- 1. Pre-requisitos -----------------------------------------------------
say "Verificando pre-requisitos"
if [ "$(uname -m)" != "x86_64" ]; then
  echo "Esta receita e para x86_64; a maquina e $(uname -m)."
  exit 1
fi
FALTANDO=()
command -v curl >/dev/null || FALTANDO+=("curl")
command -v unzip >/dev/null || FALTANDO+=("unzip")
command -v sha256sum >/dev/null || FALTANDO+=("coreutils")
if [ ${#FALTANDO[@]} -gt 0 ]; then
  echo "Faltam: ${FALTANDO[*]}"
  echo
  echo "Instale rodando, a partir do PowerShell do Windows:"
  echo "    wsl -d Ubuntu -u root -- apt-get update"
  echo "    wsl -d Ubuntu -u root -- apt-get install -y ${FALTANDO[*]}"
  exit 1
fi
echo "OK: curl, unzip e sha256sum"
mkdir -p "$BIN_DIR"

# --- 2. Terraform ----------------------------------------------------------
# O zip e conferido contra a lista de checksums que a HashiCorp publica para a
# versao. Um arquivo corrompido ou trocado no caminho para aqui, antes de
# instalar. (A lista tambem tem assinatura GPG; ela nao e conferida aqui.)
say "Terraform $TERRAFORM_VERSION"
ATUAL="$("$BIN_DIR/terraform" version 2>/dev/null | head -1 || true)"
if [ "$ATUAL" = "Terraform v$TERRAFORM_VERSION" ]; then
  echo "ja instalado: $ATUAL"
else
  BASE="https://releases.hashicorp.com/terraform/$TERRAFORM_VERSION"
  ZIP="terraform_${TERRAFORM_VERSION}_linux_amd64.zip"
  SUMS="terraform_${TERRAFORM_VERSION}_SHA256SUMS"
  curl -fsSL -o "$TMP/$ZIP" "$BASE/$ZIP"
  curl -fsSL -o "$TMP/$SUMS" "$BASE/$SUMS"
  if ! grep " $ZIP\$" "$TMP/$SUMS" > "$TMP/conferir"; then
    echo "O $ZIP nao esta na lista de checksums da versao. Nada instalado."
    exit 1
  fi
  (cd "$TMP" && sha256sum -c conferir)
  unzip -q -o "$TMP/$ZIP" terraform -d "$TMP"
  install -m 0755 "$TMP/terraform" "$BIN_DIR/terraform"
  echo "instalado: $("$BIN_DIR/terraform" version | head -1)"
fi

# --- 3. AWS CLI v2 ---------------------------------------------------------
# O instalador oficial aceita uma pasta propria (-i) e onde criar o atalho
# (-b): assim nao precisa de sudo. Com a CLI ja instalada, --update atualiza.
say "AWS CLI v2"
if [ -n "$AWSCLI_VERSION" ]; then
  ZIP_CLI="awscli-exe-linux-x86_64-$AWSCLI_VERSION.zip"
else
  ZIP_CLI="awscli-exe-linux-x86_64.zip"
fi
curl -fsSL -o "$TMP/awscliv2.zip" "https://awscli.amazonaws.com/$ZIP_CLI"
unzip -q "$TMP/awscliv2.zip" -d "$TMP"
if [ -x "$BIN_DIR/aws" ]; then
  "$TMP/aws/install" -i "$AWSCLI_DIR" -b "$BIN_DIR" --update
else
  "$TMP/aws/install" -i "$AWSCLI_DIR" -b "$BIN_DIR"
fi
echo "instalada: $("$BIN_DIR/aws" --version)"

# --- 4. Configuracao (sem credenciais) -------------------------------------
# So a regiao e o formato de saida. Um config que ja existe nao e tocado.
say "Configuracao em ~/.aws"
mkdir -p "$HOME/.aws"
chmod 700 "$HOME/.aws"
if [ -f "$HOME/.aws/config" ]; then
  echo "~/.aws/config ja existe; nao foi alterado"
else
  printf '[default]\nregion = %s\noutput = json\n' "$REGIAO" > "$HOME/.aws/config"
  echo "~/.aws/config criado: regiao $REGIAO"
fi

# --- 5. Resumo -------------------------------------------------------------
say "Ferramentas prontas"
printf '  terraform : %s\n' "$("$BIN_DIR/terraform" version | head -1)"
printf '  aws       : %s\n' "$("$BIN_DIR/aws" --version)"
printf '  regiao    : %s\n' "$REGIAO"
case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) echo
     echo "ATENCAO: $BIN_DIR nao esta no PATH deste terminal."
     echo "Feche e abra o terminal do Ubuntu (o ~/.profile acrescenta a pasta"
     echo "quando ela existe), ou use os caminhos completos acima." ;;
esac
echo
echo "Falta o que so voce faz, em toda sessao do laboratorio:"
echo "  1. no Learner Lab, Start Lab e depois 'AWS Details' > 'AWS CLI: Show'"
echo "  2. colar o bloco inteiro em ~/.aws/credentials (nano ~/.aws/credentials)"
echo "  3. chmod 600 ~/.aws/credentials"
echo "  4. aws sts get-caller-identity"
