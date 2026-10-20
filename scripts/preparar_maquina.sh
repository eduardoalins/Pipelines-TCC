#!/usr/bin/env bash
#
# preparar_maquina.sh — deixa a maquina da AWS igual ao WSL de referencia
# (Etapa 10). Roda DENTRO da maquina, depois do `terraform apply` (passo 27):
#
#     git clone https://github.com/eduardoalins/Pipelines-TCC.git
#     bash Pipelines-TCC/scripts/preparar_maquina.sh
#
# A maquina e destruida ao fim de cada sessao, entao isto se repete em toda
# sessao. Tudo que sai na tela vai tambem para ~/tcc-data/preparacao.txt, com o
# tempo de cada passo.
#
# Os passos sao os do README (2 a 5, 12 e 13 e o relogio), na mesma ordem:
#   1. pacotes do sistema     5. Kafka e o topico de 24 particoes
#   2. Docker (versoes do WSL) 6. 6000 eventos e as duas cadeias conferidas
#   3. ambiente Python        7. versoes de tudo (scripts/versoes.sh)
#   4. as duas imagens        8. o relogio
#
# E idempotente: rodar de novo pula o que ja esta feito.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$REPO_DIR/scripts/preparar_maquina.sh"
LOG="$HOME/tcc-data/preparacao.txt"
APT=(sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=600 -y -qq)

if grep -qi microsoft /proc/sys/kernel/osrelease 2>/dev/null; then
  echo "Este script e para a maquina da AWS, nao para o WSL. Nada foi feito."
  exit 2
fi

mkdir -p "$HOME/tcc-data"
# Na segunda passada (dentro do grupo docker, abaixo) a saida ja passa pelo
# tee da primeira: redirecionar de novo gravaria cada linha duas vezes.
if [ -z "${PREPARAR_SEGUNDA_PASSADA:-}" ]; then
  : > "$LOG"
  exec > >(tee -a "$LOG") 2>&1
fi

T0="${T0:-$(date +%s)}"
passo() { printf '\n\033[1m>> [%4ds] %s\033[0m\n' "$(( $(date +%s) - T0 ))" "$*"; }

cd "$REPO_DIR"
if [ -z "${PREPARAR_SEGUNDA_PASSADA:-}" ]; then
  passo "Codigo: commit $(git rev-parse --short HEAD) — $(git log -1 --format=%s)"

  # --- 1. Pacotes do sistema ----------------------------------------------
  # No primeiro boot o cloud-init e o unattended-upgrades ainda podem estar
  # mexendo no apt; esperar por eles evita o "Could not get lock".
  passo "1. Pacotes do sistema"
  if command -v cloud-init >/dev/null; then
    cloud-init status --wait >/dev/null || true
  fi
  "${APT[@]}" update
  "${APT[@]}" install ca-certificates curl python3.12-venv
  echo "OK"

  # --- 2. Docker, nas MESMAS versoes do WSL de referencia --------------------
  # Decisao de 19/10/2026: o Docker da nuvem igual ao do Docker Desktop do WSL
  # (Engine 28.0.4, containerd 1.7.26, Compose 2.34.0, cgroups por cgroupfs).
  # E o Docker que impoe os tetos de memoria e conta a CPU dos conteineres. As
  # versoes vem do repositorio oficial, pelo numero; um numero que nao exista
  # faz o apt falhar aqui, antes de qualquer outra coisa. O que NAO da para
  # igualar fica declarado: kernel, hipervisor e processador.
  passo "2. Docker"
  DOCKER_VERSAO="5:28.0.4-1~ubuntu.24.04~noble"
  CONTAINERD_VERSAO="1.7.26-1"
  COMPOSE_VERSAO="2.34.0-1~ubuntu.24.04~noble"
  BUILDX_VERSAO="0.22.0-1~ubuntu.24.04~noble"
  if command -v docker >/dev/null; then
    echo "ja instalado"
  else
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
      | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    "${APT[@]}" update
    # Sem os "recomendados": eles puxariam o docker-ce-rootless-extras na
    # versao mais recente, fora do par travado acima.
    "${APT[@]}" install --no-install-recommends \
      "docker-ce=$DOCKER_VERSAO" "docker-ce-cli=$DOCKER_VERSAO" \
      "containerd.io=$CONTAINERD_VERSAO" "docker-compose-plugin=$COMPOSE_VERSAO" \
      "docker-buildx-plugin=$BUILDX_VERSAO"
    # Travadas: nenhum apt upgrade troca a versao no meio de uma sessao.
    sudo apt-mark hold docker-ce docker-ce-cli containerd.io docker-compose-plugin docker-buildx-plugin
    # O Docker Desktop gerencia os cgroups pelo cgroupfs; o padrao no Ubuntu
    # seria o systemd.
    echo '{ "exec-opts": ["native.cgroupdriver=cgroupfs"] }' | sudo tee /etc/docker/daemon.json >/dev/null
    sudo systemctl restart docker
  fi
  sudo usermod -aG docker "$USER"
  echo "docker $(sudo docker version --format '{{.Server.Version}}'), compose $(sudo docker compose version --short)," \
       "cgroups $(sudo docker info --format '{{.CgroupDriver}}'), armazenamento $(sudo docker info --format '{{.Driver}}')"

  # O grupo novo so vale numa sessao nova. Em vez de pedir para sair e entrar,
  # o resto do script roda de novo dentro do grupo, pelo sg.
  if ! docker info >/dev/null 2>&1; then
    exec sg docker -c "PREPARAR_SEGUNDA_PASSADA=1 T0=$T0 bash '$SCRIPT'"
  fi
fi

# --- 3. Ambiente Python (o mesmo do passo 2 do README) ----------------------
passo "3. Ambiente Python"
bash scripts/setup_env.sh

# --- 4. As duas imagens -----------------------------------------------------
# Construidas aqui, de novo, a cada sessao (decisao de 19/10/2026). A base e
# travada por digest, e os jars por checksum: se algum nao bater, o build falha.
passo "4. Imagem do Spark"
docker compose build spark
passo "4. Imagem do Flink"
docker compose build flink

# --- 5. Kafka e o topico ----------------------------------------------------
passo "5. Kafka"
docker compose up -d --wait
if ! docker exec kafka ls /var/lib/kafka/data | grep -q meta.properties; then
  echo "ERRO: o Kafka nao esta gravando no volume (README, passo 3). Parei."
  exit 1
fi
echo "OK: o Kafka grava no volume"
bash scripts/create_topic.sh

# --- 6. Eventos e as duas cadeias -------------------------------------------
# O check_chain_flink.py exige o topico povoado. 6000 eventos, em 6 s.
passo "6. Gerador (6000 eventos) e as cadeias"
RATE=1000 DURATION=6 "$HOME/.venvs/tcc/bin/python" shared/data-generator/generator.py
docker compose run --rm spark python scripts/check_chain.py
docker compose run --rm flink python scripts/check_chain_flink.py

# --- 7. Versoes ---------------------------------------------------------------
passo "7. Versoes"
bash scripts/versoes.sh

# --- 8. O relogio -------------------------------------------------------------
# Fora do WSL ha um sincronizador so, e ele ligado e o estado certo.
passo "8. Relogio"
"$HOME/.venvs/tcc/bin/python" scripts/check_clock.py

passo "Maquina pronta"
echo "disco livre .... $(df -h "$HOME" | awk 'NR==2{print $4}')"
echo "registro ....... $LOG"
