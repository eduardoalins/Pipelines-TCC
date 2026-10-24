#!/usr/bin/env bash
#
# sessao_aws.sh — uma combinacao na nuvem, numa sessao do laboratorio, sem
# precisar de ninguem olhando (Etapa 10, decisao de 24/10/2026). Roda no WSL:
#
#     INTERVALO=5 CARGA=1000 DURACAO=60 bash scripts/sessao_aws.sh
#
#     INTERVALO, CARGA, DURACAO   a combinacao, como no run_combination.py
#     RODADAS                     opcional, ex. RODADAS=1,2,3,4,5 (padrao: 1 a 10)
#
# Antes: o codigo publicado no GitHub (a maquina clona de la) e o ritual das
# credenciais (README, passo 26) com o Start Lab apertado AGORA — o relogio da
# sessao recomeca em 4 h e as credenciais vencem com ela.
#
# O que faz, em ordem:
#   1. confere credenciais, o commit publicado e se a combinacao cabe antes da
#      autodestruicao da maquina; se nao couber, NAO comeca
#   2. terraform apply (a maquina ja nasce com a autodestruicao agendada)
#   3. prepara a maquina (preparar_maquina.sh)
#   4. lanca a combinacao DENTRO da maquina, desacoplada do SSH: ela continua
#      mesmo que a conexao caia ou este computador trave
#   5. acompanha a cada 5 min
#   6. traz os .txt/.md para Informações/Resultados AWS e os registros da sessao
#      para ~/tcc-data/sessoes_aws/
#   7. terraform destroy e conferencia — SEMPRE, mesmo se algo falhar no meio
#
# Combinacao em duas sessoes: se nao cabe numa, rode RODADAS=1,2,3,4,5 numa e
# RODADAS=6,7,8,9,10 na outra. Uma combinacao que nao fechou deixa o material
# bruto em ~/tcc-data/bruto_aws/<combinacao>.tgz, e a sessao seguinte da mesma
# combinacao o devolve a maquina antes de comecar: o run_combination.py ve as
# rodadas ja validas e fecha quando as 20 estiverem prontas.
#
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$REPO/infra"
RESULTADOS_AWS="$REPO/../../Informações/Resultados AWS"   # o padrao do write_results.py
AUTODESTRUICAO_MIN=210        # o mesmo padrao do infra/variables.tf
PREPARO_MIN=15                # apply (~4 min) + preparacao (~8 min), com folga
FOLGA=1.3                     # margem para a nuvem ser mais lenta que o WSL
ACOMPANHAR_S=300

: "${INTERVALO:?defina INTERVALO}" "${CARGA:?defina CARGA}" "${DURACAO:?defina DURACAO}"
RODADAS="${RODADAS:-1,2,3,4,5,6,7,8,9,10}"
NOME="Intervalo${INTERVALO}Carga${CARGA}"
BRUTO=~/tcc-data/bruto_aws/$NOME.tgz
REGISTRO=~/tcc-data/sessoes_aws/$(date +%Y%m%d_%H%M)_$NOME
mkdir -p "$REGISTRO" "$(dirname "$BRUTO")"
exec > >(tee "$REGISTRO/sessao.txt") 2>&1

passo() { echo; echo "===== $* ($(date +%H:%M:%S))"; }
export TF_IN_AUTOMATION=1

# --- 1. Conferencias ------------------------------------------------------------
passo "1. Conferencias"
if [ -f "$RESULTADOS_AWS/Resultado$NOME.txt" ]; then
  echo "NAO COMECEI: esta combinacao ja foi fechada — $RESULTADOS_AWS/Resultado$NOME.txt"
  exit 2
fi
aws sts get-caller-identity --query Arn --output text >/dev/null \
  || { echo "Credenciais invalidas ou expiradas: refaca o ritual (README, passo 26)."; exit 2; }
echo "credenciais OK"

git -C "$REPO" fetch -q origin 2>/dev/null
echo "a maquina vai rodar o publicado: $(git -C "$REPO" log --oneline -1 origin/main)"
if [ "$(git -C "$REPO" rev-parse HEAD)" != "$(git -C "$REPO" rev-parse origin/main)" ]; then
  echo "AVISO: o commit local ($(git -C "$REPO" log --oneline -1 HEAD)) nao e o publicado."
fi
[ -f "$BRUTO" ] && echo "continuacao: as rodadas ja feitas desta combinacao sobem de $BRUTO"

# Uma rodada leva DURACAO + o preparo dela + a espera pela fila; medido no WSL:
# 150 s com DURACAO=60 e 413 s com DURACAO=300, isto e, ~1,1 x DURACAO + 85 s.
N=$(( $(echo "$RODADAS" | tr ',' '\n' | wc -l) * 2 ))
POR_RODADA=$(( DURACAO + DURACAO / 10 + 85 ))
ESTIMADO_MIN=$(awk -v n=$N -v r=$POR_RODADA -v f=$FOLGA -v p=$PREPARO_MIN 'BEGIN{printf "%d", n*r*f/60 + p}')
echo "combinacao $NOME, rodadas $RODADAS: ate $N rodadas de ~${POR_RODADA} s"
echo "estimado, com a preparacao e margem de ${FOLGA}x: ${ESTIMADO_MIN} min; autodestruicao em ${AUTODESTRUICAO_MIN} min"
if [ "$ESTIMADO_MIN" -gt $(( AUTODESTRUICAO_MIN - 10 )) ]; then
  echo "NAO COMECEI: nao cabe com folga antes da autodestruicao."
  echo "Divida em duas sessoes: RODADAS=1,2,3,4,5 e depois RODADAS=6,7,8,9,10."
  exit 2
fi

# --- 6 e 7. O fim, sempre -------------------------------------------------------
IP=""
encerrar() {
  trap '' INT TERM   # um Ctrl+C aqui nao pode impedir o destroy
  if [ -n "$IP" ]; then
    passo "6. Trazendo os arquivos"
    mkdir -p "$RESULTADOS_AWS"
    if timeout 120 scp "${OPC[@]}" "ubuntu@$IP:resultados/Resultado$NOME.*" "$RESULTADOS_AWS/" 2>/dev/null; then
      echo "COMBINACAO FECHADA: resultados em $RESULTADOS_AWS"
      [ -f "$BRUTO" ] && echo "o bruto das sessoes anteriores ($BRUTO) ja nao e necessario"
    elif timeout 300 "${SSH[@]}" "test -d Pipelines-TCC/results/$NOME && tar czf /tmp/bruto.tgz -C Pipelines-TCC/results $NOME" \
         && timeout 600 scp "${OPC[@]}" "ubuntu@$IP:/tmp/bruto.tgz" "$BRUTO.novo"; then
      mv "$BRUTO.novo" "$BRUTO"
      echo "combinacao AINDA ABERTA: material bruto guardado em $BRUTO para a proxima sessao"
    else
      echo "!! nem resultado nem material bruto vieram (a maquina caiu ou a combinacao nao comecou)"
    fi
    timeout 120 scp "${OPC[@]}" "ubuntu@$IP:tcc-data/preparacao.txt" "ubuntu@$IP:tcc-data/combinacao.txt" "$REGISTRO/" 2>/dev/null
    echo "registros da sessao em $REGISTRO"
  fi
  passo "7. terraform destroy"
  (cd "$INFRA" && terraform destroy -auto-approve -input=false -no-color | grep -E 'Destroy complete|Error|error')
  echo "maquinas com a etiqueta do projeto (so pode haver terminated, ou nada):"
  aws ec2 describe-instances --filters Name=tag:Projeto,Values=tcc-pipelines \
    --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
  passo "fim"
}
trap encerrar EXIT

# --- 2. A maquina -----------------------------------------------------------------
passo "2. terraform apply"
(cd "$INFRA" && terraform apply -auto-approve -input=false -no-color | grep -E 'Apply complete|Error|error') \
  || exit 1
IP=$(cd "$INFRA" && terraform output -raw ip_publico)
OPC=(-i ~/.ssh/labsuser.pem -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30
     -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$HOME/.ssh/known_hosts_tcc")
SSH=(ssh "${OPC[@]}" "ubuntu@$IP")
for _ in $(seq 1 30); do "${SSH[@]}" true 2>/dev/null && break; sleep 10; done
"${SSH[@]}" 'echo "maquina no ar; autodestruicao: $(sudo shutdown --show 2>&1)"' \
  || { echo "o SSH nao entrou em 5 min"; exit 1; }

# --- 3. Preparar ------------------------------------------------------------------
passo "3. Preparacao (a saida completa fica em preparacao.txt, nos registros)"
"${SSH[@]}" 'git clone -q https://github.com/eduardoalins/Pipelines-TCC.git && bash Pipelines-TCC/scripts/preparar_maquina.sh' \
  | grep -aE '>> \[|ERRO|CADEIA|OK: relogio|NAO MEDIR|disco livre'
[ "${PIPESTATUS[0]}" -eq 0 ] || { echo "A PREPARACAO FALHOU."; exit 1; }

# --- 4. Lancar a combinacao, desacoplada ------------------------------------------
passo "4. Combinacao $NOME (rodadas $RODADAS), dentro da maquina"
if [ -f "$BRUTO" ]; then
  scp "${OPC[@]}" "$BRUTO" "ubuntu@$IP:/tmp/bruto.tgz" \
    && "${SSH[@]}" "mkdir -p Pipelines-TCC/results && tar xzf /tmp/bruto.tgz -C Pipelines-TCC/results" \
    || { echo "nao consegui devolver o material bruto a maquina"; exit 1; }
  echo "material bruto das sessoes anteriores devolvido"
fi
# So o python vai para segundo plano, com as tres saidas desligadas do SSH: assim
# o SSH volta na hora e a combinacao nao depende dele.
"${SSH[@]}" "mkdir -p resultados && cd Pipelines-TCC || exit 1
  nohup env AMBIENTE=aws SAIDA=\$HOME/resultados INTERVALO=$INTERVALO CARGA=$CARGA \
    DURACAO=$DURACAO RODADAS=$RODADAS \$HOME/.venvs/tcc/bin/python -u benchmarks/run_combination.py \
    > \$HOME/tcc-data/combinacao.txt 2>&1 < /dev/null &" || exit 1
echo "lancada; acompanhando a cada $((ACOMPANHAR_S / 60)) min"

# --- 5. Acompanhar ----------------------------------------------------------------
# [b] no padrao: o pgrep nao se acha no proprio comando.
sleep 30
sem_resposta=0
while true; do
  estado=$("${SSH[@]}" "pgrep -f '[b]enchmarks/run_combination.py' >/dev/null && echo rodando || echo terminou" 2>/dev/null)
  case "$estado" in
    terminou) break ;;
    rodando)
      sem_resposta=0
      "${SSH[@]}" "grep -aE '^--- \[|RODADA (VALIDA|NAO VALE)' \$HOME/tcc-data/combinacao.txt | tail -2" \
        | sed "s/^/  ($(date +%H:%M)) /" ;;
    *)
      sem_resposta=$((sem_resposta + 1))
      echo "  ($(date +%H:%M)) a maquina nao responde ($sem_resposta/3)"
      [ "$sem_resposta" -ge 3 ] && { echo "DESISTI: a maquina sumiu (autodestruicao?)"; exit 1; } ;;
  esac
  sleep $ACOMPANHAR_S
done
passo "5. A combinacao terminou"
"${SSH[@]}" "tail -30 \$HOME/tcc-data/combinacao.txt"
