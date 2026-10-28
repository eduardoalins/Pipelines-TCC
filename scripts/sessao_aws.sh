#!/usr/bin/env bash
#
# sessao_aws.sh — uma sessao do laboratorio na nuvem, sem precisar de ninguem
# olhando (Etapa 10, decisoes de 24/10/2026). Roda no WSL. Dois modos:
#
#   OFICIAL — UMA combinacao por sessao:
#     INTERVALO=5 CARGA=1000 DURACAO=60 bash scripts/sessao_aws.sh
#       INTERVALO, CARGA, DURACAO   a combinacao, como no run_combination.py
#       RODADAS                     opcional, ex. RODADAS=1,2,3,4,5 (padrao: 1 a 10)
#
#   ENSAIO — varias cargas numa sessao, para escolher as cargas da nuvem:
#     CARGAS="1000 2500 5000 7500" bash scripts/sessao_aws.sh
#       Uma rodada de cada motor por carga, nos pares intervalo:duracao de PARES
#       (padrao "5:60 30:300"), subindo. Roda TODAS as cargas, mesmo depois de
#       uma falha (27/10/2026); so um problema do ambiente para o ensaio. Nada do
#       ensaio entra nos resultados oficiais:
#       o material bruto e o resumo ficam so nos registros da sessao.
#
# Antes: o codigo publicado no GitHub (a maquina clona de la) e o ritual das
# credenciais (README, passo 26) com o Start Lab apertado AGORA — o relogio da
# sessao recomeca em 4 h e as credenciais vencem com ela.
#
# O que faz, em ordem:
#   1. confere credenciais, o commit publicado e se tudo cabe antes da
#      autodestruicao da maquina; se nao couber, NAO comeca
#   2. terraform apply (a maquina ja nasce com a autodestruicao agendada)
#   3. prepara a maquina (preparar_maquina.sh)
#   4. lanca as rodadas DENTRO da maquina, desacopladas do SSH: continuam
#      mesmo que a conexao caia ou este computador trave
#   5. acompanha a cada 5 min
#   6. traz os arquivos: os .txt/.md oficiais para Informações/Resultados AWS; os
#      registros da sessao (e, no ensaio, o bruto e o resumo) para
#      ~/tcc-data/sessoes_aws/
#   7. terraform destroy e conferencia — SEMPRE, mesmo se algo falhar no meio
#
# Combinacao oficial em duas sessoes: se nao cabe numa, rode RODADAS=1,2,3,4,5
# numa e RODADAS=6,7,8,9,10 na outra. Uma combinacao que nao fechou deixa o
# material bruto em ~/tcc-data/bruto_aws/<combinacao>.tgz, e a sessao seguinte
# da mesma combinacao o devolve a maquina antes de comecar: o run_combination.py
# ve as rodadas ja validas e fecha quando as 20 estiverem prontas.
#
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA="$REPO/infra"
RESULTADOS_AWS="$REPO/../../Informações/Resultados AWS"   # o padrao do write_results.py
AUTODESTRUICAO_MIN=210        # o mesmo padrao do infra/variables.tf
PREPARO_MIN=15                # apply (~4 min) + preparacao (~8 min), com folga
FOLGA=1.3                     # margem para a nuvem ser mais lenta que o WSL
ACOMPANHAR_S=300

# As combinacoes da sessao, uma por linha: "intervalo carga duracao".
if [ -n "${CARGAS:-}" ]; then
  ENSAIO=1
  NOME=Ensaio
  RODADAS=1
  COMBOS=$(for c in $CARGAS; do for p in ${PARES:-5:60 30:300}; do
             echo "${p%%:*} $c ${p##*:}"; done; done)
  MARCA='[e]nsaio_aws.sh'           # o processo que diz se ainda esta rodando
else
  ENSAIO=0
  : "${INTERVALO:?defina INTERVALO (ou CARGAS, para o ensaio)}" "${CARGA:?defina CARGA}" "${DURACAO:?defina DURACAO}"
  RODADAS="${RODADAS:-1,2,3,4,5,6,7,8,9,10}"
  NOME="Intervalo${INTERVALO}Carga${CARGA}"
  COMBOS="$INTERVALO $CARGA $DURACAO"
  MARCA='[b]enchmarks/run_combination.py'   # [b]: o pgrep nao se acha no proprio comando
fi
BRUTO=~/tcc-data/bruto_aws/$NOME.tgz
REGISTRO=~/tcc-data/sessoes_aws/$(date +%Y%m%d_%H%M)_$NOME
mkdir -p "$REGISTRO" "$(dirname "$BRUTO")"
exec > >(tee "$REGISTRO/sessao.txt") 2>&1

passo() { echo; echo "===== $* ($(date +%H:%M:%S))"; }
export TF_IN_AUTOMATION=1

# --- 1. Conferencias ------------------------------------------------------------
passo "1. Conferencias"
if [ "$ENSAIO" = 0 ] && [ -f "$RESULTADOS_AWS/Resultado$NOME.txt" ]; then
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
[ "$ENSAIO" = 0 ] && [ -f "$BRUTO" ] && echo "continuacao: as rodadas ja feitas desta combinacao sobem de $BRUTO"

# Uma rodada leva DURACAO + o preparo dela + a espera pela fila; medido no WSL:
# 150 s com DURACAO=60 e 413 s com DURACAO=300, isto e, ~1,1 x DURACAO + 85 s.
# Cada motor faz cada rodada: 2 rodadas por numero de rodada.
POR_NUMERO=$(( $(echo "$RODADAS" | tr ',' '\n' | wc -l) * 2 ))
TOTAL_S=0
while read -r i c d; do
  echo "  intervalo $i s, carga $c evt/s, $d s: $POR_NUMERO rodadas de ~$(( d + d / 10 + 85 )) s"
  TOTAL_S=$(( TOTAL_S + POR_NUMERO * (d + d / 10 + 85) ))
done <<< "$COMBOS"
ESTIMADO_MIN=$(awk -v t=$TOTAL_S -v f=$FOLGA -v p=$PREPARO_MIN 'BEGIN{printf "%d", t*f/60 + p}')
echo "estimado, com a preparacao e margem de ${FOLGA}x: ${ESTIMADO_MIN} min; autodestruicao em ${AUTODESTRUICAO_MIN} min"
if [ "$ESTIMADO_MIN" -gt $(( AUTODESTRUICAO_MIN - 10 )) ]; then
  echo "NAO COMECEI: nao cabe com folga antes da autodestruicao."
  [ "$ENSAIO" = 0 ] && echo "Divida em duas sessoes: RODADAS=1,2,3,4,5 e depois RODADAS=6,7,8,9,10."
  [ "$ENSAIO" = 1 ] && echo "Divida as CARGAS em duas sessoes."
  exit 2
fi

# --- 6 e 7. O fim, sempre -------------------------------------------------------
# O resumo do ensaio: uma linha por rodada, lida dos manifestos trazidos, com a
# classificacao do ensaio local (decisao de 27/10/2026):
#   ACOMPANHOU    rodada valida e fila zerada em ate 120 s depois do fim do envio
#   CORREU ATRAS  rodada valida, mas a fila levou mais que isso
#   NAO AGUENTOU  rodada invalida
resumir_ensaio() {
  local d; d=$(mktemp -d)
  tar xzf "$REGISTRO/bruto.tgz" -C "$d" && python3 - "$d" > "$REGISTRO/ensaio.txt" <<'PY'
import json, re, sys
from pathlib import Path
print(f"{'intervalo':>9} {'carga':>6} {'motor':<6}{'pasta':<12}{'classe':<13}{'fila s':>7} {'esperas':>8}"
      f" {'envio':>8} {'thr':>8} {'proc P50':>9} {'vis P50':>9} {'CPU motor':>9} {'maq med':>8}"
      f" {'maq pico':>8} {'mem MiB':>8}  motivos")
linhas = []
for m in Path(sys.argv[1]).glob("results/Intervalo*Carga*/*/rodada-*/manifesto.json"):
    i, c = map(int, re.findall(r"\d+", m.parts[-4]))
    j = json.loads(m.read_text("utf-8")); r = j.get("resultado") or {}
    fila = (j.get("fila") or {}).get("espera_s")
    classe = ("NAO AGUENTOU" if not j.get("valida")
              else "ACOMPANHOU" if fila is not None and fila <= 120 else "CORREU ATRAS")
    gen = next(m.parent.glob("*.gen0.json"), None)
    esperas = json.loads(gen.read_text("utf-8")).get("esperas_fila_cheia", "-") if gen else "-"
    p50 = lambda k: (r.get(k) or {}).get("p50", "-")
    linhas.append((c, i, m.parts[-3], m.parts[-2], classe, "-" if fila is None else fila, esperas,
                   j.get("ritmo_envio_evt_s", "-"), r.get("throughput_medio_evt_s", "-"),
                   p50("latencia_processamento_ms"), p50("latencia_visibilidade_ms"),
                   r.get("cpu_media_pct", "-"), r.get("maquina_cpu_media_pct", "-"),
                   r.get("maquina_cpu_pico_pct", "-"), r.get("mem_pico_mib", "-"),
                   "; ".join(j.get("motivos") or [])))
for c, i, motor, pasta, *resto in sorted(linhas, key=lambda l: l[:4]):
    v = [str(x) for x in resto]
    print(f"{i:>9} {c:>6} {motor:<6}{pasta:<12}{v[0]:<13}{v[1]:>7} {v[2]:>8} {v[3]:>8} {v[4]:>8}"
          f" {v[5]:>9} {v[6]:>9} {v[7]:>9} {v[8]:>8} {v[9]:>8} {v[10]:>8}  {v[11]}")
print("\nCPU em % de UM nucleo (a m5.large tem 2: o teto e 200). envio = ritmo do replayer, evt/s.")
print("fila s = segundos para a fila zerar depois do fim do envio; esperas = vezes que o replayer")
print("esperou a fila do produtor (em vez de descartar).")
PY
  rm -rf "$d"
  cat "$REGISTRO/ensaio.txt"
}

IP=""
encerrar() {
  trap '' INT TERM   # um Ctrl+C aqui nao pode impedir o destroy
  if [ -n "$IP" ]; then
    passo "6. Trazendo os arquivos"
    if [ "$ENSAIO" = 1 ]; then
      if timeout 300 "${SSH[@]}" "tar czf /tmp/bruto.tgz -C Pipelines-TCC results" \
         && timeout 600 scp "${OPC[@]}" "ubuntu@$IP:/tmp/bruto.tgz" "$REGISTRO/bruto.tgz"; then
        echo "material bruto do ensaio em $REGISTRO/bruto.tgz"
        resumir_ensaio
      else
        echo "!! o material bruto do ensaio nao veio (a maquina caiu?)"
      fi
    else
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

# --- 4. Lancar as rodadas, desacopladas -------------------------------------------
PY_MAQ='$HOME/.venvs/tcc/bin/python -u benchmarks/run_combination.py'
if [ "$ENSAIO" = 1 ]; then
  passo "4. Ensaio: cargas $CARGAS, dentro da maquina"
  # O laco roda na maquina. Codigo 2 do run_combination.py = problema do
  # ambiente: para tudo. Codigo 1 = alguma rodada nao valeu: segue para a
  # proxima carga mesmo assim (decisao de 27/10/2026) — os motores se separam
  # muito, e parar na primeira falha esconderia o maximo do outro motor. Ate
  # entao (24/10) o ensaio parava na primeira carga que falhava.
  "${SSH[@]}" "cat > ensaio_aws.sh" <<FIM || exit 1
cd \$HOME/Pipelines-TCC || exit 1
while read -r i c d; do
  echo "##### ensaio: intervalo \$i s, carga \$c evt/s, \$d s"
  AMBIENTE=aws SAIDA=\$HOME/resultados INTERVALO=\$i CARGA=\$c DURACAO=\$d RODADAS=1 $PY_MAQ < /dev/null
  codigo=\$?
  echo "##### codigo \$codigo"
  [ \$codigo -eq 2 ] && { echo "##### problema do ambiente: o ensaio para aqui"; exit 2; }
done <<'LISTA'
$COMBOS
LISTA
echo "##### ensaio terminado"
FIM
  LANCAR="bash \$HOME/ensaio_aws.sh"
else
  passo "4. Combinacao $NOME (rodadas $RODADAS), dentro da maquina"
  if [ -f "$BRUTO" ]; then
    scp "${OPC[@]}" "$BRUTO" "ubuntu@$IP:/tmp/bruto.tgz" \
      && "${SSH[@]}" "mkdir -p Pipelines-TCC/results && tar xzf /tmp/bruto.tgz -C Pipelines-TCC/results" \
      || { echo "nao consegui devolver o material bruto a maquina"; exit 1; }
    echo "material bruto das sessoes anteriores devolvido"
  fi
  LANCAR="env AMBIENTE=aws SAIDA=\$HOME/resultados INTERVALO=$INTERVALO CARGA=$CARGA DURACAO=$DURACAO RODADAS=$RODADAS $PY_MAQ"
fi
# So o processo principal vai para segundo plano, com as tres saidas desligadas
# do SSH: assim o SSH volta na hora e as rodadas nao dependem dele.
"${SSH[@]}" "mkdir -p resultados && cd Pipelines-TCC || exit 1
  nohup $LANCAR > \$HOME/tcc-data/combinacao.txt 2>&1 < /dev/null &" || exit 1
echo "lancado; acompanhando a cada $((ACOMPANHAR_S / 60)) min"

# --- 5. Acompanhar ----------------------------------------------------------------
sleep 30
sem_resposta=0
while true; do
  estado=$("${SSH[@]}" "pgrep -f '$MARCA' >/dev/null && echo rodando || echo terminou" 2>/dev/null)
  case "$estado" in
    terminou) break ;;
    rodando)
      sem_resposta=0
      "${SSH[@]}" "grep -aE '^#####|^--- \[|RODADA (VALIDA|NAO VALE)' \$HOME/tcc-data/combinacao.txt | tail -2" \
        | sed "s/^/  ($(date +%H:%M)) /" ;;
    *)
      sem_resposta=$((sem_resposta + 1))
      echo "  ($(date +%H:%M)) a maquina nao responde ($sem_resposta/3)"
      [ "$sem_resposta" -ge 3 ] && { echo "DESISTI: a maquina sumiu (autodestruicao?)"; exit 1; } ;;
  esac
  sleep $ACOMPANHAR_S
done
passo "5. Terminou"
"${SSH[@]}" "tail -30 \$HOME/tcc-data/combinacao.txt"
