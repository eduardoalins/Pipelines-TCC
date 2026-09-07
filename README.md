# Pipelines-TCC

Artefatos do TCC **"Estudo Comparativo entre Arquiteturas Near Real-Time e
Streaming para Pipelines ELT Escaláveis"** — Eduardo Alves Lins, CESAR School,
2026.

O trabalho constrói **dois pipelines ELT equivalentes** sobre a mesma fonte de
dados e mede a diferença entre eles:

| Protótipo | Motor | Paradigma |
|---|---|---|
| NRT | Apache Spark Structured Streaming | micro-batch |
| Streaming | Apache Flink | evento a evento |

Ambos consomem o mesmo tópico Kafka, aplicam as mesmas transformações e escrevem
Parquet no mesmo layout. O que varia é o motor — é isso que torna a comparação
válida. As invariantes que garantem essa equivalência estão no `CONTRATO.md`.

---

## O que este documento é

**Um passo a passo para reconstruir o sistema inteiro em outra máquina**, do zero
até o pipeline rodando.

Ele não é um resumo do projeto: é a receita. Cada passo tem o comando, o que
esperar como saída e o que fazer quando não for isso. A ordem importa — nenhum
passo funciona sem o anterior.

O documento acompanha o desenvolvimento: à medida que as etapas avançam, novos
passos são acrescentados ao fim da sequência. Hoje ele vai até a **Etapa 4**
(o Spark lendo o tópico).

**Por que isso existe.** Reprodutibilidade é critério declarado do trabalho. Um
ambiente que só funciona porque comandos foram digitados uma vez, numa máquina
específica, não é resultado de pesquisa — é acidente.

---

## Pré-requisitos

| Item | Valor usado | Observação |
|---|---|---|
| Sistema | Windows 11 | O Linux puro também serve, pulando os passos 1 e 2 |
| WSL | WSL2 com Ubuntu 24.04 | `wsl --install -d Ubuntu` |
| Docker | Docker Desktop com integração WSL ligada | |
| RAM | 15,7 GB | Regra de dimensionamento no passo 1 |
| Disco | ~20 GB livres | Imagens, venv, jars do Maven e dados de saída |

A máquina de referência é um Dell G15 5520 (i5-12500H, 12 núcleos / 16 threads,
15,7 GB RAM). Nada aqui depende desse hardware, mas os números medidos sim.

---

## Passo 1 — Configurar a VM do WSL

O WSL, sem limite, reserva até metade da RAM e negocia com o Windows de forma
preguiçosa. No meio de um benchmark isso vira ruído na medição — e você não quer
descobrir depois que a variância dos seus resultados era o gerenciador de
memória do Windows.

No **PowerShell do Windows**, dentro do repositório clonado:

```powershell
git clone <url-do-repositorio>
cd Pipelines-TCC
Copy-Item env\wslconfig "$env:USERPROFILE\.wslconfig"
wsl --shutdown
```

O arquivo `env/wslconfig` é a configuração versionada e comentada. Ele fixa
`memory=4GB`, `processors=8` e `swap=0`.

> **Se a sua máquina tem outra quantidade de RAM**, ajuste pela regra derivada na
> Etapa 0: `WSL ≤ RAM_total − 10 GB`. O valor é um **teto, não uma reserva** — o
> WSL cresce sob demanda e devolve o que não usa. Ele funciona como freio de
> emergência: se o Spark exceder, ele falha com erro visível em vez de arrastar o
> Windows junto.

**Conferir**, na aba do Ubuntu:

```bash
free -h     # deve mostrar ~3,8 GiB
nproc       # deve mostrar 8
```

> **Terminal.** Use o **Windows Terminal** (`Win+R` → `wt`), não o aplicativo
> "Ubuntu" do menu Iniciar — aquele usa o console legado e abre em tela preta sem
> aceitar digitação, prendendo a distro inteira.

---

## Passo 2 — Criar o ambiente Python

Na aba do Ubuntu, dentro do repositório:

```bash
bash scripts/setup_env.sh
```

O script cria o venv em `~/.venvs/tcc`, cria as pastas de dados em `~/tcc-data/`
e diz o que falta caso algum pacote do sistema esteja ausente. É idempotente:
rodar de novo não quebra nada.

**Se ele reclamar de pacotes do sistema**, instale como root direto do Windows —
o `sudo` pede senha em execução não interativa e trava:

```powershell
wsl -d Ubuntu -u root -- apt-get update
wsl -d Ubuntu -u root -- apt-get install -y python3.12-venv openjdk-17-jdk-headless
```

**Instalar as dependências Python:**

```bash
~/.venvs/tcc/bin/pip install -r requirements.txt
```

> **Atenção ao caminho.** No PowerShell, `~` é `C:\Users\<usuário>`, e o venv vive
> em `/home/<usuário>/.venvs/tcc`, dentro do WSL. `~/.venvs/tcc/bin/pip` no
> PowerShell falha com `CommandNotFoundException`. **Regra prática:** aba do
> Ubuntu para Python, Kafka e Spark; PowerShell para o `git`.

---

## Passo 3 — Subir o Kafka

```bash
docker compose up -d
docker compose ps          # esperar o status "healthy", não "running"
```

O `healthcheck` pergunta ao próprio broker se ele responde. `running` no
`docker ps` significa apenas que o processo iniciou — um cliente que conectasse
nesse intervalo falharia.

**Conferir imediatamente onde os dados estão sendo gravados:**

```bash
docker exec kafka ls /var/lib/kafka/data   # deve conter meta.properties
```

> **Esta verificação não é opcional.** A imagem `apache/kafka` **declara**
> `/var/lib/kafka/data` como `VOLUME`, mas seu `log.dirs` padrão aponta para
> `/tmp/kafka-logs`. Sem o `KAFKA_LOG_DIRS` no Compose, o broker grava na camada
> gravável do contêiner: o tópico sobrevive a `docker restart` e morre em qualquer
> `docker compose down`, com o volume permanecendo vazio e **sem nenhum aviso**.
> Se esse diretório vier vazio, pare — nada adiante faz sentido.

**Se o `docker` sumir de dentro do WSL:** todo `wsl --shutdown` derruba a distro
`docker-desktop`, que não volta sozinha. Reabra o Docker Desktop e espere o
daemon.

Para parar: `docker compose down` mantém os dados; `docker compose down -v`
apaga o tópico e as mensagens.

---

## Passo 4 — Criar o tópico

```bash
bash scripts/create_topic.sh
```

Cria `ecommerce.events.v1` com **24 partições**, fator de replicação 1. O script
espera o broker responder antes de criar, é idempotente e **falha** se o número
de partições sair diferente do esperado.

**Por que 24 e por que nunca muda.** Partição é a unidade de paralelismo do
Kafka: cada uma é lida por no máximo um consumidor do grupo por vez. Com 24 é
possível varrer de 1 a 24 unidades de paralelismo no Experimento B sem tocar no
tópico — e tocar no tópico invalidaria tudo que já rodou, porque a distribuição
das chaves entre partições mudaria.

A criação automática de tópicos está **desligada** de propósito. Com ela ligada,
um erro de digitação no nome não falha: o Kafka cria um tópico novo com **uma**
partição, o pipeline lê vazio, o experimento roda com paralelismo 1 e o número
reportado está errado sem nenhum erro na tela.

---

## Passo 5 — Verificar a cadeia até o Spark

```bash
~/.venvs/tcc/bin/python scripts/check_chain.py
```

Sobe um Spark, resolve o conector do Kafka a partir do Maven, lê o tópico e
confirma que enxerga as 24 partições. Ele verifica a cadeia inteira:

```
Python 3.12.3 → PySpark 4.0.4 → JVM 17.0.20 → conector Scala 2.13
              → broker Kafka 4.0.2 → tópico com 24 partições
```

**Rode este passo antes de qualquer outra coisa envolvendo Spark.** O plano
aponta a incompatibilidade Spark ↔ conector como o motivo pelo qual projetos
deste tipo travam, e o erro típico — `ClassNotFoundException` — não indica a
causa real. Aqui ele aparece cedo e barato.

O conector **não** vem por `pip`: o Spark o resolve do Maven em tempo de execução
e guarda em cache no `~/.ivy2`. O sufixo `_2.13` precisa casar com a versão do
Spark (a linha 4.x usa Scala 2.13; a 3.x usava 2.12). A primeira execução baixa
os jars e demora; as seguintes usam o cache.

O aviso `NativeCodeLoader: Unable to load native-hadoop library` aparece em toda
instalação de Spark e não afeta medição.

---

## Passo 6 — Gerar eventos

```bash
~/.venvs/tcc/bin/python shared/data-generator/generator.py
```

Emite 100 eventos por segundo durante 60 segundos, no schema completo do
`CONTRATO.md`, com seed fixa.

**Saída esperada:** 6000 tentados, 6000 confirmados, 0 erros, duração ~60,0 s,
payload médio ~253,8 bytes.

A carga de 100 evt/s foi escolhida porque **a resposta certa é conhecida de
antemão**. Qualquer divergência é defeito, não resultado.

**Variar a carga sem editar arquivo:**

```bash
RATE=1000 DURATION=600 ~/.venvs/tcc/bin/python shared/data-generator/generator.py
```

O relatório separa **carga oferecida** (o que o gerador tentou emitir) de **carga
atingida** (o que o broker confirmou pelo callback de entrega). É a distinção
central da análise de escalabilidade: sem ela, corre-se o risco de medir o limite
do próprio gerador e chamar isso de limite do Spark.

Cada execução grava um manifesto em `~/tcc-data/metrics/<run_id>.gen<N>.json` com
os contadores, o último `seq`, as taxas e o payload médio. O programa **sai com
código 1** se `confirmados != tentados`, para que o arnês de benchmark perceba a
falha sem ninguém precisar ler a tela.

Parâmetros da carga e do produtor ficam em `shared/data-generator/config.yaml`,
nunca no código — é o mesmo arquivo que o pipeline Flink usará.

---

## Passo 7 — Conferir a distribuição das partições

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic ecommerce.events.v1 \
  | awk -F: '{n++; s+=$3; if(min==""||$3<min)min=$3; if($3>max)max=$3}
             END {printf "particoes=%d total=%d media=%.1f min=%d max=%d\n", n, s, s/n, min, max}'
```

**Esperado:** `particoes=24 total=6000 media=250.0`, com `min` e `max` entre ~180
e ~320. A referência medida na máquina original é **186 e 317**.

Esse desequilíbrio é previsto e declarado no `CONTRATO.md` §1.4: com 2000 chaves
uniformes em 24 partições, o desvio nos extremos é de ±25%. Ele não ameaça a
comparação porque o sorteio é determinístico com seed fixa — o desequilíbrio é
idêntico em toda execução e nos dois motores.

Se o `max` vier muito acima disso, o partitioner não está espalhando como se
espera e vale investigar antes de seguir.

---

## Passo 8 — Testar a persistência

```bash
docker compose down
docker compose up -d
docker compose ps          # esperar "healthy"
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic ecommerce.events.v1 \
  | awk -F: '{s+=$3} END {print "total apos restart:", s}'
```

**Tem que continuar dando 6000.** Se sumir, o `KAFKA_LOG_DIRS` não pegou —
volte ao passo 3.

Este teste existe porque o Experimento C reinicia o broker. Sem ele, um artefato
do contêiner seria reportado como perda de dados do Kafka.

---

## Passo 9 — Rodar o pipeline NRT (Spark)

Duas abas.

**Aba 1 — o pipeline:**

```bash
bash pipeline-nrt/run_local.sh
```

Imprime a configuração usada, resolve o conector e começa a consumir. Com
`startingOffsets=earliest`, o `Batch: 0` traz os eventos que já estão no tópico.
Depois o console fica quieto, porque não há dado novo chegando.

**Aba 2 — o gerador:**

```bash
~/.venvs/tcc/bin/python shared/data-generator/generator.py
```

Agora a aba 1 imprime **um lote a cada 5 segundos**, com ~500 eventos cada e a
coluna `partition` variando entre 0 e 23. Esse é o micro-batch visível: cinco
segundos de tela parada, depois o lote inteiro de uma vez. É a característica que
define o paradigma NRT deste trabalho e o que o diferencia do Flink.

A **Spark UI** fica em <http://localhost:4040>, aba *Structured Streaming* — é
onde aparecem o número de linhas por batch e a duração de cada um.

**Variar sem editar arquivo:**

```bash
TRIGGER="30 seconds" bash pipeline-nrt/run_local.sh
STARTING_OFFSETS=latest bash pipeline-nrt/run_local.sh
```

> **Por que existe um lançador em vez de `python main.py`.** Duas configurações
> precisam existir **antes** de a JVM do driver subir, e o código Python roda
> depois disso: `--driver-memory` (configurar `spark.driver.memory` no
> `SparkSession.builder` é aceito sem erro e **silenciosamente ignorado**) e
> `--packages`, que resolve o conector na inicialização. O `spark-submit` é quem
> lança a JVM, então é por ele que os dois passam.

---

## Verificação final

Se os nove passos acima funcionaram, o ambiente está reproduzido:

- [x] `free -h` mostra o teto configurado da VM
- [x] `docker compose ps` mostra o broker `healthy`
- [x] `docker exec kafka ls /var/lib/kafka/data` lista `meta.properties`
- [x] O tópico existe com 24 partições
- [x] `check_chain.py` fecha a cadeia até o Spark
- [x] O gerador reporta tentados = confirmados, 0 erros
- [x] As 24 partições recebem ~250 eventos cada
- [x] Os eventos sobrevivem a `down` → `up`
- [x] O Spark imprime um lote a cada 5 segundos

---

## Referência

### Onde as coisas moram

Decisão E6: **código no Windows, dados em ext4 nativo do WSL.**

| | Caminho |
|---|---|
| Código (este repo) | `/mnt/c/.../Pipelines-TCC` |
| Ambiente Python | `~/.venvs/tcc` |
| Parquet de saída | `~/tcc-data/out` |
| Checkpoints do Spark | `~/tcc-data/checkpoints` |
| Métricas coletadas | `~/tcc-data/metrics` |

O motivo é medido, não estético: escrita sequencial em `/mnt/c` roda a
**77 MB/s** contra **1,2 GB/s** no ext4 — 15,8×. Com a saída no `/mnt/c`, o
gargalo do pipeline seria o sistema de arquivos do Windows, e o Experimento A
estaria medindo drvfs em vez de Spark.

Nada de `~/tcc-data` entra no git. O que reproduz aqueles dados é o gerador mais
o `run_id` da execução.

### Protocolo de execução

Antes de **qualquer execução medida**, fechar Discord, Steam e navegadores. Não é
só memória: esses processos disputam CPU de forma imprevisível, e essa disputa
entra na variância que seria reportada como efeito do parâmetro em teste.

### Versões travadas (decisão E7)

Estes números fazem parte do método experimental. Trocar qualquer um deles no
meio dos experimentos invalida as execuções já feitas.

| | Versão | Verificado em |
|---|---|---|
| Kafka | 4.0.2 (KRaft) | 03/09/2026 |
| Spark / PySpark | 4.0.4 (Scala 2.13) | 03/09/2026 |
| Python | 3.12.3 | 03/09/2026 |
| JVM | OpenJDK 17.0.20 | 03/09/2026 |
| confluent-kafka | 2.15.0 (librdkafka 2.15.0) | 04/09/2026 |

### Estrutura

```
CONTRATO.md               schema e invariantes de comparabilidade   (Etapa 2)
docker-compose.yml        Kafka 4.0.2 em modo KRaft, um broker
requirements.txt          versões travadas (decisão E7)
env/wslconfig             configuração da VM, versionada e comentada
scripts/setup_env.sh      recria o ambiente do zero
scripts/create_topic.sh   cria o tópico com 24 partições
scripts/check_chain.py    verifica Python → Spark → JVM → Kafka
shared/                   ARNÊS COMPARTILHADO — Spark e Flink usam igual
  data-generator/         gerador de eventos                       (Etapa 3)
  analysis/               reconcile.py e report.py                 (Etapa 6)
  monitoring/             collector.py, amostra consumer lag       (Etapa 6)
  reference-data/         products.parquet e seu gerador           (Etapa 7)
pipeline-nrt/             pipeline Spark Structured Streaming      (Etapa 4+)
  run_local.sh            lançador via spark-submit
  src/config.py           trigger, endereços e recursos
  src/main.py             o job
pipeline-streaming/       pipeline Flink                           (OE 2)
benchmarks/               run_experiment.sh                        (Etapa 8)
infra/                    Terraform                                (Etapa 10)
results/                  CSVs e manifestos por run_id             (Etapa 11)
```

A pasta `shared/` é fisicamente separada dos dois pipelines de propósito: é o que
impede que o Flink acabe medido com um arnês ligeiramente diferente.

### Documentos de pesquisa

Por decisão do autor, os documentos ficam **fora** deste repositório, junto com a
dissertação, em `../../Informações/` e `../../Fala/`:

| Documento | Papel |
|---|---|
| `planoNRT3.md` | plano de implementação vigente — as 12 etapas e o protocolo experimental |
| `EtapaNRT/DecisaoEtapaX.md` | registro de cada etapa: o que foi feito, como, e as decisões com a justificativa |
| `Fala/PontosSensiveis.md` | o que a banca pode cobrar, com a resposta e a evidência de cada ponto |
| `perguntas.txt` | registro das decisões de projeto, cada uma com a justificativa |
| `planoNRT.md`, `planoNRT2.md` | versões anteriores, mantidas por rastreabilidade |

Os `DecisaoEtapaX.md` são a matéria-prima do capítulo de metodologia. Eles
registram também o **percurso** das decisões que mudaram no caminho — e as
correções, quando algo que se acreditava verdadeiro não era.

O repositório guarda o que **executa**; a pasta de pesquisa guarda o que
**justifica**. O `CONTRATO.md` é a ponte entre os dois.

---

## Progresso

- [x] **Etapa 0** — Ambiente: WSL2 4 GB / 8 CPUs, Docker, OpenJDK 17.0.20,
      Python 3.12.3
- [x] **Etapa 1** — Kafka 4.0.2 em KRaft, tópico `ecommerce.events.v1` com 24
      partições, versões travadas e cadeia verificada até o Spark
- [x] **Etapa 2** — `CONTRATO.md` congelado: schema, chave da mensagem,
      watermark, retenção, layout do destino e invariantes
- [x] **Etapa 3** — Gerador a 100 evt/s: 6000 tentados, 6000 confirmados,
      0 erros, payload médio de 253,8 bytes
- [x] **Etapa 4** — Spark lê do Kafka e imprime no console: 15 micro-batches a
      cada ~5 s, as 24 partições lidas, garantia chave → partição confirmada
- [ ] **Etapa 5** — Escrita em Parquet com checkpoint
- [ ] **Etapa 6** — Instrumento de medição
- [ ] **Etapa 7** — Enriquecimento
- [ ] **Etapa 8** — Arnês de benchmark
- [ ] **Etapa 9** — Validação local e caos
- [ ] **Etapa 10** — Terraform e AWS
- [ ] **Etapa 11** — Benchmarks oficiais
- [ ] **Etapa 12** — Análise e guia de decisão
