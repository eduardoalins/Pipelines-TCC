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

Ambos consomem o mesmo tópico Kafka e escrevem os mesmos eventos em Parquet, no
mesmo layout. O que varia é o motor — é isso que torna a comparação válida. As
invariantes que garantem essa equivalência estão no `CONTRATO.md`.

**Os eventos são reais** (desde o `CONTRATO.md` 3.0, 23/10/2026): 42,4 milhões de
eventos de uma loja online, de outubro de 2019, da base pública *eCommerce behavior
data from multi category store* (REES46, no
[Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)).
Um **replayer** os envia ao Kafka na taxa pedida pelo experimento.

Os pipelines são **ELT estrito**: extraem e carregam, sem transformar. Não há
join nem limpeza dentro deles; a transformação acontece depois, no destino, é a
mesma para os dois motores e fica fora da comparação. O que o trabalho compara é
o **E** e o **L**. Os campos do evento são carregados **como texto**, do jeito
que chegam (passo 10).

---

## O que este documento é

**Um passo a passo para reconstruir o sistema inteiro em outra máquina**, do zero
até o pipeline rodando.

Ele não é um resumo do projeto: é a receita. Cada passo tem o comando, o que
esperar como saída e o que fazer quando não for isso. A ordem importa — nenhum
passo funciona sem o anterior.

O documento acompanha o desenvolvimento: à medida que as etapas avançam, novos
passos são acrescentados ao fim da sequência. É **uma receita só para os dois
motores** — quem reproduz o trabalho precisa de ambos, e duas receitas separadas
tenderiam a divergir.

| Passos | O que reconstroem | Até onde chegou |
|---|---|---|
| 1 a 5 | ambiente, Kafka, tópico — **compartilhados** (o passo 5 constrói a imagem do Spark) | Etapas 0 a 3 |
| antes do 6 | **a base de eventos reais**: baixar, conferir e descompactar | Etapa 9.1 |
| 6 a 8 | o replayer, a distribuição nas partições e a persistência | Etapas 3 e 9.1 |
| 9 a 11 | pipeline NRT (Spark), em contêiner | Etapa 5 — Parquet com checkpoint |
| 12 a 16 | pipeline Streaming (Flink), em contêiner | Etapa RT-3 — Parquet com checkpoint |
| antes do 17 | **conferir o relógio** — obrigatório antes de qualquer medição | Etapa 6.2 |
| 17 a 20 | instrumento de medição, nos dois motores | Etapa 6 / RT-4 — lag, reconciliação e latências |
| 21 | eventos malformados — **aposentado** no contrato 3.0; fica o registro do que foi medido | Etapa 7 / RT-5, revista na 8.1 |
| 22 a 24 | o benchmark: uma rodada, as dez rodadas de uma combinação e os arquivos de resultado | Etapa 8 / RT-6 |
| 25 | a bateria local: as seis combinações | Etapa 9 / RT-7 |
| 26 | as ferramentas e as credenciais da nuvem | Etapa 10 — em andamento |
| 27 | criar e destruir a máquina na AWS, com o Terraform | Etapa 10 — em andamento |
| 28 | preparar a máquina da AWS: Docker, imagens, Kafka e conferências | Etapa 10 — em andamento |

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

O script cria o venv em `~/.venvs/tcc`, cria as pastas de dados em `~/tcc-data/`,
instala as dependências e diz o que falta caso algum pacote do sistema esteja
ausente. É idempotente: rodar de novo não quebra nada.

**Se ele reclamar de pacotes do sistema**, instale como root direto do Windows —
o `sudo` pede senha em execução não interativa e trava:

```powershell
wsl -d Ubuntu -u root -- apt-get update
wsl -d Ubuntu -u root -- apt-get install -y python3.12-venv
```

**O que este venv é.** As ferramentas que ficam **fora** dos motores: o replayer, o
coletor de lag e o reconciliador. É o que o `requirements.txt` da raiz instala
(`confluent-kafka`, `PyYAML`, `duckdb`). O Spark e o Flink **não** estão nele:
cada um roda no próprio contêiner, com o próprio `requirements.txt` (passos 5 e
12).

**O host não precisa de Java.** A JVM dos dois motores vem dentro das imagens,
travada pelo digest. Não foi sempre assim: até 10/10/2026 o Spark rodava no host,
pelo venv, com o OpenJDK do Ubuntu — e em 24/09/2026 o `unattended-upgrades`
trocou esse Java de 17.0.20 para 17.0.20.1 **com o Spark rodando**, o que derrubou
o job (`Incorrect Java version … jspawnhelper`). Um Java que o sistema atualiza
sozinho não pode fazer parte de uma medição; é um dos motivos de a JVM estar hoje
na imagem.

> **Atenção ao caminho.** No PowerShell, `~` é `C:\Users\<usuário>`, e o venv vive
> em `/home/<usuário>/.venvs/tcc`, dentro do WSL. `~/.venvs/tcc/bin/python` no
> PowerShell falha com `CommandNotFoundException`. **Regra prática:** aba do
> Ubuntu para Python, Docker e os pipelines; PowerShell para o `git`.

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

## Passo 5 — Construir a imagem do Spark e verificar a cadeia

O Spark roda num contêiner, como o Flink (passo 12).

```bash
docker compose build spark
docker images tcc-spark        # ~937 MB
docker compose run --rm spark python scripts/check_chain.py
```

O `docker compose up -d` do passo 3 **não** sobe o Spark — ele está atrás de um
perfil no `docker-compose.yml` e roda só quando chamado. O `run --rm` cria o
contêiner, executa e o apaga ao terminar.

O `check_chain.py` sobe um Spark, carrega o conector do Kafka, lê o tópico e
confirma que enxerga as 24 partições. Ele verifica a cadeia inteira:

```
Python 3.10.12 → PySpark 4.0.4 → JVM Temurin 17.0.20+8 → conector Scala 2.13
               → broker Kafka 4.0.2 (kafka:29092) → tópico com 24 partições
```

**Rode este passo antes de qualquer outra coisa envolvendo Spark.** O plano
aponta a incompatibilidade Spark ↔ conector como o motivo pelo qual projetos
deste tipo travam, e o erro típico — `ClassNotFoundException` — não indica a
causa real. Aqui ele aparece cedo e barato.

**Por que um contêiner.** Até 10/10/2026 o Spark rodava no host, pelo venv, e só o
Flink em contêiner. Quando as métricas medidas localmente passaram a entrar no
trabalho, essa assimetria deixou de ser aceitável: eram JVMs de fornecedores
diferentes, Pythons diferentes e caminhos de rede diferentes até o Kafka. A
vazão de disco e a leitura do Kafka foram medidas pelos dois caminhos antes da
mudança, sem lado sistematicamente mais lento.

**Por que Spark 4.0.x.** O 4.0 está em campo desde meados de 2025 e tem comunidade
grande o bastante para que um erro colado no Google encontre resposta; o 4.2 é
recente demais para isso. A linha 3.5 tem mais tutoriais, mas o suporte dela a
Python 3.12 — o do host, onde o Spark rodava quando a versão foi travada — era
incerto.

**O que a imagem contém**, e por que nessa forma:

- **A mesma imagem base do Flink**, travada pelo mesmo digest: Java Temurin
  17.0.20+8 sobre Ubuntu 22.04. A JVM é a mesma nos dois motores.
- **O conector do Kafka já está dentro dela.** Ele **não** vem por `pip`: são
  jars do Maven, `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4`. O build os
  resolve uma vez; em execução eles entram por `--jars`, e nenhuma execução toca
  na internet. O sufixo `_2.13` precisa casar com a versão do Spark (a linha 4.x
  usa Scala 2.13; a 3.x usava 2.12).
- **São 11 jars**, e o build grava o sha256 de cada um em
  `/opt/spark-ivy/jars.sha256`. O conector arrasta 10 dependências, entre elas o
  `kafka-clients` 3.9.1 — o cliente que o Spark usa **não** é da mesma versão do
  broker (4.0.2). É normal, o protocolo do Kafka é compatível entre versões, mas
  essas versões participam da medição e são fixadas indiretamente, pela versão do
  conector.
- **As dependências estão travadas.** O `pipeline-nrt/requirements.txt` fixa o
  PySpark e o cliente Kafka; o `requirements.lock` fixa também as dependências
  indiretas.
- **Os pacotes do Ubuntu também** (desde 21/10/2026). O `apt` da imagem lê o
  repositório **congelado** em 10/10/2026, 20:00 UTC, no arquivo histórico do
  Ubuntu (`snapshot.ubuntu.com`), e o `python3`, o `python3.10` e o `pip` entram
  pelo número da versão. Sem isso, um build futuro instalaria as versões do dia;
  com isso, qualquer build sai com os pacotes das imagens da bateria local,
  construídas dez minutos depois daquela data. Só fixar o número não bastaria:
  o repositório normal guarda apenas a versão mais recente, e o pedido falharia
  na primeira atualização do Ubuntu.

> **Editar um arquivo copiado para a imagem reconstrói as camadas seguintes** —
> mesmo que a mudança seja um comentário no `requirements.txt`. O resultado deve
> sair igual, porque as versões estão travadas, mas é outra imagem: depois de
> reconstruir, repita este passo antes de medir.

O aviso `NativeCodeLoader: Unable to load native-hadoop library` aparece em toda
instalação de Spark e não afeta medição.

---

## Antes do passo 6 — a base de eventos reais

Desde o `CONTRATO.md` 3.0 (23/10/2026, pedido da orientação), os eventos são
**reais**: a base *eCommerce behavior data from multi category store*, eventos de
uma loja online multicategoria coletados pela REES46 e publicados no Kaggle por
Michael Kechinov. O trabalho usa **outubro de 2019**: 42.448.764 eventos.

```bash
bash scripts/preparar_base.sh
```

O script baixa o `2019-Oct.csv.gz` (1,7 GB) pelo link direto da REES46 — sem login
— se ele ainda não estiver em `~/tcc-data/base`; confere o **sha256**; descompacta
para `2019-Oct.csv` (5,7 GB, mantendo o `.gz` ao lado); e confere a contagem. É
idempotente.

**Esperado** (máquina original, 23/10/2026):

```
>> 2. Checksum
OK: 8ebca1ad741295297368f2cf0315e3f36853a1a11768fb16babf8c9b83838147
>> 4. Contagem
OK: 42448764 eventos
>> Base pronta
  tamanho ... 5668612855 bytes
```

O `.csv` tem **exatamente o tamanho** que o Kaggle informa para o `2019-Oct.csv`:
o arquivo da REES46 é o mesmo. Quem preferir baixar pelo Kaggle (pede login) põe o
`.gz` em `~/tcc-data/base` e roda o script: o checksum é o que vale.

> **Citação obrigatória.** A base é de uso livre com a condição de citar a página
> do Kaggle e a REES46 (https://rees46.com).

> **A base nunca é alterada.** O replayer só lê o `.csv`. O que vira mensagem, e
> como, está no `CONTRATO.md` §1.5.

---

## Passo 6 — Enviar eventos reais ao Kafka

```bash
~/.venvs/tcc/bin/python shared/data-generator/replayer.py
```

Envia **100 eventos por segundo durante 60 segundos**, lidos da base real: uma
**janela** de 6000 linhas consecutivas, na ordem original, cujo início é sorteado
pela seed do `config.yaml` (42).

**Saída esperada** (23/10/2026):

```
carga ........... 100 evt/s por 60s = 6000 eventos
janela .......... linhas 27139042 a 27145041 (seed 42)
posicionamento .. 5.0s ate a linha 27139042
...
tentados ................ 6000
confirmados pelo broker . 6000
carga oferecida ......... 100.0 evt/s
carga atingida .......... 100.0 evt/s
payload medio ........... ~281 bytes
```

O payload médio depende também do tamanho do `run_id`, que vai em toda mensagem:
pelo script de rodada (passo 22), com `run_id` mais longo, ele foi de 294,0 bytes;
chamado assim, sozinho, de ~281.

A janela de 6000 eventos com a seed 42 é **sempre a mesma**: linhas 27.139.042 a
27.145.041 da base, um trecho de 20/10/2019 por volta das 12:47. O
**posicionamento** — avançar no arquivo até o início da janela — acontece antes de
o envio começar e leva alguns segundos (no pior caso, ~9 s).

A carga de 100 evt/s foi escolhida porque **a resposta certa é conhecida de
antemão**. Qualquer divergência é defeito, não resultado.

**Cada mensagem é uma linha da base**, com os valores da loja como vieram, mais os
campos que o replayer carimba para a medição:

```
{"event_id":"2019-10:00000005","seq":4,"gen_id":0,"run_id":"…","event_type":"view",
 "user_id":535871217,"product_id":1004237,"amount":"1081.98",
 "session_id":"c6bd7419-…","event_ts":1792808350162,"event_time":"2019-10-01 00:00:04 UTC"}
```

- `event_id` é o **endereço da linha** na base (o 5º evento de outubro);
- `event_ts` é o **instante do envio**, de onde saem todas as latências;
  `event_time` é o instante **original**, intacto;
- o preço (`amount`) vai **entre aspas, exatamente como na base**, para nenhum motor
  reescrevê-lo (`CONTRATO.md` §2.7).

**Variar a carga e a janela sem editar arquivo:**

```bash
RATE=1000 DURATION=600 SEED=43 ~/.venvs/tcc/bin/python shared/data-generator/replayer.py
```

No benchmark quem escolhe a carga, a duração e a seed é o script de rodada (passo
22), não você. **O ritmo é do experimento, não da loja**: a loja teve, em média, 16
eventos por segundo; o replayer acelera a base, mantendo a ordem.

O relatório separa **carga oferecida** (o que o replayer tentou enviar) de **carga
atingida** (o que o broker confirmou pelo callback de entrega). É a distinção
central da análise de escalabilidade: sem ela, corre-se o risco de medir o limite
do próprio replayer e chamar isso de limite do Spark.

Cada execução grava um manifesto em `~/tcc-data/metrics/<run_id>.gen<N>.json` com
os contadores, o último `seq`, as taxas, o payload médio, **a base e a janela**. O
programa **sai com código 1** se `confirmados != tentados`.

Parâmetros do Kafka e do produtor ficam em `shared/data-generator/config.yaml`,
lido pela mesma função que o gerador antigo usava.

> **O gerador sintético (`generator.py`) continua no repositório, sem uso.** Ele
> produziu tudo até o contrato 2.2, inclusive a bateria local de 12–13/10/2026, e
> fica para que aquilo siga reproduzível (`git checkout 013b646`). Com o esquema
> atual, os eventos dele não têm o `event_time` e o `reconcile.py` os contaria como
> inválidos: **não o use nos passos seguintes**.

---

## Passo 7 — Conferir a distribuição das partições

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic ecommerce.events.v1 \
  | awk -F: '{n++; s+=$3; if(min==""||$3<min)min=$3; if($3>max)max=$3}
             END {printf "particoes=%d total=%d media=%.1f min=%d max=%d\n", n, s, s/n, min, max}'
```

**Esperado:** `particoes=24 total=6000 media=250.0`.

> **A remedir com a base real.** O `min` e o `max` abaixo foram medidos com o
> gerador sintético (2000 usuários uniformes): **186 e 317**. Com os usuários reais
> da base (3 milhões, concentração baixa — `CONTRATO.md` §1.4) a expectativa é um
> espalhamento parecido ou mais uniforme; o valor de referência será medido de novo
> no começo da bateria local.

O desequilíbrio entre partições é declarado no `CONTRATO.md` §1.4. Ele não ameaça
a comparação: a rodada N recebe a **mesma janela** da base nos dois motores, então
a distribuição é idêntica para o Spark e para o Flink (passo 22).

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

O lançador chama o contêiner do Spark; o código é montado do repositório, então
**editar o código não exige reconstruir a imagem**.

Imprime a configuração usada, avisa se o checkpoint é novo ou está sendo
retomado, e começa a consumir. Com `startingOffsets=earliest`,
o `[batch 0]` engole de uma vez os eventos que já estão no tópico — devem ser os
6000 do passo 6. Depois fica quieto, porque não há dado novo chegando.

> **O primeiro lote é sempre atípico.** Ele processa o backlog inteiro com a JVM
> ainda fria, então costuma estourar a janela do trigger e emitir
> `Current batch is falling behind`. É o que a regra de aquecimento do protocolo
> manda descartar. Se esse aviso aparecer de forma **persistente** durante uma
> execução medida, aí sim é sinal de saturação.

**Aba 2 — o replayer:**

```bash
~/.venvs/tcc/bin/python shared/data-generator/replayer.py
```

Agora a aba 1 imprime **um lote a cada 5 segundos**, com ~500 eventos cada
(100 evt/s × 5 s). Esse é o micro-batch visível: cinco segundos de tela parada,
depois o lote inteiro de uma vez. É a característica que define o paradigma NRT
deste trabalho e o que o diferencia do Flink.

A soma dos lotes tem que fechar em **6000**. A oscilação em torno de 500 é efeito
de fronteira — evento produzido a milissegundos do corte do trigger cai no lote
seguinte.

A **Spark UI** fica em <http://localhost:4040>, aba *Structured Streaming* — é
onde aparecem o número de linhas por batch e a duração de cada um.

**Variar sem editar arquivo:**

```bash
TRIGGER="30 seconds" bash pipeline-nrt/run_local.sh
STARTING_OFFSETS=latest bash pipeline-nrt/run_local.sh
```

> **`DRIVER_MEMORY` não é só uma variável.** O contêiner tem teto de 1536 MB
> (`mem_limit`, no `docker-compose.yml`), dimensionado para o heap de 1 GB. Um
> heap maior exige subir o teto — e o teto também escolhe o coletor de lixo da
> JVM: com 1536 MB ela usa o Serial, com 2 GB usaria o G1. Os dois motores rodam
> com o Serial, por decisão de 10/10/2026; mudar o teto de um só troca o coletor
> em silêncio.

Para parar: `Ctrl+C`. O código de saída será **130** — a convenção do shell para
"terminado por SIGINT". Isso é o esperado, não erro: o sinal chega à JVM e ao
Python, e a JVM encerra por ele. O job só terá um código de saída com significado
quando ganhar uma condição de parada própria, na Etapa 8.

> **Às vezes o `Ctrl+C` suja a tela.** O sinal chega aos dois processos, e quem
> reage primeiro varia. Pode sair uma pilha de erro do Python
> (`Cannot call methods on a stopped SparkContext`) ou várias linhas
> `ERROR Inbox: Ignoring error … RejectedExecutionException`, do Java. Nos dois
> casos nada se perde: o checkpoint e os arquivos já gravados ficam íntegros.

Dois avisos aparecem em execução normal e são inofensivos nesta carga:
`Total allocation exceeds 95.00% of heap memory… Scaling row group sizes`, do
escritor de Parquet (8 escritores simultâneos reservando memória), e o
`Current batch is falling behind` do primeiro lote, já comentado acima.

> **Por que existe um lançador em vez de `python main.py`.** Duas coisas precisam
> existir **antes** de a JVM do driver subir, e o código Python roda depois
> disso: `--driver-memory` (configurar `spark.driver.memory` no
> `SparkSession.builder` é aceito sem erro e **silenciosamente ignorado**) e os
> jars do conector, que entram por `--jars`. O `spark-submit` é quem lança a JVM,
> então é por ele que os dois passam. O `run_local.sh` sobe o contêiner; lá
> dentro, quem chama o `spark-submit` é o `pipeline-nrt/submit.sh`.

---

## Passo 10 — Conferir a saída em Parquet

Com o job rodando, numa segunda aba:

```bash
find ~/tcc-data/out/spark -type d | sort
find ~/tcc-data/out/spark -name "*.parquet" | wc -l
du -h --max-depth=1 ~/tcc-data/out/spark/events
```

O destino é `~/tcc-data/out/spark`: desde o `CONTRATO.md` 1.3, cada motor tem o
próprio `<base>`, porque os dois leem o mesmo tópico e, sem isso, gravariam nas
mesmas pastas.

A estrutura deve ser `events/run_id=…/dt=…/hh=…/`, o layout do `CONTRATO.md` §4.
Dois `hh=` significam que a execução cruzou a virada de hora — previsto, é o preço
de particionar por `event_ts`.

**As colunas do evento são texto** (`CONTRATO.md` §2.7, desde 12/10/2026), nos
dois motores: o pipeline grava o que chegou, e a conversão para os tipos do
contrato acontece depois do Load. Desde o contrato 3.0 são **onze**: o
`event_time`, o instante original da base, entrou no fim, e os dois pipelines o
carregam sem nenhuma mudança no código — eles leem a lista de campos do
`shared/schema/evento.json` (verificado em 23/10/2026). O `run_id` não fica dentro
do arquivo: é o nome da pasta. `dt` e `hh` continuam vindo do
`event_ts`, mas só quando ele é um inteiro de milissegundos; senão a linha vai para
a pasta de data nula. As colunas que o pipeline acrescenta (`processed_ts`,
`batch_id`) mantêm o tipo delas.

**O número de arquivos é um resultado, não um defeito.** Cada micro-batch escreve
um arquivo por partição do Kafka, porque não há `repartition` (decisão E3). Um
lote único produz 24 arquivos; a mesma quantidade de eventos entregue em treze
lotes produz ~312. A referência medida na máquina original, para 6000 eventos:

| Forma de entrega | Arquivos | Tamanho |
|---|---|---|
| Lote único (backlog) | 24 | 632 KB |
| Trigger de 5 s | ~300 | ~3,6 MB |

Os tamanhos são de setembro, com os campos ainda tipados e eventos sintéticos;
**a remedir com a base real** (a mensagem real tem ~294 bytes, contra ~254 do
gerador antigo).

É o trade-off latência-versus-custo: trigger curto entrega mais rápido, gera mais
arquivos, e mais arquivos custam mais PUTs no S3 e mais espaço. O efeito é
acentuado a 100 evt/s, onde cada arquivo fica com ~19 linhas; nas cargas de
benchmark os arquivos têm milhares de linhas e o custo fixo amortiza.

Não deve existir `_spark_metadata` no destino — só `_SUCCESS`. A escrita é feita
em `foreachBatch`, e o escritor de lote não gera aquele diretório.

---

## Passo 11 — Testar a retomada

Este é o critério de pronto do pipeline com checkpoint.

Pare o job com `Ctrl+C`, anote o número do último batch, e suba de novo:

```bash
bash pipeline-nrt/run_local.sh
```

Agora, em vez de `>> checkpoint novo`, deve aparecer uma moldura de `!!!!`
avisando que há checkpoint existente e qual foi o último micro-batch. E **nenhuma
linha de eventos gravados** — se ele reprocessasse, o destino ganharia dados
duplicados.

Silêncio, aqui, é o resultado correto. Confirme que ele não travou gerando mais
eventos: os lotes devem voltar a aparecer.

> **Se você zerar o Kafka, zere o checkpoint junto.** O `~/tcc-data` não é volume
> do Docker, então `docker compose down -v` não o toca. Com o tópico recriado e os
> offsets em zero, o checkpoint continuaria apontando para offsets que não existem
> mais, e o job **falharia na subida** — `failOnDataLoss` está em `true` de
> propósito. O par correto é:
>
> ```bash
> docker compose down -v && rm -rf ~/tcc-data/checkpoints/nrt
> ```

---

## Passo 12 — Construir a imagem do Flink

Daqui em diante, o pipeline Streaming. Como o Spark, ele roda num contêiner.

**Por que um contêiner.** O Flink escolhido é o **1.20.5**, a versão LTS — e a
linha com mais material de consulta, já que o Flink 2.0 removeu APIs e boa parte
dos tutoriais existentes mira a 1.x. Mas o 1.20 não suporta Python 3.12, e o venv
do projeto é 3.12.3. O contêiner traz o próprio Python (3.10), e o venv do host
fica intocado: nada de PPA de terceiros, nada de segundo ambiente. Foi o Flink
que foi para contêiner primeiro, por esse motivo; o Spark o seguiu em
10/10/2026, por outro (passo 5).

```bash
docker compose build flink
docker images tcc-flink        # ~977 MB
```

A primeira construção leva cerca de dois minutos, quase todos no `pip install`.

**O que a imagem contém**, e por que nessa forma:

- **Só Java como base, não a imagem oficial do Flink.** A oficial já traz uma
  instalação do Flink, e o `pip install apache-flink` traz outra, embutida no
  pacote Python. Duas instalações do mesmo motor na mesma imagem é armadilha.
  Aqui existe uma só — e ela chega do mesmo jeito que o Spark chega na imagem
  dele, dentro do pacote Python.
- **A JVM é a Temurin 17.0.20+8**, e a imagem base está travada pelo **digest**,
  não só pela tag — a tag `17.0.20_8-jre-jammy` foi republicada em 18/09/2026, e
  tag nenhuma é imutável. A imagem do Spark parte da mesma base, pelo mesmo
  digest: a JVM é a mesma nos dois motores. (Até 10/10/2026 o Spark rodava no
  host com o OpenJDK 17.0.20.1 do Ubuntu — a mesma linha, de outro fornecedor.)
- **O conector Kafka é baixado e verificado no build.** Diferente do Spark, cujo
  build resolve o conector e as dependências dele pelo Maven, o Flink precisa
  que o jar seja obtido e o caminho declarado. O `Dockerfile` baixa o `flink-sql-connector-kafka-3.4.0-1.20`
  e confere o SHA-1 publicado no Maven Central; se não bater, o build **falha**.
- **As dependências estão travadas.** O `requirements.txt` fixa o PyFlink; o
  `requirements.lock` fixa **todas** as dependências dele, que de outro modo o
  `pip` escolheria de novo a cada build.
- **Os pacotes do Ubuntu também**, pelo mesmo arquivo histórico do Spark (passo
  5): o repositório congelado em 10/10/2026, 20:00 UTC, e o `python3`, o
  `python3.10` e o `pip` pelo número.

> **As imagens da bateria local são anteriores a esse congelamento.** Elas foram
> construídas em 10/10/2026 com o `apt` do dia — que, naquele dia, era o mesmo
> do arquivo histórico. Os IDs delas (`ec241f18cf24` e `7d6d36c04457`) só voltam
> pelo cache de build daquela época; um build com o `Dockerfile` atual gera
> outro ID, com o mesmo conteúdo. Não apague as imagens locais: elas são o
> registro do que rodou na bateria.

> **O "OK" do checksum não aparece na tela.** O BuildKit recolhe a saída dos
> passos que dão certo. Chegar ao `✔ flink Built` já prova que o checksum bateu.
> Para ver com os próprios olhos:
>
> ```bash
> docker run --rm tcc-flink:1.20.5 sha1sum /opt/flink-connectors/flink-sql-connector-kafka-3.4.0-1.20.jar
> ```
>
> Tem que sair `266330f8f26fec4957339b5ff31aa67b81c31462`.

---

## Passo 13 — Verificar a cadeia até o Flink

O espelho do passo 5. Precisa do Kafka no ar e do tópico **povoado** (passo 6):

```bash
docker compose run --rm flink python scripts/check_chain_flink.py
```

O `docker compose up -d` dos passos anteriores **não** sobe o Flink — ele está
atrás de um perfil no `docker-compose.yml` e roda só quando chamado. O `run --rm`
cria o contêiner, executa e o apaga ao terminar.

**Esperado:**

```
Python   : 3.10.12
PyFlink  : 1.20.5
Java     : 17.0.20
Conector : flink-sql-connector-kafka-3.4.0-1.20.jar
Broker   : kafka:29092

mensagens no topico      : 6000
particoes com dado lido  : 24
...
CADEIA COMPLETA OK
```

**Esta verificação é mais forte que a do Spark.** O `check_chain.py` conta as
partições *atribuídas*; esta conta as partições **de onde chegou dado** — prova
que o Flink leu de cada uma das 24. Por isso exige o tópico povoado: com ele
vazio, falha avisando para rodar o replayer (passo 6).

**O broker é `kafka:29092`, não `localhost:9092`.** O contêiner está na rede do
Docker e usa o listener interno. O Spark usa o mesmo caminho desde que passou
para contêiner; quem continua em `localhost:9092` são as ferramentas do host — o
gerador e o coletor.

O aviso de `pkg_resources` depreciado que aparece no topo vem de dentro do
`apache-beam`, uma dependência do PyFlink. É inofensivo: o `setuptools` vem do
apt do Ubuntu 22.04 e não será atualizado para a versão que removeu a API.

**Memória.** Para medir, amostre numa segunda aba enquanto a verificação roda —
o contêiner vive só alguns segundos, e um `docker stats` comum costuma não pegá-lo:

```bash
while true; do docker stats --no-stream --format '{{.Name}}  {{.MemUsage}}' | grep -E 'flink|kafka'; sleep 1; done
```

Referência medida na máquina original: pico de **~830 MiB** no Flink e
**~490–570 MiB** no Kafka — cerca de 1,4 GB somados, dentro do teto de 4 GB do WSL.

---

## Passo 14 — Rodar o pipeline Streaming (Flink)

O espelho do passo 9. O job lê o tópico, lê o JSON com os campos como texto
(passo 10) e grava Parquet em `~/tcc-data/out/flink`, com checkpoint a cada 5 s.

```bash
bash pipeline-streaming/run_local.sh
```

O lançador chama o contêiner do Flink; o código é montado do repositório, então
**editar o código não exige reconstruir a imagem** — só mudanças no `Dockerfile`
exigem.

Esperado: o resumo, com destino, checkpoint e os quatro jars; `>> checkpoint
novo`; e `>> job no ar. Checkpoint a cada 5 s`.

**Depois disso, uma linha por checkpoint completo** (desde a RT-4):

```
[checkpoint 4] completo  duracao=25 ms  estado=13280 bytes
```

O Flink não tem lote, logo não tem onde imprimir "terminei um bloco" como o
`[batch N]` do Spark. O `main.py` consulta a REST API do próprio Flink a cada
segundo e anuncia cada checkpoint — que é o momento em que os arquivos ficam
visíveis (passo 15). As linhas `Got brand-new compressor [.snappy]` que aparecem
de passagem são escritores de Parquet sendo abertos, uma por subtarefa.

> **A versão que imprime os eventos na tela** — a da Etapa RT-2, onde se vê cada
> evento atravessar o job assim que chega, com o `seq` em sequência vindo de
> partições diferentes — está no histórico do git, no commit da RT-2. Ela mostra
> a diferença de modelo em relação ao Spark melhor do que qualquer arquivo.

**Para parar:** `Ctrl+C`. O encerramento é limpo:

```
>> cancelando o job...
>> encerrado pelo usuario.
```

com código de saída **130**. É o mesmo código do Spark, mas por outro caminho: no
Spark a JVM morre pelo sinal antes de qualquer limpeza; no Flink a JVM é filha do
processo Python, que cancela o job de forma ordenada.

**Variar sem editar arquivo:**

```bash
STARTUP_MODE=latest-offset bash pipeline-streaming/run_local.sh
CHECKPOINT_INTERVAL="30 s" bash pipeline-streaming/run_local.sh
```

**O que a imagem precisa para gravar Parquet**, e que o Spark não precisou: o
PyFlink não traz o formato Parquet, e o formato não traz o Hadoop. Os três jars —
`flink-sql-parquet` 1.20.5 e o `hadoop-client-api`/`runtime` 3.4.1, o mesmo Hadoop
que o Spark carrega — são baixados e verificados no build. E o contêiner roda como
o **seu** usuário (uid 1000), para os arquivos saírem com o dono certo no host.

---

## Passo 15 — Ver a visibilidade por checkpoint

Este é o teste mais importante do lado do Flink: ele verifica a hipótese que
governa o `planoRT.md` (§1). Três abas.

**Aba 1:** o job, como no passo 14.

**Aba 2** — conta, a cada segundo, os arquivos **em andamento** (o Flink os mantém
ocultos, com um ponto no início do nome) e os **visíveis**:

```bash
while true; do
  p=$(find ~/tcc-data/out/flink -type f -name '.*' 2>/dev/null | wc -l)
  c=$(find ~/tcc-data/out/flink -type f ! -name '.*' 2>/dev/null | wc -l)
  echo "$(date +%T)  em andamento: $p   visiveis: $c"
  sleep 1
done
```

**Aba 3:** o replayer (passo 6).

**Esperado, e é o ponto do teste:**

```
18:04:01  em andamento: 8   visiveis: 24
18:04:02  em andamento: 8   visiveis: 32
18:04:07  em andamento: 8   visiveis: 40
18:04:12  em andamento: 8   visiveis: 48
   ...
18:05:07  em andamento: 0   visiveis: 128
```

Os **em andamento** ficam constantes em 8 — um por subtarefa, recebendo eventos sem
parar. Os **visíveis** sobem em **degraus de 8, a cada 5 segundos**: cada degrau é
um checkpoint completando. **O Flink processa continuamente, mas o dado só fica
visível no destino quando o checkpoint completa** — o intervalo de checkpoint tem,
na visibilidade, o papel que o trigger tem no Spark. É por isso que o trabalho
reporta duas latências, a de processamento e a de visibilidade.

**Contar as linhas gravadas.** O `pyarrow` já está na imagem e ignora os arquivos
ocultos, então conta só o que foi commitado:

```bash
docker compose run --rm --no-deps flink python -c "import pyarrow.dataset as ds; d = ds.dataset('/tcc-data/out/flink/events', format='parquet', partitioning='hive'); print('linhas:', d.count_rows(), '  arquivos:', len(d.files))"
```

> **Os arquivos do Flink não têm extensão `.parquet`.** O nome é
> `part-<uuid>-<subtarefa>-<contador>`. O contrato declara o nome dos arquivos
> como não invariante (§6), mas qualquer `find -name "*.parquet"` encontra zero
> arquivos do Flink. Procure pela pasta, não pela extensão.

**O Flink gera um terço dos arquivos do Spark.** Para os mesmos 6000 eventos a 5 s,
referência medida na máquina original:

| Motor | Arquivos | Tamanho |
|---|---|---|
| Spark (passo 10) | ~300 | ~3,6 MB |
| Flink | **104** | **980 KB** |

No Spark cada partição do Kafka vira uma tarefa e escreve o próprio arquivo (24 por
lote); no Flink as 24 partições se dividem entre 8 subtarefas, e cada uma escreve um
arquivo por checkpoint. Como na Etapa 5, a magnitude é de 100 evt/s e amortiza nas
cargas de benchmark.

---

## Passo 16 — Testar a retomada (Flink)

O espelho do passo 11 — com uma diferença de mecanismo que vale entender.

`Ctrl+C` no job e suba de novo:

```bash
bash pipeline-streaming/run_local.sh
```

Agora tem que aparecer:

```
!! CHECKPOINT EXISTENTE — retomando de /tcc-data/checkpoints/rt/<id-do-job>/chk-<n>
```

Conte as linhas: o número não muda. Rode o replayer de novo e conte: tem que subir
**exatamente** 6000. Se o job tivesse reprocessado o tópico, subiria muito mais.

**A diferença em relação ao Spark.** O Spark retoma sozinho: basta apontar o
`checkpointLocation`. O Flink não. Cada execução ganha um identificador novo, e os
checkpoints ficam em `<pasta>/<id-do-job>/chk-<n>/`; o `main.py` precisa
**encontrar** o último checkpoint completo e informá-lo ao Flink. E o padrão do
Flink é **apagar** os checkpoints quando o job é cancelado — o `main.py` o configura
para retê-los, senão o `Ctrl+C` destruiria justamente o que a retomada precisa.

> **Zerar o Kafka exige zerar o checkpoint do Flink junto**, pelo mesmo motivo do
> passo 11:
>
> ```bash
> docker compose down -v && rm -rf ~/tcc-data/checkpoints/nrt ~/tcc-data/checkpoints/rt
> ```

---

## Antes de medir — conferir o relógio

**Obrigatório antes de qualquer execução cujos tempos vão ser usados** (passos 17 a
20 e tudo que vier depois). Toda latência do trabalho é uma subtração entre dois
carimbos do relógio da máquina. Um relógio com defeito falsifica todas elas **sem
erro nenhum**.

**O que aconteceu.** Em 10/10/2026, no WSL2, um teste mostrou um lote com
`processamento=-1123 ms`. Dois sincronizadores de hora acertavam o mesmo relógio:
o WSL, seguindo a hora do Windows, e o `systemd-timesyncd` do Ubuntu, seguindo a
internet — com 3,76 s de diferença entre as duas. A cada ~30 s o relógio recuava
1 s e avançava 3,8 s, e entre os saltos andava a **91,7%** da velocidade. Só foi
notado porque um número saiu negativo. Host e contêineres viram os mesmos saltos:
o kernel é um só.

**O ritual**, em toda sessão de medição:

```bash
sudo systemctl stop systemd-timesyncd          # uma autoridade só: o Windows
# esperar: o relógio leva de 2 a 15 minutos para assentar
~/.venvs/tcc/bin/python scripts/check_clock.py
```

**Esperado**, em 40 segundos:

```
  systemd-timesyncd ........ inactive                                     OK
  tick do kernel ........... 10001  (normal 10000)                        OK
  saltos acima de 10 ms .... 0                                            OK
  ritmo .................... 100.003 %  (deriva de +1.2 ms na janela)     OK
OK: relogio sao. Pode medir.
```

Se sair `NAO MEDIR`, não meça: espere mais um pouco e rode de novo (em 12 e
13/10/2026 levou de 12 a 15 minutos para passar). O script só
lê; não altera nada. Sai com **0** ou **1**: o script de rodada (passo 22) o chama
antes de cada rodada, e não começa se ele reprovar.

**Ao terminar a sessão**, religue o serviço:

```bash
sudo systemctl start systemd-timesyncd
```

> **O serviço volta sozinho a cada vez que o WSL liga** (`wsl --shutdown`,
> reinício do Windows). O ritual vale em **toda** sessão, não uma vez só.

> **Não importa a hora estar alguns segundos errada.** Importa o relógio andar no
> ritmo certo e sem saltos, igual para todos os processos. Sobra um ajuste do
> próprio WSL de 2 a 4 ms a cada 30 s, que é normal e está declarado.

> **Fora do WSL** — num Linux comum, como as máquinas da AWS — há um sincronizador
> só, e ele ligado é o estado correto. O script não exige o serviço parado nesse
> caso; as outras conferências valem igual.

---

## Passo 17 — Medir o pipeline NRT (Spark)

Daqui em diante, o **instrumento de medição** (Etapa 6 do NRT = RT-4 do Flink).
Os pipelines já gravam o que é preciso para medir; os passos 17 a 20 mostram como
coletar e conferir. **Com o relógio conferido** (seção anterior).

**O que o Spark passou a registrar**, a cada micro-batch:

| Onde | O quê |
|---|---|
| no Parquet | `batch_id` e `processed_ts` (data/hora, ver `CONTRATO.md` §2.5) |
| em `~/tcc-data/metrics/spark.progresso.<inicio>.jsonl` | uma linha por batch: `inicio_ts`, `processed_ts`, `committed_ts`, linhas, offsets |
| no Kafka | os offsets processados, commitados sob o grupo `tcc-nrt-spark` |

O Spark **não** devolve offsets ao Kafka sozinho — ele os guarda no checkpoint. O
commit é código nosso, e existe só para o consumer lag ser medido pelo mesmo
método nos dois motores (decisão F7).

**Três abas.** Aba 1, o pipeline, lendo só o que chegar:

```bash
rm -rf ~/tcc-data/checkpoints/nrt
STARTING_OFFSETS=latest bash pipeline-nrt/run_local.sh
```

> **O `rm -rf` não é enfeite.** Os dois motores leem o **mesmo** tópico: tudo que
> o replayer enviou para medir o Flink fica pendente para o Spark, e vice-versa.
> Com checkpoint existente, `STARTING_OFFSETS=latest` não vale — o Spark retoma de
> onde parou e processa essa pendência logo no primeiro lote, com minutos de
> latência, sob o `run_id` da outra medição.

Espere o `[batch 0] vazio`. Aba 2, o replayer:

```bash
RATE=100 DURATION=60 ~/.venvs/tcc/bin/python shared/data-generator/replayer.py
```

Aba 3, **assim que aparecer o primeiro batch com eventos na aba 1**, o coletor de lag:

```bash
GROUP_ID=tcc-nrt-spark DURATION=75 ~/.venvs/tcc/bin/python shared/monitoring/collector.py
```

**Esperado na aba 1**, a cada ~5 s:

```
[batch 3] 500 eventos gravados  processamento=1381 ms  escrita=431 ms  particoes=24
```

Todo `processamento=` tem de ser **positivo**, e os lotes do meio, de ~500 eventos.
Tempo negativo ou lotes irregulares (350, 420, 300…) são o sintoma do relógio com
defeito: pare e volte à seção anterior.

**Esperado na aba 3**, de segundo em segundo, o **serrilhado** do lag: sobe ~100 por
segundo e cai a cada batch.

```
     5.0s  lag total=    402
     6.0s  lag total=    502
     7.0s  lag total=    602
     8.0s  lag total=    201      ← um batch acabou de gravar
```

**Os vales não vão a zero, e isso é o correto.** O vale fica em torno de **taxa ×
duração do batch** (100 evt/s × ~2 s ≈ 200): são os eventos que chegaram enquanto o
batch trabalhava. O que indica saturação é o vale **subir de um ciclo para o outro**,
não a altura dele.

> **Por que o coletor amostra a cada 1 s, e não a cada 5.** Com a amostra no mesmo
> período do trigger, o coletor veria a fila sempre no mesmo ponto do ciclo — o
> serrilhado sumiria e o lag pareceria constante.

**CPU e memória, pelo mesmo coletor (Etapa 8).** Com `CONTAINERS`, ele lê também
os contadores de cada contêiner cujo nome contenha um dos trechos dados:

```bash
CONTAINERS=spark-run,kafka GROUP_ID=tcc-nrt-spark DURATION=75 ~/.venvs/tcc/bin/python shared/monitoring/collector.py
```

Grava um segundo CSV, `recursos.<grupo>.<inicio>.csv`, e fecha com um resumo
(rodada de teste de 11/10/2026):

```
  kafka
    CPU      media    28.5 %   pico   202.8 %
    memoria  media   520.4 MiB  pico   619.6 MiB   (69 leituras)
  pipelines-tcc-spark-run-c7c60108dfdc
    CPU      media   108.0 %   pico   372.0 %
    memoria  media   786.3 MiB  pico   912.8 MiB   (69 leituras)
```

A CPU é em **% de um núcleo**: 100 % é um núcleo inteiro ocupado, e passa de 100 %
quando o contêiner usa mais de um. A memória desconta o cache de arquivos. Para o
Flink, `CONTAINERS=flink-run,kafka`.

> **O coletor começa depois do primeiro batch**, de propósito, só neste teste: o
> primeiro commit de um grupo novo pode receber `NOT_COORDINATOR` (o broker ainda
> está elegendo o coordenador). O pipeline repete a tentativa; consultar o grupo
> antes esconderia o erro.

---

## Passo 18 — Reconciliar a execução do Spark

O `reconcile.py` confere o que foi gravado contra o manifesto do replayer. Sem
`RUN_ID`, lista as execuções disponíveis:

```bash
~/.venvs/tcc/bin/python shared/analysis/reconcile.py
```

Para reconciliar a mais recente sem copiar nada à mão:

```bash
RUN=$(ls -1 ~/tcc-data/out/spark/events | grep 'run_id=[0-9]' | tail -1 | sed 's/run_id=//'); echo "$RUN"
RUN_ID=$RUN ~/.venvs/tcc/bin/python shared/analysis/reconcile.py
```

O `grep` deixa só as execuções do replayer, cujo `run_id` começa pela data. Sem ele,
"a mais recente" poderia ser uma pasta do passo 21.

**Esperado** (referência medida na máquina original em 10/10/2026, com o Spark em
contêiner, 100 evt/s, trigger de 5 s):

```
    PERDAS (confirmados - distintos) 0
    DUPLICATAS (gravados - distintos) 0
  espera no trigger          P50   2643 ...
  LAT. PROCESSAMENTO         P50   3909 ...
  LAT. VISIBILIDADE          P50   4382 ...
  is_late ................ 0 de 6000
OK: perdas=0  duplicatas=0  invalidas=0  nulas=0  descartadas=?
```

Uma segunda execução, logo em seguida, deu 3914 e 4386 ms. Em setembro, com o
Spark ainda no host, eram 3903 e 4298 ms. **Com a base real** (23/10/2026, pelo
script de rodada, 100 evt/s): 3919 e 4410 ms — a mesma faixa. O bloco acima é
**a remedir** com o replayer à mão, no começo da bateria local.

A **espera no trigger** com mediana perto de **metade do trigger** (2,6 s de 5 s) é a
aritmética do micro-batch aparecendo no dado. O programa sai com **0** se não há
perda nem duplicata entre os eventos válidos, e com **1** se há — o script de
rodada usa isso. Inválidas, nulas e descartadas são **informação** e não mudam o
código de saída (decisão de 11/10/2026). O `descartadas=?` é o esperado neste
passo: a contagem depende de uma anotação com o número de mensagens do tópico, que
só o injetor (passo 21) e o script de rodada (passo 22) gravam.

**O aquecimento é por tempo** (`CONTRATO.md` §2.3, versão 2.2, 12/10/2026). Ficam
fora do **regime** os eventos **gerados** nos primeiros `AQUECIMENTO_S` segundos
(padrão 5), contados do primeiro evento da execução pelo `event_ts`. O relatório
mostra as latências duas vezes — em regime e na execução inteira — e diz quantos
eventos saíram. Como a seed é a mesma, os dois motores descartam o mesmo trecho; na
borda diferem poucos eventos, porque o `event_ts` é o instante real da emissão.

O relatório traz também o **throughput médio**, pela mesma regra: os eventos
gerados depois do aquecimento, divididos pelo tempo entre o fim do aquecimento e a
última gravação. Os números do bloco acima são de antes dessas duas regras (até
11/10/2026 saía o primeiro lote); numa rodada de teste de 12/10/2026, a 1000 evt/s
por 60 s:

```
  aquecimento ............ os eventos gerados nos primeiros 5 s ficam fora do regime: 4999 de 60000
  THROUGHPUT MEDIO .......... 952.4 evt/s   (55001 eventos gerados depois dos primeiros 5 s, em 57.8 s ate a ultima gravacao)
```

O throughput médio fica **abaixo da carga oferecida** por efeito de borda: a
última gravação acontece até um intervalo depois do último evento. Na bateria
local (passo 25) ficou de 6 a 10% abaixo da carga no Spark e de 1 a 5% no Flink,
sem nenhum dos dois atrasar — é borda, não capacidade.

**Ele lê com o DuckDB**, instalado no passo 2 pelo `requirements.txt`: o mesmo leitor
para os dois motores, que não pertence a nenhum deles. Os arquivos são descobertos
**pela pasta**, nunca pela extensão — os do Flink não têm `.parquet`.

---

## Passo 19 — Medir o pipeline Streaming (Flink)

O Flink passou a gravar `processed_ts` **por evento** e a commitar offsets no Kafka a
cada checkpoint, sob o grupo `tcc-rt-flink`. O console ganhou um sinal de vida: uma
linha por checkpoint completo.

```bash
rm -rf ~/tcc-data/checkpoints/rt
STARTUP_MODE=latest-offset bash pipeline-streaming/run_local.sh
```

Depois do `>> job no ar`, o replayer e o coletor como no passo 17, trocando o grupo:

```bash
GROUP_ID=tcc-rt-flink DURATION=75 ~/.venvs/tcc/bin/python shared/monitoring/collector.py
```

**Esperado no console do Flink**, a cada 5 s:

```
[checkpoint 4] completo  duracao=25 ms  estado=13280 bytes
```

> **Rodando o Flink de um script, e não do terminal**, duas armadilhas: o Python
> acumula a saída e o log parece travado (usar `python -u`); e mandar sinal ao
> processo do `docker compose run` **não para o contêiner** — o job segue rodando.
> Parar com `docker stop` ou pelo `Ctrl+C` no terminal.

**As três opções do destino que protegem o `processed_ts`** (`CONTRATO.md` §2.5),
todas verificadas: `parquet.write.int64.timestamp` (sem ela, o formato legado
INT96), `parquet.timestamp.time.unit=micros` (o mesmo que o Spark grava) e
**`parquet.utc-timezone`** — sem esta, com a JVM fora de UTC, **todos os carimbos
saem 3 h deslocados, sem erro**.

---

## Passo 20 — Reconciliar a execução do Flink

```bash
RUN=$(ls -1 ~/tcc-data/out/flink/events | grep 'run_id=[0-9]' | tail -1 | sed 's/run_id=//'); echo "$RUN"
MOTOR=flink RUN_ID=$RUN ~/.venvs/tcc/bin/python shared/analysis/reconcile.py
```

**Esperado** (referência medida na máquina original em 10/10/2026, 100 evt/s,
checkpoint de 5 s):

```
    PERDAS (confirmados - distintos) 0
    DUPLICATAS (gravados - distintos) 0
  checkpoints com dados .. 13
  instantes de ctime ..... 15   (arquivos a ate 1000 ms um do outro = mesmo checkpoint)
  committed_ts ........... ctime de cada arquivo (CONTRATO.md 1.6, 2.2)
  espera pelo checkpoint     P50   2599 ...
  LAT. PROCESSAMENTO         P50      7 ...
  LAT. VISIBILIDADE          P50   2606 ...
  conferencia ctime x log de checkpoints: -10..7 ms em 15 instantes de ctime
OK: perdas=0  duplicatas=0  invalidas=0  nulas=0  descartadas=?
```

Em setembro eram 8 ms e 2626 ms. **Com a base real** (23/10/2026, pelo script de
rodada, 100 evt/s): 7 ms e 2535 ms. Como no passo 18, os números são de antes do
aquecimento por tempo; na rodada de teste de 12/10/2026, a 1000 evt/s por 60 s: 5002
de 60000 eventos fora do regime (o Spark tirou 4999), processamento P50 6 ms,
visibilidade P50 2548 ms e 958,0 evt/s.

**Checkpoints e instantes.** Os 8 arquivos de um checkpoint ficam visíveis quase
juntos, mas nem sempre no mesmo milissegundo. O reconciliador agrupa os arquivos
cujo `ctime` dista até 1 s e conta o grupo como um checkpoint. A latência de cada
evento é calculada com o `ctime` do próprio arquivo, e o aquecimento é o mesmo do
Spark (passo 18).

**É o achado central do trabalho, visível numa tela só.** Latência de
**processamento**: 7 ms no Flink contra 3,9 s no Spark. Latência de
**visibilidade**: 2,6 s contra 4,4 s. O Flink processa cada evento em milissegundos,
mas o dado espera o checkpoint para ficar visível — o intervalo de checkpoint tem
na visibilidade o papel do trigger no Spark. Por isso o trabalho reporta as duas.

**Por que o Flink mede a visibilidade pelo arquivo.** O Flink não grava `batch_id`:
cada arquivo é finalizado por exatamente um checkpoint, e o instante em que ele
deixa de ser oculto — o `ctime`, gravado pelo sistema de arquivos na renomeação — é
o instante de visibilidade. O Spark mede pelo log de progresso. A assimetria é
declarada no `CONTRATO.md` §2.2, com o motivo e o tamanho do erro de cada um.

> **Nada pode tocar nos arquivos do Flink depois de gravados.** `chmod`, `chown` ou
> cópia mudam o `ctime` e falsificam a latência de visibilidade sem erro. A linha
> "conferência" existe para pegar isso: um desvio acima de 1 s é sinalizado.

---

## Depois do passo 20 — o Transform: a tabela tipada

Os passos 9 a 20 fizeram o **E** e o **L**: os dois motores carregam os onze
campos do evento como **texto**, exatamente como vieram. O **T** vem depois, fora
dos pipelines e fora da medição (`CONTRATO.md` §2.8). Ele lê o destino bruto de uma
execução e grava ao lado uma **tabela tipada**: os onze campos nos tipos do
contrato, sem duplicatas.

```bash
RUN=$(ls -1 ~/tcc-data/out/flink/events | grep 'run_id=[0-9]' | tail -1 | sed 's/run_id=//'); echo "$RUN"
MOTOR=flink RUN_ID=$RUN ~/.venvs/tcc/bin/python shared/transform/transform.py
```

Para o Spark, o mesmo com `out/spark` e `MOTOR=spark`.

**Esperado** (rodada do Spark de 24/10/2026, 6.000 eventos reais, antes da remoção
de duplicatas existir):

```
destino bruto ..... /home/<usuário>/tcc-data/out/spark/events/run_id=20261024T191535Z-spark-i5-c100-r01   (312 arquivos)
linhas ............ 6000

campo         tipo            nulos   (nao vieram / texto que nao converte)
event_id      string               0   (0 / 0)
seq           int64                0   (0 / 0)
...
amount        decimal(10,2)        0   (0 / 0)
session_id    string               0   (0 / 0)
event_ts      int64                0   (0 / 0)
event_time    timestamp            0   (0 / 0)

tabela tipada ..... /home/<usuário>/tcc-data/transformado/spark/run_id=…/eventos.parquet   (0.30 MB)
    ...
    amount        DECIMAL(10,2)
    event_time    TIMESTAMP

6000 linhas gravadas; 0 valores nao converteram
```

Agora a saída tem também duas linhas novas, entre a tabela de campos e a tabela
tipada: `duplicatas tecnicas removidas` e `repetidas da origem removidas`.
**A remedir:** a remoção de duplicatas foi escrita em 24/10/2026 e ainda não rodou
sobre uma execução real. Numa janela de 6.000 eventos, o esperado é 0 técnicas e
poucas repetidas da origem (a base tem 0,07% de linhas repetidas).

**O que a tabela tipada tem:**

| Campo | No destino bruto | Na tabela tipada |
|---|---|---|
| `amount` | texto `"543.10"` | `DECIMAL(10,2)` **543.10** — exato, o zero à direita fica |
| `event_time` | texto `"2019-10-20 12:47:26 UTC"` | `TIMESTAMP` 2019-10-20 12:47:26, em UTC |
| `seq`, `event_ts` | texto | `BIGINT` |
| `gen_id`, `user_id`, `product_id` | texto | `INTEGER` |
| os outros | texto | texto |

**Só os onze campos.** As colunas de medição (`processed_ts`, `batch_id`) ficam no
destino bruto: são de cada motor. Por isso a tabela tipada do Spark e a do Flink,
da mesma janela, saem **iguais** — salvo o `run_id` e o `event_ts`, que são de cada
execução. Verificado em 24/10/2026: 0 diferenças entre as duas, e 0 contra o texto
da base nos seis campos que vêm da loja.

**As duas espécies de duplicata** (`CONTRATO.md` §2.8): a **técnica** é o motor ter
gravado a mesma linha da base duas vezes (mesmo `event_id`); a **da origem** é a
própria base ter a linha repetida (os seis campos da loja iguais, `event_id`
diferente). Fica uma cópia de cada. O destino bruto continua com todas.

**O que ele não faz:** não conserta (um valor que não converte fica nulo, e a
linha fica), não mexe no destino bruto e não roda sozinho — o script de rodada
(passo 22) não o chama.

---

## Passo 21 — Eventos malformados (aposentado no contrato 3.0)

**Este passo não roda mais com o código atual** (decisão do autor, 23/10/2026). O
injetor (`shared/data-generator/inject_malformed.py`) monta os eventos com o
gerador sintético, que não tem o campo `event_time` do contrato 3.0: o
`reconcile.py` contaria todas as linhas como inválidas. Ele fica no repositório
como registro, e o benchmark não leva mais eventos malformados.

**O que ele demonstrava, e foi medido em 12/10/2026:** com os campos carregados
como texto (contrato 2.0), um evento com defeito chega ao destino **do mesmo jeito
nos dois motores**. 420 eventos válidos e 27 com defeito, de 9 tipos:

| | Spark | Flink |
|---|---|---|
| Linhas no destino | 447 | 447 |
| Inválidas | 18 | 18 |
| Nulas | 0 | 0 |
| Descartadas | 0 | 0 |

Antes do contrato 2.0, com os campos tipados, os motores divergiam: 462 linhas, 21
inválidas e 15 nulas no Spark contra 447 linhas, 15 inválidas e 15 descartadas no
Flink. A diferença que sobra, declarada no `CONTRATO.md` §6: uma mensagem que
**não é JSON** vira linha nula no Spark e é descartada pelo Flink.

**Para reproduzir a demonstração**, voltar ao último commit anterior ao contrato
3.0 e seguir o passo 21 daquela versão do README:

```bash
git checkout 013b646
```

O registro completo está em `Informações/EtapaNRT/DecisaoEtapa8.1.md`.

---

## Passo 22 — Uma rodada do benchmark

Os passos 17 a 21 fizeram à mão, em três abas, o que uma medição exige. O
`benchmarks/run_round.py` faz o mesmo com **um comando** — e não esquece de zerar
o checkpoint nem de conferir o relógio. É o **script de rodada**, que os planos
chamam de arnês.

**Com o relógio conferido** (seção antes do passo 17), o Kafka no ar e as duas
imagens construídas:

```bash
MOTOR=spark INTERVALO=5 CARGA=7500 DURACAO=60 RODADA=1 ~/.venvs/tcc/bin/python benchmarks/run_round.py
```

| Variável | O que é |
|---|---|
| `MOTOR` | `spark` ou `flink` |
| `INTERVALO` | segundos — o trigger do Spark, e o nome da combinação. O checkpoint do Flink é **fixo em 5 s**, seja qual for o `INTERVALO` (`CONTRATO.md` §5.2) |
| `CARGA` | mensagens por segundo |
| `DURACAO` | segundos de geração — na bateria, 60 com intervalo 5 e 300 com intervalo 30 |
| `RODADA` | 1 a 10 |

> **Uma rodada feita aqui conta para a combinação do passo 23** — é a mesma pasta
> em `results/`. Para um teste que não se misture com a bateria, use uma carga fora
> dela, como `CARGA=100`, e apague a pasta depois.

**O que o número da rodada decide** (`CONTRATO.md` §5.1 e §2.6):

- **a janela da base**: carga × duração linhas consecutivas, com o início sorteado
  pela seed 41 + rodada — a rodada 1 usa 42, a 10 usa 51. A rodada N do Spark e a
  do Flink recebem **a mesma janela** (`CONTRATO.md` §5.1). O manifesto registra a
  primeira e a última linha.
- Sem malformados desde o contrato 3.0. Até a 2.2, as rodadas 2, 5, 7 e 10 levavam
  1% de mensagens malformadas.

**O que o intervalo decide**, além do trigger: o **aquecimento**. Ficam fora do
regime os eventos gerados nos primeiros `INTERVALO` segundos — 5 s ou 30 s —, o
mesmo trecho nos dois motores (passo 18).

**Esperado** (a primeira rodada da combinação Intervalo5Carga7500 da bateria
local, 12/10/2026, na máquina original):

```
=== rodada 01 — spark — intervalo 5 s, carga 7500 msg/s, 60 s ===
    run_id 20261012T214042Z-spark-i5-c7500-r01
    pasta  /mnt/c/.../Pipelines-TCC/results/Intervalo5Carga7500/spark/rodada-01
>> [18:40:42] 1/12 conferencias
>> [18:41:25] 2/12 zerando o checkpoint
>> [18:41:25] 3/12 recriando o topico
>> [18:41:37] 4/12 offsets do topico, antes
>> [18:41:37] 5/12 subindo o pipeline
>> [18:41:57] 6/12 ligando o coletor
>> [18:42:00] 7/12 gerador: 7500 msg/s por 60 s, seed 42      ← desde o 3.0: "7/12 replayer: … janela da base pela seed 42"
>> [18:43:01] 8/12 esperando a fila zerar (ate 60 s)
>> [18:43:08] 9/12 offsets do topico, depois
>> [18:43:08] 10/12 arquivo de parada
>> [18:43:09] 11/12 reconciliador
>> [18:43:11] 12/12 manifesto e arquivos da rodada

RODADA VALIDA.
  latencia de processamento P50 3428 ms   visibilidade P50 4089 ms
  throughput medio 6749.1 evt/s   CPU media 128.9 % de um nucleo   memoria pico 949.6 MiB
  perdas 0  duplicatas 0  invalidas 0  nulas 0  descartadas 0
manifesto: /mnt/c/.../Pipelines-TCC/results/Intervalo5Carga7500/spark/rodada-01/manifesto.json
```

São cerca de 2,5 minutos para uma rodada de 60 s: ~40 s de conferências (o relógio
é observado por 40 s), ~35 s para recriar o tópico e subir o pipeline, os 60 s de
geração e a espera pela fila. Uma rodada de 300 s leva cerca de 7 minutos.

A **CPU da tela** é em % de **um** núcleo, como o coletor mede (passo 17): 128,9 %
é pouco mais de um núcleo inteiro. Os arquivos de resultado (passo 24) a mostram em
% da máquina inteira.

**O Flink numa combinação de intervalo 30** avisa que o intervalo dele é outro (a
primeira rodada do Flink na Intervalo30Carga1000, 12/10/2026):

```
=== rodada 01 — flink — intervalo 30 s, carga 1000 msg/s, 300 s ===
    o flink roda com intervalo de 5 s, fixo
...
RODADA VALIDA.
  latencia de processamento P50 6 ms   visibilidade P50 2537 ms
  throughput medio 992.44 evt/s   CPU media 44.8 % de um nucleo   memoria pico 913.3 MiB
  perdas 0  duplicatas 0  invalidas 0  nulas 0  descartadas 0
```

A saída acima é da bateria com o gerador sintético (contrato 2.2); com a base real
a forma é a mesma, e a linha do passo 7 passa a nomear o replayer e a janela.

**O que ela confere antes de começar.** Se algo falhar, não começa e sai com
código **2**:

- a base real pronta, com o tamanho certo (`preparar_base.sh`);
- 50 GB livres, no menor entre o disco dos dados e o do repositório;
- a pasta da rodada ainda não existe (`REFAZER=1` refaz, guardando a anterior ao lado);
- o relógio (`scripts/check_clock.py`);
- o Kafka `healthy`, e nenhum contêiner de pipeline esquecido no ar;
- as imagens `tcc-spark:4.0.4` e `tcc-flink:1.20.5`.

> **A rodada apaga e recria o tópico.** Toda rodada começa com as 24 partições
> vazias: sem sobra da rodada anterior, sem pendência do outro motor. Qualquer
> mensagem que estivesse no tópico é perdida.

**O que fica**, em `results/Intervalo5Carga7500/spark/rodada-01/`, até a
combinação fechar (passo 23):

| Arquivo | O quê |
|---|---|
| `manifesto.json` | a rodada inteira: parâmetros, seed, **janela da base**, commit do código, imagens, relógio, cada passo com horário, códigos de saída e os números |
| `metrics/lag.*.csv`, `metrics/recursos.*.csv` | a fila, a CPU e a memória, uma amostra por segundo |
| `spark.progresso.*.jsonl` | o log de progresso do motor |
| `reconcile.txt` e `reconcile.spark.*.json` | o relatório do reconciliador |
| `pipeline.txt`, `replayer.txt` (`gerador.txt` até o 3.0), `coletor.txt`, `relogio.txt` | as telas de cada programa |

O **Parquet não fica**: é apagado assim que o reconciliador dá `OK`, porque a
15 mil evt/s uma rodada de 5 minutos grava cerca de 250 MB por motor. Se a rodada não valer, ele
fica, para investigar. `MANTER_PARQUET=1` guarda sempre.

Sai com **0** se a rodada vale; **1** se não vale, e o `manifesto.json` diz por
quê; **2** se nem começou.

---

## Passo 23 — As dez rodadas de uma combinação

Uma **combinação** é um intervalo e uma carga. Cada combinação tem 10 rodadas de
cada motor — 20 chamadas ao passo 22, que o `benchmarks/run_combination.py` faz na
ordem certa:

```bash
INTERVALO=5 CARGA=7500 DURACAO=60 ~/.venvs/tcc/bin/python benchmarks/run_combination.py
```

Para fazer só algumas rodadas, `RODADAS=1,2` na frente do comando.

**A ordem.** Os motores alternam, e quem começa inverte a cada rodada: Spark e
Flink na rodada 1, Flink e Spark na 2, e assim por diante. O que muda ao longo da
sessão — a máquina esquentar, outro programa abrir — pesa igual sobre os dois.

**Quando uma rodada falha:**

| O que houve | O que a sequência faz |
|---|---|
| nem começou (código 2: relógio, disco, Kafka) | **para** — a rodada seguinte esbarraria no mesmo defeito |
| começou e não valeu (código 1) | tenta **uma** vez de novo, guardando a tentativa anterior; se falhar outra vez, registra e segue |

**Retomada.** Rodar o mesmo comando de novo pula as rodadas que já estão válidas
em `results/` e faz só as que faltam.

Ao fim, um resumo — aqui as quatro primeiras das vinte linhas da
Intervalo30Carga1000, da bateria local (12/10/2026):

```
=== resumo =========================================================
rodada  motor  vale   proc P50  vis P50     thr CPU 1nuc  mem MiB  inval  nulas  desc
1       spark  sim       15849    16391  913.13     22.7    974.3      0      0     0
1       flink  sim           6     2537  992.44     44.8    913.3      0      0     0
2       flink  sim           6     2551  984.77     46.3   1001.4   2250      0     0
2       spark  sim       15842    16549  913.89     24.6    995.3   2250      0     0
...
Todas as rodadas pedidas estao validas.
```

A coluna `CPU 1nuc` é em % de um núcleo, como na tela da rodada.

**O fechamento.** Quando as **20** rodadas da combinação estão válidas, a
sequência:

1. escreve os arquivos de resultado (passo 24);
2. confere que os dois saíram agora, e completos;
3. **apaga a pasta da combinação em `results/`.**

```
=== fechamento: as 20 rodadas estao validas ========================
lendo de     /mnt/c/.../Pipelines-TCC/results
escrevendo em /mnt/c/Users/turbi/Cesar/TCC/Informações/Resultados Local

ResultadoIntervalo30Carga1000  (.txt e .md)
   rodadas validas: NRT 10 de 10, RT 10 de 10

material bruto apagado: /mnt/c/.../Pipelines-TCC/results/Intervalo30Carga1000
A combinacao esta fechada. O que fica dela:
   /mnt/c/Users/turbi/Cesar/TCC/Informações/Resultados Local/ResultadoIntervalo30Carga1000.txt
   /mnt/c/Users/turbi/Cesar/TCC/Informações/Resultados Local/ResultadoIntervalo30Carga1000.md
```

O material bruto não é guardado nem vai para o git; o que fica da combinação são
os dois arquivos. Se eles não saírem, a pasta **não** é apagada, e rodar o mesmo
comando refaz só o fechamento. `MANTER_BRUTO=1` escreve os arquivos e não apaga
nada.

> **Uma combinação fechada não recomeça por engano.** Se a pasta dela não existe
> em `results/` e o `.txt` dela existe na pasta de saída, a sequência não começa.
> Para refazê-la, mova ou apague os dois arquivos antes.

> **Onde os arquivos são escritos.** Por padrão, em `../../Informações/Resultados
> Local`, a pasta de pesquisa do autor. Em outra máquina, aponte uma pasta que
> exista com `SAIDA=`, como no passo 24 — sem isso o fechamento avisa e não apaga
> nada.

Sai com **0** se as rodadas pedidas estão todas válidas; **1** se alguma ficou
faltando, ou se os arquivos do fechamento não saíram; **2** se parou por problema
do ambiente.

A parada e a segunda tentativa foram verificadas com rodadas de teste (11 e
12/10/2026). O resto, de ponta a ponta, na bateria local (passo 25): seis
combinações, 120 rodadas válidas na primeira tentativa, cada combinação fechada e
apagada sozinha.

---

## Passo 24 — Os arquivos de resultado

O `benchmarks/write_results.py` lê `results/` e escreve, para cada combinação, os
dois arquivos que se **leem**. O passo 23 o chama sozinho no fechamento; rodá-lo à
mão serve para olhar uma combinação ainda incompleta.

```bash
mkdir -p ~/tcc-resultados
SAIDA=~/tcc-resultados ~/.venvs/tcc/bin/python benchmarks/write_results.py
```

**Esperado**, uma linha por combinação que esteja em `results/`:

```
ResultadoIntervalo30Carga1000  (.txt e .md)
   rodadas validas: NRT 10 de 10, RT 10 de 10
```

Com rodadas faltando, a linha termina em `<- INCOMPLETO`. NRT é o Spark; RT é o
Flink.

| Arquivo | O quê |
|---|---|
| `ResultadoIntervalo30Carga15000.txt` | uma linha por rodada e por motor, com as 19 variáveis por extenso; depois a média e o desvio padrão de cada motor |
| `ResultadoIntervalo30Carga15000.md` | as mesmas rodadas em tabelas, e o que o `.txt` não mostra: integridade por rodada, arquivos gravados, de onde vem o tempo de cada latência, ocorrências |

O `.txt` começa assim (bateria local, 13/10/2026):

```
Resultados Local: Intervalo de 30 s, Carga de 15000 eventos por segundo
Intervalo de cada motor: trigger do NRT de 30 s; checkpoint do RT de 5 s.
Duração de cada rodada: 300 s.
CPU em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados.
Com falha induzida — rodadas 2, 5, 7 e 10: 1% de eventos malformados.

Rodada 1 NRT: Latência de processamento P50 - 18624 ms, Latência de processamento P95 - 31970 ms, ...
```

**As regras** (decisões de 11 e 12/10/2026):

- a **média** é sobre as rodadas válidas do motor, sem separar as que levaram
  malformados; a linha diz sobre quantas foi calculada;
- o **desvio padrão** é o amostral (divide por n − 1);
- uma rodada que **não valeu** aparece no lugar dela, com o motivo, e fica fora da
  média; o título avisa `INCOMPLETO`;
- as rodadas com malformados eram sinalizadas no título (até o contrato 2.2; com a
  base real não há malformados, e a linha não aparece);
- a **CPU** sai em **% da máquina inteira**, média e máximo: o que o coletor mede em
  % de um núcleo, dividido pelo número de núcleos que o Docker enxerga (8 na
  máquina original, gravado no manifesto de cada rodada). 100% são todos os
  núcleos ocupados (decisão de 12/10/2026).

Nenhum número é copiado à mão, e rodar de novo com as mesmas rodadas escreve
exatamente os mesmos arquivos.

> **A última seção do `.md`, "Leitura", é escrita à mão**, e o programa não a
> reescreve: preserva tudo o que estiver abaixo da linha de marcação, e avisa
> quando os números mudaram com ela já escrita. Um `.md` de mesmo nome **sem** a
> linha de marcação não é sobrescrito — o programa para, com código 2.

`INTERVALO=5 CARGA=7500` na frente do comando escreve só essa combinação.

---

## Passo 25 — A bateria local: as seis combinações

O desenho da Etapa 9 (`CONTRATO.md` §5.2): só o trigger do Spark e a carga variam.

| | Valores |
|---|---|
| Intervalo do Spark (trigger) | 5 s e 30 s |
| Intervalo do Flink (checkpoint) | **5 s, fixo** — nas combinações de intervalo 30 ele roda de novo, ao lado do Spark |
| Carga | 1.000, 7.500 e 15.000 eventos por segundo |
| Duração da rodada | 60 s nas combinações de intervalo 5; 300 s nas de intervalo 30 |
| Aquecimento | os eventos dos primeiros 5 s ou 30 s — o intervalo da combinação |
| Rodadas | 10 por motor e por combinação; sem malformados desde o contrato 3.0 (até a 2.2, nas rodadas 2, 5, 7 e 10) |
| Eventos | a base real, uma janela por rodada (desde o contrato 3.0) |

São seis chamadas ao passo 23:

```bash
INTERVALO=5 CARGA=1000 DURACAO=60 ~/.venvs/tcc/bin/python benchmarks/run_combination.py
INTERVALO=5 CARGA=7500 DURACAO=60 ~/.venvs/tcc/bin/python benchmarks/run_combination.py
INTERVALO=5 CARGA=15000 DURACAO=60 ~/.venvs/tcc/bin/python benchmarks/run_combination.py
INTERVALO=30 CARGA=1000 DURACAO=300 ~/.venvs/tcc/bin/python benchmarks/run_combination.py
INTERVALO=30 CARGA=7500 DURACAO=300 ~/.venvs/tcc/bin/python benchmarks/run_combination.py
INTERVALO=30 CARGA=15000 DURACAO=300 ~/.venvs/tcc/bin/python benchmarks/run_combination.py
```

**Quanto tempo.** Na máquina original, ~50 minutos por combinação de intervalo 5 e
~2,3 horas por combinação de intervalo 30: **cerca de 9 horas** a bateria inteira.
A original rodou a primeira combinação em 12/10/2026 à tarde e as outras cinco em
sequência, das 18:40 às 03:15 de 13/10.

**Antes de começar:** o relógio conferido (seção antes do passo 17), a suspensão do
Windows desligada, os outros programas fechados, 50 GB livres e a árvore do git
limpa — o manifesto de cada rodada registra o commit do código.

> **Em sequência, sem acompanhar.** O autor rodou as seis com um script local, fora
> do git (`benchmarks/run_all.sh`), que só faz as seis chamadas acima em ordem:
> pula a combinação já fechada, segue para a próxima quando uma termina com código
> 0 ou 1, para tudo no código 2 e copia a tela para `~/tcc-data/bateria.txt`. Os
> seis comandos, um depois do outro, dão o mesmo resultado — e cada um retoma de
> onde parou, se for interrompido.

**Esperado:** seis pares `.txt`/`.md` na pasta de saída, `results/` vazia, e em
cada combinação as 20 rodadas válidas. Na máquina original: 120 de 120 válidas na
primeira tentativa; 0 perdas, duplicatas, atrasados, nulas e descartadas em todas;
inválidas iguais nos dois motores em todas as rodadas com malformados.

> **A bateria abaixo foi feita com o gerador sintético** (contrato 2.2,
> 12–13/10/2026) e **será refeita com a base real** (decisão de 23/10/2026). Os
> números ficam como histórico até a bateria nova. Os arquivos dela foram movidos
> para `../../Informações/Resultados Local/Resultados Sinteticos/`, para a pasta
> principal ficar livre: o `run_combination.py` não começa uma combinação cujo
> `.txt` já esteja na pasta de saída.

**Médias de referência** (Spark / Flink; latências P50 em ms; CPU média em % da
máquina de 8 núcleos):

| Intervalo | Carga | Processamento P50 | Visibilidade P50 | Throughput médio (evt/s) | CPU média | Memória pico (MiB) |
|---|---|---|---|---|---|---|
| 5 | 1.000 | 3183 / 6 | 3718 / 2553 | 929 / 960 | 14,1 / 6,9 | 961 / 877 |
| 5 | 7.500 | 3395 / 6 | 4097 / 2555 | 6786 / 7166 | 16,4 / 12,2 | 999 / 918 |
| 5 | 15.000 | 3733 / 6 | 4496 / 2571 | 13842 / 14452 | 18,8 / 13,5 | 1025 / 980 |
| 30 | 1.000 | 15874 / 6 | 16478 / 2543 | 935 / 989 | 3,0 / 5,7 | 970 / 937 |
| 30 | 7.500 | 17336 / 6 | 18297 / 2543 | 7029 / 7428 | 4,9 / 10,6 | 1082 / 937 |
| 30 | 15.000 | 18651 / 5 | 20069 / 2550 | 13794 / 14830 | 6,8 / 12,6 | 1211 / 956 |

Os números são desta máquina; outra máquina dá outros. O que deve se repetir é a
forma: o Flink processa em milissegundos e fica visível em ~2,5 s (metade do
checkpoint de 5 s), o Spark acompanha o trigger, e nenhum evento se perde.

**Ao terminar**, religue o `systemd-timesyncd` (seção antes do passo 17).

---

## Passo 26 — Ferramentas da nuvem

Daqui em diante, a Etapa 10: o mesmo benchmark numa máquina da AWS, pelo **AWS
Academy Learner Lab**. Esta parte da receita é escrita junto com a etapa.

Na aba do Ubuntu, dentro do repositório:

```bash
bash scripts/setup_aws_tools.sh
```

O script instala, na pasta do usuário e **sem `sudo`**, o Terraform na versão
travada (o zip é conferido contra a lista de checksums da HashiCorp antes de
instalar) e a AWS CLI v2, pelo instalador oficial. Escreve também o
`~/.aws/config`, só com a região. Credenciais ele não toca. É idempotente: rodar de
novo não quebra nada, e a AWS CLI é atualizada.

**Esperado** (máquina original, 17/10/2026):

```
>> Terraform 1.16.4
terraform_1.16.4_linux_amd64.zip: OK
instalado: Terraform v1.16.4
...
>> Ferramentas prontas
  terraform : Terraform v1.16.4
  aws       : aws-cli/2.38.1 Python/3.14.6 Linux/5.15.167.4-microsoft-standard-WSL2 exe/x86_64.ubuntu.24
  regiao    : us-east-1
```

Se faltar o `unzip`, o script para antes de mudar qualquer coisa e diz o comando
para instalá-lo. Se avisar que `~/.local/bin` não está no PATH, feche e abra o
terminal do Ubuntu: o `~/.profile` acrescenta a pasta quando ela existe.

**Por que `us-east-1`.** O laboratório só permite `us-east-1` e `us-west-2`, e o
par de chaves `vockey`, que ele já traz pronto, só existe na primeira.

**As credenciais — em toda sessão do laboratório.** Elas mudam a cada sessão, e
quem as coloca é você, à mão:

1. no Learner Lab, **Start Lab**, e esperar a bolinha ao lado de **AWS** ficar
   verde; depois **AWS Details** → **AWS CLI: Show**. O **AWS Details** fica na
   barra da página do laboratório, não dentro do console da AWS;
2. colar o bloco inteiro — do `[default]` até o fim da linha
   `aws_session_token=…`, a mais longa — em `~/.aws/credentials`
   (`nano ~/.aws/credentials`; colar com `Ctrl+Shift+V`, salvar com `Ctrl+O` e
   `Enter`, sair com `Ctrl+X`);
3. `chmod 600 ~/.aws/credentials`;
4. conferir:

```bash
aws sts get-caller-identity
```

Esperado: um JSON com a conta do laboratório. Um erro de credencial expirada
(`ExpiredToken`) quer dizer que o bloco colado é de uma sessão anterior.

> **Credenciais nunca entram no repositório.** O `.gitignore` exclui `.aws/`,
> `credentials`, `aws_credentials`, `.env` e `*.pem`, mas a regra é não as copiar
> para dentro da pasta do projeto em momento nenhum.

> **Os limites do laboratório são duros.** Tentar passar de um deles — tamanho de
> máquina acima de `large`, mais de 9 instâncias, mais de 32 vCPUs, volume acima de
> 100 GB — pode desativar a conta e apagar tudo o que há nela. O orçamento é de
> USD 50 e **não é reiniciado** pelo botão Reset. O saldo mostrado atrasa de 8 a 12
> horas.

---

## Passo 27 — Criar e destruir a máquina na AWS

A infraestrutura é descrita em `infra/`, com o Terraform: o arquivo diz o que deve
existir, o `apply` cria e o `destroy` apaga tudo de uma vez. **Com as credenciais
da sessão coladas** (passo 26).

| O que é criado | Detalhe |
|---|---|
| Rede própria | VPC `10.42.0.0/16`, sub-rede pública `10.42.1.0/24` em `us-east-1a`, internet gateway e rotas |
| Regra de acesso | SSH (porta 22) **só do IP de quem roda o `apply`**, lido de `checkip.amazonaws.com` |
| A máquina | `m5.large` (2 vCPU, 8 GB), Ubuntu 24.04.5 LTS oficial da Canonical **fixado pelo ID** (`ami-02a9cdd4e98bc3c78`, de 17/10/2026), chave `vockey`, perfil `LabInstanceProfile`, metadados só com token |
| O disco | 80 GB `gp3`, apagado junto com a máquina |

As variáveis em `infra/variables.tf` **recusam, antes de chegar à AWS**, um tamanho
acima de `large`, um disco acima de 100 GB ou uma região fora das duas do
laboratório.

**Por que a imagem é fixa** (decisão de 21/10/2026). A bateria da nuvem não cabe
numa sessão, e cada combinação roda num dia. Pedindo "a imagem mais recente",
combinações diferentes poderiam rodar sobre sistemas — e kernels — diferentes, e
uma diferença entre elas poderia vir da troca do sistema e não da carga. Fixada,
toda sessão liga o mesmo disco de fábrica, com o mesmo kernel
(`7.0.0-1014-aws`). Se a imagem deixar de existir, o `plan` falha antes de criar
qualquer coisa.

**A chave de SSH, uma vez só.** No laboratório, **AWS Details** → **Download PEM**.
O arquivo baixado (`labsuser.pem`) vai para o WSL, e depois sai da pasta Downloads:

```bash
mkdir -p ~/.ssh
cp /mnt/c/Users/turbi/Downloads/labsuser.pem ~/.ssh/labsuser.pem
chmod 400 ~/.ssh/labsuser.pem
```

O SSH recusa uma chave que outros possam ler: `ls -l ~/.ssh/labsuser.pem` tem de
começar com `-r--------`.

**Criar, entrar e destruir:**

```bash
cd infra
terraform init        # só na primeira vez: baixa os plugins travados no .terraform.lock.hcl
terraform plan        # o que SERIA criado; não cria nada
terraform apply       # cria; pede "yes"
ssh -i ~/.ssh/labsuser.pem ubuntu@$(terraform output -raw ip_publico)
terraform destroy     # apaga tudo; pede "yes"
```

No primeiro SSH a uma máquina nova o terminal pergunta se confia nela (`Are you
sure you want to continue connecting`): `yes`. Como cada `apply` cria uma máquina
nova, a pergunta volta a cada sessão.

**Esperado** (máquina original, 19/10/2026):

```
Plan: 7 to add, 0 to change, 0 to destroy.
...
Apply complete! Resources: 7 added, 0 changed, 0 destroyed.

Outputs:
ami_id = "ami-02a9cdd4e98bc3c78"
ami_nome = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20261017"
...
Destroy complete! Resources: 7 destroyed.
```

Dentro da máquina, `nproc` dá **2**, `free -h` dá **7,6 Gi** e `df -h /` dá **77 G**.

**Conferir que nada sobrou**, pela própria AWS:

```bash
aws ec2 describe-instances --filters Name=tag:Projeto,Values=tcc-pipelines --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
```

Só pode aparecer `terminated`, ou nada. Uma máquina `terminated` continua na lista
do console por cerca de uma hora e não cobra mais.

> **`terraform destroy` ao fim de TODA sessão.** Quando a sessão do laboratório
> acaba, as máquinas ligadas são só **paradas**, e voltam a ligar sozinhas na
> sessão seguinte — cobrando. O disco cobra mesmo com a máquina parada.

> **Cada comando do Terraform leva uns 2 minutos a mais.** O plugin da AWS é um
> executável de 803 MB e fica em `infra/.terraform/`, no disco do Windows, que o
> WSL lê devagar. Medido em 19/10/2026: `validate` 120 s, `plan` 163 s, `apply`
> 240 s, `destroy` 270 s — dos quais a AWS levou poucos segundos. Ficou assim por
> decisão do autor; `export TF_DATA_DIR=~/tcc-data/terraform`, seguido de um
> `terraform init`, leva a pasta para o disco do WSL.

> **Não apague o `infra/terraform.tfstate` com a máquina no ar.** É o registro do
> que o Terraform criou (fora do git). Sem ele o `destroy` não sabe o que apagar, e
> a limpeza tem de ser feita à mão, no console.

> **Se o seu IP mudar**, o SSH para de entrar. Um novo `terraform apply` lê o IP
> atual e atualiza só a regra, sem recriar a máquina.

---

## Passo 28 — Preparar a máquina da AWS

A máquina nasce como um Ubuntu vazio, e é destruída ao fim de cada sessão: a
preparação se repete em **toda** sessão. Um script a deixa igual ao WSL de
referência, refazendo dentro dela os passos 2 a 5, a preparação da base, os passos 12 e 13 e
a conferência do relógio.

**Dentro da máquina** (depois do `ssh` do passo 27):

```bash
git clone https://github.com/eduardoalins/Pipelines-TCC.git
bash Pipelines-TCC/scripts/preparar_maquina.sh
```

**O código vem do GitHub**, não do seu computador: a máquina roda exatamente um
commit publicado, e o manifesto de cada rodada registra qual. Uma correção só vale
na nuvem depois de `commit` **e** `push`.

**O que o script faz**, nesta ordem:

| # | Passo | Detalhe |
|---|---|---|
| 1 | Pacotes do sistema | espera o primeiro boot da máquina terminar de mexer no `apt`; **desliga as atualizações automáticas**; **congela o `apt` do host** no arquivo histórico de 18/10/2026; instala o `python3.12-venv` |
| 2 | Docker | **as mesmas versões do Docker Desktop do WSL**: Engine 28.0.4, containerd 1.7.26, Compose 2.34.0, buildx 0.22.0, cgroups por `cgroupfs`; os pacotes ficam travados (`apt-mark hold`) |
| 3 | Ambiente Python | o próprio `setup_env.sh` do passo 2 |
| 4 | As duas imagens | `docker compose build`, de novo a cada sessão; a base é travada por digest, os pacotes do Ubuntu pelo arquivo histórico (passos 5 e 12) e os jars por checksum |
| 5 | A base real | o `preparar_base.sh` (antes do passo 6): baixa o `.gz` da REES46 **dentro da AWS**, confere o sha256 — se não bater, para —, descompacta e conta |
| 6 | Kafka e tópico | o mesmo `docker-compose.yml` e o `create_topic.sh` (24 partições) |
| 7 | Cadeias | 6000 eventos **reais** pelo replayer (1000 evt/s por 6 s), `check_chain.py` e `check_chain_flink.py` |
| 8 | Versões | `scripts/versoes.sh` |
| 9 | Relógio | `check_clock.py` — fora do WSL há um sincronizador só, e ele ligado é o estado certo |

Tudo que sai na tela vai também para `~/tcc-data/preparacao.txt`, **na máquina**,
com o tempo de cada passo. Como o disco é apagado no `destroy`, traga o arquivo
para o seu computador antes (no WSL, dentro de `infra/`):

```bash
scp -i ~/.ssh/labsuser.pem ubuntu@$(terraform output -raw ip_publico):tcc-data/preparacao.txt ~/tcc-data/preparacao_aws_$(date +%Y%m%d_%H%M).txt
```

É a evidência de com que ambiente aquela sessão rodou.

**Esperado** (máquina original, 24/10/2026), cerca de **8 minutos**:

```
>> [   0s] Codigo: commit 1e10b76 — Step 10: machine preparation downloads the real base
>> [   0s] 1. Pacotes do sistema
atualizacoes automaticas desligadas
apt do host no arquivo historico de 20261018T000000Z
>> [  49s] 2. Docker
docker 28.0.4, compose 2.34.0, cgroups cgroupfs, armazenamento overlay2
>> [  69s] 3. Ambiente Python
>> [  76s] 4. Imagem do Spark
>> [ 176s] 4. Imagem do Flink
>> [ 267s] 5. A base real
baixando de https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz (1,7 GB)
OK: 8ebca1ad741295297368f2cf0315e3f36853a1a11768fb16babf8c9b83838147
OK: 42448764 eventos
>> [ 345s] 6. Kafka
OK: o Kafka grava no volume
OK: ecommerce.events.v1 com 24 particoes.
>> [ 375s] 7. Replayer (6000 eventos reais) e as cadeias
janela .......... linhas 27139042 a 27145041 (seed 42)
CADEIA COMPLETA OK
CADEIA COMPLETA OK
>> [ 447s] 8. Versoes
>> [ 450s] 9. Relogio
OK: relogio sao. Pode medir.
>> [ 490s] Maquina pronta
disco livre .... 65G
```

**A base, dentro da AWS:** o download de 1,7 GB levou **28 s** (~58 MB/s), e o
passo inteiro — download, checksum, descompactação e contagem — 78 s. Nada passa
pela sua internet. Sobram **65 GB**, acima da guarda de 50 GB do `run_round.py`.
O posicionamento do replayer até a janela levou 11,8 s na nuvem, contra ~5 s no
WSL; ele acontece antes do envio e não entra na medição.

No build das imagens, todas as linhas `Get:` do `apt` têm de vir de
`https://snapshot.ubuntu.com/ubuntu/20261010T200000Z` — em 21/10/2026 foram 68, e
nenhuma do repositório normal.

Somando o `apply` e o `destroy` do passo 27, cada sessão gasta uns **18 minutos**
só para existir e deixar de existir.

**Comparar com o WSL.** O `scripts/versoes.sh` roda nos dois ambientes:

```bash
bash scripts/versoes.sh
```

Na comparação de 19 e 20/10/2026, tudo o que ele lê saiu igual, no Docker e dentro
das duas imagens: Python 3.10.12, `python3.10` 3.10.12-1~22.04.18, `pip`
22.0.2, Java 17.0.20, PySpark 4.0.4, PyFlink 1.20.5 e os checksums dos jars. Os
IDs das imagens diferem — cada build gera o seu — e o kernel, o hipervisor e o
processador são outros (kernel 7.0 da AWS contra 5.15 do WSL; 2 vCPU contra 8).

**O ambiente da nuvem é congelado** (decisões de 19 a 21/10/2026). Toda sessão
monta exatamente a mesma máquina, e nada muda sozinho durante uma medição:

| Camada | Como fica fixa |
|---|---|
| Sistema da máquina e kernel | a imagem pelo ID (passo 27) |
| Pacotes do host | o arquivo histórico do Ubuntu de 18/10/2026 |
| Atualizações automáticas | desligadas no começo da preparação |
| Docker | as versões do WSL, pelo número, travadas com `apt-mark hold` |
| Pacotes das imagens | o arquivo histórico de 10/10/2026 e os números das versões |
| Bibliotecas Python e jars | `requirements.lock` e checksums, como no local |

O que não dá para congelar fica declarado: o hipervisor e o processador da AWS, e
as 2 vCPU. Os resultados da nuvem valem para **este** ambiente — que, por estar
congelado, qualquer pessoa consegue montar de novo.

> **O arquivo histórico depende do `snapshot.ubuntu.com` no ar.** Se o serviço
> cair, o build falha com erro — nunca instala pacotes do dia em silêncio.

> **Rodar de novo** pula o que já está feito: o script é idempotente. Ele também
> se recusa a rodar no WSL, para não instalar outra versão do Docker no seu
> computador por engano.

---

## Verificação final

Se os vinte e oito passos acima funcionaram, o ambiente está reproduzido:

- [x] `free -h` mostra o teto configurado da VM
- [x] `docker compose ps` mostra o broker `healthy`
- [x] `docker exec kafka ls /var/lib/kafka/data` lista `meta.properties`
- [x] O tópico existe com 24 partições
- [x] A imagem `tcc-spark:4.0.4` constrói, e o `check_chain.py` fecha a cadeia até o Spark dentro dela
- [x] O `preparar_base.sh` confere o sha256 da base e conta 42.448.764 eventos
- [x] O replayer reporta tentados = confirmados, 0 erros, e a janela da seed 42 começa na linha 27.139.042
- [x] As 24 partições recebem os 6000 eventos (a distribuição de referência, a remedir com a base real)
- [x] Os eventos sobrevivem a `down` → `up`
- [x] O Spark grava um lote a cada 5 segundos, somando 6000 eventos
- [x] O destino tem o layout `run_id=…/dt=…/hh=…` em Parquet
- [x] O job reiniciado retoma sem reprocessar
- [x] A imagem `tcc-flink:1.20.5` constrói, com o checksum do conector conferido
- [x] `check_chain_flink.py` lê dado das 24 partições
- [x] O Flink grava Parquet em `~/tcc-data/out/flink` e encerra com cancelamento limpo
- [x] Os arquivos do Flink ficam visíveis em degraus, um por checkpoint
- [x] O job do Flink reiniciado retoma do último checkpoint sem reprocessar
- [x] Com o `systemd-timesyncd` parado, o `check_clock.py` diz `OK: relogio sao`
- [x] O Spark registra o log de progresso e commita offsets sob `tcc-nrt-spark`
- [x] O coletor mostra o serrilhado do lag, com vales estáveis
- [x] O `reconcile.py` do Spark: 0 perdas, 0 duplicatas, espera no trigger ~metade do trigger
- [x] O Flink imprime uma linha por checkpoint e commita offsets sob `tcc-rt-flink`
- [x] O `reconcile.py` do Flink: 0 perdas, 0 duplicatas, processamento em ms, conferência do `ctime` em poucos ms
- [x] Os dois motores gravam os campos do evento como texto, inclusive o `event_time` do contrato 3.0, sem mudança nos pipelines
- [x] Com a base real, os dois destinos saem idênticos entre si e iguais às linhas da base, com o preço exatamente como veio (`543.10`)
- [x] O `transform.py` grava a tabela tipada de cada motor: `amount` em `DECIMAL(10,2)` com o `543.10` intacto, `event_time` em `TIMESTAMP`, e as duas tabelas iguais entre si e à base
- [ ] O `transform.py` remove as duplicatas técnicas e as repetidas da origem — escrito em 24/10/2026, a testar
- [ ] ~~Com eventos malformados, o `reconcile.py` conta 18 inválidas~~ — passo 21 aposentado no contrato 3.0 (resultado de 12/10/2026)
- [x] O `run_round.py` fecha uma rodada válida de cada motor: 0 perdas, 0 duplicatas, fila zerada
- [x] Nas rodadas com malformados a rodada vale, e as inválidas saem iguais nos dois motores (até o contrato 2.2)
- [x] O Flink roda com checkpoint de 5 s nas combinações de intervalo 30, e avisa na tela
- [x] O `run_combination.py` alterna os motores, pula as rodadas válidas e, com as 20 válidas, escreve os arquivos e apaga o material bruto
- [x] O `write_results.py` escreve o `.txt` e o `.md`, com a CPU em % da máquina, e rodar de novo dá arquivos idênticos
- [x] A bateria local fecha as seis combinações: 120 rodadas válidas, `results/` vazia, seis pares de arquivos
- [x] O `setup_aws_tools.sh` instala o Terraform e a AWS CLI, e o `aws sts get-caller-identity` responde com a conta do laboratório
- [x] O `terraform apply` cria 7 recursos, o SSH entra numa `m5.large` com 2 núcleos, e o `terraform destroy` apaga os 7 sem deixar sobra
- [x] O `preparar_maquina.sh` deixa a máquina pronta em ~6 minutos, com o mesmo Docker do WSL, as duas cadeias completas e o relógio são
- [x] O `apt` das imagens e o do host leem só do arquivo histórico do Ubuntu, e as versões de dentro das imagens saem iguais às da bateria local

---

## Referência

### Onde as coisas moram

Decisão E6: **código no Windows, dados em ext4 nativo do WSL.**

| | Caminho |
|---|---|
| Código (este repo) | `/mnt/c/.../Pipelines-TCC` |
| Ambiente Python do host | `~/.venvs/tcc` — gerador, coletor e reconciliador |
| A base real | `~/tcc-data/base/2019-Oct.csv` e o `.gz` de origem — `scripts/preparar_base.sh` |
| Parquet de saída | `~/tcc-data/out/spark` e `~/tcc-data/out/flink` — um `<base>` por motor |
| Tabela tipada (Transform) | `~/tcc-data/transformado/<motor>/run_id=<run_id>/eventos.parquet` — gerada à mão, depois do Load |
| Checkpoints | `~/tcc-data/checkpoints/nrt` (Spark) e `~/tcc-data/checkpoints/rt` (Flink) |
| Métricas coletadas | `~/tcc-data/metrics` — manifestos do replayer, logs de progresso dos dois motores, CSVs do coletor e relatórios do `reconcile.py` |
| Material bruto das rodadas | `results/`, neste repositório e **fora do git** — apagado quando a combinação fecha (passo 23) |
| Arquivos de resultado | `../../Informações/Resultados Local`, ou a pasta dada em `SAIDA` (passo 24) |
| Ambiente do Spark | imagem Docker `tcc-spark:4.0.4` — fora do venv |
| Ambiente do Flink | imagem Docker `tcc-flink:1.20.5` — fora do venv |

O motivo é medido, não estético: escrita sequencial em `/mnt/c` roda a
**77 MB/s** contra **1,2 GB/s** no ext4 — 15,8×. Com a saída no `/mnt/c`, o
gargalo do pipeline seria o sistema de arquivos do Windows, e o Experimento A
estaria medindo drvfs em vez de Spark.

Nada de `~/tcc-data` entra no git. O que reproduz aqueles dados é a base (com o sha256), a janela e
o `run_id` da execução.

### Protocolo de execução

Antes de **qualquer execução medida**, fechar Discord, Steam e navegadores. Não é
só memória: esses processos disputam CPU de forma imprevisível, e essa disputa
entra na variância que seria reportada como efeito do parâmetro em teste.

E **conferir o relógio** — parar o `systemd-timesyncd`, esperar, rodar o
`scripts/check_clock.py` —, como descrito antes do passo 17. Ao terminar, religar
o serviço.

Antes de medir um motor, **zerar o checkpoint dele**: os dois leem o mesmo tópico,
e a medição de um deixa pendência para o outro.

O script de rodada (passo 22) confere o relógio, zera o checkpoint e recria o
tópico sozinho. Fechar os outros programas e parar o `systemd-timesyncd` continua
sendo com você.

### Versões travadas (decisão E7)

Estes números fazem parte do método experimental. Trocar qualquer um deles no
meio dos experimentos invalida as execuções já feitas.

| | Versão | Verificado em |
|---|---|---|
| Kafka | 4.0.2 (KRaft) | 03/09/2026 |
| Spark / PySpark | 4.0.4 (Scala 2.13), em contêiner | 03/09/2026 no host; 10/10/2026 no contêiner |
| Conector Kafka do Spark | `spark-sql-kafka-0-10_2.13` 4.0.4 e 10 dependências (`kafka-clients` 3.9.1), 11 jars com sha256 igual ao do host | 10/10/2026 |
| Python do Spark | 3.10.12 (Ubuntu 22.04 da imagem) — era 3.12.3 no host | 10/10/2026 |
| Flink / PyFlink | 1.20.5 (LTS), em contêiner | 21/09/2026 |
| Conector Kafka do Flink | `flink-sql-connector-kafka` 3.4.0-1.20 | 21/09/2026 |
| Python do Flink | 3.10.12 (Ubuntu 22.04 da imagem) | 21/09/2026 |
| JVM dos dois motores | Temurin 17.0.20+8; base travada pelo digest `sha256:e85989f3…`. O Spark usou o OpenJDK 17.0.20.1 do host até 10/10/2026 | 24/09/2026 (Flink); 10/10/2026 (Spark) |
| Coletor de lixo da JVM | Serial nos dois — escolhido pela JVM por causa do teto de 1536 MB do contêiner | 10/10/2026 |
| Python do host (gerador, coletor, reconciliador) | 3.12.3 | 03/09/2026 |
| confluent-kafka | 2.15.0 (librdkafka 2.15.0) — no host e na imagem do Spark | 04/09/2026 |
| DuckDB (leitor do `reconcile.py` e do Transform) | 1.5.5 | 24/09/2026 |
| Base de eventos | REES46 `2019-Oct.csv.gz`, sha256 `8ebca1ad…38147`, 42.448.764 eventos | 23/10/2026 |
| Terraform | 1.16.4, no WSL, conferido pelo checksum da HashiCorp | 17/10/2026 |
| AWS CLI | 2.38.1 — a mais recente na instalação; só cria e lê recursos, não entra em medida | 17/10/2026 |
| Docker | Engine 28.0.4, containerd 1.7.26, runc 1.2.5, Compose 2.34.0, buildx 0.22.0, cgroups `cgroupfs` — o Docker Desktop do WSL, e o mesmo na máquina da AWS (`preparar_maquina.sh`) | 19 e 20/10/2026 |
| Máquina da AWS | `m5.large`, Ubuntu 24.04.5, imagem **fixa** `ami-02a9cdd4e98bc3c78` (17/10/2026), kernel `7.0.0-1014-aws`; `apt` do host no arquivo histórico de 18/10/2026; atualizações automáticas desligadas | 21/10/2026 |
| Pacotes do Ubuntu nas imagens | arquivo histórico de 10/10/2026, 20:00 UTC; `python3` 3.10.6-1~22.04.1, `python3.10` 3.10.12-1~22.04.18, `python3-pip` 22.0.2+dfsg-1ubuntu0.7 | 21/10/2026 |

Cada ambiente tem o seu arquivo de versões. As das ferramentas do host estão no
`requirements.txt` da raiz. As do Spark, em `pipeline-nrt/requirements.txt`; as do
Flink, em `pipeline-streaming/requirements.txt`. Nas duas imagens, o
`requirements.lock` da mesma pasta trava também as dependências indiretas.

### Estrutura

```
CONTRATO.md               schema e invariantes de comparabilidade   (Etapa 2)
docker-compose.yml        Kafka 4.0.2 (KRaft) e os contêineres do Spark e do Flink (perfis)
requirements.txt          versões travadas das ferramentas do host (decisão E7)
env/wslconfig             configuração da VM, versionada e comentada
scripts/setup_env.sh      recria o ambiente do host do zero
scripts/create_topic.sh   cria o tópico com 24 partições
scripts/check_chain.py    verifica Python → Spark → JVM → Kafka, no contêiner
scripts/check_chain_flink.py  verifica Python → Flink → JVM → Kafka  (RT-1)
scripts/check_clock.py    confere o relógio antes de medir         (Etapa 6.2)
scripts/preparar_base.sh  baixa, confere e descompacta a base real (Etapa 9.1)
scripts/setup_aws_tools.sh  instala o Terraform e a AWS CLI no WSL   (Etapa 10)
scripts/preparar_maquina.sh prepara a máquina da AWS, dentro dela    (Etapa 10)
scripts/versoes.sh        versões do Docker e de dentro das imagens, para comparar ambientes
shared/                   ARNÊS COMPARTILHADO — Spark e Flink usam igual
  schema/evento.json      fonte única do schema — lida pelos dois  (RT-3)
  data-generator/         a origem dos eventos
    replayer.py           envia a base real ao Kafka, uma janela por rodada (contrato 3.0)
    generator.py          o gerador sintético — sem uso desde o 3.0, mantido como registro (Etapas 3 e 8)
    malformed.py          os 8 tipos de malformado do gerador antigo — sem uso no benchmark desde o 3.0
    inject_malformed.py   injetor de eventos quebrados — aposentado no 3.0 (Etapa 7)
  analysis/reconcile.py   perdas, duplicatas, inválidas, latências, throughput e is_late, via DuckDB (Etapas 6 a 9)
  transform/              o "T" do ELT, depois do Load e fora da medição (contrato 3.1)
    conversao.py          a regra do texto ao tipo do contrato — usada também pelo reconcile.py
    transform.py          grava a tabela tipada de uma execução, sem duplicatas
  monitoring/collector.py consumer lag, CPU e memória a cada 1 s, os dois motores (Etapas 6 e 8)
pipeline-nrt/             pipeline Spark Structured Streaming      (Etapa 4+)
  Dockerfile              runtime: Java 17.0.20, Python, PySpark, conector (Etapa 6.2)
  requirements.txt        as versões do PySpark e do cliente Kafka
  requirements.lock       todas as dependências, travadas
  run_local.sh            lançador, via docker compose run
  submit.sh               roda no contêiner: chama o spark-submit
  src/config.py           trigger, endereços, destino, codec e checkpoint
  src/esquema.py          traduz o evento.json para StructType
  src/main.py             o job
pipeline-streaming/       pipeline Flink                           (RT-1+)
  Dockerfile              runtime: Java 17.0.20, Python, PyFlink, conector
  requirements.txt        a versão do PyFlink
  requirements.lock       todas as dependências, travadas
  run_local.sh            lançador, via docker compose run         (RT-2)
  src/config.py           endereços, destino, checkpoint e intervalo
  src/esquema.py          traduz o evento.json para a Table API     (RT-3)
  src/main.py             o job
benchmarks/               o benchmark                              (Etapas 8 e 9)
  run_round.py            uma rodada de um motor, do começo ao fim
  run_combination.py      as 10 rodadas de cada motor para um intervalo e uma carga
  write_results.py        escreve o .txt e o .md de cada combinação
infra/                    Terraform: rede, acesso e a máquina na AWS (Etapa 10)
  versions.tf             Terraform 1.16.4 e os plugins; o .terraform.lock.hcl trava as versões
  variables.tf            região, tipo, disco e chave — com as guardas dos limites do laboratório
  main.tf                 VPC, sub-rede, rotas, regra de SSH, imagem do Ubuntu e a máquina
  outputs.tf              IP, comando de SSH e a imagem usada
results/                  material bruto das rodadas — fora do git, apagado ao fechar a combinação
```

A pasta `shared/` é fisicamente separada dos dois pipelines de propósito: é o que
impede que o Flink acabe medido com um arnês ligeiramente diferente.

### Documentos de pesquisa

Por decisão do autor, os documentos ficam **fora** deste repositório, junto com a
dissertação, em `../../Informações/`:

| Documento | Papel |
|---|---|
| `planoNRT3.md` | plano do pipeline NRT (Spark) — as 12 etapas e o protocolo experimental |
| `planoRT.md` | plano do pipeline Streaming (Flink) — complementa o `planoNRT3.md`, não o substitui |
| `EtapaNRT/DecisaoEtapaX.md` | registro de cada etapa do NRT: o que foi feito, como, e as decisões com a justificativa |
| `EtapaRT/DecisaoEtapaRT<N>.md` | o mesmo registro, para as etapas do Flink |
| `Fala/PontosSensiveis.md` | o que a banca pode cobrar, com a resposta e a evidência de cada ponto |
| `Resultados Local/` | o `.txt` e o `.md` de cada combinação, escritos pelo `write_results.py` |
| `perguntas.txt`, `perguntas2.txt` | decisões de projeto do NRT e do Flink, cada uma com a justificativa |
| `planoNRT.md`, `planoNRT2.md` | versões anteriores, mantidas por rastreabilidade |

Os `DecisaoEtapaX.md` são a matéria-prima do capítulo de metodologia. Eles
registram também o **percurso** das decisões que mudaram no caminho — e as
correções, quando algo que se acreditava verdadeiro não era.

O repositório guarda o que **executa**; a pasta de pesquisa guarda o que
**justifica**. O `CONTRATO.md` é a ponte entre os dois.

---

## Progresso

O trabalho alternou para o Flink em 11/09/2026, com o NRT concluído até a Etapa 5.
A Etapa 6 do NRT e a RT-4 do Flink foram a **mesma etapa**, concluída em
25/09/2026: o instrumento de medição foi construído uma vez, para os dois motores.
Os dois pipelines estão agora no mesmo ponto.

Em 10/10/2026, com as métricas locais passando a entrar no trabalho, o Spark foi
para contêiner, sobre a mesma imagem base do Flink (Etapa 6.2). Os números de
setembro — Spark no host, relógio não conferido — ficam como histórico.

Em 12 e 13/10/2026 a bateria local fechou as seis combinações (Etapa 9 = RT-7),
com os dois motores carregando os campos como texto (Etapa 8.1). Os resultados
ficam fora do repositório, em `../../Informações/Resultados Local`.

Em 23/10/2026, a pedido da orientação, os eventos passaram a ser **reais** (Etapa
9.1, contrato 3.0): a base da REES46, reproduzida por um replayer. A bateria local
de 12–13/10, feita com o gerador sintético, fica como histórico e **será refeita**
com a base real. A Etapa 10 (AWS) está pausada nas decisões que dependem disso.

Em 24/10/2026 o **T** do ELT deixou de ser só declarado (contrato 3.1): o módulo
`shared/transform/` converte o texto do destino para os tipos do contrato — o
preço em decimal exato, o instante original em data e hora — e grava uma tabela
tipada por execução, sem duplicatas, depois do Load e fora da medição.

### Pipeline Streaming (Flink) — `planoRT.md`

- [x] **RT-1** — Versões travadas e cadeia verificada: Flink 1.20.5 LTS em
      contêiner, conector 3.4.0, dado lido das 24 partições, ~830 MiB de pico
- [x] **RT-2** — Flink lê do Kafka e imprime: fluxo contínuo, os mesmos eventos
      que o Spark recebeu, encerramento limpo com código 130
- [x] **RT-3** — Parquet + checkpoint: visibilidade por checkpoint confirmada,
      retomada verificada (18000 → 24000), um terço dos arquivos do Spark
- [x] **RT-4** — Instrumento de medição, para os dois motores: `processed_ts` por
      evento, visibilidade pelo `ctime` do arquivo, offsets no Kafka; 0 perdas,
      processamento P50 8 ms e visibilidade P50 2,6 s a 100 evt/s
- [x] **RT-5** — Redefinida: ELT estrito, sem join. Eventos ilegíveis medidos e
      contados; o Flink descartava 15 de 42 e convertia tipos que o Spark anulava —
      igualados na Etapa 8.1, com a carga em texto
- [x] **RT-6** — Arnês unificado: é a Etapa 8 do NRT, a mesma para os dois
      motores — ver abaixo
- [x] **RT-7** — Validação local: é a Etapa 9 do NRT, a mesma bateria para os
      dois motores, com o checkpoint do Flink fixo em 5 s — ver abaixo. Os
      eventos reais (Etapa 9.1) valem para os dois

### Pipeline NRT (Spark) — `planoNRT3.md`

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
- [x] **Etapa 5** — Escrita em Parquet com checkpoint: layout do contrato,
      codec `snappy`, retomada verificada, 13 micro-batches somando 6000 eventos
- [x] **Etapa 6** — Instrumento de medição: log de progresso, lag a cada 1 s,
      `reconcile.py` que detecta perdas injetadas; 0 perdas, processamento P50
      3,9 s e visibilidade P50 4,3 s a 100 evt/s
- [x] **Etapa 6.2** — Spark em contêiner, relógio conferido antes de medir:
      retomada e regressão do Flink verificadas, 0 perdas; processamento P50
      3,9 s (Spark) e 7 ms (Flink), visibilidade P50 4,4 s e 2,6 s a 100 evt/s
- [x] **Etapa 7** — Redefinida: ELT estrito, sem join nem limpeza no pipeline. O
      que cada motor faz com eventos ilegíveis foi medido, declarado no contrato
      1.8 e passou a ser contado em toda execução (inválidas, nulas, descartadas)
- [x] **Etapa 8** — Arnês de benchmark: os pipelines encerram por pedido ou por
      inatividade; seed por rodada e 1% de malformados em 4 das 10; CPU e memória
      no coletor; uma rodada, uma combinação e os arquivos de resultado por
      comando. Contrato 1.9
- [x] **Etapa 8.1** — Os dois pipelines carregam os dez campos como texto; a
      conversão vai para depois do Load, e os motores passam a gravar o mesmo para
      os eventos malformados. Contrato 2.0
- [x] **Etapa 9** — Validação local: seis combinações (trigger do Spark de 5 e
      30 s, Flink fixo em 5 s; 1.000, 7.500 e 15.000 evt/s), 120 rodadas válidas,
      0 perdas; aquecimento por tempo e CPU em % da máquina. Contratos 2.1 e 2.2.
      Derrubar serviços saiu do plano na Etapa 8; os testes de quebra por volume
      seguem em aberto. Feita com o gerador sintético: será refeita com a base real
- [x] **Etapa 9.1** — Eventos reais: a base da REES46 (outubro de 2019, 42,4
      milhões de eventos, sha256 conferido), um replayer com uma janela da base por
      rodada, o `event_time` original como 11º campo, sem malformados. Testado de
      ponta a ponta nos dois motores: destinos idênticos entre si e iguais à base.
      Contrato 3.0
- [ ] **Etapa 10** — Terraform e AWS: ferramentas, Terraform e preparação da
      máquina feitos e testados, ambiente da nuvem congelado; o ensaio de carga
      mostrou que uma `m5.large` com tudo dentro não aguenta 15.000 evt/s
- [ ] **Etapa 11** — Benchmarks oficiais
- [ ] **Etapa 12** — Análise e guia de decisão
