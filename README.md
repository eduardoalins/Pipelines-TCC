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

Os pipelines são **ELT estrito**: extraem e carregam, sem transformar. Não há
join nem limpeza dentro deles; a transformação acontece depois, no destino, é a
mesma para os dois motores e fica fora da comparação. O que o trabalho compara é
o **E** e o **L**.

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
| 1 a 8 | ambiente, Kafka, tópico e gerador — **compartilhados** (o passo 5 constrói a imagem do Spark) | Etapas 0 a 3 |
| 9 a 11 | pipeline NRT (Spark), em contêiner | Etapa 5 — Parquet com checkpoint |
| 12 a 16 | pipeline Streaming (Flink), em contêiner | Etapa RT-3 — Parquet com checkpoint |
| antes do 17 | **conferir o relógio** — obrigatório antes de qualquer medição | Etapa 6.2 |
| 17 a 20 | instrumento de medição, nos dois motores | Etapa 6 / RT-4 — lag, reconciliação e latências |
| 21 | eventos malformados: o que cada motor faz, e como isso é contado | Etapa 7 / RT-5 |

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

**O que este venv é.** As ferramentas que ficam **fora** dos motores: o gerador, o
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

> **Editar um arquivo copiado para a imagem reconstrói as camadas seguintes** —
> mesmo que a mudança seja um comentário no `requirements.txt`. O resultado deve
> sair igual, porque as versões estão travadas, mas é outra imagem: depois de
> reconstruir, repita este passo antes de medir.

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

**Aba 2 — o gerador:**

```bash
~/.venvs/tcc/bin/python shared/data-generator/generator.py
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

**O número de arquivos é um resultado, não um defeito.** Cada micro-batch escreve
um arquivo por partição do Kafka, porque não há `repartition` (decisão E3). Um
lote único produz 24 arquivos; a mesma quantidade de eventos entregue em treze
lotes produz ~312. A referência medida na máquina original, para 6000 eventos:

| Forma de entrega | Arquivos | Tamanho |
|---|---|---|
| Lote único (backlog) | 24 | 632 KB |
| Trigger de 5 s | ~300 | ~3,6 MB |

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
vazio, falha avisando para rodar o gerador.

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

O espelho do passo 9. O job lê o tópico, interpreta o JSON e grava Parquet em
`~/tcc-data/out/flink`, com checkpoint a cada 5 s.

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

**Aba 3:** o gerador.

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

Conte as linhas: o número não muda. Rode o gerador de novo e conte: tem que subir
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
# esperar ~2 minutos: o relógio leva esse tempo para assentar
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

Se sair `NAO MEDIR`, não meça: espere mais um pouco e rode de novo. O script só
lê; não altera nada. Sai com **0** ou **1**, para o arnês da Etapa 8 chamá-lo
antes de cada execução.

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
> o gerador produziu para medir o Flink fica pendente para o Spark, e vice-versa.
> Com checkpoint existente, `STARTING_OFFSETS=latest` não vale — o Spark retoma de
> onde parou e processa essa pendência logo no primeiro lote, com minutos de
> latência, sob o `run_id` da outra medição.

Espere o `[batch 0] vazio`. Aba 2, o gerador:

```bash
RATE=100 DURATION=60 ~/.venvs/tcc/bin/python shared/data-generator/generator.py
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

> **O coletor começa depois do primeiro batch**, de propósito, só neste teste: o
> primeiro commit de um grupo novo pode receber `NOT_COORDINATOR` (o broker ainda
> está elegendo o coordenador). O pipeline repete a tentativa; consultar o grupo
> antes esconderia o erro.

---

## Passo 18 — Reconciliar a execução do Spark

O `reconcile.py` confere o que foi gravado contra o manifesto do gerador. Sem
`RUN_ID`, lista as execuções disponíveis:

```bash
~/.venvs/tcc/bin/python shared/analysis/reconcile.py
```

Para reconciliar a mais recente sem copiar nada à mão:

```bash
RUN=$(ls -1 ~/tcc-data/out/spark/events | grep 'run_id=[0-9]' | tail -1 | sed 's/run_id=//'); echo "$RUN"
RUN_ID=$RUN ~/.venvs/tcc/bin/python shared/analysis/reconcile.py
```

O `grep` deixa só as execuções do gerador, cujo `run_id` começa pela data. Sem ele,
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
Spark ainda no host, eram 3903 e 4298 ms.

A **espera no trigger** com mediana perto de **metade do trigger** (2,6 s de 5 s) é a
aritmética do micro-batch aparecendo no dado. O programa sai com **0** se não há
perda, duplicata, linha inválida, linha nula nem descarte, e com **1** se há — o
arnês da Etapa 8 usa isso. O `descartadas=?` é o esperado por enquanto: essa
contagem depende de uma anotação que o arnês passará a gravar (passo 21).

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

Depois do `>> job no ar`, o gerador e o coletor como no passo 17, trocando o grupo:

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
  checkpoints com dados .. 13   (o 1o e descartado como aquecimento)
  instantes de ctime ..... 15   (arquivos a ate 1000 ms um do outro = mesmo checkpoint)
  committed_ts ........... ctime de cada arquivo (CONTRATO.md 1.6, 2.2)
  espera pelo checkpoint     P50   2599 ...
  LAT. PROCESSAMENTO         P50      7 ...
  LAT. VISIBILIDADE          P50   2606 ...
  conferencia ctime x log de checkpoints: -10..7 ms em 15 instantes de ctime
OK: perdas=0  duplicatas=0  invalidas=0  nulas=0  descartadas=?
```

Em setembro eram 8 ms e 2626 ms.

**Checkpoints e instantes.** Os 8 arquivos de um checkpoint ficam visíveis quase
juntos, mas nem sempre no mesmo milissegundo. O reconciliador agrupa os arquivos
cujo `ctime` dista até 1 s e trata o grupo como um checkpoint — é o grupo inteiro
que a regra de aquecimento descarta. A latência de cada evento continua calculada
com o `ctime` do próprio arquivo.

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

## Passo 21 — Eventos malformados: a diferença declarada

Os pipelines são **ELT estrito**: carregam o que chega, não limpam. Limpeza é
transformação, e fica para o destino. Este passo mostra o que cada motor faz,
sozinho, com um evento que não consegue ler — e que os dois **não** fazem a mesma
coisa. A diferença não foi igualada; é declarada (`CONTRATO.md`) e **contada** pelo
`reconcile.py` em toda execução.

> **Este passo suja o tópico.** As 42 mensagens quebradas de cada rodada ficam lá
> até a retenção apagá-las ou o tópico ser recriado (`docker compose down -v`).
> Nunca rode no meio de uma bateria medida.

**Com o Spark.** Aba 1:

```bash
rm -rf ~/tcc-data/checkpoints/nrt
STARTING_OFFSETS=latest bash pipeline-nrt/run_local.sh
```

Espere o `[batch 0] vazio`. Aba 2, o injetor — 420 eventos válidos e 42 quebrados,
de 14 tipos:

```bash
~/.venvs/tcc/bin/python shared/data-generator/inject_malformed.py
```

Quando a aba 1 mostrar os 462 eventos gravados (num lote ou em dois), `Ctrl+C` e:

```bash
RUN=$(ls -1 ~/tcc-data/out/spark/events | grep 'run_id=malformados' | tail -1 | sed 's/run_id=//'); echo "$RUN"
RUN_ID=$RUN ~/.venvs/tcc/bin/python shared/analysis/reconcile.py
```

**Com o Flink.** Aba 1:

```bash
rm -rf ~/tcc-data/checkpoints/rt
STARTUP_MODE=latest-offset bash pipeline-streaming/run_local.sh
```

Espere o `>> job no ar` e **mais uns 10 segundos**. Aba 2, o mesmo injetor. Espere
três linhas de `[checkpoint N]` depois da que tiver o `estado=` maior, `Ctrl+C` e:

```bash
RUN=$(ls -1 ~/tcc-data/out/flink/events | grep 'run_id=malformados' | tail -1 | sed 's/run_id=//'); echo "$RUN"
MOTOR=flink RUN_ID=$RUN ~/.venvs/tcc/bin/python shared/analysis/reconcile.py
```

**Esperado** (referência medida na máquina original em 11/10/2026):

| | Spark | Flink |
|---|---|---|
| Válidos gravados | 420 de 420 | 420 de 420 |
| Linhas no destino | 462 | 447 |
| **INVÁLIDAS** — algum campo obrigatório nulo | 21 | 15 |
| **NULAS** — todos os campos nulos | 15 | 0 |
| **DESCARTADAS** — a mensagem sumiu | 0 | 15 |
| Erro ou aviso do motor | nenhum | nenhum |

Nos dois o programa termina com `DIVERGENCIA` e código **1** — é o correto: uma
execução com evento quebrado não pode sair como `OK`. O bloco do `gen_id 99`, com
`PERDAS ?` e muitas lacunas, são os próprios eventos quebrados: eles não têm
manifesto.

**De onde vem a diferença:**

| Evento | Spark | Flink |
|---|---|---|
| Não é JSON, JSON cortado, lista, mensagem vazia ou nula | **grava** uma linha toda nula | **descarta**, sem aviso |
| Número entre aspas (`"17"`) | campo nulo | **converte** para 17 |
| Fração em campo inteiro (`1.5`) | campo nulo | **trunca** para 1 |
| Texto em campo numérico, número grande demais, campo ausente | campo nulo | campo nulo |
| Campo a mais | passa | passa |

**Onde vão as linhas sem `run_id`.** Para uma pasta à parte, com nome diferente em
cada motor: `run_id=__HIVE_DEFAULT_PARTITION__` no Spark e
`run_id=__DEFAULT_PARTITION__` no Flink. O `reconcile.py` lê essas pastas e
atribui as linhas à execução pelo instante em que foram processadas.

**As DESCARTADAS dependem de uma anotação.** Uma mensagem que sumiu não aparece no
destino: a conta é *mensagens que o tópico recebeu − linhas gravadas*. O injetor
grava esse número em `~/tcc-data/metrics/<run_id>.topico.json`. Nas execuções
normais essa anotação ainda não existe — será gravada pelo arnês da Etapa 8 —, e
o `reconcile.py` mostra `descartadas=?`, sem estimar.

> **Num benchmark, as três contagens têm de dar zero.** O gerador só emite eventos
> válidos. Um número diferente de zero numa execução medida é sinal de defeito, e
> o código de saída 1 existe para o arnês perceber.

---

## Verificação final

Se os vinte e um passos acima funcionaram, o ambiente está reproduzido:

- [x] `free -h` mostra o teto configurado da VM
- [x] `docker compose ps` mostra o broker `healthy`
- [x] `docker exec kafka ls /var/lib/kafka/data` lista `meta.properties`
- [x] O tópico existe com 24 partições
- [x] A imagem `tcc-spark:4.0.4` constrói, e o `check_chain.py` fecha a cadeia até o Spark dentro dela
- [x] O gerador reporta tentados = confirmados, 0 erros
- [x] As 24 partições recebem ~250 eventos cada
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
- [x] Com eventos malformados, o `reconcile.py` acusa 21 inválidas e 15 nulas no Spark, 15 inválidas e 15 descartadas no Flink, e sai com código 1

---

## Referência

### Onde as coisas moram

Decisão E6: **código no Windows, dados em ext4 nativo do WSL.**

| | Caminho |
|---|---|
| Código (este repo) | `/mnt/c/.../Pipelines-TCC` |
| Ambiente Python do host | `~/.venvs/tcc` — gerador, coletor e reconciliador |
| Parquet de saída | `~/tcc-data/out/spark` e `~/tcc-data/out/flink` — um `<base>` por motor |
| Checkpoints | `~/tcc-data/checkpoints/nrt` (Spark) e `~/tcc-data/checkpoints/rt` (Flink) |
| Métricas coletadas | `~/tcc-data/metrics` — manifestos do gerador, logs de progresso dos dois motores, CSVs do coletor e relatórios do `reconcile.py` |
| Ambiente do Spark | imagem Docker `tcc-spark:4.0.4` — fora do venv |
| Ambiente do Flink | imagem Docker `tcc-flink:1.20.5` — fora do venv |

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

E **conferir o relógio** — parar o `systemd-timesyncd`, esperar, rodar o
`scripts/check_clock.py` —, como descrito antes do passo 17. Ao terminar, religar
o serviço.

Antes de medir um motor, **zerar o checkpoint dele**: os dois leem o mesmo tópico,
e a medição de um deixa pendência para o outro.

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
| DuckDB (leitor do `reconcile.py`) | 1.5.5 | 24/09/2026 |

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
shared/                   ARNÊS COMPARTILHADO — Spark e Flink usam igual
  schema/evento.json      fonte única do schema — lida pelos dois  (RT-3)
  data-generator/         gerador de eventos                       (Etapa 3)
    inject_malformed.py   injetor de eventos quebrados, para o passo 21 (Etapa 7)
  analysis/reconcile.py   perdas, duplicatas, latências e is_late, via DuckDB (Etapa 6)
  monitoring/collector.py consumer lag a cada 1 s, os dois motores (Etapa 6)
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
benchmarks/               run_experiment.sh                        (Etapa 8)
infra/                    Terraform                                (Etapa 10)
results/                  CSVs e manifestos por run_id             (Etapa 11)
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
setembro — Spark no host, relógio não conferido — ficam como histórico; os de
referência serão medidos de novo nas condições atuais.

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
      contados; o Flink descarta 15 de 42 e converte tipos que o Spark anula
- [ ] **RT-6** — Arnês unificado
- [ ] **RT-7** — Validação local e caos

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
- [ ] **Etapa 8** — Arnês de benchmark
- [ ] **Etapa 9** — Validação local e caos
- [ ] **Etapa 10** — Terraform e AWS
- [ ] **Etapa 11** — Benchmarks oficiais
- [ ] **Etapa 12** — Análise e guia de decisão
