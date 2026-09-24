# CONTRATO DE DADOS

**Versão 1.4 — 23/09/2026**
**Status: congelado, sem pendências**

---

## Como este documento funciona

Este é o documento que torna a comparação entre o pipeline Near Real-Time
(Apache Spark) e o pipeline Streaming (Apache Flink) defensável.

Ele fixa tudo aquilo que precisa ser **idêntico nos dois pipelines** para que a
diferença medida entre eles seja atribuível ao motor de processamento, e não a
uma escolha de implementação. Qualquer item ambíguo aqui vira, na prática, uma
segunda explicação possível para todo resultado obtido.

Por isso ele é escrito **antes** do primeiro código de pipeline.

**Regra de alteração.** A partir do congelamento, mudar qualquer item deste
documento obriga a refazer as execuções já realizadas que dependem dele. Toda
alteração é registrada na seção 8, com data, motivo e execuções invalidadas.

---

## 1. Schema do evento no Kafka

### 1.1 Campos

```json
{
  "event_id":   "string — uuid",
  "seq":        "int64  — sequencial monotônico por processo gerador",
  "gen_id":     "int    — identificador do processo gerador",
  "run_id":     "string — identificador da execução do benchmark",
  "event_type": "string — pageview | add_to_cart | purchase | payment",
  "user_id":    "int    — 1 a 2000, distribuição uniforme",
  "product_id": "int",
  "amount":     "float  — nulo para pageview e add_to_cart",
  "session_id": "string — uuid",
  "event_ts":   "int64  — epoch em MILISSEGUNDOS, momento da geração"
}
```

**Forma executável: `shared/schema/evento.json`.** É a fonte única deste schema
desde 21/09/2026. Spark e Flink leem o arquivo e montam a forma nativa de cada um,
de modo que as duas definições **não têm como divergir**. Mudar um nome, um tipo
ou a ordem dos campos exige mudar o arquivo **e** esta seção, com as consequências
da regra de alteração deste contrato.

**Três campos não têm relação com e-commerce e existem apenas para a medição:**

| Campo | Por que existe |
|---|---|
| `seq` | Sequencial monotônico por gerador. Sem ele só é possível contar totais, o que esconde perda compensada por duplicação. Buracos na sequência são perda; repetições são duplicata |
| `gen_id` | Identifica o processo gerador, permitindo que várias instâncias produzam em paralelo sem colidir a sequência |
| `run_id` | Etiqueta cada execução, permitindo acumular tudo no mesmo destino e filtrar na análise |

**`event_ts` é epoch em milissegundos, não texto ISO.** Haverá aritmética
temporal sobre milhões de linhas; o parse de string viraria gargalo do próprio
instrumento de medição.

### 1.2 Serialização

**JSON.** Menos eficiente que Avro, mas dispensa Schema Registry — uma peça a
menos para instalar, configurar e explicar sem que ela agregue nada à
comparação NRT vs Streaming.

O tamanho médio do payload é **medido e registrado** por execução, porque entra
no cálculo de custo de rede e de armazenamento (seção 6 do planoNRT3).

### 1.3 Chave da mensagem

**A chave é o `user_id`**, serializado como sua representação decimal em UTF-8
(o inteiro `1234` vira os bytes `"1234"`).

**Por que a chave é parte do contrato e não detalhe de implementação.** O Kafka
aplica hash sobre a chave para escolher a partição. A chave, portanto, decide
como os eventos se distribuem entre as 24 partições e, por consequência, como o
trabalho se distribui entre os executores. Se o Flink usasse chave diferente, os
dois motores estariam lendo dados distribuídos de forma diferente.

**Consequências da escolha do `user_id`:**

- **A ordem por sessão é preservada.** Todo evento de uma sessão pertence ao
  mesmo usuário, logo cai na mesma partição. A sequência
  `pageview → add_to_cart → purchase` chega ordenada.
- **A porta do caminho agregado fica aberta.** Se a extensão com janela e
  watermark nativo (decisão B1.1) entrar depois da Etapa 9, agregação por
  usuário é possível sem tocar no contrato.

### 1.4 Distribuição de `user_id`

**`user_id` é sorteado com distribuição uniforme sobre o intervalo fechado
[1, 2000], com seed fixa.**

**Por que a cardinalidade entra no contrato.** Ela não tem relação com o volume
de carga — este é controlado pela taxa do gerador e pela duração da execução. O
que a cardinalidade de `user_id` controla é a uniformidade da distribuição das
chaves entre as 24 partições.

**Característica declarada.** Com 2000 chaves em 24 partições, a distribuição
esperada é de ~83 chaves por partição, com desvio padrão de ~9 — na prática as
partições variam entre aproximadamente 60 e 105 chaves, cerca de ±25% nos
extremos. Com 100.000 chaves o desvio seria de ~1,5%.

**Isso não ameaça a comparação**, por um motivo: o sorteio é determinístico com
seed fixa, portanto o desequilíbrio é exatamente o mesmo em toda execução e
idêntico nos dois motores. O efeito é um leve pessimismo na latência absoluta —
o micro-batch termina quando a partição mais pesada termina. Registrado como
característica do desenho, não como ameaça à validade.

---

## 2. Colunas adicionadas na saída

Não existem no Kafka. São gravadas pelo pipeline no `foreachBatch`,
imediatamente antes da escrita.

**Gravadas no arquivo Parquet:**

| Coluna | Tipo | Origem |
|---|---|---|
| `batch_id` | int64 | Id do micro-batch no Spark; no Flink, ver 2.3 |
| `processed_ts` | int64 | Epoch ms, momento imediatamente anterior à escrita |

**Registrados fora do Parquet** (versão 1.4, 23/09/2026):

| Dado | Onde vive | Origem |
|---|---|---|
| `committed_ts` | log por unidade de progresso, ao lado do manifesto | Epoch ms, **depois** do retorno da escrita — ver 2.2 |
| `watermark_ts`, `is_late` | derivados na análise | Calculados pelo `reconcile.py` a partir dos `event_ts` gravados — ver 2.1 |

### 2.1 O watermark é calculado na análise, não no pipeline

**`watermark_ts` = (máximo de `event_ts` acumulado desde o início da execução)
menos 60 segundos**, e `is_late` = `event_ts < watermark_ts`.

**O máximo é acumulado, não por batch.** Um watermark é, por definição, uma
fronteira monotônica não decrescente. Calculado como o máximo do batch corrente,
ele retrocederia em intervalos de menor movimento — e eventos já marcados como
atrasados deixariam de ser, dependendo apenas de em qual batch caíram.

**Onde o cálculo acontece — decisão de 23/09/2026.** Na **análise**, pelo
`reconcile.py`, a partir dos `event_ts` já gravados no destino. Não no caminho
quente de nenhum dos dois motores.

**Por quê.** A definição acima foi desenhada para o Spark, onde o watermark é uma
variável do driver — um ponto único que vê todos os eventos. **O Flink não tem
esse ponto único:** cada subtarefa calcula o próprio watermark, e o operador a
jusante vê o **mínimo** entre os das suas entradas. Uma definição fica governada
pela partição mais adiantada, a outra pela mais atrasada; com 24 partições e o
desequilíbrio de ±25% já medido, `is_late` mediria coisas diferentes nos dois
motores.

As alternativas custavam caro. Replicar o cálculo do contrato dentro do Flink
exigiria uma agregação global — um operador com paralelismo 1 vendo todos os
eventos —, criando ali um gargalo que o Spark não tem e contaminando a medição de
throughput. Usar o mecanismo nativo de cada um preservaria os motores, mas tornaria
a coluna incomparável entre eles.

**Por que calcular fora é viável aqui, e não seria em outro trabalho.** O pipeline
é **stateless** (decisão B1): o watermark não governa janela nenhuma, não dispara
nada, não descarta nada — ele só preenche duas colunas de diagnóstico. Não há razão
para ele ser calculado no caminho quente.

**O que se ganha.** A invariante sobrevive intacta, os dois motores produzem
exatamente o mesmo `is_late` para a mesma entrada, e nenhum deles é distorcido para
acomodar o outro. Some também a persistência do watermark ao lado do checkpoint, e
com ela o risco que a versão anterior desta seção descrevia: o cenário C4 zerando o
watermark e contaminando as contagens logo após a recuperação.

**O que se perde, e fica declarado.** `watermark_ts` deixa de ser "o que o motor
sabia no momento do processamento" e passa a ser "o que era verdade sobre o dado".
Como o uso é diagnóstico — detectar saturação —, a perda é pequena.

### 2.2 `committed_ts` — o quarto instante

**Epoch ms, tomado depois que a escrita retorna.**

**Por que ele existe.** A decisão B6 manda reportar **duas** latências: a de
**processamento** (`processed_ts − event_ts`) e a de **visibilidade**
(`committed_ts − event_ts`). Sem este instante, a segunda não é mensurável — e ela
é a definição que o Projeto de Pesquisa §3.2.1 adota para latência fim a fim.

**Por que fora do Parquet.** Uma coluna gravada depois da escrita não pode estar
dentro do arquivo que acabou de ser escrito. O valor é registrado num log por
unidade de progresso — micro-batch no Spark, checkpoint no Flink —, ao lado do
manifesto, e cruzado na análise pelo `batch_id`/`checkpoint_id`. O `reconcile.py`
já cruza as duas fontes de qualquer forma.

### 2.3 `batch_id` no Flink

No Spark o valor vem de graça, como parâmetro do `foreachBatch`. No Flink um
registro é processado **antes** de se saber a qual checkpoint pertencerá, o que
torna o equivalente não trivial. **Pendência aberta** — a seção 6 já declara que os
**valores** de `batch_id`/`checkpoint_id` não são invariantes.

### 2.4 O que `is_late` mede

O corte de 60 segundos precisa ser lido corretamente na análise.

O gerador emite em ordem crescente de `event_ts`. Com uma única partição, nada
jamais seria marcado como atrasado e a coluna seria sempre `false`.

A fonte real de desordem são as **24 partições lidas em paralelo**: dentro de um
micro-batch, o motor vê eventos de partições diferentes intercalados, e um
evento vindo de uma partição atrasada pode ter `event_ts` anterior à fronteira.

Com o corte em 60 s, `is_late` só passa de zero quando **alguma partição fica
mais de um minuto para trás** — isto é, quando o pipeline não está dando conta
da carga oferecida.

**Portanto `is_late` é um indicador secundário de saturação**, independente do
consumer lag, e deve ser reportado como tal.

---

## 3. Parâmetros do tópico Kafka

| Parâmetro | Valor | Justificativa |
|---|---|---|
| Nome | `ecommerce.events.v1` | Sufixo de versão evita ambiguidade se o schema mudar |
| Partições | **24** | Teto do paralelismo dos dois frameworks; fixado uma vez e nunca alterado |
| Fator de replicação | 1 local, **3** na nuvem | RF 3 é necessário para o teste de queda de broker (cenário C2) |
| `min.insync.replicas` | 1 local, **2** na nuvem | Sem isso, `acks=all` com RF 3 ainda perde dados (decisão E1) |
| `retention.ms` | ver 3.1 | |
| `retention.bytes` | **1 GiB por partição** | ver 3.2 |
| `compression.type` (tópico) | `producer` | O broker preserva a compressão aplicada pelo produtor |
| `acks` (produtor) | `all` | Sem isso, "evento perdido" pode ser falha do gerador e não do pipeline |
| Compressão (produtor) | `lz4` | Registrar a taxa obtida — afeta o custo de rede |
| `partitioner` (produtor) | **`murmur2_random`** | ver 3.3 |
| `linger.ms` (produtor) | **5** | ver 3.3 |
| `auto.create.topics.enable` | `false` | Guarda de validade: com auto-create, um nome errado cria topico de 1 partição e o experimento roda com paralelismo 1 sem erro na tela |

### 3.1 Retenção por tempo — dois valores declarados

| Fase | `retention.ms` | Motivo |
|---|---|---|
| Aprendizado (Etapas 2 a 7) | **168 h** | As mensagens de teste sobrevivem entre sessões de trabalho |
| Medição (Etapa 8 em diante) | **2 h** | Cobre a maior execução com folga e controla o consumo de disco |

A execução mais longa do protocolo é o Experimento A com trigger de 10 minutos,
que dura 60 minutos.

**Guarda obrigatória.** O `benchmarks/run_experiment.sh` (Etapa 8) verifica o
valor configurado antes de iniciar e **aborta** se estiver no valor de
aprendizado. Mesmo princípio do `auto.create.topics.enable=false`: um parâmetro
errado numa execução medida não pode falhar em silêncio.

### 3.2 Retenção por tamanho — a unidade importa

**`retention.bytes` no Kafka é por partição, não por tópico.**

O planoNRT3 §1.3 registra "2 horas ou 20 GB". Interpretado literalmente como
`retention.bytes=20GB` com 24 partições, isso autorizaria **480 GB** em disco —
o oposto da intenção da linha, e o erro só apareceria com o disco cheio no meio
de uma bateria de execuções.

**Dimensionamento.** A 10k evt/s com payload de ~200 bytes, a maior execução
(60 minutos) gera cerca de 7,2 GB brutos no tópico inteiro, ou ~300 MB por
partição.

**Valor adotado: 1 GiB por partição**, teto total de ~24 GB. Próximo da intenção
original dos 20 GB, com mais de 3× de margem sobre a maior execução. Funciona
como freio de segurança contra um gerador descontrolado; o controle do dia a dia
é a retenção por tempo.

### 3.3 Partitioner e `linger.ms` do produtor

Fecha a pendência de partitioner declarada na versão 1.0. Ambos os valores foram
decididos na Etapa 3, quando o gerador foi escrito, e são registrados aqui por
serem parte do contrato e não da implementação.

**`partitioner = murmur2_random`.** O `confluent-kafka` usa a biblioteca C
`librdkafka`, cujo padrão é `consistent_random`, baseado em CRC32. O cliente Java
do Kafka usa **murmur2**. Os dois espalham chaves uniformemente, mas produzem
atribuições **diferentes** para a mesma chave.

Adotado `murmur2_random` por dois motivos: é o comportamento que o resto do
ecossistema assume, de modo que quem reproduzir o experimento com outro cliente
cai nas mesmas partições; e faz o `kafka-console-producer`, que é Java, colocar
uma chave na mesma partição que o gerador colocaria — o que torna a verificação
manual possível em vez de enganosa.

Não afeta a comparação Spark × Flink, porque o gerador é o mesmo binário nos
dois. Afeta a reprodução por terceiros, que é critério declarado do trabalho.

**`linger.ms = 5`.** O produtor espera até 5 ms acumulando mensagens antes de
enviar. A 100 evt/s é irrelevante; a 17k evt/s é o que separa atingir a taxa de
não atingir, porque sem agrupamento seria uma ida e volta ao broker por evento,
com `acks=all`.

**Custo declarado:** esses 5 ms **entram na latência medida**, porque o
`event_ts` é carimbado antes da fila do produtor. São 0,1% de um trigger de 5 s.

---

## 4. Layout do destino

```
<base>/events/run_id=<run_id>/dt=<YYYY-MM-DD>/hh=<HH>/part-*.parquet
```

### 4.1 `dt` e `hh` derivam de `event_ts`, em UTC

**Não de `processed_ts`.** O motivo decisivo é de comparabilidade, não
analítico: `event_ts` é propriedade do evento e é idêntico nos dois motores para
a mesma seed, enquanto `processed_ts` é produzido pela máquina e difere por
construção.

Particionar por `processed_ts` faria os dois pipelines produzirem **estruturas
de diretório diferentes a partir da mesma entrada** — e qualquer comparação de
número de arquivos, tamanho médio ou custo de PUT viraria artefato do motor em
vez de resultado.

**O fuso é UTC, explicitamente.** Sem essa fixação, a mesma entrada geraria
diretórios diferentes na máquina local (`America/Recife`) e na nuvem
(`us-east-1`), e as execuções deixariam de ser diretamente comparáveis.

**Preço da escolha:** um micro-batch que cruze a virada de hora escreve em dois
diretórios em vez de um.

### 4.2 `<base>`

| Ambiente | Spark (NRT) | Flink (Streaming) |
|---|---|---|
| Local | `~/tcc-data/out/spark` | `~/tcc-data/out/flink` |
| Nuvem | `s3a://<bucket>/spark/` | `s3a://<bucket>/flink/` |

`<base>` é **por motor** e é a única diferença entre ambientes, isolada em
configuração. **Abaixo dele, o layout é idêntico nos dois** — é o layout, e não o
`<base>`, que é invariante de comparabilidade.

**Por que um `<base>` por motor (versão 1.3, 21/09/2026).** Com um `<base>` único,
a separação entre os motores dependeria de cada execução ter `run_id` próprio. Isso
vale nos benchmarks, mas não no desenvolvimento: os dois leem o **mesmo tópico**,
com os **mesmos** `run_id` — verificado na RT-2, quando o Flink recebeu exatamente
os eventos que o Spark havia recebido. Os arquivos dos dois motores se misturariam
nas mesmas pastas, e a reconciliação acusaria duplicatas que não existem, em
silêncio. Com um `<base>` por motor, a separação é garantida por construção, e o
`reconcile.py` recebe o `<base>` de um motor e reconcilia só ele.

**Por que o local não é `./data/out` dentro do repositório.** A Etapa 0 mediu a
escrita sequencial de 512 MB nos dois sistemas de arquivos: **1,2 GB/s** em ext4
nativo do WSL contra **77,4 MB/s** em `/mnt/c` (drvfs) — diferença de 15,8×, e
esse é o caso mais favorável ao `/mnt/c`. Com a saída dentro do repositório, o
gargalo do pipeline seria o sistema de arquivos do Windows, e o Experimento A
estaria medindo drvfs em vez de Spark.

### 4.3 Escrita

| Item | Valor |
|---|---|
| Formato | Parquet |
| Modo | `append` |
| Codec de compressão | **`snappy`** — ver 4.4 |
| `repartition` / `coalesce` | **não usar** — decisão E3, **definitiva** desde 08/09/2026 |

**Sobre a ausência de `repartition`.** Escreve-se com o paralelismo natural. O
número de arquivos por execução é **registrado como métrica**, porque governa o
custo de requisições PUT no S3 e é parte do trade-off latência-versus-custo da
seção 6 do planoNRT3.

A decisão E3 estava marcada como PROVISÓRIA no plano. Foi **fechada em
08/09/2026**, depois de a Etapa 5 produzir a contagem real de arquivos.

**A evidência que a sustenta.** Três execuções de 6000 eventos cada, mesmo schema
e mesmo codec, diferindo apenas em como o Spark fatiou a escrita: 24 arquivos e
632 KB em lote único, contra 312 arquivos e 3,7 MB com trigger de 5 s. O número de
arquivos não é ruído — é o resultado que a comparação existe para revelar.

**Por que corrigi-lo seria pior do que reportá-lo.** Um `repartition` acrescenta
um *shuffle*, e o custo desse shuffle cai dentro da parcela "processamento" da
latência decomposta — exatamente a parcela que compete com o Flink. Como o shuffle
do Spark e o do Flink têm custos diferentes, igualar o número de arquivos
introduziria uma assimetria **maior** do que a que se pretendia remover. Um
`coalesce` evita o shuffle, mas reduz o paralelismo de todo o estágio anterior, e
não apenas o da escrita.

**Limitação declarada.** Um pipeline de produção normalmente teria compactação
posterior dos arquivos pequenos. Ela não é feita aqui, e não deve ser: é um job
separado, cobrado à parte, e executá-lo dentro do pipeline misturaria seu custo ao
da ingestão. A análise de custo da seção 6 do plano deve mencionar a compactação
como mitigador conhecido do efeito medido.

### 4.4 Codec de compressão: `snappy`

Fecha a pendência declarada na versão 1.0 deste contrato, antes da primeira
escrita em Parquet (Etapa 5). Candidatos considerados: `snappy` (~2–3×, CPU
muito baixa), `zstd` (~3–4×, mais CPU) e `lz4`.

**Por que a escolha precisa existir, mesmo coincidindo com o padrão do Spark.**
Confiar no default significa confiar que Spark e Flink escolhem o mesmo valor por
conta própria — dois projetos independentes, com versões que mudam ao longo do
tempo. Se divergirem, os arquivos saem com tamanhos diferentes **a partir da
mesma entrada**, e a comparação de volume armazenado e de tráfego de rede vira
artefato do motor em vez de resultado. Mesmo raciocínio da fixação do fuso em
UTC (seção 4.1): não é que o valor seja melhor, é que "não fixado" produz
divergência silenciosa.

**Por que `snappy` e não `zstd`.** O tempo de compressão cai dentro da parcela
**"processamento"** da latência decomposta (planoNRT3 §3.1) — precisamente a
parcela que compete com o Flink. Um codec mais pesado adiciona uma constante aos
dois motores, que não interessa à pergunta de pesquisa e só reduz a resolução da
comparação. `snappy` é o de menor custo de CPU entre os que comprimem.

Pesa também o ambiente local: teto de 4 GB no WSL e CPU disputada com o broker e
o gerador durante a validação da Etapa 9.

**Custo declarado da escolha.** `snappy` comprime menos que `zstd` — algo entre
30% e 40% mais bytes em disco e na rede. Isso aparece em duas das cinco
categorias de custo da §6 do plano (armazenamento e rede) e deve ser mencionado
ao reportar o custo por GB armazenado. **Não** afeta a categoria dominante de
custo em S3 apontada pelo plano, que é o número de requisições PUT: essa depende
da **quantidade** de arquivos, não do tamanho deles.

**Por que `lz4` foi descartado.** O Parquet tem duas variantes de LZ4 (`LZ4` e
`LZ4_RAW`), e leitores diferentes esperam variantes diferentes — arquivos que um
motor escreve e outro não lê. Num trabalho com dois escritores distintos e um
reconciliador em DuckDB (decisão B3), é exatamente o tipo de incompatibilidade
que custa tempo sem produzir conhecimento.

**Sem dependência nova:** o jar `snappy-java` já vem entre as dependências
transitivas do conector `spark-sql-kafka-0-10`.

---

## 5. Invariantes de comparabilidade

Se estes itens não forem idênticos no pipeline Flink, o OE 3 não se sustenta.

| Invariante | Valor fixado |
|---|---|
| Schema do evento | Seção 1.1, sem alteração |
| Chave da mensagem | `user_id`, representação decimal em UTF-8 (seção 1.3) |
| Distribuição de `user_id` | Uniforme em [1, 2000], seed fixa (seção 1.4) |
| Colunas de saída | Seção 2; o equivalente de `batch_id` no Flink é o `checkpoint_id` |
| Cálculo do watermark | Máximo acumulado de `event_ts` menos 60 s, monotônico — **derivado na análise**, igual para os dois motores (seção 2.1) |
| Tópico e partições | `ecommerce.events.v1`, 24 partições |
| Gerador e seed | Mesmo binário, mesma seed, mesmo `config.yaml` |
| Carga | Mesmos níveis, mesma duração, mesmas repetições |
| Transformações | Mesma limpeza, mesmo broadcast join, mesmo cálculo de `watermark_ts` |
| Tabela de referência | O mesmo `products.parquet` versionado |
| Particionamento do destino | Derivado de `event_ts`, em UTC (seção 4.1) |
| Codec do Parquet | `snappy`, declarado explicitamente nos dois (seção 4.4) |
| Destino | Mesmo formato, mesmo particionamento, mesma política de escrita (sem `repartition`) |
| Definições de métrica | planoNRT3 seção 3, sem alteração |
| Análise | O mesmo `reconcile.py` |
| Infraestrutura | Mesmos tipos de instância, mesma AZ, mesma região |

### 5.1 Reprodução da carga: seed fixa, sem replay

O gerador é pseudoaleatório e semeado. Exige duas garantias de implementação:

1. **Um único thread produtor por `gen_id`**
2. **Nenhum uso de relógio ou entropia** no sorteio dos campos

O `event_ts` difere entre as execuções do Spark e do Flink, o que é irrelevante:
toda latência é calculada em relação ao próprio `event_ts` de cada evento. O que
precisa ser idêntico é a **sequência de eventos**, não o instante absoluto.

---

## 6. O que NÃO é invariante

Esta seção existe porque um contrato que só diz o que precisa ser igual convida
a igualar o que não deve ser igualado.

| Item | Por que não é invariante |
|---|---|
| Valores absolutos de `event_ts` entre execuções | A latência é sempre relativa ao próprio `event_ts` do evento |
| `processed_ts`, `batch_id` / `checkpoint_id` | A semântica é idêntica; os valores são produzidos pela máquina |
| Nomes dos arquivos `part-*` | Convenção de cada motor. O que precisa casar é a estrutura de diretórios |
| **Número de arquivos por execução** | É **resultado medido**, não parâmetro a igualar |
| Retenção do tópico entre fase de aprendizado e medição | Precisa ser igual entre os motores dentro da mesma fase, não entre fases |

**Sobre o número de arquivos.** É tentador tratá-lo como invariante. Forçá-lo a
ser igual nos dois motores exigiria um `repartition`, o que violaria a decisão
E3 e apagaria justamente o achado: o número de arquivos governa o custo de PUT
no S3 e é uma das diferenças que a comparação existe para revelar.

---

## 7. Pendências deste contrato

**Nenhuma em aberto.** As duas declaradas na versão 1.0 foram fechadas:

| Pendência | Prazo | Resolução |
|---|---|---|
| Codec de compressão do Parquet | antes da Etapa 5 | **`snappy`** — seção 4.4, versão 1.1 |
| Partitioner do produtor | Etapa 3 | **`murmur2_random`** — seção 3, versão 1.1 |

---

## 8. Histórico de alterações

| Versão | Data | Alteração | Execuções invalidadas |
|---|---|---|---|
| 1.0 | 04/09/2026 | Congelamento inicial | — |
| 1.4 | 23/09/2026 | **§2 reescrito.** `watermark_ts` e `is_late` deixam de ser gravados pelo pipeline e passam a ser **derivados na análise** (§2.1), porque a definição do contrato não tem equivalente no modelo distribuído do Flink. Acrescentado `committed_ts` (§2.2), registrado **fora do Parquet**, num log por unidade de progresso — sem ele a latência de visibilidade da decisão B6 não é mensurável. A persistência do watermark ao lado do checkpoint deixa de existir | **Nenhuma.** Nenhuma execução medida foi feita, e as de aprendizado não produziram as colunas em questão |
| 1.3 | 21/09/2026 | **§4.2 alterado:** `<base>` passa a ser **por motor** (`~/tcc-data/out/spark` e `~/tcc-data/out/flink`; `s3a://<bucket>/spark/` e `s3a://<bucket>/flink/`). O layout abaixo do `<base>` não muda. Motivo: com um `<base>` único, os dois motores gravariam nas mesmas pastas sempre que lessem os mesmos `run_id`, como no desenvolvimento | **Nenhuma medida.** As execuções de aprendizado do Spark (Etapa 5) ficam no destino antigo, `~/tcc-data/out/events`, e podem ser descartadas |
| 1.2.1 | 21/09/2026 | **Nenhum item alterado.** O §1.1 ganhou forma executável em `shared/schema/evento.json`, que passa a ser a fonte única do schema para Spark e Flink | **Nenhuma** — o schema produzido pelo Spark foi verificado idêntico antes e depois |
| 1.2 | 09/09/2026 | **Nenhum item alterado.** A decisão E3 (sem `repartition`/`coalesce`), que o plano marcava como PROVISÓRIA, foi fechada como definitiva e a seção 4.3 passou a registrar a evidência que a sustenta e a limitação declarada sobre compactação | **Nenhuma** |
| 1.1 | 08/09/2026 | Fechadas as duas pendências que a v1.0 declarava, **sem alterar nenhum item já fixado**. Codec do Parquet = `snappy` (seção 4.4), decidido antes da primeira escrita. Partitioner do produtor = `murmur2_random` e `linger.ms` = 5 (seção 3.3), decididos na Etapa 3 e transcritos para cá | **Nenhuma.** O codec entra antes de qualquer escrita em Parquet existir; o partitioner apenas registra o que já vigorava desde a Etapa 3 |
