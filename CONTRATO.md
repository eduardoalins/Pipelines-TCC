# CONTRATO DE DADOS

**Versão 1.0 — 04/09/2026**
**Status: congelado**

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

| Coluna | Tipo | Origem |
|---|---|---|
| `batch_id` | int64 | Id do micro-batch (decisão B2) |
| `processed_ts` | int64 | Epoch ms, momento imediatamente anterior à escrita |
| `watermark_ts` | int64 | Ver 2.1 |
| `is_late` | bool | `event_ts < watermark_ts` |

### 2.1 Cálculo do watermark

**`watermark_ts` = (máximo de `event_ts` acumulado desde o início da execução)
menos 60 segundos.**

**O máximo é acumulado, não por batch.** Um watermark é, por definição, uma
fronteira monotônica não decrescente. Calculado como o máximo do batch corrente,
ele retrocederia em intervalos de menor movimento — e eventos já marcados como
atrasados deixariam de ser, dependendo apenas de em qual batch caíram.

**Onde o valor vive.** Numa variável do driver, mantida entre micro-batches.
Isso **não viola a decisão B1** (pipeline stateless): B1 trata de estado
gerenciado pelo Spark, com state store e checkpoint de estado. Aqui é uma
variável do processo driver.

**Persistência, e por que ela é obrigatória.** O valor corrente é gravado num
arquivo ao lado do checkpoint a cada batch, e recarregado na subida do job.

Sem isso, uma reinicialização zeraria o watermark. No cenário C4 do Experimento
C — kill do driver Spark —, as contagens de `is_late` imediatamente após a
recuperação estariam erradas, e o defeito apareceria como resultado do
experimento de tolerância a falhas.

### 2.2 O que `is_late` mede

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

| Ambiente | Valor |
|---|---|
| Local | `~/tcc-data/out` |
| Nuvem | `s3a://<bucket>/` |

`<base>` é a única diferença entre local e nuvem, isolada em configuração.

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
| Codec de compressão | **pendente** — ver seção 7 |
| `repartition` / `coalesce` | **não usar** (decisão E3) |

**Sobre a ausência de `repartition`.** Escreve-se com o paralelismo natural. O
número de arquivos por execução é **registrado como métrica**, porque governa o
custo de requisições PUT no S3 e é parte do trade-off latência-versus-custo da
seção 6 do planoNRT3.

---

## 5. Invariantes de comparabilidade

Se estes itens não forem idênticos no pipeline Flink, o OE 3 não se sustenta.

| Invariante | Valor fixado |
|---|---|
| Schema do evento | Seção 1.1, sem alteração |
| Chave da mensagem | `user_id`, representação decimal em UTF-8 (seção 1.3) |
| Distribuição de `user_id` | Uniforme em [1, 2000], seed fixa (seção 1.4) |
| Colunas de saída | Seção 2; o equivalente de `batch_id` no Flink é o `checkpoint_id` |
| Cálculo do watermark | Máximo acumulado de `event_ts` menos 60 s, monotônico e persistido (seção 2.1) |
| Tópico e partições | `ecommerce.events.v1`, 24 partições |
| Gerador e seed | Mesmo binário, mesma seed, mesmo `config.yaml` |
| Carga | Mesmos níveis, mesma duração, mesmas repetições |
| Transformações | Mesma limpeza, mesmo broadcast join, mesmo cálculo de `watermark_ts` |
| Tabela de referência | O mesmo `products.parquet` versionado |
| Particionamento do destino | Derivado de `event_ts`, em UTC (seção 4.1) |
| Codec do Parquet | O mesmo nos dois — valor pendente (seção 7) |
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

| Pendência | Prazo | Consequência de não resolver |
|---|---|---|
| **Codec de compressão do Parquet.** Candidatos: `snappy` (padrão de fato, rápido, taxa ~2–3×), `zstd` (taxa ~3–4×, mais CPU), `lz4`. Afeta o tamanho dos arquivos e, portanto, o custo de armazenamento e o volume de PUTs | **Antes da Etapa 5** | O Spark usa o default dele silenciosamente e as execuções ficam gravadas sob um codec que nunca foi escolhido |
| **Partitioner do produtor.** O `librdkafka` (`confluent-kafka`) usa `consistent_random` por padrão, baseado em CRC32; o cliente Java do Kafka usa murmur2. Os dois produzem atribuições de partição diferentes para a mesma chave | **Etapa 3** | Comparabilidade entre Spark e Flink não é afetada (o gerador é o mesmo), mas a reprodução por terceiros com outro cliente daria distribuição diferente |

---

## 8. Histórico de alterações

| Versão | Data | Alteração | Execuções invalidadas |
|---|---|---|---|
| 1.0 | 04/09/2026 | Congelamento inicial | — |
