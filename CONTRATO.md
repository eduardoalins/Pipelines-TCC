# CONTRATO DE DADOS

**Versão 3.1 — 24/10/2026**
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
  "event_id":   "string — endereço da linha na base: 2019-10:00000005",
  "seq":        "int64  — sequencial monotônico por processo replayer",
  "gen_id":     "int    — identificador do processo replayer",
  "run_id":     "string — identificador da execução do benchmark",
  "event_type": "string — da base: view | cart | purchase",
  "user_id":    "int    — da base: o usuário real",
  "product_id": "int    — da base: o produto real",
  "amount":     "decimal(10,2) — da base (price): o preço do produto, em todo evento; viaja como texto",
  "session_id": "string — da base (user_session)",
  "event_ts":   "int64  — epoch em MILISSEGUNDOS, momento do ENVIO",
  "event_time": "timestamp — da base: o instante original do evento na loja, em UTC; viaja como texto"
}
```

**Desde a versão 3.0 os eventos são reais** (seção 1.5). Os campos marcados "da
base" levam o valor que a loja registrou; os outros são carimbados pelo replayer
para a medição. Até a versão 2.2 eram dez campos, todos produzidos por um gerador
sintético; o `event_time` entrou como 11º, no fim, para a ordem dos dez anteriores
não mudar.

**Estes são os tipos do evento, não os do destino.** Desde a versão 2.0 os
pipelines carregam os campos como **texto**, e a conversão para os tipos acima
acontece depois do Load (seção 2.7).

**Forma executável: `shared/schema/evento.json`.** É a fonte única deste schema
desde 21/09/2026. Spark e Flink leem o arquivo — os nomes, a ordem e a regra de
particionamento —, de modo que as duas definições **não têm como divergir**. A
conversão de tipos, depois do Load, lê os tipos do mesmo arquivo. Mudar um nome, um tipo
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
  `view → cart → purchase` chega ordenada.
- **A porta do caminho agregado fica aberta.** Se a extensão com janela e
  watermark nativo (decisão B1.1) entrar depois da Etapa 9, agregação por
  usuário é possível sem tocar no contrato.

### 1.4 Distribuição de `user_id`

**Desde a versão 3.0 o `user_id` é o da base real** (seção 1.5): 3.022.290
usuários diferentes no mês, com a distribuição que a loja teve. Medido na base
inteira (23/10/2026): o usuário mais ativo tem 7.436 eventos, a mediana é 4 por
usuário e o percentil 99 é 141; os 100 mais ativos somam 0,38% dos eventos. Com
milhões de chaves e concentração baixa, as 24 partições recebem cargas próximas —
mais próximas do que com as 2000 chaves de antes. Como a rodada N recebe a mesma
janela da base nos dois motores (seção 5.1), a distribuição de cada rodada é
idêntica para o Spark e para o Flink.

O texto a seguir descreve a regra **até a versão 2.2**, mantida como registro.

**`user_id` era sorteado com distribuição uniforme sobre o intervalo fechado
[1, 2000], com a seed da rodada.**

**Por que a cardinalidade entra no contrato.** Ela não tem relação com o volume
de carga — este é controlado pela taxa do gerador e pela duração da execução. O
que a cardinalidade de `user_id` controla é a uniformidade da distribuição das
chaves entre as 24 partições.

**Característica declarada.** Com 2000 chaves em 24 partições, a distribuição
esperada é de ~83 chaves por partição, com desvio padrão de ~9 — na prática as
partições variam entre aproximadamente 60 e 105 chaves, cerca de ±25% nos
extremos. Com 100.000 chaves o desvio seria de ~1,5%.

**Isso não ameaça a comparação**, por um motivo: o sorteio é determinístico pela
seed, e a rodada N usa a mesma seed nos dois motores (seção 5.1). O desequilíbrio
muda de uma rodada para outra, mas dentro de cada rodada é idêntico para o Spark e
para o Flink. O efeito é um leve pessimismo na latência absoluta —
o micro-batch termina quando a partição mais pesada termina. Registrado como
característica do desenho, não como ameaça à validade.

### 1.5 A fonte dos eventos: uma base real (versão 3.0, 23/10/2026)

**Decisão**, do autor, a pedido da orientação: os eventos deixam de ser gerados
por nós e passam a ser **reais**, reproduzidos de uma base pública.

| | |
|---|---|
| Base | *eCommerce behavior data from multi category store* — eventos de uma loja online multicategoria, coletados pela REES46 e publicados no Kaggle por Michael Kechinov |
| Endereços | https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store ; https://rees46.com |
| Licença | uso livre, **citando as duas fontes acima** |
| Arquivo | `2019-Oct.csv.gz`, baixado de `https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz` — o mesmo arquivo do Kaggle (descompactado, 5.668.612.855 bytes, o tamanho que o Kaggle informa) |
| Impressão digital | sha256 `8ebca1ad741295297368f2cf0315e3f36853a1a11768fb16babf8c9b83838147` (1.741.928.540 bytes) |
| Conteúdo | **42.448.764 eventos**, de 01/10/2019 00:00:00 a 31/10/2019 23:59:59 UTC, já ordenados pelo instante; 3.022.290 usuários, 166.794 produtos |
| Estado | estática: sem atualização desde 09/12/2019 (versão 8 no Kaggle) |

`scripts/preparar_base.sh` baixa (se faltar), confere o sha256, descompacta e
confere a contagem. **O arquivo da base nunca é alterado**: o replayer só o lê.

**Como uma linha da base vira mensagem** (`shared/data-generator/replayer.py`):

| Campo da mensagem | Origem |
|---|---|
| `event_type`, `user_id`, `product_id`, `event_time` | a coluna de mesmo nome da base, como veio |
| `amount` | a coluna `price`, em **todo** evento — o preço do produto, inclusive em `view` e `cart`; 68.673 eventos têm preço 0. Vai **como texto, exatamente como na base** (`"543.10"`), para nenhum motor reescrevê-lo (seção 2.7) |
| `session_id` | a coluna `user_session` (vazia em 2 linhas do mês) |
| `event_id` | o endereço da linha: `2019-10:` e o número da linha de dados, com 8 dígitos. É único — o instante não é: há até 116 eventos no mesmo segundo |
| `seq`, `gen_id`, `run_id` | o replayer, como o gerador fazia, para a contagem de perdas e duplicatas |
| `event_ts` | o replayer: o **instante do envio**. Toda latência, o atraso, o aquecimento e as pastas `dt=`/`hh=` saem dele |

`category_id`, `category_code` e `brand` **não entram na mensagem**; continuam na
base, ligadas ao evento pelo `event_id`.

**Por que o `event_ts` não é o instante original.** Com o instante da loja, cada
evento teria anos de latência, todos sairiam atrasados e cairiam em pastas de
2019. O instante original viaja intacto, em outra coluna: o `event_time`.

**O que fica como a loja registrou, de propósito** (ELT estrito, seção 2.6 — limpar
é do Transform):
- os **tipos de evento** da base (`view`, `cart`, `purchase`). Outubro de 2019
  não tem `remove_from_cart`, embora a página da base o descreva;
- as **30.220 linhas idênticas** que a base tem repetidas (0,07%). Para a medição
  não são duplicatas: cada uma recebe o seu `seq` e o seu `event_id`;
- os 2 eventos sem `user_session`. Se caírem numa janela, saem como **inválidos**
  na contagem (seção 2.6) — defeito da base, não do pipeline.

**O ritmo é do experimento, não da loja.** A loja teve, em média, 16 eventos por
segundo (pico de 116). As cargas do benchmark (seção 5.2) são de 60 a 1000 vezes
maiores: o replayer **acelera** a base, enviando os eventos na ordem original, na
taxa pedida. Os eventos são reais; o ritmo, não.

**Sem eventos malformados.** A base não tem nenhum, e desde a versão 3.0 nenhum é
injetado (seção 2.6).

---

## 2. Colunas adicionadas na saída

Não existem no Kafka. São gravadas pelo pipeline imediatamente antes da escrita
— no Spark, dentro do `foreachBatch`; no Flink, na projeção SQL do job.

**Gravadas no arquivo Parquet:**

| Coluna | Tipo | Origem |
|---|---|---|
| `batch_id` | int64 | Id do micro-batch — **só no Spark**; o Flink não grava esta coluna (2.3, versão 1.6) |
| `processed_ts` | timestamp (UTC, precisão de ms; `INT64` em µs, sem marca de UTC) | Momento imediatamente anterior à escrita — ver 2.5 (versão 1.5) |

**Registrados fora do Parquet** (versão 1.4, 23/09/2026):

| Dado | Onde vive | Origem |
|---|---|---|
| `committed_ts` | **Spark:** log por micro-batch, ao lado do manifesto. **Flink:** o `ctime` de cada arquivo (versão 1.6) | O instante em que o dado fica visível — origem diferente por motor, ver 2.2 |
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

**O instante em que o dado fica visível no destino**, em epoch ms.

**Por que ele existe.** A decisão B6 manda reportar **duas** latências: a de
**processamento** (`processed_ts − event_ts`) e a de **visibilidade**
(`committed_ts − event_ts`). Sem este instante, a segunda não é mensurável — e ela
é a definição que o Projeto de Pesquisa §3.2.1 adota para latência fim a fim.

**Por que fora do Parquet.** Uma coluna gravada depois da escrita não pode estar
dentro do arquivo que acabou de ser escrito.

**Origem diferente por motor — assimetria de instrumento declarada (versão 1.6,
25/09/2026):**

| | Spark | Flink |
|---|---|---|
| De onde vem | **log de progresso**, uma linha por micro-batch, tomada depois que o `write()` retorna | **`ctime` do arquivo**: o instante em que ele é renomeado de oculto (`.part-…inprogress`) para visível |
| Como se liga ao evento | pelo par (`batch_id`, `processed_ts`), gravado nos dois lugares | pelo arquivo em que o evento foi gravado |
| Exatidão medida | ~20 ms **depois** do arquivo ficar visível, sempre (batches 2 a 7 em 25/09) | do fim do checkpoint registrado pela REST API do Flink: −7 a +8 ms (6 checkpoints em 25/09), −10 a +7 ms (13 checkpoints em 10/10) e **−13 a +16 ms** nas execuções de 11/10 (uma conferência com −13 a +3 ms e cinco rodadas de teste, 59 checkpoints, com −12 a +16 ms) |

**Por que o Flink pelo arquivo.** Cada arquivo do Flink é finalizado por
**exatamente um** checkpoint: a associação evento → instante de visibilidade é
exata, sem código no caminho do dado e sem aproximação. Verificado em 25/09/2026:
o `ctime` vem sempre depois do último evento do arquivo, 20–40 ms depois do
`mtime` (a última escrita), e os 8 arquivos de cada checkpoint tinham `ctime`
idêntico.

**Os arquivos de um checkpoint nem sempre têm o mesmo `ctime` (versão 1.7).** Em
10/10/2026, em 2 de 13 checkpoints os 8 arquivos saíram divididos em dois
instantes, a 10 ms um do outro (7 + 1 e 3 + 5). O instante de visibilidade de cada
evento continua sendo o `ctime` do **próprio arquivo**; o que muda é como os
arquivos são agrupados em checkpoints (2.3).

**Por que o Spark não pelo arquivo.** Quando a pasta de destino ainda não existe —
o primeiro batch da execução e **toda virada de hora** (`hh=` novo) —, o Spark move
a pasta inteira, e os arquivos dentro dela ficam com o `ctime` de antes: até
~300 ms **mais cedo** que a visibilidade real (observado em 25/09 no batch 1; causa
provável, não verificada). O log não tem esse erro.

**Três limites, declarados:**
- **O `ctime` só existe onde há renomeação.** Vale no disco local. No S3 não há
  renomeação: verificar na Etapa 10 que instante ele registra. Plano B para o
  Flink: o fim do checkpoint pela REST API (`latest_ack_timestamp`).
- **Nada pode tocar nos arquivos do Flink depois de gravados.** `chmod`, `chown` ou
  cópia mudam o `ctime` e falsificam a latência de visibilidade, sem erro.
- **Os dois instrumentos têm erros pequenos e diferentes:** o log do Spark ~20 ms
  tarde; o `ctime` do Flink entre −13 e +16 ms do fim do checkpoint (as medições
  acima; até a versão 1.8 esta linha dizia "±10 ms"). Numa latência de
  visibilidade de segundos, a diferença é desprezível, mas existe e está
  registrada. A causa da faixa do `ctime` não foi verificada.

### 2.3 `batch_id` no Flink — não existe (versão 1.6, 25/09/2026)

No Spark o valor vem de graça, como parâmetro do `foreachBatch`. No Flink um
registro é processado **antes** de se saber a qual checkpoint pertencerá: não há
em SQL uma coluna "checkpoint atual", e etiquetar cada evento exigiria um operador
com estado em Python (viola a decisão F2) ou em Java.

**Decisão.** O Flink **não grava** `batch_id`. Todo evento de um arquivo pertence
ao mesmo checkpoint, e o `reconcile.py` mede pelo arquivo (2.2). A seção 6 já
declarava que os **valores** de `batch_id`/`checkpoint_id` não são invariantes;
agora a coluna em si é específica do Spark.

**Unidade de progresso do Flink — o checkpoint, reconhecido pelos arquivos (versão
1.7, 10/10/2026).** A unidade de progresso é o que a análise usa para avançar o
watermark (2.1); até a versão 2.1 era também o que se descartava como aquecimento
(a primeira unidade). Na versão 1.6
ela era o arquivo, o que na prática significava "cada `ctime` distinto". Como os
arquivos de um checkpoint podem sair em instantes vizinhos (2.2), isso contava
unidades a mais — 15 para 13 checkpoints em 10/10 — e deixava possível descartar
só parte do primeiro checkpoint.

A regra passa a ser: **arquivos cujo `ctime` dista até 1 s do anterior pertencem ao
mesmo checkpoint.** O limite precisa ficar bem abaixo do intervalo de checkpoint
(5 s no protocolo). O agrupamento usa só os próprios arquivos; o log de checkpoints
do Flink continua sendo conferência independente, e não entra na conta.

**O descarte de aquecimento é um tempo fixo, igual para os dois motores (versão
2.2, decisão de 12/10/2026).** Ficam fora das latências "em regime" e do
throughput médio os eventos **gerados** nos primeiros segundos da rodada,
contados do primeiro evento, pelo `event_ts`:

| Rodadas | Descarte |
|---|---|
| de 60 s (combinações de intervalo 5) | os primeiros **5 s** de eventos |
| de 300 s (combinações de intervalo 30) | os primeiros **30 s** de eventos |

Os dois motores descartam **o mesmo trecho da carga**. A rodada N dos dois recebe
os mesmos eventos (seção 5.1), mas o `event_ts` é o instante real da emissão, e na
borda do corte poucos eventos diferem: numa rodada de 60.000 eventos, 4.999
ficaram fora num motor e 5.002 no outro (12/10/2026). O relatório traz também as
duas latências da execução inteira, com o aquecimento.

Até a versão 2.1 descartava-se a primeira unidade de progresso de cada motor — o
primeiro lote do Spark, o primeiro checkpoint do Flink. Não era a mesma coisa nos
dois: o primeiro checkpoint do Flink às vezes trazia pouquíssimos eventos (8,
numa execução de 11/10/2026), e parte do aquecimento escapava.

O **throughput médio** segue a mesma regra: os eventos gerados depois do
aquecimento, divididos pelo tempo desde o fim dele até a última gravação. Como a
última gravação vem depois de o gerador parar, o número fica abaixo da carga
quando o pipeline a acompanha — e mais abaixo quanto mais o motor demora a gravar o
fim da carga: cerca de 4% a 5% nas rodadas de 60 s, e mais no Spark com trigger de
30 s, cujo último lote sai até 30 s depois. **Entre motores que acompanham a
carga, a diferença de throughput médio é efeito de borda, não capacidade.** Ele só
mostra capacidade quando o pipeline não acompanha.

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

### 2.5 `processed_ts` é data/hora, não inteiro (versão 1.5, 24–25/09/2026)

**Tipo.** O instante em **UTC**, com precisão de **milissegundos**, gravado no
Parquet como `INT64` com anotação lógica de *timestamp* em **microssegundos**,
**sem a marca de UTC** (`isAdjustedToUTC=false`). **Idêntico nos dois motores**,
verificado em 25/09/2026 lendo o esquema dos arquivos de cada um.

| | Spark | Flink |
|---|---|---|
| Como produz | `timestamp_millis(...)` convertido para `timestamp_ntz`, uma vez por micro-batch | `CURRENT_ROW_TIMESTAMP()`, por evento |
| Declaração no sink | — | `parquet.write.int64.timestamp=true`, `parquet.timestamp.time.unit=micros`, `parquet.utc-timezone=true` |
| Arquivo | `INT64`, µs, sem marca | `INT64`, µs, sem marca |

**Precisão de ms, armazenamento em µs.** Os dois relógios são lidos em
milissegundos; o valor gravado termina sempre em `000`. A unidade de µs não
acrescenta precisão — existe porque é a única que os dois motores conseguem
gravar iguais sem a marca (ver abaixo).

**Por quê.** No Flink o carimbo precisa ser lido **por evento, dentro do job**, e
em SQL — uma função Python no caminho do dado mediria o PyFlink, não o Flink
(decisão F2). O SQL do Flink 1.20 não tem expressão confiável para "agora em
milissegundos como inteiro": verificado em 24/09/2026, das cinco candidatas duas
dão erro, uma só tem precisão de segundos, uma devolve valores sem sentido e a
última combina dois relógios lidos em separado, podendo errar 1 s na virada de
segundo. Já `CURRENT_ROW_TIMESTAMP()` — reavaliado a cada evento por definição —
grava o instante no tipo nativo sem conversão nenhuma, e o valor foi verificado
contra o relógio do sistema e lido corretamente pelo DuckDB. A conversão para
milissegundos passa para a análise (`epoch_ms`), como o watermark (2.1): tirar do
caminho quente o que pode ser feito depois.

**Por que o Spark muda junto.** A coluna precisa ter o mesmo tipo nos dois motores
(seção 5). O Spark continua tomando o instante uma vez por micro-batch, no driver.

**Por que sem a marca de UTC, e em µs** (decisões de 25/09/2026):
- **O Flink 1.20 não consegue marcar UTC.** O escritor de Parquet dele grava
  `isAdjustedToUTC` com um `false` fixo no código
  (`ParquetSchemaConverter`: `timestampType(false, timeUnit)`), para os dois tipos
  de data/hora, sem opção de configuração. O Spark marca por padrão. Para os
  arquivos ficarem idênticos, o Spark passou a gravar sem a marca
  (`timestamp_ntz`).
- **Sem a marca, o Spark só grava em µs.** A opção `outputTimestampType` do Spark
  vale apenas para o tipo marcado; o `timestamp_ntz` sai sempre em microssegundos.
  O Flink aceita a unidade por configuração, e passou a gravar em µs também.

**Consequência declarada.** Sem a marca, o arquivo não diz que o horário é UTC. O
`reconcile.py` não é afetado — converte o número, não o rótulo. Outra ferramenta,
numa máquina fora de UTC, pode exibir os carimbos deslocados; quem ler estes
arquivos fora do arnês precisa tratá-los como UTC.

**Três armadilhas, todas silenciosas:**
- **O VALOR do Flink depende do fuso da JVM.** Sem `parquet.utc-timezone=true`, o
  escritor converte o carimbo pelo fuso da JVM: com a JVM em `America/Recife`,
  todos os valores saíram **3 h deslocados**, sem erro; em UTC saíam certos por
  coincidência. O `table.local-time-zone=UTC` da sessão **não** protege isso.
  Com a opção, o valor sai certo nos dois fusos — verificado em 25/09/2026.
- **O formato padrão do Flink é o legado `INT96`**, e o do Spark também. O Flink
  declara `parquet.write.int64.timestamp=true`; o Spark, com `timestamp_ntz`,
  grava `INT64` de qualquer forma.
- **O log de progresso continua em inteiro** (epoch ms), por ser JSON. A análise
  converte o Parquet para inteiro antes de cruzar os dois.

### 2.6 O pipeline não transforma: ELT estrito (versão 1.8, 11/10/2026)

**Decisão.** Os dois pipelines **carregam** os dez campos do evento como chegam, e
acrescentam apenas as colunas de medição desta seção. Não há enriquecimento (o
join com uma tabela de produtos, previsto até a versão 1.7) nem limpeza dentro do
pipeline.

**Por quê.** O trabalho compara pipelines **ELT**: extrair, carregar e só então
transformar, no destino. Um join ou um filtro antes da escrita é transformação em
trânsito, que é a marca do ETL. A transformação ocorre no destino, é a mesma para
os dois motores e está **fora da comparação**. O que se compara é o **E** e o
**L**, onde os motores diferem.

**O pipeline também não decide o que é válido (versão 2.0).** Os dez campos são
carregados como texto, do jeito que vieram (seção 2.7). Um campo com o tipo errado
— texto onde se esperava número, fração num inteiro, número entre aspas — chega ao
destino **como veio**, nos dois motores. Converter para o tipo do contrato e
decidir o que fazer com o que não converte é trabalho do **Transform**, depois do
Load.

**O que isso substitui.** Até a versão 1.9 os campos eram carregados já tipados,
e cada motor decidia sozinho o que fazer com um valor de tipo errado — e os dois
decidiam diferente (medido em 11/10/2026): o Spark anulava `"17"` num campo
numérico e o Flink convertia para 17; o Spark anulava `1.5` num campo inteiro e o
Flink truncava para 1. A diferença era declarada e contada. Com a carga em texto
ela deixou de existir.

**A diferença que sobra: a mensagem que não é JSON.** Texto solto, JSON cortado,
lista, mensagem vazia ou nula: o Spark grava uma linha com **todos os campos
nulos**, e o Flink **descarta** a mensagem, sem erro nem aviso (medido em
11/10/2026 e de novo em 12/10/2026). Nenhum dos dois derruba o job. **O gerador
não envia mensagens assim** (decisão de 12/10/2026), e por isso a diferença não
aparece nas rodadas. Ela não foi igualada: fica declarada aqui.

Uma linha sem `run_id` vai para a **partição nula** do destino, cujo nome difere
por motor: `run_id=__HIVE_DEFAULT_PARTITION__` no Spark e
`run_id=__DEFAULT_PARTITION__` no Flink. O mesmo vale para `dt` e `hh` quando o
`event_ts` não é um instante (seção 4.1).

**O que é contado.** O `reconcile.py` reporta, em toda execução e pelo mesmo
método nos dois motores, **depois de converter** os campos (seção 2.7):

| Contagem | Definição |
|---|---|
| **Inválidas** | linhas gravadas com **algum campo obrigatório nulo depois da conversão** — porque não veio, ou porque veio um texto que não converte para o tipo do contrato. **Desde a versão 3.1, os onze campos do evento são obrigatórios**, inclusive o `amount` e o `event_time`, que todo evento da base traz (um preço 0 é valor, não falta); e o `run_id`, quando a linha está na partição nula. Até a 3.0 o `amount` ficava de fora, porque o gerador sintético o deixava nulo em parte dos eventos |
| **Nulas** | linhas gravadas com **todos** os campos do evento nulos |
| **Descartadas** | mensagens que estavam no tópico e não viraram linha: *mensagens recebidas pelo tópico na execução − linhas no destino*, descontadas as duplicatas |

As linhas da partição nula não trazem `run_id`; são atribuídas à execução pelo
`processed_ts`, com 5 s de folga em cada ponta. As **descartadas** dependem de uma
anotação por execução com o número de mensagens que o tópico recebeu
(`<run_id>.topico.json`); sem ela a contagem é reportada como indisponível, nunca
estimada.

Com a carga em texto, **nulas e descartadas só aparecem com mensagens que não são
JSON**: numa rodada do benchmark as duas têm de dar zero nos dois motores, e as
**inválidas têm de dar o mesmo número** nos dois.

**As três contagens são informação, e nunca reprovam a execução (versão 1.9,
decisão de 11/10/2026).** O que reprova é **perda ou duplicata entre os eventos
válidos** — é o que faz o `reconcile.py` sair com código 1.

**Desde a versão 3.0 o benchmark não leva eventos malformados** (decisão do autor,
23/10/2026): a base real não tem nenhum, e eles deixaram de ser injetados, para o
dado chegar como a loja o registrou. As três contagens continuam sendo feitas, como
guarda, e o esperado é zero — salvo os 2 eventos da base sem `user_session`, que
saem como inválidos se caírem numa janela (seção 1.5). O destino do teste dedicado
(`inject_malformed.py`, README passo 21), que monta os malformados sobre eventos do
gerador sintético, ainda será decidido.

O que segue descreve a regra **até a versão 2.2**, mantida como registro.

**No benchmark, 4 das 10 rodadas de cada combinação levavam eventos malformados de
propósito** — as rodadas 2, 5, 7 e 10, as mesmas para os dois motores e para todas
as combinações. Nelas, **1% das mensagens** era malformada: o próprio gerador as
emite (`MALFORMED_RATE`), **no lugar** de eventos válidos, a intervalos fixos, com
os **8 tipos** de `shared/data-generator/malformed.py` em rodízio. Todos são JSON
válido: um objeto com um campo de tipo errado, ou sem um campo.

| Tipo | O que vai no evento | O que os dois motores gravam |
|---|---|---|
| `texto_em_seq` | `"seq": "abc"` | `abc` |
| `numero_como_texto` | `"user_id": "17"` | `17` |
| `fracao_em_inteiro` | `"seq": 900000.5` | `900000.5` |
| `estouro_int32` | `"user_id": 9999999999` | `9999999999` |
| `texto_em_amount` | `"amount": "caro"` | `caro` |
| `texto_em_event_ts` | `"event_ts": "ontem"` | `ontem`, na pasta sem data |
| `sem_seq` | o campo `seq` não vem | nulo |
| `sem_run_id` | o campo `run_id` não vem | nulo, na partição nula |

Até a versão 1.9 eram 13 tipos; saíram os cinco que não são JSON. Os malformados
levam `gen_id` 99, para se distinguirem dos eventos válidos (`gen_id` 0), que são
os que têm manifesto e sobre os quais se contam as perdas.

As médias e os desvios padrão de uma combinação são calculados sobre as 10
rodadas, sem separar as que levaram malformados.

**Dois limites, declarados:**
- Um `amount` que não converte **não** entra nas inválidas: nulo é valor legítimo
  para o `amount`. O `reconcile.py` o mostra à parte, na linha "texto que não
  converte".
- `"user_id": "17"` **não** é inválido: o texto `17` converte. É o caso que o
  Transform conserta sozinho.

### 2.7 Os dez campos são carregados como texto (versão 2.0, 12/10/2026)

**Decisão**, do autor com a orientação. Os dois pipelines carregam os dez campos
do evento como **texto**. Nenhum tipo é interpretado antes do Load. O tratamento
dos eventos malformados — a conversão para o tipo do contrato e o que fazer com o
que não converte — fica para o **Transform**, depois do Load, igual para os dois
motores e fora da comparação.

**Por quê.** Carregando tipado, quem decidia o que era um valor válido era o
leitor de JSON de cada motor, e os dois decidiam diferente (seção 2.6). Igualar
isso dentro do pipeline era possível — verificado por sonda em 12/10/2026 —, mas
obrigava o Flink a consultar o JSON uma vez por campo, a cada evento, onde hoje
ele lê a mensagem uma vez. Carregando texto, nenhum motor decide: os dois gravam
o que veio, e a regra de conversão passa a ser uma só, aplicada depois.

**Como cada motor faz.**

| | Spark | Flink |
|---|---|---|
| Leitura | `from_json` com os dez campos declarados `StringType` | `'format' = 'json'` com as dez colunas declaradas `STRING` |
| No Parquet | dez colunas de texto | dez colunas de texto |
| Campo ausente ou `null` | nulo | nulo |

**O que foi verificado (12/10/2026).**
- Por sonda, com 3.000 eventos válidos do gerador: o texto gravado é **idêntico
  nos dois motores e idêntico ao enviado**, nos dez campos — inclusive o `amount`,
  de uma e de duas casas decimais.
- Pelas rodadas 1 e 2 do script de rodada, nos dois motores, com o destino
  comparado campo a campo: 6.000 linhas de cada lado, **nenhuma diferença**; na
  rodada 2, as mesmas 45 inválidas, 0 nulas e 0 descartadas.

**Limites, medidos em casos que o gerador não produz:**
- **Número decimal escrito de forma não canônica.** O Flink grava o texto como
  veio (`49.90`, `1.5e3`); o Spark reescreve o número (`49.9`, `1500.0`), e passa
  para notação científica a partir de 10 milhões (`1.23456789E7`). O **valor** é o
  mesmo depois de convertido; o texto difere. O gerador escreve o `amount` com no
  máximo duas casas, sem zero à direita, entre 5 e 500.
- **Chave repetida no JSON.** O Flink fica com a primeira ocorrência, o Spark com
  a última.
- Inteiros saem iguais, até com 20 dígitos. Um objeto ou uma lista num campo vira
  o próprio JSON em texto, nos dois.

**A conversão, depois do Load.** Hoje é feita pelo `reconcile.py`, ao ler o
destino, com uma regra provisória e sem conserto:

| Tipo do contrato | Converte quando o texto é | Senão |
|---|---|---|
| inteiro (`int32`, `int64`) | só dígitos, com sinal opcional, e cabe no tipo | nulo |
| decimal (`float64`) | um número, por extenso ou em notação científica | nulo |
| texto | — fica como veio | — |

Nada é arredondado, truncado ou adivinhado. O lugar definitivo dessa regra é um
**módulo de Transform** separado, a escrever depois da Etapa 9, onde também se
decide o que fazer com cada tipo que não converte.

**Consequência declarada: o tamanho do destino muda.** Número guardado como texto
ocupa diferente. Numa rodada de 6.000 eventos: 287,9 bytes por evento no Spark
(eram ~282) e 147,9 no Flink (eram ~144).

**Uma exceção, e só uma: o `event_ts` é lido para montar as pastas** `dt=` e
`hh=` (seção 4.1).

**Desde a versão 3.0 são onze campos.** O `event_time` original (seção 1.5) entra
como texto, como os outros, e não é interpretado por nenhum motor.

**O `amount` da base viaja como texto (decisão do autor, 23/10/2026).** A base
escreve preços com zero à direita (`543.10`, `33.20`), justamente o caso em que, se
o preço fosse enviado como número JSON, o Spark reescreveria o texto (`543.1`) e o
Flink não (limite acima). O replayer envia o `price` **exatamente como está na
base, entre aspas** (`"amount": "543.10"`): nenhum valor é reescrito antes do Load,
e os dois motores gravam o mesmo texto, igual ao da base. O tipo do contrato
(`float64`) continua sendo o do evento: a conversão, depois do Load, transforma
`"543.10"` em 543,1 pela regra da tabela acima. Reescrever o número para a forma
canônica no replayer foi considerado e recusado, por ser transformação antes do
Load. Os inteiros (`user_id`, `product_id`) seguem como número: nesses os dois
motores gravam o mesmo texto.

**Desde a versão 3.1 (decisão do autor, 24/10/2026), dois tipos novos.** Com o
`float64`, a conversão perdia de novo, depois do Load, o que a decisão acima
protegeu na carga: `"543.10"` virava 543,1, e somas de preços em ponto flutuante
acumulam erros pequenos. E o `event_time` em texto obrigava cada consulta a
convertê-lo. Por isso:

| Campo | Tipo até a 3.0 | Tipo desde a 3.1 | Converte quando o texto é | Senão |
|---|---|---|---|---|
| `amount` | `float64` | `decimal(10,2)`: exato, duas casas | um número com até 8 dígitos inteiros e até 2 casas decimais | nulo — nada é arredondado |
| `event_time` | `string` | `timestamp`: data e hora em UTC | exatamente `AAAA-MM-DD HH:MM:SS UTC`, a forma da base | nulo |

Os pipelines **não mudam**: continuam carregando os onze campos como texto, e só
aprendem os dois nomes de tipo, para não recusarem o `evento.json`. O destino é o
mesmo byte a byte.

---

## 3. Parâmetros do tópico Kafka

| Parâmetro | Valor | Justificativa |
|---|---|---|
| Nome | `ecommerce.events.v1` | Sufixo de versão evita ambiguidade se o schema mudar |
| Partições | **24** | Teto do paralelismo dos dois frameworks; fixado uma vez e nunca alterado |
| Fator de replicação | 1 local, **3** na nuvem | RF 3 é necessário para o teste de queda de broker (cenário C2) |
| `min.insync.replicas` | 1 local, **2** na nuvem | Sem isso, `acks=all` com RF 3 ainda perde dados (decisão E1) |
| `retention.ms` | **168 h** | ver 3.1 |
| `retention.bytes` | **1 GiB por partição** | ver 3.2 |
| `compression.type` (tópico) | `producer` | O broker preserva a compressão aplicada pelo produtor |
| `acks` (produtor) | `all` | Sem isso, "evento perdido" pode ser falha do gerador e não do pipeline |
| Compressão (produtor) | `lz4` | Registrar a taxa obtida — afeta o custo de rede |
| `partitioner` (produtor) | **`murmur2_random`** | ver 3.3 |
| `linger.ms` (produtor) | **5** | ver 3.3 |
| `auto.create.topics.enable` | `false` | Guarda de validade: com auto-create, um nome errado cria topico de 1 partição e o experimento roda com paralelismo 1 sem erro na tela |

### 3.1 Retenção por tempo — 168 h, e o tópico recriado a cada rodada (versão 1.9)

**`retention.ms` é de 168 h, um valor só**, declarado no `docker-compose.yml`.

**O que controla o tópico numa medição não é a retenção: é a recriação.** O
`benchmarks/run_round.py` apaga e recria o tópico no começo de cada rodada (seção
5.2). O tópico nunca guarda mais que uma rodada, e uma rodada dura minutos — a
retenção por tempo não chega a agir. O freio de tamanho da seção 3.2 continua
valendo.

**O que esta seção dizia até a versão 1.8, e não foi implementado.** Dois valores
— 168 h na fase de aprendizado e **2 h** na de medição — e uma "guarda
obrigatória": o script de benchmark abortaria se encontrasse o valor de
aprendizado. A retenção nunca saiu das 168 h e a guarda nunca foi escrita. Com o
tópico recriado a cada rodada (decisão de 11/10/2026) as duas perderam a função: a
recriação garante mais do que elas garantiriam — nenhuma mensagem de outra rodada,
nem do outro motor.

**Limite declarado.** Fora do script de rodada — nas execuções feitas à mão — as
mensagens ficam no tópico por até 168 h, e passado esse prazo o Kafka apaga tudo
sem aviso (visto em 07/10/2026). Na nuvem o valor é revisto junto com o fator de
replicação.

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
é a recriação do tópico a cada rodada (seção 3.1).

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

**Que texto vale como instante (versão 2.0).** O `event_ts` chega como texto
(seção 2.7), e montar as pastas é a única leitura de valor que o pipeline faz. Só
vira data o texto que é um **inteiro de 1 a 13 dígitos**, e nada mais — 13 dígitos
de milissegundos vão até o ano 2286. Qualquer outro (`ontem`, um número com
fração, com sinal, com espaço ou com mais dígitos) deixa a linha **sem data**, na
partição nula de `dt` e `hh`. A regra é uma expressão regular escrita **uma vez**,
no `shared/schema/evento.json`, e aplicada igual pelos dois motores. Sem ela, um
`event_ts` que não é número derrubaria o Spark, que roda em modo ANSI.

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
| Schema do evento | Seção 1.1, sem alteração: os onze campos |
| Fonte dos eventos | A base real da seção 1.5, conferida pelo sha256 |
| Carga dos campos | Os onze campos como **texto**, sem interpretar tipo (seção 2.7) |
| Chave da mensagem | `user_id`, representação decimal em UTF-8 (seção 1.3) |
| Distribuição de `user_id` | A da base, pela janela da rodada (seções 1.4 e 5.1) |
| Colunas de saída | Seção 2. `processed_ts` idêntica nos dois (2.5); `batch_id` só no Spark — no Flink a unidade de progresso é o checkpoint, reconhecido pelo grupo de arquivos que ficam visíveis juntos (2.3). `committed_ts` com origem por motor, declarada (2.2) |
| Cálculo do watermark | Máximo acumulado de `event_ts` menos 60 s, monotônico — **derivado na análise**, igual para os dois motores (seção 2.1) |
| Tópico e partições | `ecommerce.events.v1`, 24 partições |
| Replayer e janela | Mesmo programa, mesma base, e **a mesma janela na mesma rodada**: a rodada N do Spark e a do Flink recebem os mesmos eventos, na mesma ordem (seção 5.1) |
| Carga | Mesmos níveis, mesma duração, mesmas 10 rodadas por combinação, sem eventos malformados (seções 2.6 e 5.1) |
| Estado inicial de cada rodada | Tópico recriado e vazio, checkpoint do motor apagado, leitura desde o início do tópico (seção 5.2) |
| Transformações | **Nenhuma dentro do pipeline** (ELT estrito, seção 2.6): os dois carregam os dez campos como chegam, **em texto** (seção 2.7), sem limpeza e sem join. A conversão para os tipos do contrato é depois do Load, pela mesma regra para os dois. O watermark é derivado na análise (2.1). Até a versão 1.7 esta linha previa "mesma limpeza, mesmo broadcast join", e havia uma linha para a tabela de produtos; nenhum dos dois chegou a ser implementado |
| Particionamento do destino | Derivado de `event_ts`, em UTC, pela mesma regra do que vale como instante (seção 4.1) |
| Codec do Parquet | `snappy`, declarado explicitamente nos dois (seção 4.4) |
| Destino | Mesmo formato, mesmo particionamento, mesma política de escrita (sem `repartition`) |
| Definições de métrica | planoNRT3 seção 3, sem alteração |
| Análise | O mesmo `reconcile.py` |
| Infraestrutura | Mesmos tipos de instância, mesma AZ, mesma região |

### 5.1 Reprodução da carga: uma janela da base por rodada (versão 3.0)

**Cada rodada recebe uma janela contínua da base** (decisão do autor, 23/10/2026):
os eventos de uma faixa de linhas consecutivas, na ordem original. O tamanho da
janela é o número de eventos da rodada — **carga × duração**: 60 mil a 1.000 evt/s
por 60 s, 4,5 milhões a 15.000 evt/s por 300 s.

**Onde a janela começa é sorteado pela seed da rodada**, a mesma de antes: 41 + N.
A linha inicial é sorteada, com um gerador pseudoaleatório semeado por ela, entre
a primeira linha da base e a última que ainda deixa a janela inteira. A rodada N
do Spark e a do Flink recebem **a mesma janela**. O manifesto de cada rodada
registra as linhas de início e de fim.

**Por que janela contínua, e não eventos sorteados.** Sortear linhas soltas
desfaria as sessões (`view → cart → purchase` de um mesmo usuário) e os picos de
movimento da loja — justamente o que torna o dado real. A janela os preserva.

**As janelas podem se sobrepor**, e isso é aceito. Na carga mais pesada, dez
janelas de 4,5 milhões não cabem nos 42,4 milhões do mês sem se cruzar. Não dá
vantagem a nenhuma rodada: cada uma começa do zero — tópico recriado, checkpoint
apagado, JVM nova (seção 5.2) — e o motor não guarda nada da anterior.

**A sobreposição medida** (seeds 42 a 51, 23/10/2026), em eventos que aparecem em
mais de uma das 10 rodadas: **0%** nas combinações de 1.000 × 60 s, 7.500 × 60 s,
15.000 × 60 s e 1.000 × 300 s; **10,8%** em 7.500 × 300 s; **34,2%** em
15.000 × 300 s — o mínimo inevitável nesta seria ~6%. O sorteio é livre em todo o
mês; sortear a rodada N dentro de uma faixa N do mês reduziria a sobreposição, e
foi considerado e recusado pelo autor, para manter a regra mais simples.

**A janela é lida durante a medição, linha a linha** (decisão do autor,
23/10/2026). **Antes** de o relógio da rodada começar, o replayer só avança no
arquivo até a linha inicial — sem isso, a rodada passaria o começo sem enviar nada.
Do relógio em diante, ele lê uma linha, monta a mensagem e a envia, uma de cada
vez, na taxa pedida: como um produtor real, que envia o evento quando ele acontece.
O trabalho de ler e montar disputa a CPU com o motor, e **essa disputa faz parte do
que se mede**. Considerou-se carregar a janela inteira antes, com as mensagens
prontas; recusado porque, na combinação mais pesada, ocuparia ~1,4 GB e não
caberia com folga no teto de 4 GB do WSL de referência. A escolha é revista se, na
nuvem, o replayer continuar na mesma máquina do motor.

Exige as mesmas garantias de antes: **um único thread produtor por `gen_id`**, e
**nenhum uso de relógio ou entropia** no sorteio da janela.

O `event_ts` difere entre as execuções do Spark e do Flink, o que é irrelevante:
toda latência é calculada em relação ao próprio `event_ts` de cada evento. O que
precisa ser idêntico é a **sequência de eventos**, não o instante absoluto.

O que segue descreve a regra **até a versão 2.2**, mantida como registro.

O gerador era pseudoaleatório e semeado. **Cada rodada tem a sua seed, e ela é a
mesma para os dois motores (versão 1.9):** a rodada N usa a seed 41 + N — a
rodada 1 usa 42, a rodada 10 usa 51. Até a versão 1.8 a seed era uma só, fixa.

**Por quê.** Com uma seed só, as 10 rodadas de uma combinação repetiriam a mesma
sequência de eventos, e a variação entre elas mediria apenas o ruído da máquina.
Com uma seed por rodada, a variação inclui a dos dados. E com a mesma seed nos
dois motores, a rodada N continua sendo uma comparação sobre entrada idêntica.

Nas rodadas 2, 5, 7 e 10 (seção 2.6) os malformados entravam em posições fixas e
com os tipos em rodízio, de modo que os dois motores recebiam os mesmos
malformados, nas mesmas posições. O gerador sintético
(`shared/data-generator/generator.py`) continua no repositório, sem uso nas
rodadas, para que a bateria feita com ele siga reproduzível.

### 5.2 Como uma rodada é feita (versão 1.9)

Uma rodada é executada por `benchmarks/run_round.py`, sempre do mesmo estado
inicial:

1. o checkpoint do motor é apagado;
2. **o tópico é apagado e recriado**, e a posição do grupo de consumo é apagada:
   toda rodada começa com as 24 partições vazias, sem mensagens da rodada anterior
   nem do outro motor;
3. o pipeline lê **desde o início do tópico** (`earliest`);
4. o replayer carrega a janela da rodada e então a envia, na taxa pedida, pela
   duração da rodada (seção 5.1);
5. a rodada termina quando o replayer terminou **e** a fila do grupo de consumo
   zerou; o pipeline é então encerrado por um arquivo de parada.

**As 10 rodadas de uma combinação alternam os motores**, e quem começa inverte a
cada rodada: Spark e Flink na rodada 1, Flink e Spark na 2, e assim por diante
(`benchmarks/run_combination.py`). O que muda ao longo da sessão — a máquina
esquentar, outro programa abrir — pesa igual sobre os dois.

**O relógio é conferido antes de cada rodada** (`scripts/check_clock.py`); uma
rodada com o relógio reprovado não começa.

**O que se varia (versão 2.1, decisão de 12/10/2026).** Só duas coisas: o
intervalo do Spark e a carga.

| Combinação | Trigger do Spark | Checkpoint do Flink | Cargas (mensagens/s) | Duração da rodada |
|---|---|---|---|---|
| Intervalo 5 | 5 s | 5 s | 1.000, 7.500 e 15.000 | 60 s |
| Intervalo 30 | 30 s | **5 s** | 1.000, 7.500 e 15.000 | 300 s |

São seis combinações, com 10 rodadas de cada motor em cada uma.

- **O intervalo do Flink é fixo em 5 s.** O "intervalo" que dá nome à combinação é
  o do Spark. Na combinação de intervalo 30 o Flink roda de novo, ao lado do
  Spark, com a mesma carga, a mesma duração e as mesmas seeds — é uma segunda
  medição da mesma configuração do Flink, feita nas condições da combinação.
- **A duração acompanha o intervalo do Spark.** Com trigger de 30 s, uma rodada de
  60 s produz dois ou três lotes, e sobra um depois do descarte de aquecimento;
  em 300 s são dez ou onze. No intervalo de 5 s, 60 s dão doze.
- Até a versão 2.0 o mesmo intervalo valia para os dois motores (trigger e
  checkpoint), e os valores e as cargas não estavam fixados.

Verificado em 12/10/2026, com o gerador sintético, por um ensaio de uma rodada por caso: o gerador atinge
15.000 mensagens por segundo, e os dois motores fecham as rodadas de 7.500 e de
15.000 sem perda, inclusive o Spark com trigger de 30 s (lotes de ~450 mil
eventos, pico de memória de 1.178 MiB num contêiner de 1.536).

**O que fica de uma combinação.** Quando as 20 rodadas estão válidas, são escritos
os arquivos de resultado (`benchmarks/write_results.py`) e o material bruto das
rodadas é apagado. O Parquet de cada rodada é apagado antes, logo depois de
reconciliado. Consequência declarada: os números publicados não podem ser
regerados a partir do material bruto; para isso é preciso refazer a combinação.

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
| Retenção do tópico | É a mesma para os dois motores (168 h) e não age numa rodada: o tópico é recriado no começo de cada uma (seção 3.1) |
| **Destino de uma mensagem que não é JSON** | O Spark grava uma linha nula e o Flink descarta (seção 2.6). O gerador não envia mensagens assim; a diferença fica **declarada**, não foi igualada. Até a versão 1.9 esta linha cobria também os campos de tipo errado — que, com a carga em texto, os dois motores passaram a gravar igual |
| O **texto** de um número decimal escrito de forma incomum | O Spark reescreve o número, o Flink guarda como veio (seção 2.7). O valor convertido é o mesmo. O replayer envia o `amount` como texto, exatamente como na base, e por isso o caso não ocorre no benchmark |
| O **ritmo** dos eventos | É a taxa pedida pelo benchmark, não a da loja: a base é acelerada (seção 1.5). A ordem original é mantida |

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
| 3.1 | 24/10/2026 | **§1.1, §2.6 e §2.7 alterados.** Dois tipos novos, para a conversão depois do Load: `amount` passa de `float64` a **`decimal(10,2)`**, exato, para `"543.10"` continuar 543,10 também depois de convertido; `event_time` passa de `string` a **`timestamp`** em UTC. **Os onze campos passam a ser obrigatórios** na contagem de inválidas: entram o `amount`, que a base traz em todo evento, e o `event_time`. Decisões do autor, ao começar o módulo de Transform. Os pipelines continuam carregando texto e só aprendem os nomes dos tipos novos | **Nenhuma medida.** O destino não muda. Nenhuma rodada do benchmark com a base real havia sido feita: só testes (23/10 local, 24/10 na nuvem) |
| 3.0 | 23/10/2026 | **§1.5 novo; §1.1, §1.3, §1.4, §2.6, §2.7, §5, §5.1, §5.2 e §6 alterados.** Os eventos passam a ser **reais**: a base *eCommerce behavior data from multi category store* (REES46, outubro de 2019, 42.448.764 eventos, sha256 conferido), pedido da orientação. Um **replayer** substitui o gerador sintético nas rodadas: cada rodada recebe uma **janela contínua** da base, com o início sorteado pela seed da rodada, a mesma nos dois motores, carregada antes da medição. O evento ganha o 11º campo, **`event_time`**, o instante original; o `event_ts` passa a ser o instante do envio, e o `event_id`, o endereço da linha na base. Tipos de evento, preço, usuários e produtos passam a ser os da loja, como vieram. **Sem eventos malformados no benchmark.** Versão maior porque **a fonte dos dados** muda | **Todas as do benchmark.** A bateria local de 12–13/10/2026 (120 rodadas, gerador sintético) e o ensaio da nuvem de 21/10/2026 foram feitos com eventos gerados; ficam como histórico, e a bateria local **será refeita** com a base real (decisão do autor, 23/10/2026) |
| 2.2 | 12/10/2026 | **§2.3 alterado.** O descarte de aquecimento deixa de ser a primeira unidade de progresso de cada motor e passa a ser um **tempo fixo de eventos, igual nos dois motores**: os eventos gerados nos primeiros 5 s nas rodadas de 60 s, e nos primeiros 30 s nas de 300 s. Os dois motores descartam o mesmo trecho da carga. O throughput médio segue a mesma regra | **Nenhuma medida.** Nenhuma rodada do benchmark havia sido feita |
| 2.1 | 12/10/2026 | **§5.2 ampliado.** Fixado o que se varia no benchmark: **o trigger do Spark em 5 s e 30 s, com o checkpoint do Flink fixo em 5 s**; cargas de 1.000, 7.500 e 15.000 mensagens por segundo; rodadas de 60 s nas combinações de intervalo 5 e de 300 s nas de intervalo 30. Até a 2.0 o intervalo era o mesmo para os dois motores. | **Nenhuma medida.** Nenhuma rodada do benchmark havia sido feita |
| 2.0 | 12/10/2026 | **§2.7 novo; §2.6, §4.1, §5 e §6 alterados; §1.1 ajustado.** Os dois pipelines passam a **carregar os dez campos como texto**, sem interpretar tipo: decisão do autor com a orientação. O tratamento dos eventos malformados sai do pipeline e vai para o Transform, depois do Load; a conversão é feita, por ora, pelo `reconcile.py`, com uma regra só para os dois motores. **O gerador deixa de enviar mensagens que não são JSON:** os tipos de malformado passam de 13 para 8. A diferença entre os motores nos campos de tipo errado deixa de existir; sobra, declarada, a da mensagem que não é JSON. As pastas `dt=`/`hh=` passam a sair só de um `event_ts` que seja um inteiro de 1 a 13 dígitos, por uma regra única. Versão maior porque o **formato do destino** muda | **Nenhuma medida.** Nenhuma rodada do benchmark havia sido feita. Os números de teste anteriores (carga tipada) deixam de servir de referência: o tamanho do destino mudou, e as contagens de inválidas, nulas e descartadas têm outra definição |
| 1.9 | 12/10/2026 | **§2.6, §3.1, §5 e §5.1 alterados; §5.2 novo; §1.4 e §2.2 ajustados.** A seed deixa de ser fixa: **uma por rodada (41 + N), a mesma para os dois motores**. **Quatro das dez rodadas de cada combinação (2, 5, 7 e 10) levam 1% de eventos malformados**, emitidos pelo gerador no lugar de eventos válidos. As contagens de inválidas, nulas e descartadas **deixam de reprovar** a execução e passam a ser resultado; perdas e duplicatas entre os eventos válidos continuam reprovando. Fica registrado como uma rodada é feita (tópico recriado, leitura desde o início, motores alternados, relógio conferido) e que o material bruto é apagado quando a combinação fecha. A exatidão medida do `ctime` do Flink passa de "±10 ms" para −13 a +16 ms. **Retenção (§3.1):** saem o valor de 2 h para a medição e a guarda que abortaria, que nunca foram implementados; fica um valor só, 168 h, e o controle passa a ser a recriação do tópico | **Nenhuma medida.** Nenhuma rodada do benchmark havia sido feita. As execuções de aprendizado anteriores usaram a seed 42 e só eventos válidos, e não entram nos resultados |
| 1.8 | 11/10/2026 | **§2.6 novo; §5 e §6 alterados.** O pipeline passa a ser declarado **ELT estrito**: carrega, não transforma. Saem das invariantes a "mesma limpeza", o "mesmo broadcast join" e a tabela de produtos, que nunca foram implementados. O tratamento de eventos que o motor não consegue ler **não é igualado**: o Spark grava linha nula, o Flink descarta, e os dois divergem na conversão de tipos (medido em 11/10/2026). A diferença é declarada e passa a ser contada em toda execução — inválidas, nulas e descartadas | **Nenhuma.** Os pipelines não mudam: gravam exatamente o que gravavam. As execuções de 10/10/2026, reconciliadas de novo, dão as mesmas latências, com 0 inválidas e 0 nulas |
| 1.7 | 10/10/2026 | **§2.3 alterado; §2.2 corrigido.** A unidade de progresso do Flink deixa de ser o arquivo e passa a ser o **checkpoint, reconhecido pelo grupo de arquivos** cujo `ctime` dista até 1 s: em 10/10/2026 os 8 arquivos de 2 dos 13 checkpoints saíram em dois instantes, a 10 ms um do outro, e a regra antiga contava 15 unidades. O `committed_ts` de cada evento continua sendo o `ctime` do próprio arquivo. A exatidão do `ctime` passa de "±8 ms" para a faixa das duas medições (−7..+8 ms e −10..+7 ms). Nenhuma coluna, layout ou parâmetro de execução muda | **Nenhuma.** É regra de análise, sem efeito no que os pipelines gravam. A execução `20261010T213810Z-r100`, reconciliada pelas duas regras, deu latências idênticas (o primeiro checkpoint não estava dividido); só a contagem passou de 15 para 13 |
| 1.6 | 25/09/2026 | **§2.2 e §2.3 alterados (decisão C2).** O Flink não grava `batch_id`: a unidade de progresso dele é o arquivo. O `committed_ts` passa a ter **origem por motor**: log de progresso no Spark; `ctime` do arquivo no Flink, verificado a ±8 ms do fim do checkpoint. O Spark fica no log porque o `ctime` dele adianta até ~300 ms quando a pasta de destino é nova. Assimetria de instrumento declarada | **Nenhuma.** O Flink ainda não gravava as colunas de medição, e o Spark não muda |
| 1.5 | 24–25/09/2026 | **§2 alterado:** `processed_ts` passa de `int64` (epoch ms) a **timestamp** nos dois motores: instante em UTC com precisão de ms, gravado como `INT64` em µs **sem a marca de UTC**, idêntico nos dois arquivos (§2.5). Motivo: no Flink não há expressão SQL confiável para o instante em ms como inteiro, e função Python no caminho do dado viola a decisão F2. Sem marca porque o Flink 1.20 não consegue marcar; em µs porque o Spark sem marca só grava em µs. O Flink declara `parquet.utc-timezone=true`, sem o qual o valor depende do fuso da JVM. A conversão para ms passa à análise | **Nenhuma medida.** As execuções de aprendizado do Spark de 24/09/2026 têm a coluna em `int64`; o `reconcile.py` aceita os dois tipos |
| 1.4 | 23/09/2026 | **§2 reescrito.** `watermark_ts` e `is_late` deixam de ser gravados pelo pipeline e passam a ser **derivados na análise** (§2.1), porque a definição do contrato não tem equivalente no modelo distribuído do Flink. Acrescentado `committed_ts` (§2.2), registrado **fora do Parquet**, num log por unidade de progresso — sem ele a latência de visibilidade da decisão B6 não é mensurável. A persistência do watermark ao lado do checkpoint deixa de existir | **Nenhuma.** Nenhuma execução medida foi feita, e as de aprendizado não produziram as colunas em questão |
| 1.3 | 21/09/2026 | **§4.2 alterado:** `<base>` passa a ser **por motor** (`~/tcc-data/out/spark` e `~/tcc-data/out/flink`; `s3a://<bucket>/spark/` e `s3a://<bucket>/flink/`). O layout abaixo do `<base>` não muda. Motivo: com um `<base>` único, os dois motores gravariam nas mesmas pastas sempre que lessem os mesmos `run_id`, como no desenvolvimento | **Nenhuma medida.** As execuções de aprendizado do Spark (Etapa 5) ficam no destino antigo, `~/tcc-data/out/events`, e podem ser descartadas |
| 1.2.1 | 21/09/2026 | **Nenhum item alterado.** O §1.1 ganhou forma executável em `shared/schema/evento.json`, que passa a ser a fonte única do schema para Spark e Flink | **Nenhuma** — o schema produzido pelo Spark foi verificado idêntico antes e depois |
| 1.2 | 09/09/2026 | **Nenhum item alterado.** A decisão E3 (sem `repartition`/`coalesce`), que o plano marcava como PROVISÓRIA, foi fechada como definitiva e a seção 4.3 passou a registrar a evidência que a sustenta e a limitação declarada sobre compactação | **Nenhuma** |
| 1.1 | 08/09/2026 | Fechadas as duas pendências que a v1.0 declarava, **sem alterar nenhum item já fixado**. Codec do Parquet = `snappy` (seção 4.4), decidido antes da primeira escrita. Partitioner do produtor = `murmur2_random` e `linger.ms` = 5 (seção 3.3), decididos na Etapa 3 e transcritos para cá | **Nenhuma.** O codec entra antes de qualquer escrita em Parquet existir; o partitioner apenas registra o que já vigorava desde a Etapa 3 |
