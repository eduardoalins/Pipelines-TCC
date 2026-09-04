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

Ambos consomem o mesmo tópico Kafka, aplicam as mesmas transformações e
escrevem Parquet no mesmo layout. O que varia é o motor — é isso que torna a
comparação válida. As invariantes que garantem essa equivalência estão no
`CONTRATO.md`.

## Documentos de pesquisa

Por decisão do autor, os documentos ficam **fora** deste repositório, junto com
a dissertação, em `../../Informações/`:

| Documento | Papel |
|---|---|
| `planoNRT3.md` | plano de implementação vigente — as 12 etapas e o protocolo experimental |
| `EtapaNRT/DecisaoEtapaX.md` | registro de cada etapa: o que foi feito, como, e as decisões com a justificativa |
| `perguntas.txt` | registro das decisões de projeto, cada uma com a justificativa |
| `planoNRT.md`, `planoNRT2.md` | versões anteriores, mantidas por rastreabilidade |

Os `DecisaoEtapaX.md` são a matéria-prima do capítulo de metodologia. Eles
registram também o **percurso** das decisões que mudaram no caminho — e as
correções, quando algo que se acreditava verdadeiro não era.

O repositório guarda o que **executa**; a pasta de pesquisa guarda o que
**justifica**. O `CONTRATO.md` (Etapa 2) é a ponte entre os dois: ele transcreve
para dentro do código as invariantes que o plano define.

---

## Reconstruir o ambiente

Pré-requisitos: Windows com WSL2 e Docker Desktop (com integração WSL ligada
para a distro Ubuntu).

```powershell
# 1. Instalar a configuração da VM (PowerShell do Windows)
Copy-Item env\wslconfig "$env:USERPROFILE\.wslconfig"
wsl --shutdown
```

```bash
# 2. Criar venv, pastas de dados e checar pacotes (dentro do WSL)
bash scripts/setup_env.sh
```

O script é idempotente e diz o que falta caso algum pacote do sistema esteja
ausente.

---

## Onde as coisas moram

Decisão E6: **código no Windows, dados em ext4 nativo do WSL.**

| | Caminho |
|---|---|
| Código (este repo) | `/mnt/c/.../Pipelines-TCC` |
| Ambiente Python | `~/.venvs/tcc` |
| Parquet de saída | `~/tcc-data/out` |
| Checkpoints do Spark | `~/tcc-data/checkpoints` |
| Métricas coletadas | `~/tcc-data/metrics` |

O motivo é medido, não estético: escrita sequencial em `/mnt/c` roda a
**77 MB/s** contra **1.2 GB/s** no ext4 — 15,8×. Com a saída no `/mnt/c`, o
gargalo do pipeline seria o sistema de arquivos do Windows e o Experimento A
estaria medindo drvfs, não Spark.

Nada de `~/tcc-data` entra no git. O que reproduz aqueles dados é o gerador
mais o `run_id` da execução.

---

## Protocolo de execução

Antes de **qualquer execução medida**, fechar Discord, Steam e navegadores.
Não é só memória: esses processos disputam CPU de forma imprevisível, e essa
disputa entra na variância que seria reportada como efeito do parâmetro em
teste.

---

## Estrutura

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
pipeline-streaming/       pipeline Flink                           (OE 2)
benchmarks/               run_experiment.sh                        (Etapa 8)
infra/                    Terraform                                (Etapa 10)
results/                  CSVs e manifestos por run_id             (Etapa 11)
```

A pasta `shared/` é fisicamente separada dos dois pipelines de propósito: é o
que impede que o Flink acabe medido com um arnês ligeiramente diferente.

---

## Subir o Kafka

```bash
docker compose up -d              # broker no ar
bash scripts/create_topic.sh      # tópico com 24 partições
~/.venvs/tcc/bin/python scripts/check_chain.py   # confere a cadeia inteira
```

Para parar: `docker compose down` mantém os dados; `docker compose down -v`
apaga o tópico e as mensagens.

**Se o Docker sumir de dentro do WSL**, é porque um `wsl --shutdown` derrubou
o backend do Docker Desktop, que não volta sozinho — basta reabrir o Docker
Desktop e esperar o daemon.

**A persistência depende do `KAFKA_LOG_DIRS`.** A imagem `apache/kafka` declara
`/var/lib/kafka/data` como `VOLUME`, mas seu `log.dirs` padrão aponta para
`/tmp/kafka-logs`. Sem a variável no Compose, o broker grava na camada gravável
do contêiner: o tópico sobrevive a um `docker restart` e morre em qualquer
`docker compose down`, com o volume permanecendo vazio e sem nenhum aviso. Para
conferir que está correto:

```bash
docker exec kafka ls /var/lib/kafka/data   # deve conter meta.properties
```

---

## Gerar eventos

```bash
~/.venvs/tcc/bin/python shared/data-generator/generator.py

# variar a carga sem editar o config.yaml
RATE=1000 DURATION=600 ~/.venvs/tcc/bin/python shared/data-generator/generator.py
```

O relatório separa **carga oferecida** (o que o gerador tentou emitir) de **carga
atingida** (o que o broker confirmou pelo callback de entrega). É a distinção
central da análise de escalabilidade: sem ela, corre-se o risco de medir o limite
do próprio gerador e chamar isso de limite do Spark.

Cada execução grava um manifesto em `~/tcc-data/metrics/<run_id>.gen<N>.json`
com os contadores, o último `seq`, as taxas e o payload médio (decisão E2). O
programa sai com código 1 se `confirmados != tentados`.

Parâmetros da carga e do produtor ficam em `shared/data-generator/config.yaml`,
nunca no código — é o mesmo arquivo que o pipeline Flink usará.

### Versões travadas (decisão E7)

| | Versão | Verificado em |
|---|---|---|
| Kafka | 4.0.2 (KRaft) | 03/09/2026 |
| Spark / PySpark | 4.0.4 (Scala 2.13) | 03/09/2026 |
| Python | 3.12.3 | 03/09/2026 |
| JVM | OpenJDK 17.0.20 | 03/09/2026 |
| confluent-kafka | 2.15.0 (librdkafka 2.15.0) | 04/09/2026 |

O conector `spark-sql-kafka-0-10_2.13:4.0.4` não vem por `pip` — o Spark o
resolve do Maven em tempo de execução. O sufixo `_2.13` precisa casar com a
versão do Spark; errá-lo produz um `ClassNotFoundException` que não indica a
causa real.

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
- [ ] **Etapa 4** — Spark lê do Kafka e imprime no console
- [ ] **Etapa 5** — Escrita em Parquet com checkpoint
- [ ] **Etapa 6** — Instrumento de medição
- [ ] **Etapa 7** — Enriquecimento
- [ ] **Etapa 8** — Arnês de benchmark
- [ ] **Etapa 9** — Validação local e caos
- [ ] **Etapa 10** — Terraform e AWS
- [ ] **Etapa 11** — Benchmarks oficiais
- [ ] **Etapa 12** — Análise e guia de decisão
