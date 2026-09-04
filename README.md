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
| `perguntas.txt` | registro das decisões de projeto, cada uma com a justificativa |
| `planoNRT.md`, `planoNRT2.md` | versões anteriores, mantidas por rastreabilidade |

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
docker-compose.yml     Kafka 4.0.2 em modo KRaft, um broker
requirements.txt       versões travadas (decisão E7)
env/wslconfig          configuração da VM, versionada e comentada
scripts/setup_env.sh   recria o ambiente do zero
scripts/create_topic.sh cria o tópico com 24 partições
scripts/check_chain.py verifica Python → Spark → JVM → Kafka
CONTRATO.md            schema e invariantes de comparabilidade  (Etapa 2)
src/                   gerador, pipeline e coletor de métricas  (Etapas 3+)
```

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

### Versões travadas (decisão E7)

| | Versão | Verificado em |
|---|---|---|
| Kafka | 4.0.2 (KRaft) | 03/09/2026 |
| Spark / PySpark | 4.0.4 (Scala 2.13) | 03/09/2026 |
| Python | 3.12.3 | 03/09/2026 |
| JVM | OpenJDK 17.0.20 | 03/09/2026 |

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
- [ ] **Etapa 2** — `CONTRATO.md` congelado
- [ ] **Etapa 3** — Gerador de eventos a 100 evt/s
- [ ] **Etapa 4** — Spark lê do Kafka e imprime no console
- [ ] **Etapa 5** — Escrita em Parquet com checkpoint
- [ ] **Etapa 6** — Instrumento de medição
- [ ] **Etapa 7** — Enriquecimento
- [ ] **Etapa 8** — Arnês de benchmark
- [ ] **Etapa 9** — Validação local e caos
- [ ] **Etapa 10** — Terraform e AWS
- [ ] **Etapa 11** — Benchmarks oficiais
- [ ] **Etapa 12** — Análise e guia de decisão
