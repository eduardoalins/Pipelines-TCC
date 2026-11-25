# Resultado — Intervalo de 5 s, Carga de 15000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo5Carga15000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 5 s; checkpoint do RT de 5 s |
| Carga | 15000 eventos por segundo |
| Duração de cada rodada | 60 s |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | rodadas 2, 5, 7 e 10: 1% de eventos malformados |
| Período | 12/10/2026 19:32 a 12/10/2026 20:23 |
| Commit | `5188b79` |
| Imagens | `tcc-spark:4.0.4` ec241f18cf24 · `tcc-flink:1.20.5` 7d6d36c04457 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 3733,1 ms | 49,9 ms | 5,6 ms | 0,5 ms |
| Latência de processamento P95 | 5995,5 ms | 47,1 ms | 15,3 ms | 1,6 ms |
| Latência de processamento P99 | 6850,2 ms | 123,5 ms | 50,2 ms | 15,1 ms |
| Latência de processamento Média | 3732,5 ms | 49,9 ms | 6,9 ms | 0,7 ms |
| Latência de visibilidade P50 | 4495,7 ms | 94,6 ms | 2571,1 ms | 14,9 ms |
| Latência de visibilidade P95 | 6764,9 ms | 84,0 ms | 4820,1 ms | 15,3 ms |
| Latência de visibilidade P99 | 7379,0 ms | 134,5 ms | 5020,1 ms | 15,4 ms |
| Latência de visibilidade Média | 4496,1 ms | 93,8 ms | 2571,8 ms | 14,9 ms |
| Throughput médio | 13841,90 evt/s | 281,10 evt/s | 14451,83 evt/s | 335,97 evt/s |
| CPU média | 18,76% | 1,05% | 13,48% | 0,47% |
| CPU máximo | 58,74% | 1,80% | 50,51% | 2,60% |
| Memória média | 903,3 MiB | 21,5 MiB | 947,4 MiB | 27,5 MiB |
| Memória pico | 1025,0 MiB | 22,8 MiB | 980,1 MiB | 31,3 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 2700,0 | 3485,7 | 2700,0 | 3485,7 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |

## 3. Por rodada

| Rodada | Motor | Semente | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 42 | 3768 ms | 6896 ms | 4469 ms | 7548 ms | 13494,7 evt/s | 17,8% | 55,9% | 1001 MiB |
| 1 | RT | 42 | 6 ms | 55 ms | 2573 ms | 5022 ms | 14253,7 evt/s | 14,0% | 49,2% | 942 MiB |
| **2** | NRT | 43 | 3759 ms | 6898 ms | 4640 ms | 7478 ms | 13840,2 evt/s | 20,4% | 59,8% | 1030 MiB |
| **2** | RT | 43 | 6 ms | 69 ms | 2605 ms | 5055 ms | 14493,6 evt/s | 13,8% | 55,5% | 1012 MiB |
| 3 | NRT | 44 | 3757 ms | 6855 ms | 4419 ms | 7268 ms | 14089,4 evt/s | 18,2% | 60,1% | 1039 MiB |
| 3 | RT | 44 | 6 ms | 44 ms | 2578 ms | 5028 ms | 14919,5 evt/s | 13,6% | 50,7% | 956 MiB |
| 4 | NRT | 45 | 3802 ms | 7025 ms | 4497 ms | 7630 ms | 14138,1 evt/s | 19,0% | 61,6% | 1036 MiB |
| 4 | RT | 45 | 6 ms | 51 ms | 2566 ms | 5016 ms | 14367,4 evt/s | 13,8% | 47,8% | 945 MiB |
| **5** | NRT | 46 | 3715 ms | 6798 ms | 4631 ms | 7415 ms | 13854,9 evt/s | 20,0% | 57,8% | 1036 MiB |
| **5** | RT | 46 | 6 ms | 83 ms | 2582 ms | 5031 ms | 14644,2 evt/s | 13,7% | 53,9% | 1012 MiB |
| 6 | NRT | 47 | 3736 ms | 6880 ms | 4423 ms | 7248 ms | 13860,1 evt/s | 18,3% | 59,7% | 1018 MiB |
| 6 | RT | 47 | 6 ms | 41 ms | 2558 ms | 5004 ms | 13825,7 evt/s | 13,8% | 51,2% | 1000 MiB |
| **7** | NRT | 48 | 3630 ms | 6627 ms | 4594 ms | 7238 ms | 13379,1 evt/s | 19,6% | 59,4% | 1032 MiB |
| **7** | RT | 48 | 5 ms | 40 ms | 2571 ms | 5021 ms | 14380,0 evt/s | 13,7% | 49,5% | 1007 MiB |
| 8 | NRT | 49 | 3727 ms | 6927 ms | 4391 ms | 7335 ms | 14235,7 evt/s | 18,2% | 59,4% | 985 MiB |
| 8 | RT | 49 | 5 ms | 34 ms | 2555 ms | 5005 ms | 14824,9 evt/s | 13,1% | 51,3% | 950 MiB |
| 9 | NRT | 50 | 3762 ms | 6935 ms | 4401 ms | 7333 ms | 13590,8 evt/s | 17,0% | 56,8% | 1007 MiB |
| 9 | RT | 50 | 5 ms | 43 ms | 2557 ms | 5007 ms | 14697,2 evt/s | 12,5% | 48,9% | 963 MiB |
| **10** | NRT | 51 | 3675 ms | 6661 ms | 4492 ms | 7297 ms | 13935,9 evt/s | 19,1% | 56,9% | 1065 MiB |
| **10** | RT | 51 | 5 ms | 42 ms | 2566 ms | 5012 ms | 14112,1 evt/s | 13,0% | 47,3% | 1014 MiB |

Em negrito, as rodadas com eventos malformados.

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são informação: é o que cada motor fez com os eventos malformados.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **2** | NRT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |
| **2** | RT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |
| 3 | NRT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **5** | NRT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |
| **5** | RT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |
| 6 | NRT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **7** | NRT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |
| **7** | RT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |
| 8 | NRT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 900000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **10** | NRT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |
| **10** | RT | 900000 | 0 | 0 | 0 | 6750 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 431,3 | 146,4 |
| Tamanho (MiB) | 50,73 | 49,97 |
| Bytes por linha | 59,1 | 58,3 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 2575,2 ms | 2575,7 ms |
| NRT | processamento | 1063,4 ms | 1156,8 ms |
| NRT | escrita | 754,9 ms | 763,6 ms |
| RT | espera pelo checkpoint | 2562,8 ms | 2564,7 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
