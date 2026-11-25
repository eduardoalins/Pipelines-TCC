# Resultado — Intervalo de 5 s, Carga de 7500 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo5Carga7500/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 5 s; checkpoint do RT de 5 s |
| Carga | 7500 eventos por segundo |
| Duração de cada rodada | 60 s |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | rodadas 2, 5, 7 e 10: 1% de eventos malformados |
| Período | 12/10/2026 18:40 a 12/10/2026 19:32 |
| Commit | `5188b79` |
| Imagens | `tcc-spark:4.0.4` ec241f18cf24 · `tcc-flink:1.20.5` 7d6d36c04457 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 3395,1 ms | 45,4 ms | 6,0 ms | 0,0 ms |
| Latência de processamento P95 | 5699,9 ms | 42,7 ms | 15,5 ms | 0,8 ms |
| Latência de processamento P99 | 6488,4 ms | 122,5 ms | 38,2 ms | 10,7 ms |
| Latência de processamento Média | 3412,5 ms | 44,6 ms | 7,1 ms | 0,3 ms |
| Latência de visibilidade P50 | 4097,2 ms | 105,7 ms | 2554,6 ms | 11,8 ms |
| Latência de visibilidade P95 | 6375,7 ms | 108,6 ms | 4802,6 ms | 12,3 ms |
| Latência de visibilidade P99 | 6952,8 ms | 119,8 ms | 5002,6 ms | 12,3 ms |
| Latência de visibilidade Média | 4102,1 ms | 106,1 ms | 2554,6 ms | 11,9 ms |
| Throughput médio | 6785,62 evt/s | 100,28 evt/s | 7165,64 evt/s | 162,53 evt/s |
| CPU média | 16,40% | 0,92% | 12,18% | 0,20% |
| CPU máximo | 59,56% | 1,80% | 48,01% | 4,22% |
| Memória média | 867,7 MiB | 26,5 MiB | 893,1 MiB | 36,8 MiB |
| Memória pico | 998,6 MiB | 42,2 MiB | 918,1 MiB | 38,4 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 1350,0 | 1742,8 | 1350,0 | 1742,8 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |

## 3. Por rodada

| Rodada | Motor | Semente | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 42 | 3428 ms | 6450 ms | 4089 ms | 7061 ms | 6749,1 evt/s | 16,1% | 59,2% | 950 MiB |
| 1 | RT | 42 | 6 ms | 27 ms | 2550 ms | 4997 ms | 7017,1 evt/s | 12,0% | 42,0% | 930 MiB |
| **2** | NRT | 43 | 3387 ms | 6394 ms | 4182 ms | 6893 ms | 6828,2 evt/s | 17,2% | 58,4% | 1059 MiB |
| **2** | RT | 43 | 6 ms | 51 ms | 2569 ms | 5015 ms | 7069,9 evt/s | 12,4% | 50,1% | 957 MiB |
| 3 | NRT | 44 | 3333 ms | 6380 ms | 3969 ms | 6722 ms | 6760,8 evt/s | 15,9% | 59,1% | 1078 MiB |
| 3 | RT | 44 | 6 ms | 31 ms | 2555 ms | 4999 ms | 7021,1 evt/s | 12,1% | 50,2% | 899 MiB |
| 4 | NRT | 45 | 3449 ms | 6663 ms | 4018 ms | 7031 ms | 6941,1 evt/s | 15,8% | 63,7% | 1000 MiB |
| 4 | RT | 45 | 6 ms | 28 ms | 2542 ms | 4992 ms | 7220,6 evt/s | 12,0% | 49,8% | 856 MiB |
| **5** | NRT | 46 | 3317 ms | 6514 ms | 4238 ms | 6915 ms | 6754,6 evt/s | 17,6% | 59,1% | 955 MiB |
| **5** | RT | 46 | 6 ms | 40 ms | 2561 ms | 5011 ms | 7274,5 evt/s | 12,2% | 49,7% | 968 MiB |
| 6 | NRT | 47 | 3374 ms | 6517 ms | 3955 ms | 6849 ms | 6714,5 evt/s | 15,0% | 61,0% | 986 MiB |
| 6 | RT | 47 | 6 ms | 27 ms | 2548 ms | 4996 ms | 6943,0 evt/s | 12,3% | 56,2% | 869 MiB |
| **7** | NRT | 48 | 3427 ms | 6429 ms | 4240 ms | 7059 ms | 6730,7 evt/s | 17,8% | 58,2% | 1013 MiB |
| **7** | RT | 48 | 6 ms | 57 ms | 2576 ms | 5026 ms | 7297,2 evt/s | 12,1% | 44,0% | 932 MiB |
| 8 | NRT | 49 | 3387 ms | 6381 ms | 4085 ms | 6999 ms | 6726,7 evt/s | 15,8% | 60,8% | 978 MiB |
| 8 | RT | 49 | 6 ms | 47 ms | 2543 ms | 4993 ms | 7474,1 evt/s | 11,9% | 47,5% | 887 MiB |
| 9 | NRT | 50 | 3450 ms | 6736 ms | 4027 ms | 7110 ms | 6978,4 evt/s | 15,8% | 58,2% | 968 MiB |
| 9 | RT | 50 | 6 ms | 33 ms | 2542 ms | 4987 ms | 7103,4 evt/s | 12,3% | 43,0% | 930 MiB |
| **10** | NRT | 51 | 3399 ms | 6420 ms | 4169 ms | 6889 ms | 6672,2 evt/s | 17,1% | 57,9% | 999 MiB |
| **10** | RT | 51 | 6 ms | 41 ms | 2560 ms | 5010 ms | 7235,3 evt/s | 12,5% | 47,4% | 953 MiB |

Em negrito, as rodadas com eventos malformados.

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são informação: é o que cada motor fez com os eventos malformados.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **2** | NRT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |
| **2** | RT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |
| 3 | NRT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **5** | NRT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |
| **5** | RT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |
| 6 | NRT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **7** | NRT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |
| **7** | RT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |
| 8 | NRT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 450000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **10** | NRT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |
| **10** | RT | 450000 | 0 | 0 | 0 | 3375 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 414,1 | 145,3 |
| Tamanho (MiB) | 26,98 | 26,66 |
| Bytes por linha | 62,9 | 62,2 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 2581,4 ms | 2581,5 ms |
| NRT | processamento | 749,2 ms | 830,9 ms |
| NRT | escrita | 679,5 ms | 689,6 ms |
| RT | espera pelo checkpoint | 2548,1 ms | 2547,7 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
