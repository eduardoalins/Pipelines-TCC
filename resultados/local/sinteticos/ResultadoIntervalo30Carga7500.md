# Resultado — Intervalo de 30 s, Carga de 7500 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga7500/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 7500 eventos por segundo |
| Duração de cada rodada | 300 s |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | rodadas 2, 5, 7 e 10: 1% de eventos malformados |
| Período | 12/10/2026 22:39 a 13/10/2026 00:56 |
| Commit | `5188b79` |
| Imagens | `tcc-spark:4.0.4` ec241f18cf24 · `tcc-flink:1.20.5` 7d6d36c04457 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 17335,8 ms | 70,9 ms | 6,0 ms | 0,0 ms |
| Latência de processamento P95 | 30824,4 ms | 80,3 ms | 15,0 ms | 0,0 ms |
| Latência de processamento P99 | 32007,0 ms | 95,3 ms | 27,8 ms | 3,0 ms |
| Latência de processamento Média | 17332,8 ms | 71,0 ms | 6,4 ms | 0,5 ms |
| Latência de visibilidade P50 | 18296,8 ms | 124,0 ms | 2542,8 ms | 7,3 ms |
| Latência de visibilidade P95 | 31756,4 ms | 159,7 ms | 4792,7 ms | 7,2 ms |
| Latência de visibilidade P99 | 32955,1 ms | 157,8 ms | 4992,6 ms | 7,2 ms |
| Latência de visibilidade Média | 18292,4 ms | 131,0 ms | 2542,8 ms | 7,1 ms |
| Throughput médio | 7028,79 evt/s | 284,41 evt/s | 7427,72 evt/s | 46,42 evt/s |
| CPU média | 4,93% | 0,24% | 10,62% | 0,09% |
| CPU máximo | 59,43% | 1,85% | 43,76% | 4,62% |
| Memória média | 973,8 MiB | 22,4 MiB | 901,5 MiB | 17,4 MiB |
| Memória pico | 1081,5 MiB | 33,3 MiB | 936,8 MiB | 20,1 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 6750,0 | 8714,2 | 6750,0 | 8714,2 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |

## 3. Por rodada

| Rodada | Motor | Semente | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 42 | 17300 ms | 31932 ms | 18175 ms | 32810 ms | 6729,0 evt/s | 4,8% | 58,1% | 1108 MiB |
| 1 | RT | 42 | 6 ms | 26 ms | 2536 ms | 4986 ms | 7463,0 evt/s | 10,7% | 40,2% | 933 MiB |
| **2** | NRT | 43 | 17308 ms | 32006 ms | 18361 ms | 33083 ms | 7344,7 evt/s | 5,2% | 60,5% | 1146 MiB |
| **2** | RT | 43 | 6 ms | 31 ms | 2548 ms | 4998 ms | 7460,9 evt/s | 10,7% | 45,8% | 953 MiB |
| 3 | NRT | 44 | 17269 ms | 31907 ms | 18145 ms | 32754 ms | 6761,5 evt/s | 4,6% | 63,2% | 1081 MiB |
| 3 | RT | 44 | 6 ms | 25 ms | 2538 ms | 4988 ms | 7499,6 evt/s | 10,6% | 47,2% | 915 MiB |
| 4 | NRT | 45 | 17321 ms | 32002 ms | 18198 ms | 32790 ms | 6921,1 evt/s | 4,7% | 58,9% | 1022 MiB |
| 4 | RT | 45 | 6 ms | 26 ms | 2536 ms | 4986 ms | 7434,8 evt/s | 10,5% | 46,5% | 970 MiB |
| **5** | NRT | 46 | 17368 ms | 32066 ms | 18479 ms | 33182 ms | 7331,8 evt/s | 5,2% | 60,2% | 1106 MiB |
| **5** | RT | 46 | 6 ms | 30 ms | 2547 ms | 4997 ms | 7371,8 evt/s | 10,6% | 38,8% | 940 MiB |
| 6 | NRT | 47 | 17510 ms | 32211 ms | 18359 ms | 33064 ms | 7285,8 evt/s | 5,1% | 57,9% | 1061 MiB |
| 6 | RT | 47 | 6 ms | 25 ms | 2536 ms | 4986 ms | 7478,6 evt/s | 10,5% | 50,3% | 920 MiB |
| **7** | NRT | 48 | 17298 ms | 31928 ms | 18412 ms | 33013 ms | 6703,1 evt/s | 4,9% | 56,5% | 1078 MiB |
| **7** | RT | 48 | 6 ms | 32 ms | 2555 ms | 5004 ms | 7397,7 evt/s | 10,8% | 38,3% | 965 MiB |
| 8 | NRT | 49 | 17339 ms | 32037 ms | 18209 ms | 32907 ms | 7258,4 evt/s | 5,0% | 60,7% | 1064 MiB |
| 8 | RT | 49 | 6 ms | 25 ms | 2541 ms | 4990 ms | 7389,0 evt/s | 10,8% | 45,3% | 934 MiB |
| 9 | NRT | 50 | 17271 ms | 31909 ms | 18197 ms | 32817 ms | 6718,3 evt/s | 4,6% | 58,6% | 1085 MiB |
| 9 | RT | 50 | 6 ms | 26 ms | 2538 ms | 4988 ms | 7368,6 evt/s | 10,6% | 37,5% | 914 MiB |
| **10** | NRT | 51 | 17374 ms | 32072 ms | 18433 ms | 33131 ms | 7234,2 evt/s | 5,2% | 59,6% | 1063 MiB |
| **10** | RT | 51 | 6 ms | 32 ms | 2553 ms | 5003 ms | 7413,2 evt/s | 10,6% | 47,8% | 924 MiB |

Em negrito, as rodadas com eventos malformados.

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são informação: é o que cada motor fez com os eventos malformados.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **2** | NRT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |
| **2** | RT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |
| 3 | NRT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **5** | NRT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |
| **5** | RT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |
| 6 | NRT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **7** | NRT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |
| **7** | RT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |
| 8 | NRT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 2250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **10** | NRT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |
| **10** | RT | 2250000 | 0 | 0 | 0 | 16875 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 365,1 | 681,5 |
| Tamanho (MiB) | 124,46 | 132,95 |
| Bytes por linha | 58,0 | 62,0 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 15084,6 ms | 15083,8 ms |
| NRT | processamento | 2197,1 ms | 2249,2 ms |
| NRT | escrita | 965,6 ms | 959,5 ms |
| RT | espera pelo checkpoint | 2536,0 ms | 2536,6 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
