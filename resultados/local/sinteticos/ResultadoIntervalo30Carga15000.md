# Resultado — Intervalo de 30 s, Carga de 15000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga15000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 15000 eventos por segundo |
| Duração de cada rodada | 300 s |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | rodadas 2, 5, 7 e 10: 1% de eventos malformados |
| Período | 13/10/2026 00:56 a 13/10/2026 03:15 |
| Commit | `5188b79` |
| Imagens | `tcc-spark:4.0.4` ec241f18cf24 · `tcc-flink:1.20.5` 7d6d36c04457 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 18650,6 ms | 70,6 ms | 5,1 ms | 0,3 ms |
| Latência de processamento P95 | 32003,9 ms | 76,8 ms | 13,4 ms | 0,5 ms |
| Latência de processamento P99 | 33260,8 ms | 68,2 ms | 32,3 ms | 4,3 ms |
| Latência de processamento Média | 18612,8 ms | 62,7 ms | 6,1 ms | 0,3 ms |
| Latência de visibilidade P50 | 20068,8 ms | 174,7 ms | 2549,9 ms | 9,6 ms |
| Latência de visibilidade P95 | 33343,2 ms | 164,9 ms | 4799,6 ms | 9,6 ms |
| Latência de visibilidade P99 | 34666,7 ms | 170,1 ms | 4999,6 ms | 9,6 ms |
| Latência de visibilidade Média | 20011,5 ms | 157,2 ms | 2550,0 ms | 9,5 ms |
| Throughput médio | 13793,98 evt/s | 262,57 evt/s | 14830,20 evt/s | 61,06 evt/s |
| CPU média | 6,81% | 0,21% | 12,58% | 0,55% |
| CPU máximo | 56,95% | 2,98% | 42,58% | 2,14% |
| Memória média | 1092,8 MiB | 39,3 MiB | 935,0 MiB | 32,3 MiB |
| Memória pico | 1210,5 MiB | 47,8 MiB | 956,1 MiB | 34,8 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 13500,0 | 17428,4 | 13500,0 | 17428,4 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |

## 3. Por rodada

| Rodada | Motor | Semente | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 42 | 18624 ms | 33249 ms | 19978 ms | 34587 ms | 13637,2 evt/s | 6,7% | 59,1% | 1194 MiB |
| 1 | RT | 42 | 5 ms | 34 ms | 2545 ms | 4995 ms | 14900,5 evt/s | 12,2% | 43,3% | 993 MiB |
| **2** | NRT | 43 | 18618 ms | 33209 ms | 20210 ms | 34772 ms | 13726,2 evt/s | 7,0% | 53,8% | 1209 MiB |
| **2** | RT | 43 | 5 ms | 38 ms | 2562 ms | 5011 ms | 14736,9 evt/s | 12,3% | 43,1% | 988 MiB |
| 3 | NRT | 44 | 18648 ms | 33271 ms | 20011 ms | 34620 ms | 13710,3 evt/s | 6,6% | 56,5% | 1120 MiB |
| 3 | RT | 44 | 5 ms | 27 ms | 2544 ms | 4993 ms | 14846,2 evt/s | 11,9% | 42,5% | 934 MiB |
| 4 | NRT | 45 | 18538 ms | 33237 ms | 19783 ms | 34482 ms | 14469,2 evt/s | 6,7% | 61,0% | 1275 MiB |
| 4 | RT | 45 | 5 ms | 30 ms | 2542 ms | 4991 ms | 14847,4 evt/s | 12,1% | 42,0% | 918 MiB |
| **5** | NRT | 46 | 18571 ms | 33195 ms | 20182 ms | 34784 ms | 13610,1 evt/s | 6,8% | 56,2% | 1191 MiB |
| **5** | RT | 46 | 5 ms | 37 ms | 2558 ms | 5008 ms | 14822,3 evt/s | 12,2% | 46,9% | 998 MiB |
| 6 | NRT | 47 | 18664 ms | 33215 ms | 19954 ms | 34489 ms | 13700,5 evt/s | 6,6% | 56,6% | 1233 MiB |
| 6 | RT | 47 | 5 ms | 27 ms | 2542 ms | 4992 ms | 14831,7 evt/s | 12,2% | 41,5% | 960 MiB |
| **7** | NRT | 48 | 18695 ms | 33293 ms | 20312 ms | 34898 ms | 13649,1 evt/s | 7,1% | 54,9% | 1232 MiB |
| **7** | RT | 48 | 5 ms | 35 ms | 2565 ms | 5015 ms | 14803,4 evt/s | 13,3% | 44,6% | 972 MiB |
| 8 | NRT | 49 | 18663 ms | 33254 ms | 19985 ms | 34600 ms | 14002,4 evt/s | 6,8% | 54,1% | 1264 MiB |
| 8 | RT | 49 | 5 ms | 29 ms | 2541 ms | 4991 ms | 14875,5 evt/s | 13,1% | 42,4% | 900 MiB |
| 9 | NRT | 50 | 18693 ms | 33249 ms | 19963 ms | 34496 ms | 13648,0 evt/s | 6,6% | 54,9% | 1232 MiB |
| 9 | RT | 50 | 5 ms | 29 ms | 2542 ms | 4992 ms | 14728,8 evt/s | 13,2% | 39,5% | 924 MiB |
| **10** | NRT | 51 | 18792 ms | 33436 ms | 20310 ms | 34939 ms | 13786,7 evt/s | 7,2% | 62,5% | 1154 MiB |
| **10** | RT | 51 | 6 ms | 37 ms | 2558 ms | 5008 ms | 14909,2 evt/s | 13,3% | 40,1% | 975 MiB |

Em negrito, as rodadas com eventos malformados.

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são informação: é o que cada motor fez com os eventos malformados.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **2** | NRT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |
| **2** | RT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |
| 3 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **5** | NRT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |
| **5** | RT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |
| 6 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **7** | NRT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |
| **7** | RT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |
| 8 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **10** | NRT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |
| **10** | RT | 4500000 | 0 | 0 | 0 | 33750 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 371,2 | 683,9 |
| Tamanho (MiB) | 240,87 | 249,61 |
| Bytes por linha | 56,2 | 58,2 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 15083,5 ms | 15083,3 ms |
| NRT | processamento | 3519,5 ms | 3529,6 ms |
| NRT | escrita | 1424,8 ms | 1398,7 ms |
| RT | espera pelo checkpoint | 2544,2 ms | 2544,0 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
