# Resultado — Intervalo de 5 s, Carga de 1000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo5Carga1000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 5 s; checkpoint do RT de 5 s |
| Carga | 1000 eventos por segundo |
| Duração de cada rodada | 60 s |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | rodadas 2, 5, 7 e 10: 1% de eventos malformados |
| Período | 12/10/2026 16:58 a 12/10/2026 17:49 |
| Commit | `48da2ce` |
| Imagens | `tcc-spark:4.0.4` ec241f18cf24 · `tcc-flink:1.20.5` 7d6d36c04457 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 3183,3 ms | 42,7 ms | 6,0 ms | 0,0 ms |
| Latência de processamento P95 | 5471,8 ms | 29,7 ms | 8,0 ms | 0,0 ms |
| Latência de processamento P99 | 6420,5 ms | 65,7 ms | 28,1 ms | 4,6 ms |
| Latência de processamento Média | 3188,1 ms | 24,2 ms | 6,5 ms | 0,5 ms |
| Latência de visibilidade P50 | 3718,4 ms | 36,1 ms | 2552,6 ms | 8,1 ms |
| Latência de visibilidade P95 | 6018,3 ms | 31,3 ms | 4801,1 ms | 7,4 ms |
| Latência de visibilidade P99 | 6878,1 ms | 127,4 ms | 5001,0 ms | 7,2 ms |
| Latência de visibilidade Média | 3722,5 ms | 26,5 ms | 2552,9 ms | 7,1 ms |
| Throughput médio | 929,23 evt/s | 21,30 evt/s | 959,94 evt/s | 23,16 evt/s |
| CPU média | 14,09% | 0,35% | 6,91% | 0,21% |
| CPU máximo | 65,43% | 3,32% | 25,85% | 2,23% |
| Memória média | 839,5 MiB | 17,9 MiB | 842,9 MiB | 33,6 MiB |
| Memória pico | 960,8 MiB | 23,2 MiB | 876,9 MiB | 36,9 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 180,0 | 232,4 | 180,0 | 232,4 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |

## 3. Por rodada

| Rodada | Motor | Semente | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 42 | 3141 ms | 6369 ms | 3707 ms | 6735 ms | 913,9 evt/s | 13,9% | 59,6% | 936 MiB |
| 1 | RT | 42 | 6 ms | 26 ms | 2556 ms | 5001 ms | 934,0 evt/s | 7,0% | 25,1% | 923 MiB |
| **2** | NRT | 43 | 3145 ms | 6404 ms | 3731 ms | 6880 ms | 894,3 evt/s | 13,9% | 68,0% | 986 MiB |
| **2** | RT | 43 | 6 ms | 25 ms | 2561 ms | 5005 ms | 927,2 evt/s | 7,1% | 21,8% | 913 MiB |
| 3 | NRT | 44 | 3257 ms | 6497 ms | 3772 ms | 6856 ms | 937,7 evt/s | 14,1% | 65,9% | 990 MiB |
| 3 | RT | 44 | 6 ms | 25 ms | 2552 ms | 5000 ms | 931,0 evt/s | 7,0% | 25,1% | 867 MiB |
| 4 | NRT | 45 | 3230 ms | 6340 ms | 3742 ms | 6686 ms | 949,8 evt/s | 14,1% | 71,7% | 963 MiB |
| 4 | RT | 45 | 6 ms | 27 ms | 2547 ms | 4997 ms | 986,4 evt/s | 6,7% | 25,6% | 865 MiB |
| **5** | NRT | 46 | 3189 ms | 6413 ms | 3714 ms | 6956 ms | 946,6 evt/s | 14,8% | 64,2% | 981 MiB |
| **5** | RT | 46 | 6 ms | 28 ms | 2560 ms | 5009 ms | 973,9 evt/s | 6,7% | 23,7% | 862 MiB |
| 6 | NRT | 47 | 3201 ms | 6462 ms | 3717 ms | 6875 ms | 952,9 evt/s | 14,2% | 62,0% | 959 MiB |
| 6 | RT | 47 | 6 ms | 22 ms | 2538 ms | 4988 ms | 964,8 evt/s | 6,6% | 25,0% | 822 MiB |
| **7** | NRT | 48 | 3171 ms | 6386 ms | 3701 ms | 6773 ms | 942,6 evt/s | 13,7% | 66,0% | 964 MiB |
| **7** | RT | 48 | 6 ms | 35 ms | 2557 ms | 5005 ms | 951,2 evt/s | 7,1% | 27,0% | 917 MiB |
| 8 | NRT | 49 | 3138 ms | 6359 ms | 3635 ms | 6890 ms | 914,2 evt/s | 13,7% | 67,5% | 922 MiB |
| 8 | RT | 49 | 6 ms | 35 ms | 2550 ms | 5000 ms | 982,5 evt/s | 6,7% | 29,2% | 820 MiB |
| 9 | NRT | 50 | 3219 ms | 6550 ms | 3745 ms | 7087 ms | 938,8 evt/s | 14,1% | 64,9% | 934 MiB |
| 9 | RT | 50 | 6 ms | 25 ms | 2543 ms | 4993 ms | 961,3 evt/s | 7,0% | 28,0% | 882 MiB |
| **10** | NRT | 51 | 3142 ms | 6425 ms | 3720 ms | 7043 ms | 901,4 evt/s | 14,5% | 64,6% | 972 MiB |
| **10** | RT | 51 | 6 ms | 33 ms | 2562 ms | 5012 ms | 987,1 evt/s | 7,1% | 28,0% | 899 MiB |

Em negrito, as rodadas com eventos malformados.

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são informação: é o que cada motor fez com os eventos malformados.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **2** | NRT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |
| **2** | RT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |
| 3 | NRT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **5** | NRT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |
| **5** | RT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |
| 6 | NRT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **7** | NRT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |
| **7** | RT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |
| 8 | NRT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 60000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **10** | NRT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |
| **10** | RT | 60000 | 0 | 0 | 0 | 450 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 337,8 | 125,2 |
| Tamanho (MiB) | 5,49 | 4,76 |
| Bytes por linha | 96,0 | 83,3 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 2579,2 ms | 2579,4 ms |
| NRT | processamento | 518,5 ms | 608,7 ms |
| NRT | escrita | 526,4 ms | 534,4 ms |
| RT | espera pelo checkpoint | 2546,4 ms | 2546,3 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
