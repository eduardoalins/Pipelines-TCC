# Resultado — Intervalo de 30 s, Carga de 1000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga1000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 1000 eventos por segundo |
| Duração de cada rodada | 300 s |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | rodadas 2, 5, 7 e 10: 1% de eventos malformados |
| Período | 12/10/2026 20:23 a 12/10/2026 22:39 |
| Commit | `5188b79` |
| Imagens | `tcc-spark:4.0.4` ec241f18cf24 · `tcc-flink:1.20.5` 7d6d36c04457 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 15874,1 ms | 73,5 ms | 6,0 ms | 0,0 ms |
| Latência de processamento P95 | 29425,0 ms | 49,4 ms | 8,0 ms | 0,0 ms |
| Latência de processamento P99 | 30627,9 ms | 50,0 ms | 20,2 ms | 2,6 ms |
| Latência de processamento Média | 15878,8 ms | 54,9 ms | 6,1 ms | 0,3 ms |
| Latência de visibilidade P50 | 16478,0 ms | 107,9 ms | 2543,0 ms | 7,8 ms |
| Latência de visibilidade P95 | 30009,2 ms | 95,7 ms | 4792,7 ms | 7,6 ms |
| Latência de visibilidade P99 | 31217,8 ms | 100,4 ms | 4992,6 ms | 7,8 ms |
| Latência de visibilidade Média | 16480,5 ms | 99,5 ms | 2543,0 ms | 7,9 ms |
| Throughput médio | 934,87 evt/s | 30,33 evt/s | 988,97 evt/s | 5,32 evt/s |
| CPU média | 2,99% | 0,20% | 5,69% | 0,18% |
| CPU máximo | 66,34% | 1,60% | 27,17% | 2,43% |
| Memória média | 857,7 MiB | 15,1 MiB | 904,0 MiB | 29,2 MiB |
| Memória pico | 969,7 MiB | 29,4 MiB | 937,2 MiB | 33,8 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 900,0 | 1161,9 | 900,0 | 1161,9 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |

## 3. Por rodada

| Rodada | Motor | Semente | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 42 | 15849 ms | 30651 ms | 16391 ms | 31154 ms | 913,1 evt/s | 2,8% | 65,4% | 974 MiB |
| 1 | RT | 42 | 6 ms | 18 ms | 2537 ms | 4987 ms | 992,4 evt/s | 5,6% | 25,6% | 913 MiB |
| **2** | NRT | 43 | 15842 ms | 30653 ms | 16549 ms | 31308 ms | 913,9 evt/s | 3,1% | 65,7% | 995 MiB |
| **2** | RT | 43 | 6 ms | 24 ms | 2551 ms | 5000 ms | 984,8 evt/s | 5,8% | 26,2% | 1001 MiB |
| 3 | NRT | 44 | 16009 ms | 30707 ms | 16502 ms | 31202 ms | 971,9 evt/s | 3,0% | 66,5% | 931 MiB |
| 3 | RT | 44 | 6 ms | 21 ms | 2541 ms | 4991 ms | 997,7 evt/s | 5,7% | 26,8% | 886 MiB |
| 4 | NRT | 45 | 15821 ms | 30519 ms | 16374 ms | 31072 ms | 895,3 evt/s | 2,7% | 66,8% | 949 MiB |
| 4 | RT | 45 | 6 ms | 16 ms | 2534 ms | 4984 ms | 991,9 evt/s | 5,6% | 32,5% | 929 MiB |
| **5** | NRT | 46 | 15912 ms | 30613 ms | 16610 ms | 31337 ms | 967,7 evt/s | 3,3% | 65,3% | 972 MiB |
| **5** | RT | 46 | 6 ms | 22 ms | 2550 ms | 5000 ms | 992,5 evt/s | 5,8% | 24,6% | 956 MiB |
| 6 | NRT | 47 | 15814 ms | 30633 ms | 16347 ms | 31155 ms | 912,2 evt/s | 2,9% | 66,2% | 944 MiB |
| 6 | RT | 47 | 6 ms | 18 ms | 2529 ms | 4978 ms | 982,2 evt/s | 5,6% | 28,5% | 902 MiB |
| **7** | NRT | 48 | 15947 ms | 30662 ms | 16645 ms | 31355 ms | 965,6 evt/s | 3,3% | 68,7% | 976 MiB |
| **7** | RT | 48 | 6 ms | 22 ms | 2545 ms | 4995 ms | 990,3 evt/s | 5,3% | 25,5% | 961 MiB |
| 8 | NRT | 49 | 15821 ms | 30607 ms | 16357 ms | 31117 ms | 929,4 evt/s | 2,9% | 69,2% | 954 MiB |
| 8 | RT | 49 | 6 ms | 20 ms | 2550 ms | 4999 ms | 989,3 evt/s | 5,8% | 29,5% | 927 MiB |
| 9 | NRT | 50 | 15942 ms | 30641 ms | 16467 ms | 31172 ms | 969,8 evt/s | 2,9% | 63,9% | 966 MiB |
| 9 | RT | 50 | 6 ms | 18 ms | 2542 ms | 4991 ms | 988,6 evt/s | 5,7% | 27,4% | 934 MiB |
| **10** | NRT | 51 | 15784 ms | 30593 ms | 16538 ms | 31306 ms | 909,7 evt/s | 3,1% | 65,5% | 1035 MiB |
| **10** | RT | 51 | 6 ms | 23 ms | 2551 ms | 5001 ms | 980,0 evt/s | 6,0% | 25,2% | 962 MiB |

Em negrito, as rodadas com eventos malformados.

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são informação: é o que cada motor fez com os eventos malformados.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **2** | NRT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |
| **2** | RT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |
| 3 | NRT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **5** | NRT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |
| **5** | RT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |
| 6 | NRT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **7** | NRT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |
| **7** | RT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |
| 8 | NRT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 300000 | 0 | 0 | 0 | 0 | 0 | 0 |
| **10** | NRT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |
| **10** | RT | 300000 | 0 | 0 | 0 | 2250 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 340,6 | 597,1 |
| Tamanho (MiB) | 18,38 | 23,64 |
| Bytes por linha | 64,3 | 82,7 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 15086,4 ms | 15086,1 ms |
| NRT | processamento | 703,4 ms | 792,8 ms |
| NRT | escrita | 579,2 ms | 601,9 ms |
| RT | espera pelo checkpoint | 2536,8 ms | 2536,6 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
