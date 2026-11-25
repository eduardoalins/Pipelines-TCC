# Resultado Local — Intervalo de 30 s, Carga de 15000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga15000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 15000 eventos por segundo |
| Duração de cada rodada | 300 s |
| Eventos | base real da REES46, outubro de 2019 (sha256 do .gz 8ebca1ad7412…), uma janela contínua de linhas por rodada |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | nenhuma rodada com eventos malformados |
| Período | 01/11/2026 11:53 a 01/11/2026 14:14 |
| Commit | `8cff8dc` |
| Imagens | `tcc-spark:4.0.4` b06240c72ba2 · `tcc-flink:1.20.5` 88595865e2fb · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 19229,7 ms | 123,7 ms | 5,5 ms | 0,5 ms |
| Latência de processamento P95 | 32586,8 ms | 173,9 ms | 15,1 ms | 0,6 ms |
| Latência de processamento P99 | 33852,4 ms | 147,5 ms | 36,9 ms | 6,7 ms |
| Latência de processamento Média | 19195,3 ms | 122,6 ms | 6,7 ms | 0,5 ms |
| Latência de visibilidade P50 | 20815,4 ms | 156,7 ms | 2545,1 ms | 4,5 ms |
| Latência de visibilidade P95 | 34126,4 ms | 199,4 ms | 4794,9 ms | 4,7 ms |
| Latência de visibilidade P99 | 35437,3 ms | 174,2 ms | 4994,9 ms | 4,7 ms |
| Latência de visibilidade Média | 20764,5 ms | 144,9 ms | 2545,2 ms | 4,6 ms |
| Throughput médio | 14012,80 evt/s | 364,71 evt/s | 14843,00 evt/s | 89,34 evt/s |
| CPU média | 7,65% | 0,20% | 13,68% | 0,49% |
| CPU máximo | 57,97% | 3,01% | 53,92% | 2,88% |
| Memória média | 1162,6 MiB | 27,8 MiB | 932,1 MiB | 22,4 MiB |
| Memória pico | 1290,0 MiB | 37,3 MiB | 966,6 MiB | 36,8 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 0,1 | 0,3 | 0,1 | 0,3 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |
| CPU da máquina média | 27,98% | 0,66% | 40,75% | 1,28% |
| CPU da máquina máximo | 78,75% | 2,73% | 78,16% | 2,74% |

## 3. Por rodada

| Rodada | Motor | Janela (linhas) | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | CPU máquina | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 24.265.458–28.765.457 | 19028 ms | 33737 ms | 20547 ms | 35245 ms | 14299,7 evt/s | 7,6% | 61,3% | 27,8% | 1337 MiB |
| 1 | RT | 24.265.458–28.765.457 | 5 ms | 34 ms | 2545 ms | 4995 ms | 14921,2 evt/s | 13,5% | 56,5% | 40,6% | 1017 MiB |
| 2 | NRT | 1.462.995–5.962.994 | 19397 ms | 34102 ms | 21015 ms | 35740 ms | 14583,3 evt/s | 8,1% | 62,0% | 28,3% | 1262 MiB |
| 2 | RT | 1.462.995–5.962.994 | 5 ms | 53 ms | 2545 ms | 4994 ms | 14817,6 evt/s | 13,3% | 56,6% | 39,5% | 935 MiB |
| 3 | NRT | 15.503.433–20.003.432 | 19080 ms | 33603 ms | 20637 ms | 35142 ms | 13803,4 evt/s | 7,5% | 56,0% | 26,7% | 1302 MiB |
| 3 | RT | 15.503.433–20.003.432 | 5 ms | 32 ms | 2543 ms | 4993 ms | 14983,4 evt/s | 13,2% | 53,5% | 39,4% | 971 MiB |
| 4 | NRT | 10.317.337–14.817.336 | 19308 ms | 34040 ms | 20867 ms | 35599 ms | 14616,2 evt/s | 7,9% | 55,7% | 28,5% | 1318 MiB |
| 4 | RT | 10.317.337–14.817.336 | 5 ms | 35 ms | 2544 ms | 4993 ms | 14852,7 evt/s | 13,1% | 48,5% | 38,9% | 1011 MiB |
| 5 | NRT | 33.708.677–38.208.676 | 19152 ms | 33839 ms | 20634 ms | 35396 ms | 14034,8 evt/s | 7,5% | 52,5% | 27,0% | 1266 MiB |
| 5 | RT | 33.708.677–38.208.676 | 5 ms | 32 ms | 2542 ms | 4992 ms | 14975,3 evt/s | 13,2% | 52,9% | 39,6% | 996 MiB |
| 6 | NRT | 13.352.131–17.852.130 | 19243 ms | 33777 ms | 20832 ms | 35331 ms | 13613,3 evt/s | 7,5% | 59,4% | 28,1% | 1358 MiB |
| 6 | RT | 13.352.131–17.852.130 | 6 ms | 36 ms | 2541 ms | 4992 ms | 14744,3 evt/s | 14,0% | 58,5% | 41,6% | 950 MiB |
| 7 | NRT | 20.801.913–25.301.912 | 19167 ms | 33765 ms | 20850 ms | 35426 ms | 13689,4 evt/s | 7,5% | 58,0% | 28,1% | 1286 MiB |
| 7 | RT | 20.801.913–25.301.912 | 6 ms | 32 ms | 2539 ms | 4988 ms | 14736,4 evt/s | 13,9% | 52,2% | 41,9% | 976 MiB |
| 8 | NRT | 2.538.165–7.038.164 | 19271 ms | 33845 ms | 20891 ms | 35445 ms | 13756,4 evt/s | 7,6% | 57,8% | 28,2% | 1258 MiB |
| 8 | RT | 2.538.165–7.038.164 | 6 ms | 37 ms | 2554 ms | 5005 ms | 14777,8 evt/s | 14,2% | 53,9% | 41,8% | 917 MiB |
| 9 | NRT | 18.880.899–23.380.898 | 19254 ms | 33869 ms | 20901 ms | 35488 ms | 13803,5 evt/s | 7,6% | 55,9% | 28,7% | 1251 MiB |
| 9 | RT | 18.880.899–23.380.898 | 6 ms | 34 ms | 2549 ms | 4999 ms | 14798,0 evt/s | 14,3% | 51,7% | 41,8% | 979 MiB |
| 10 | NRT | 9.241.199–13.741.198 | 19397 ms | 33947 ms | 20980 ms | 35561 ms | 13928,1 evt/s | 7,7% | 61,0% | 28,4% | 1261 MiB |
| 10 | RT | 9.241.199–13.741.198 | 6 ms | 44 ms | 2549 ms | 4998 ms | 14823,4 evt/s | 14,2% | 55,0% | 42,4% | 913 MiB |

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são guarda: sem eventos malformados, o esperado é zero — salvo os 2 eventos da base sem sessão, que saem como inválidos se caírem na janela.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 5 | NRT | 4500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 5 | RT | 4500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 6 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | NRT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | RT | 4500000 | 0 | 0 | 0 | 0 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 264,0 | 488,8 |
| Tamanho (MiB) | 170,51 | 168,28 |
| Bytes por linha | 39,7 | 39,2 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 15092,5 ms | 15090,3 ms |
| NRT | processamento | 4057,8 ms | 4105,1 ms |
| NRT | escrita | 1587,2 ms | 1569,2 ms |
| RT | espera pelo checkpoint | 2538,8 ms | 2538,5 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
