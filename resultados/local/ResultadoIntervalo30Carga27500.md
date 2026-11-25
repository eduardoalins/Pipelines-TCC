# Resultado Local — Intervalo de 30 s, Carga de 27500 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga27500/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 27500 eventos por segundo |
| Duração de cada rodada | 300 s |
| Eventos | base real da REES46, outubro de 2019 (sha256 do .gz 8ebca1ad7412…), uma janela contínua de linhas por rodada |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | nenhuma rodada com eventos malformados |
| Período | 31/10/2026 15:30 a 31/10/2026 18:01 |
| Commit | `5e1ebb3` |
| Imagens | `tcc-spark:4.0.4` b06240c72ba2 · `tcc-flink:1.20.5` 88595865e2fb · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 21754,2 ms | 248,1 ms | 5,5 ms | 0,5 ms |
| Latência de processamento P95 | 35155,3 ms | 224,5 ms | 48,2 ms | 24,5 ms |
| Latência de processamento P99 | 36499,3 ms | 223,5 ms | 165,7 ms | 147,8 ms |
| Latência de processamento Média | 21788,0 ms | 216,8 ms | 12,9 ms | 5,6 ms |
| Latência de visibilidade P50 | 23995,8 ms | 348,4 ms | 2569,7 ms | 15,0 ms |
| Latência de visibilidade P95 | 37449,5 ms | 309,5 ms | 4819,1 ms | 15,0 ms |
| Latência de visibilidade P99 | 38821,5 ms | 316,4 ms | 5019,2 ms | 15,0 ms |
| Latência de visibilidade Média | 24060,0 ms | 298,4 ms | 2569,5 ms | 15,2 ms |
| Throughput médio | 25557,36 evt/s | 312,24 evt/s | 27273,40 evt/s | 125,80 evt/s |
| CPU média | 11,80% | 0,42% | 15,62% | 0,41% |
| CPU máximo | 58,02% | 1,59% | 51,52% | 2,67% |
| Memória média | 1337,5 MiB | 22,8 MiB | 969,5 MiB | 32,9 MiB |
| Memória pico | 1492,6 MiB | 26,4 MiB | 1018,2 MiB | 36,3 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 0,3 | 0,5 | 0,3 | 0,5 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |
| CPU da máquina média | 33,34% | 1,29% | 43,44% | 1,06% |
| CPU da máquina máximo | 81,59% | 2,08% | 77,67% | 4,24% |

## 3. Por rodada

| Rodada | Motor | Janela (linhas) | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | CPU máquina | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 21.867.607–30.117.606 | 21666 ms | 36524 ms | 23929 ms | 38873 ms | 25632,4 evt/s | 11,9% | 57,7% | 34,5% | 1502 MiB |
| 1 | RT | 21.867.607–30.117.606 | 6 ms | 566 ms | 2572 ms | 5022 ms | 27262,7 evt/s | 16,0% | 53,9% | 44,6% | 1042 MiB |
| 2 | NRT | 1.318.426–9.568.425 | 22112 ms | 36897 ms | 24458 ms | 39312 ms | 25943,6 evt/s | 12,5% | 56,1% | 35,3% | 1469 MiB |
| 2 | RT | 1.318.426–9.568.425 | 6 ms | 137 ms | 2601 ms | 5051 ms | 27443,5 evt/s | 15,9% | 48,3% | 44,6% | 972 MiB |
| 3 | NRT | 13.971.423–22.221.422 | 21899 ms | 36726 ms | 24157 ms | 39122 ms | 25769,6 evt/s | 12,4% | 59,3% | 35,3% | 1470 MiB |
| 3 | RT | 13.971.423–22.221.422 | 6 ms | 220 ms | 2561 ms | 5010 ms | 27231,0 evt/s | 16,3% | 47,6% | 44,6% | 1014 MiB |
| 4 | NRT | 9.297.804–17.547.803 | 22026 ms | 36652 ms | 24317 ms | 38962 ms | 25288,8 evt/s | 11,9% | 60,2% | 33,3% | 1496 MiB |
| 4 | RT | 9.297.804–17.547.803 | 6 ms | 173 ms | 2570 ms | 5019 ms | 27344,9 evt/s | 16,0% | 53,0% | 44,6% | 1089 MiB |
| 5 | NRT | 30.377.672–38.627.671 | 21513 ms | 36313 ms | 23707 ms | 38642 ms | 25833,5 evt/s | 11,6% | 61,0% | 32,8% | 1442 MiB |
| 5 | RT | 30.377.672–38.627.671 | 5 ms | 77 ms | 2559 ms | 5009 ms | 27243,8 evt/s | 15,8% | 52,8% | 43,1% | 956 MiB |
| 6 | NRT | 12.032.708–20.282.707 | 21590 ms | 36455 ms | 23721 ms | 38638 ms | 25585,1 evt/s | 11,4% | 57,1% | 32,0% | 1518 MiB |
| 6 | RT | 12.032.708–20.282.707 | 5 ms | 100 ms | 2559 ms | 5009 ms | 27357,2 evt/s | 15,1% | 52,5% | 42,3% | 1023 MiB |
| 7 | NRT | 18.746.321–26.996.320 | 21434 ms | 36213 ms | 23454 ms | 38302 ms | 25780,3 evt/s | 11,4% | 57,8% | 32,2% | 1492 MiB |
| 7 | RT | 18.746.321–26.996.320 | 5 ms | 93 ms | 2553 ms | 5002 ms | 27247,3 evt/s | 15,2% | 54,8% | 42,2% | 1026 MiB |
| 8 | NRT | 2.287.350–10.537.349 | 21713 ms | 36316 ms | 24119 ms | 38703 ms | 24916,3 evt/s | 11,4% | 57,1% | 32,0% | 1530 MiB |
| 8 | RT | 2.287.350–10.537.349 | 5 ms | 122 ms | 2568 ms | 5017 ms | 27123,6 evt/s | 15,4% | 50,0% | 43,0% | 1020 MiB |
| 9 | NRT | 17.015.137–25.265.136 | 21539 ms | 36277 ms | 23679 ms | 38516 ms | 25525,1 evt/s | 11,5% | 57,4% | 32,5% | 1515 MiB |
| 9 | RT | 17.015.137–25.265.136 | 6 ms | 83 ms | 2564 ms | 5014 ms | 27431,5 evt/s | 15,4% | 48,5% | 43,1% | 1011 MiB |
| 10 | NRT | 8.328.007–16.578.006 | 22050 ms | 36620 ms | 24417 ms | 39145 ms | 25299,0 evt/s | 12,1% | 56,7% | 33,6% | 1493 MiB |
| 10 | RT | 8.328.007–16.578.006 | 5 ms | 86 ms | 2590 ms | 5039 ms | 27048,7 evt/s | 15,3% | 53,9% | 42,2% | 1028 MiB |

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são guarda: sem eventos malformados, o esperado é zero — salvo os 2 eventos da base sem sessão, que saem como inválidos se caírem na janela.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | NRT | 8250000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 2 | RT | 8250000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 3 | NRT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 5 | NRT | 8250000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 5 | RT | 8250000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 6 | NRT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | NRT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | RT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | NRT | 8250000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 8 | RT | 8250000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 9 | NRT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | NRT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | RT | 8250000 | 0 | 0 | 0 | 0 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 265,9 | 488,0 |
| Tamanho (MiB) | 302,14 | 289,30 |
| Bytes por linha | 38,4 | 36,8 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 15091,4 ms | 15091,4 ms |
| NRT | processamento | 6689,4 ms | 6696,8 ms |
| NRT | escrita | 2293,0 ms | 2272,0 ms |
| RT | espera pelo checkpoint | 2557,1 ms | 2556,4 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Rodada 1 NRT: tentativa anterior guardada (`20261031T182530Z-spark-i30-c27500-r01`) — o pipeline caiu durante a geracao; o reconciliador saiu com codigo 1 (ver reconcile.txt).
- Rodada 10 NRT: tentativa anterior guardada (`20261031T204934Z-spark-i30-c27500-r10`) — o pipeline caiu durante a geracao; o reconciliador saiu com codigo 1 (ver reconcile.txt).

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
