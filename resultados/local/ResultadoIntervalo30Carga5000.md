# Resultado Local — Intervalo de 30 s, Carga de 5000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga5000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 5000 eventos por segundo |
| Duração de cada rodada | 300 s |
| Eventos | base real da REES46, outubro de 2019 (sha256 do .gz 8ebca1ad7412…), uma janela contínua de linhas por rodada |
| CPU | em porcentagem da máquina inteira (8 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | nenhuma rodada com eventos malformados |
| Período | 31/10/2026 18:01 a 31/10/2026 20:23 |
| Commit | `5e1ebb3` |
| Imagens | `tcc-spark:4.0.4` b06240c72ba2 · `tcc-flink:1.20.5` 88595865e2fb · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 17061,3 ms | 129,3 ms | 6,0 ms | 0,0 ms |
| Latência de processamento P95 | 30554,7 ms | 123,2 ms | 13,9 ms | 0,3 ms |
| Latência de processamento P99 | 31760,8 ms | 136,0 ms | 26,0 ms | 1,8 ms |
| Latência de processamento Média | 17057,6 ms | 129,3 ms | 6,4 ms | 0,5 ms |
| Latência de visibilidade P50 | 17979,1 ms | 148,6 ms | 2536,4 ms | 3,9 ms |
| Latência de visibilidade P95 | 31419,0 ms | 133,5 ms | 4786,1 ms | 4,0 ms |
| Latência de visibilidade P99 | 32636,8 ms | 162,2 ms | 4986,0 ms | 3,9 ms |
| Latência de visibilidade Média | 17963,4 ms | 145,1 ms | 2536,5 ms | 3,9 ms |
| Throughput médio | 4628,73 evt/s | 87,45 evt/s | 4955,80 evt/s | 28,12 evt/s |
| CPU média | 4,58% | 0,09% | 10,68% | 0,11% |
| CPU máximo | 60,18% | 1,43% | 50,09% | 4,06% |
| Memória média | 957,1 MiB | 29,6 MiB | 875,3 MiB | 18,4 MiB |
| Memória pico | 1067,8 MiB | 30,7 MiB | 917,5 MiB | 24,3 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 0,0 | 0,0 | 0,0 | 0,0 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |
| CPU da máquina média | 21,65% | 0,35% | 34,27% | 0,38% |
| CPU da máquina máximo | 74,71% | 3,08% | 70,47% | 5,61% |

## 3. Por rodada

| Rodada | Motor | Janela (linhas) | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | CPU máquina | Mem. pico |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 26.183.738–27.683.737 | 17116 ms | 31814 ms | 17971 ms | 32690 ms | 4827,8 evt/s | 4,7% | 59,8% | 22,1% | 1062 MiB |
| 1 | RT | 26.183.738–27.683.737 | 6 ms | 25 ms | 2534 ms | 4983 ms | 4928,1 evt/s | 10,6% | 43,5% | 34,3% | 906 MiB |
| 2 | NRT | 1.578.651–3.078.650 | 17051 ms | 31765 ms | 18039 ms | 32715 ms | 4623,1 evt/s | 4,6% | 62,3% | 22,1% | 1041 MiB |
| 2 | RT | 1.578.651–3.078.650 | 6 ms | 26 ms | 2534 ms | 4984 ms | 4931,1 evt/s | 10,8% | 55,5% | 34,5% | 865 MiB |
| 3 | NRT | 16.729.040–18.229.039 | 17381 ms | 32101 ms | 18312 ms | 33021 ms | 4543,3 evt/s | 4,6% | 58,5% | 21,9% | 1016 MiB |
| 3 | RT | 16.729.040–18.229.039 | 6 ms | 26 ms | 2533 ms | 4983 ms | 4989,8 evt/s | 10,5% | 44,9% | 34,7% | 923 MiB |
| 4 | NRT | 11.132.963–12.632.962 | 17116 ms | 31830 ms | 18050 ms | 32691 ms | 4591,2 evt/s | 4,7% | 59,9% | 21,5% | 1077 MiB |
| 4 | RT | 11.132.963–12.632.962 | 6 ms | 31 ms | 2545 ms | 4995 ms | 4988,9 evt/s | 10,7% | 49,3% | 34,8% | 937 MiB |
| 5 | NRT | 36.373.481–37.873.480 | 17027 ms | 31725 ms | 17970 ms | 32618 ms | 4593,6 evt/s | 4,6% | 58,6% | 21,6% | 1070 MiB |
| 5 | RT | 36.373.481–37.873.480 | 6 ms | 26 ms | 2541 ms | 4990 ms | 4912,8 evt/s | 10,8% | 50,4% | 33,7% | 920 MiB |
| 6 | NRT | 14.407.670–15.907.669 | 16924 ms | 31628 ms | 17819 ms | 32532 ms | 4726,9 evt/s | 4,6% | 62,0% | 21,6% | 1126 MiB |
| 6 | RT | 14.407.670–15.907.669 | 6 ms | 25 ms | 2536 ms | 4986 ms | 4990,8 evt/s | 10,6% | 53,1% | 34,1% | 909 MiB |
| 7 | NRT | 22.446.387–23.946.386 | 17064 ms | 31703 ms | 18052 ms | 32611 ms | 4598,6 evt/s | 4,6% | 61,3% | 21,0% | 1056 MiB |
| 7 | RT | 22.446.387–23.946.386 | 6 ms | 26 ms | 2537 ms | 4987 ms | 4960,3 evt/s | 10,6% | 50,0% | 34,2% | 906 MiB |
| 8 | NRT | 2.738.817–4.238.816 | 17006 ms | 31712 ms | 17915 ms | 32560 ms | 4538,8 evt/s | 4,5% | 59,9% | 21,8% | 1049 MiB |
| 8 | RT | 2.738.817–4.238.816 | 6 ms | 25 ms | 2537 ms | 4986 ms | 4937,3 evt/s | 10,7% | 56,0% | 34,0% | 915 MiB |
| 9 | NRT | 20.373.509–21.873.508 | 16956 ms | 31666 ms | 17847 ms | 32477 ms | 4606,5 evt/s | 4,5% | 61,2% | 21,1% | 1099 MiB |
| 9 | RT | 20.373.509–21.873.508 | 6 ms | 25 ms | 2533 ms | 4983 ms | 4962,5 evt/s | 10,8% | 50,3% | 34,6% | 952 MiB |
| 10 | NRT | 9.971.752–11.471.751 | 16972 ms | 31664 ms | 17816 ms | 32453 ms | 4637,5 evt/s | 4,5% | 58,5% | 21,8% | 1081 MiB |
| 10 | RT | 9.971.752–11.471.751 | 6 ms | 25 ms | 2534 ms | 4983 ms | 4956,4 evt/s | 10,7% | 47,8% | 33,8% | 942 MiB |

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são guarda: sem eventos malformados, o esperado é zero — salvo os 2 eventos da base sem sessão, que saem como inválidos se caírem na janela.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 5 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 5 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | NRT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | RT | 1500000 | 0 | 0 | 0 | 0 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 266,3 | 488,0 |
| Tamanho (MiB) | 58,15 | 64,69 |
| Bytes por linha | 40,6 | 45,2 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 15120,7 ms | 15119,7 ms |
| NRT | processamento | 1868,2 ms | 1938,1 ms |
| NRT | escrita | 910,9 ms | 905,7 ms |
| RT | espera pelo checkpoint | 2529,8 ms | 2529,8 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
