# Resultado AWS — Intervalo de 30 s, Carga de 5000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga5000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 5000 eventos por segundo |
| Duração de cada rodada | 300 s |
| Eventos | base real da REES46, outubro de 2019 (sha256 do .gz 8ebca1ad7412…), uma janela contínua de linhas por rodada |
| CPU | em porcentagem da máquina inteira (2 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | nenhuma rodada com eventos malformados |
| Período | 03/11/2026 21:21 a 03/11/2026 23:42 |
| Commit | `8cff8dc` |
| Imagens | `tcc-spark:4.0.4` 3f13e2ebaa0b · `tcc-flink:1.20.5` be57e9fcf2d6 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |
| Preços | m5.large US$ 0,096/h + gp3 US$ 0,08/GB-mês × 80 GB + IPv4 público US$ 0,005/h = US$ 0,1098/h (tabelas públicas da AWS, us-east-1: EC2/EBS publicada em 23/10/2026, VPC conferida em 24/10/2026) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 27099,8 ms | 659,0 ms | 774,7 ms | 608,1 ms |
| Latência de processamento P95 | 41188,2 ms | 1641,7 ms | 3110,9 ms | 1349,5 ms |
| Latência de processamento P99 | 45955,5 ms | 3994,6 ms | 3556,4 ms | 1461,8 ms |
| Latência de processamento Média | 27032,4 ms | 674,7 ms | 1069,2 ms | 570,5 ms |
| Latência de visibilidade P50 | 33379,8 ms | 310,0 ms | 3714,2 ms | 529,7 ms |
| Latência de visibilidade P95 | 48344,7 ms | 2943,9 ms | 6779,4 ms | 1142,2 ms |
| Latência de visibilidade P99 | 54542,8 ms | 5655,8 ms | 7778,4 ms | 1405,0 ms |
| Latência de visibilidade Média | 33421,3 ms | 373,3 ms | 3729,6 ms | 595,7 ms |
| Throughput médio | 4572,38 evt/s | 96,69 evt/s | 4934,24 evt/s | 27,60 evt/s |
| CPU média | 30,03% | 0,67% | 25,23% | 0,40% |
| CPU máximo | 83,62% | 12,64% | 54,86% | 2,31% |
| Memória média | 718,7 MiB | 14,5 MiB | 646,5 MiB | 49,0 MiB |
| Memória pico | 798,3 MiB | 19,1 MiB | 660,0 MiB | 49,1 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 0,0 | 0,0 | 0,0 | 0,0 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |
| CPU da máquina média | 83,98% | 2,09% | 96,16% | 0,34% |
| CPU da máquina máximo | 100,86% | 0,10% | 100,94% | 0,27% |
| Custo da rodada | 0,01310 US$ | 0,00024 US$ | 0,01273 US$ | 0,00027 US$ |
| Custo da ingestão | 0,00994 US$ | 0,00019 US$ | 0,00950 US$ | 0,00003 US$ |
| Custo por milhão de eventos | 0,00873 US$ | 0,00016 US$ | 0,00848 US$ | 0,00018 US$ |

## 3. Por rodada

| Rodada | Motor | Janela (linhas) | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | CPU máquina | Mem. pico | Custo rodada |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 26.183.738–27.683.737 | 27590 ms | 44587 ms | 33849 ms | 51392 ms | 4557,2 evt/s | 29,9% | 71,9% | 85,2% | 811 MiB | 0,0132 US$ |
| 1 | RT | 26.183.738–27.683.737 | 618 ms | 2805 ms | 3546 ms | 7089 ms | 4967,7 evt/s | 24,6% | 50,9% | 96,3% | 719 MiB | 0,0127 US$ |
| 2 | NRT | 1.578.651–3.078.650 | 27916 ms | 43855 ms | 33614 ms | 51016 ms | 4541,1 evt/s | 30,0% | 82,2% | 82,3% | 835 MiB | 0,0131 US$ |
| 2 | RT | 1.578.651–3.078.650 | 2454 ms | 7695 ms | 5188 ms | 11744 ms | 4945,9 evt/s | 24,5% | 56,0% | 96,3% | 631 MiB | 0,0126 US$ |
| 3 | NRT | 16.729.040–18.229.039 | 26404 ms | 42734 ms | 33050 ms | 50301 ms | 4657,4 evt/s | 30,7% | 96,2% | 84,2% | 793 MiB | 0,0128 US$ |
| 3 | RT | 16.729.040–18.229.039 | 470 ms | 3242 ms | 3437 ms | 7465 ms | 4987,7 evt/s | 25,1% | 56,6% | 96,2% | 623 MiB | 0,0127 US$ |
| 4 | NRT | 11.132.963–12.632.962 | 27181 ms | 47775 ms | 33604 ms | 58194 ms | 4494,3 evt/s | 29,4% | 96,5% | 83,7% | 768 MiB | 0,0132 US$ |
| 4 | RT | 11.132.963–12.632.962 | 577 ms | 3016 ms | 3522 ms | 7502 ms | 4911,6 evt/s | 25,5% | 59,4% | 96,0% | 619 MiB | 0,0125 US$ |
| 5 | NRT | 36.373.481–37.873.480 | 28114 ms | 55549 ms | 33583 ms | 66612 ms | 4465,5 evt/s | 29,8% | 94,2% | 81,7% | 805 MiB | 0,0136 US$ |
| 5 | RT | 36.373.481–37.873.480 | 562 ms | 3136 ms | 3568 ms | 7333 ms | 4920,9 evt/s | 25,1% | 53,5% | 95,7% | 644 MiB | 0,0134 US$ |
| 6 | NRT | 14.407.670–15.907.669 | 26559 ms | 48847 ms | 33517 ms | 60290 ms | 4485,3 evt/s | 29,8% | 80,8% | 81,7% | 804 MiB | 0,0132 US$ |
| 6 | RT | 14.407.670–15.907.669 | 504 ms | 2982 ms | 3485 ms | 7109 ms | 4906,9 evt/s | 25,5% | 54,6% | 96,2% | 750 MiB | 0,0127 US$ |
| 7 | NRT | 22.446.387–23.946.386 | 26399 ms | 45621 ms | 33055 ms | 55474 ms | 4732,5 evt/s | 31,3% | 89,5% | 87,7% | 783 MiB | 0,0129 US$ |
| 7 | RT | 22.446.387–23.946.386 | 958 ms | 3248 ms | 3761 ms | 7633 ms | 4910,6 evt/s | 25,1% | 54,2% | 96,7% | 652 MiB | 0,0128 US$ |
| 8 | NRT | 2.738.817–4.238.816 | 27352 ms | 45328 ms | 33245 ms | 52549 ms | 4517,6 evt/s | 29,2% | 65,1% | 83,0% | 807 MiB | 0,0131 US$ |
| 8 | RT | 2.738.817–4.238.816 | 405 ms | 3009 ms | 3376 ms | 7184 ms | 4951,6 evt/s | 25,6% | 55,8% | 95,6% | 626 MiB | 0,0125 US$ |
| 9 | NRT | 20.373.509–21.873.508 | 26306 ms | 43181 ms | 32883 ms | 51485 ms | 4719,3 evt/s | 30,8% | 94,9% | 87,0% | 777 MiB | 0,0128 US$ |
| 9 | RT | 20.373.509–21.873.508 | 624 ms | 3132 ms | 3686 ms | 7471 ms | 4916,3 evt/s | 25,6% | 52,9% | 96,4% | 716 MiB | 0,0127 US$ |
| 10 | NRT | 9.971.752–11.471.751 | 27177 ms | 42078 ms | 33398 ms | 48115 ms | 4553,6 evt/s | 29,4% | 65,0% | 83,5% | 798 MiB | 0,0130 US$ |
| 10 | RT | 9.971.752–11.471.751 | 575 ms | 3299 ms | 3573 ms | 7254 ms | 4923,2 evt/s | 25,6% | 55,0% | 96,1% | 619 MiB | 0,0126 US$ |

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
| Arquivos | 264,0 | 122,6 |
| Tamanho (MiB) | 58,61 | 54,65 |
| Bytes por linha | 41,0 | 38,2 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 15420,2 ms | 15377,1 ms |
| NRT | processamento | 11153,6 ms | 11655,2 ms |
| NRT | escrita | 6234,3 ms | 6389,1 ms |
| RT | espera pelo checkpoint | 2651,6 ms | 2660,6 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
