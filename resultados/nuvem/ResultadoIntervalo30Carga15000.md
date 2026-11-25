# Resultado AWS — Intervalo de 30 s, Carga de 15000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga15000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 15000 eventos por segundo |
| Duração de cada rodada | 300 s |
| Eventos | base real da REES46, outubro de 2019 (sha256 do .gz 8ebca1ad7412…), uma janela contínua de linhas por rodada |
| CPU | em porcentagem da máquina inteira (2 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | nenhuma rodada com eventos malformados |
| Período | 10/11/2026 16:51 a 10/11/2026 19:20 |
| Commit | `8cff8dc` |
| Imagens | `tcc-spark:4.0.4` 01d6b9834e10 · `tcc-flink:1.20.5` f068d5b75942 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |
| Preços | m5.large US$ 0,096/h + gp3 US$ 0,08/GB-mês × 80 GB + IPv4 público US$ 0,005/h = US$ 0,1098/h (tabelas públicas da AWS, us-east-1: EC2/EBS publicada em 23/10/2026, VPC conferida em 24/10/2026) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 113653,6 ms | 6553,4 ms | 1464,4 ms | 153,3 ms |
| Latência de processamento P95 | 178538,1 ms | 11088,4 ms | 4362,5 ms | 381,0 ms |
| Latência de processamento P99 | 189175,8 ms | 11349,5 ms | 5760,8 ms | 912,4 ms |
| Latência de processamento Média | 116430,3 ms | 5834,2 ms | 1726,1 ms | 113,5 ms |
| Latência de visibilidade P50 | 133287,6 ms | 5996,7 ms | 4465,2 ms | 116,6 ms |
| Latência de visibilidade P95 | 200168,3 ms | 8581,4 ms | 7920,4 ms | 324,0 ms |
| Latência de visibilidade P99 | 210436,5 ms | 9978,8 ms | 9437,3 ms | 826,3 ms |
| Latência de visibilidade Média | 134513,3 ms | 5284,3 ms | 4460,3 ms | 153,8 ms |
| Throughput médio | 12282,26 evt/s | 69,58 evt/s | 14809,78 evt/s | 138,59 evt/s |
| CPU média | 38,55% | 0,76% | 26,48% | 0,25% |
| CPU máximo | 97,77% | 0,63% | 58,93% | 4,06% |
| Memória média | 897,3 MiB | 19,3 MiB | 695,2 MiB | 20,5 MiB |
| Memória pico | 1238,2 MiB | 63,6 MiB | 716,6 MiB | 19,8 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 0,1 | 0,3 | 0,1 | 0,3 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |
| CPU da máquina média | 96,84% | 0,58% | 97,00% | 0,21% |
| CPU da máquina máximo | 101,06% | 0,69% | 102,09% | 1,94% |
| Custo da rodada | 0,01446 US$ | 0,00012 US$ | 0,01292 US$ | 0,00020 US$ |
| Custo da ingestão | 0,01099 US$ | 0,00006 US$ | 0,00952 US$ | 0,00003 US$ |
| Custo por milhão de eventos | 0,00321 US$ | 0,00003 US$ | 0,00287 US$ | 0,00004 US$ |

## 3. Por rodada

| Rodada | Motor | Janela (linhas) | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | CPU máquina | Mem. pico | Custo rodada |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 24.265.458–28.765.457 | 109273 ms | 181485 ms | 131311 ms | 212398 ms | 12278,6 evt/s | 38,5% | 97,6% | 97,7% | 1292 MiB | 0,0145 US$ |
| 1 | RT | 24.265.458–28.765.457 | 1746 ms | 5999 ms | 4648 ms | 10932 ms | 14832,8 evt/s | 26,2% | 53,6% | 96,8% | 712 MiB | 0,0132 US$ |
| 2 | NRT | 1.462.995–5.962.994 | 106479 ms | 177481 ms | 128937 ms | 206439 ms | 12346,6 evt/s | 39,1% | 99,3% | 97,0% | 1272 MiB | 0,0143 US$ |
| 2 | RT | 1.462.995–5.962.994 | 1535 ms | 7377 ms | 4571 ms | 10776 ms | 14888,4 evt/s | 26,4% | 56,1% | 97,2% | 694 MiB | 0,0126 US$ |
| 3 | NRT | 15.503.433–20.003.432 | 108156 ms | 179341 ms | 131438 ms | 210024 ms | 12246,8 evt/s | 38,5% | 97,2% | 96,5% | 1262 MiB | 0,0146 US$ |
| 3 | RT | 15.503.433–20.003.432 | 1465 ms | 5292 ms | 4525 ms | 8743 ms | 14801,4 evt/s | 26,5% | 58,4% | 96,8% | 742 MiB | 0,0130 US$ |
| 4 | NRT | 10.317.337–14.817.336 | 117789 ms | 197326 ms | 140119 ms | 221632 ms | 12222,5 evt/s | 37,9% | 97,4% | 96,3% | 1115 MiB | 0,0144 US$ |
| 4 | RT | 10.317.337–14.817.336 | 1468 ms | 7151 ms | 4446 ms | 9791 ms | 14987,5 evt/s | 26,2% | 55,6% | 97,0% | 698 MiB | 0,0127 US$ |
| 5 | NRT | 33.708.677–38.208.676 | 112180 ms | 185490 ms | 133908 ms | 216356 ms | 12365,5 evt/s | 38,5% | 98,0% | 97,0% | 1332 MiB | 0,0146 US$ |
| 5 | RT | 33.708.677–38.208.676 | 1486 ms | 6267 ms | 4512 ms | 9435 ms | 14561,0 evt/s | 26,6% | 60,5% | 96,8% | 750 MiB | 0,0133 US$ |
| 6 | NRT | 13.352.131–17.852.130 | 115134 ms | 192620 ms | 132744 ms | 205162 ms | 12279,2 evt/s | 39,9% | 98,1% | 97,6% | 1213 MiB | 0,0145 US$ |
| 6 | RT | 13.352.131–17.852.130 | 1252 ms | 5254 ms | 4244 ms | 9118 ms | 14642,2 evt/s | 26,4% | 54,0% | 97,0% | 732 MiB | 0,0129 US$ |
| 7 | NRT | 20.801.913–25.301.912 | 104041 ms | 172316 ms | 119984 ms | 186134 ms | 12412,3 evt/s | 39,5% | 97,6% | 97,1% | 1264 MiB | 0,0144 US$ |
| 7 | RT | 20.801.913–25.301.912 | 1535 ms | 5015 ms | 4435 ms | 8651 ms | 14901,8 evt/s | 26,2% | 64,3% | 97,1% | 718 MiB | 0,0130 US$ |
| 8 | NRT | 2.538.165–7.038.164 | 119946 ms | 201517 ms | 138474 ms | 216915 ms | 12240,7 evt/s | 37,6% | 97,2% | 96,5% | 1216 MiB | 0,0143 US$ |
| 8 | RT | 2.538.165–7.038.164 | 1198 ms | 4710 ms | 4321 ms | 8682 ms | 14674,5 evt/s | 26,6% | 60,6% | 96,9% | 708 MiB | 0,0128 US$ |
| 9 | NRT | 18.880.899–23.380.898 | 122243 ms | 203613 ms | 139006 ms | 217803 ms | 12218,5 evt/s | 38,1% | 97,5% | 97,0% | 1254 MiB | 0,0145 US$ |
| 9 | RT | 18.880.899–23.380.898 | 1429 ms | 5366 ms | 4494 ms | 9230 ms | 14903,5 evt/s | 27,0% | 65,2% | 97,4% | 693 MiB | 0,0129 US$ |
| 10 | NRT | 9.241.199–13.741.198 | 121295 ms | 200569 ms | 136955 ms | 211502 ms | 12211,8 evt/s | 37,9% | 97,8% | 95,8% | 1162 MiB | 0,0145 US$ |
| 10 | RT | 9.241.199–13.741.198 | 1530 ms | 5177 ms | 4456 ms | 9015 ms | 14904,5 evt/s | 26,7% | 60,9% | 97,2% | 718 MiB | 0,0128 US$ |

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
| Arquivos | 110,4 | 122,6 |
| Tamanho (MiB) | 164,96 | 146,27 |
| Bytes por linha | 38,4 | 34,1 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 48594,1 ms | 50903,9 ms |
| NRT | processamento | 71964,1 ms | 65526,3 ms |
| NRT | escrita | 17740,9 ms | 18083,2 ms |
| RT | espera pelo checkpoint | 2707,4 ms | 2734,2 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
