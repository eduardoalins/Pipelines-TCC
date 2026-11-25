# Resultado AWS — Intervalo de 30 s, Carga de 25000 eventos por segundo

> Gerado por `benchmarks/write_results.py` a partir de `results/Intervalo30Carga25000/`. As seções 1 a 6 são reescritas a cada geração; a seção 7 é preservada.

## 1. Cabeçalho

| | |
|---|---|
| Intervalo | trigger do NRT de 30 s; checkpoint do RT de 5 s |
| Carga | 25000 eventos por segundo |
| Duração de cada rodada | 300 s |
| Eventos | base real da REES46, outubro de 2019 (sha256 do .gz 8ebca1ad7412…), uma janela contínua de linhas por rodada |
| CPU | em porcentagem da máquina inteira (2 núcleos): 100% são todos os núcleos ocupados |
| Rodadas válidas | NRT 10 de 10 · RT 10 de 10 |
| Falha induzida | nenhuma rodada com eventos malformados |
| Período | 13/11/2026 13:56 a 13/11/2026 16:46 |
| Commit | `8cff8dc` |
| Imagens | `tcc-spark:4.0.4` 6e5e4b96ec95 · `tcc-flink:1.20.5` 45538ad37c25 · `apache/kafka:4.0.2` 63077b1420ad |
| Desvio padrão | amostral (divide por n − 1) |
| Preços | m5.large US$ 0,096/h + gp3 US$ 0,08/GB-mês × 80 GB + IPv4 público US$ 0,005/h = US$ 0,1098/h (tabelas públicas da AWS, us-east-1: EC2/EBS publicada em 23/10/2026, VPC conferida em 24/10/2026) |

## 2. Resumo

Média e desvio padrão entre as rodadas válidas de cada motor, com e sem eventos malformados juntas.

| Variável | NRT média | NRT desvio padrão | RT média | RT desvio padrão |
|---|---:|---:|---:|---:|
| Latência de processamento P50 | 203867,9 ms | 11092,8 ms | 52503,1 ms | 5378,5 ms |
| Latência de processamento P95 | 280646,5 ms | 8299,6 ms | 72685,2 ms | 6903,3 ms |
| Latência de processamento P99 | 291153,4 ms | 8482,8 ms | 77058,6 ms | 6792,7 ms |
| Latência de processamento Média | 204124,5 ms | 5494,1 ms | 51569,2 ms | 5087,3 ms |
| Latência de visibilidade P50 | 231430,0 ms | 11658,3 ms | 55189,9 ms | 5316,0 ms |
| Latência de visibilidade P95 | 309356,9 ms | 12647,7 ms | 75522,9 ms | 6904,8 ms |
| Latência de visibilidade P99 | 318881,5 ms | 14152,6 ms | 80075,8 ms | 6799,9 ms |
| Latência de visibilidade Média | 232515,7 ms | 5391,6 ms | 54305,8 ms | 5107,9 ms |
| Throughput médio | 16409,28 evt/s | 155,22 evt/s | 22771,99 evt/s | 302,49 evt/s |
| CPU média | 41,30% | 0,46% | 29,44% | 0,31% |
| CPU máximo | 97,66% | 0,92% | 94,52% | 0,78% |
| Memória média | 990,8 MiB | 14,3 MiB | 747,8 MiB | 32,5 MiB |
| Memória pico | 1431,4 MiB | 5,2 MiB | 816,5 MiB | 27,2 MiB |
| Perdas | 0,0 | 0,0 | 0,0 | 0,0 |
| Duplicatas | 0,0 | 0,0 | 0,0 | 0,0 |
| Atrasados | 0,0 | 0,0 | 0,0 | 0,0 |
| Inválidas | 0,3 | 0,5 | 0,3 | 0,5 |
| Nulas | 0,0 | 0,0 | 0,0 | 0,0 |
| Descartadas | 0,0 | 0,0 | 0,0 | 0,0 |
| CPU da máquina média | 96,34% | 0,31% | 97,09% | 0,14% |
| CPU da máquina máximo | 101,46% | 0,99% | 106,35% | 6,34% |
| Custo da rodada | 0,01729 US$ | 0,00016 US$ | 0,01392 US$ | 0,00014 US$ |
| Custo da ingestão | 0,01349 US$ | 0,00013 US$ | 0,01020 US$ | 0,00011 US$ |
| Custo por milhão de eventos | 0,00230 US$ | 0,00002 US$ | 0,00186 US$ | 0,00002 US$ |

## 3. Por rodada

| Rodada | Motor | Janela (linhas) | Proc. P50 | Proc. P99 | Vis. P50 | Vis. P99 | Throughput | CPU média | CPU máx. | CPU máquina | Mem. pico | Custo rodada |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 22.347.177–29.847.176 | 198356 ms | 304471 ms | 220874 ms | 334882 ms | 16237,0 evt/s | 41,5% | 98,0% | 96,8% | 1423 MiB | 0,0176 US$ |
| 1 | RT | 22.347.177–29.847.176 | 61852 ms | 85105 ms | 64190 ms | 88940 ms | 22518,7 evt/s | 29,4% | 95,3% | 97,0% | 800 MiB | 0,0141 US$ |
| 2 | NRT | 1.347.340–8.847.339 | 198501 ms | 303471 ms | 240162 ms | 339692 ms | 16271,4 evt/s | 42,1% | 96,9% | 96,8% | 1436 MiB | 0,0173 US$ |
| 2 | RT | 1.347.340–8.847.339 | 62153 ms | 86481 ms | 64905 ms | 89077 ms | 22297,3 evt/s | 29,1% | 95,0% | 97,0% | 790 MiB | 0,0140 US$ |
| 3 | NRT | 14.277.825–21.777.824 | 178188 ms | 295453 ms | 201924 ms | 325021 ms | 16370,9 evt/s | 41,6% | 98,2% | 96,5% | 1425 MiB | 0,0174 US$ |
| 3 | RT | 14.277.825–21.777.824 | 48686 ms | 75746 ms | 51286 ms | 78925 ms | 22721,7 evt/s | 29,4% | 92,9% | 97,3% | 853 MiB | 0,0139 US$ |
| 4 | NRT | 9.501.710–17.001.709 | 210371 ms | 290319 ms | 234893 ms | 312139 ms | 16320,9 evt/s | 40,9% | 97,6% | 96,2% | 1432 MiB | 0,0173 US$ |
| 4 | RT | 9.501.710–17.001.709 | 52083 ms | 85542 ms | 54749 ms | 88434 ms | 22536,3 evt/s | 29,3% | 93,7% | 97,2% | 822 MiB | 0,0139 US$ |
| 5 | NRT | 31.043.873–38.543.872 | 210190 ms | 280151 ms | 233589 ms | 299723 ms | 16536,1 evt/s | 41,2% | 98,3% | 96,3% | 1436 MiB | 0,0173 US$ |
| 5 | RT | 31.043.873–38.543.872 | 47535 ms | 72427 ms | 50145 ms | 75237 ms | 22821,2 evt/s | 29,6% | 94,0% | 97,2% | 866 MiB | 0,0141 US$ |
| 6 | NRT | 12.296.593–19.796.592 | 210354 ms | 289947 ms | 239985 ms | 326211 ms | 16374,9 evt/s | 40,5% | 97,5% | 95,9% | 1430 MiB | 0,0173 US$ |
| 6 | RT | 12.296.593–19.796.592 | 46609 ms | 65706 ms | 49467 ms | 68894 ms | 23217,2 evt/s | 29,1% | 94,7% | 97,1% | 811 MiB | 0,0137 US$ |
| 7 | NRT | 19.157.440–26.657.439 | 212082 ms | 282679 ms | 235719 ms | 313600 ms | 16578,8 evt/s | 41,7% | 98,1% | 96,6% | 1437 MiB | 0,0171 US$ |
| 7 | RT | 19.157.440–26.657.439 | 51011 ms | 72888 ms | 53636 ms | 75876 ms | 23164,0 evt/s | 29,6% | 95,2% | 97,0% | 793 MiB | 0,0139 US$ |
| 8 | NRT | 2.337.513–9.837.512 | 211227 ms | 282570 ms | 234796 ms | 301236 ms | 16389,2 evt/s | 41,2% | 98,5% | 96,1% | 1436 MiB | 0,0172 US$ |
| 8 | RT | 2.337.513–9.837.512 | 51487 ms | 72557 ms | 54365 ms | 75700 ms | 23075,5 evt/s | 29,4% | 94,6% | 96,8% | 838 MiB | 0,0137 US$ |
| 9 | NRT | 17.388.289–24.888.288 | 213115 ms | 287278 ms | 236559 ms | 306551 ms | 16290,7 evt/s | 41,1% | 98,2% | 96,1% | 1434 MiB | 0,0174 US$ |
| 9 | RT | 17.388.289–24.888.288 | 52654 ms | 76900 ms | 55436 ms | 79740 ms | 22767,5 evt/s | 30,2% | 95,0% | 97,2% | 796 MiB | 0,0140 US$ |
| 10 | NRT | 8.510.646–16.010.645 | 196295 ms | 295195 ms | 235799 ms | 329760 ms | 16723,0 evt/s | 41,0% | 95,4% | 96,2% | 1425 MiB | 0,0170 US$ |
| 10 | RT | 8.510.646–16.010.645 | 50961 ms | 77234 ms | 53720 ms | 79935 ms | 22600,5 evt/s | 29,4% | 94,8% | 97,2% | 797 MiB | 0,0138 US$ |

## 4. Integridade

Perdas e duplicatas reprovam a rodada. Inválidas, nulas e descartadas são guarda: sem eventos malformados, o esperado é zero — salvo os 2 eventos da base sem sessão, que saem como inválidos se caírem na janela.

| Rodada | Motor | Mensagens no tópico | Perdas | Duplicatas | Atrasados | Inválidas | Nulas | Descartadas |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | NRT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 1 | RT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | NRT | 7500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 2 | RT | 7500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 3 | NRT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 3 | RT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | NRT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 4 | RT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 5 | NRT | 7500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 5 | RT | 7500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 6 | NRT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 6 | RT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | NRT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 7 | RT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 8 | NRT | 7500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 8 | RT | 7500000 | 0 | 0 | 0 | 1 | 0 | 0 |
| 9 | NRT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 9 | RT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | NRT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |
| 10 | RT | 7500000 | 0 | 0 | 0 | 0 | 0 | 0 |

## 5. Apoio

O que foi gravado no destino, em média por rodada válida:

| | NRT | RT |
|---|---:|---:|
| Arquivos | 84,0 | 132,1 |
| Tamanho (MiB) | 262,35 | 249,37 |
| Bytes por linha | 36,7 | 34,9 |

De onde vem o tempo — média, entre as rodadas válidas, do P50 e da média de cada parcela:

| Motor | Parcela | P50 | Média |
|---|---|---:|---:|
| NRT | espera no trigger | 85147,0 ms | 90061,2 ms |
| NRT | processamento | 94605,7 ms | 114063,3 ms |
| NRT | escrita | 30992,5 ms | 28391,1 ms |
| RT | espera pelo checkpoint | 2700,3 ms | 2736,6 ms |

No NRT a latência de visibilidade é, aproximadamente, espera no trigger + processamento + escrita. No RT é a latência de processamento + a espera pelo checkpoint.

## 6. Ocorrências

- Nenhuma.

<!-- LEITURA: daqui para baixo o texto e escrito a mao; benchmarks/write_results.py preserva o que estiver abaixo desta linha -->

## 7. Leitura

_Ainda não escrita._
