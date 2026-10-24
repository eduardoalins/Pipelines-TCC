"""
Script de rodada — Etapa 8 (= RT-6). Executa UMA rodada de UM motor, do comeco ao
fim, e deixa registrado o que rodou, com qual codigo e em que condicoes.

    MOTOR=spark INTERVALO=5 CARGA=1000 DURACAO=600 RODADA=2 \\
        ~/.venvs/tcc/bin/python benchmarks/run_round.py

    MOTOR      spark | flink
    INTERVALO  segundos — o trigger do SPARK. E tambem o nome da combinacao. O
               checkpoint do Flink e FIXO em 5 s, seja qual for o INTERVALO
    CARGA      mensagens por segundo
    DURACAO    segundos de geracao
    RODADA     1 a 10

E o que o plano chama de "arnes": o que antes eram tres abas e nove passos feitos
a mao, na ordem certa, vira um comando. O valor dele nao e a comodidade — e que
ele nao esquece de zerar o checkpoint nem de conferir o relogio.

--- O que se varia (decisao de 12/10/2026) -----------------------------------
  intervalo    so o do Spark varia: 5 s e 30 s. O Flink grava a cada checkpoint,
               e o intervalo dele fica em 5 s nas duas combinacoes. Na combinacao
               de intervalo 30 o Flink roda de novo, ao lado do Spark, com a
               mesma carga e a mesma duracao.
  carga        1.000, 7.500 e 15.000 mensagens por segundo
  duracao      60 s nas combinacoes de intervalo 5; 300 s nas de intervalo 30 —
               com trigger de 30 s, 60 s deixariam UM lote depois do descarte de
               aquecimento

--- O que a RODADA decide (CONTRATO.md 3.0, decisoes de 23/10/2026) ----------
  janela       os eventos vem da base real (CONTRATO.md 1.5), pelo replayer:
               uma janela continua de CARGA x DURACAO linhas, com o inicio
               sorteado pela seed 41 + RODADA (a rodada 1 usa 42, a 10 usa 51).
               A rodada N do Spark e a do Flink recebem a mesma janela.
  sem malformados desde o contrato 3.0. Ate a 2.2 as rodadas 2, 5, 7 e 10
               levavam 1% de mensagens malformadas, do gerador sintetico.

--- Os passos -----------------------------------------------------------------
   1. conferencias: disco, relogio, Kafka, nenhum pipeline esquecido, imagens
   2. zera o checkpoint do motor
   3. RECRIA o topico: toda rodada comeca com ele vazio — sem pendencia do outro
      motor, sem posicao antiga do grupo, sem sobra de rodada anterior
   4. anota os offsets do topico (antes)
   5. sobe o pipeline e espera ficar pronto
   6. liga o coletor: lag, CPU e memoria, uma amostra por segundo
   7. roda o replayer: posiciona na janela, depois envia na taxa pedida
   8. espera a fila zerar — replayer terminado E lag zero —, por ate DURACAO
      segundos; se o pipeline cair no caminho, a rodada e registrada como quebra
   9. anota os offsets (depois): e o que permite contar as descartadas
  10. cria o arquivo de parada e espera o pipeline encerrar
  11. roda o reconciliador
  12. grava o manifesto e junta os arquivos da rodada em results/

--- O que fica -------------------------------------------------------------------
  results/Intervalo<I>Carga<C>/<motor>/rodada-<NN>/
      manifesto.json      a rodada inteira: parametros, codigo, imagens, relogio,
                          passos com horario, codigos de saida, numeros
      e os arquivos de medicao: lag, CPU e memoria, progresso do motor,
      manifesto do gerador, relatorio do reconciliador, telas

O Parquet NAO vai para results/. Fica em ~/tcc-data e e APAGADO quando o
reconciliador da OK: a 10 mil evt/s, uma rodada de 10 min passa de 1 GB. Com
DIVERGENCIA ele fica, porque e nele que se investiga. MANTER_PARQUET=1 guarda sempre.

Mudancas nao commitadas NAO impedem a rodada: o manifesto anota o commit e se a
pasta local diferia dele.

Sai com 0 se a rodada vale; 1 se nao vale (o manifesto diz por que); 2 se nem
chegou a comecar.
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

REPO = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", Path.home() / "tcc-data"))
METRICS = DATA_DIR / "metrics"
CONTROLE = DATA_DIR / "control"
PY = sys.executable

# A base real e onde ela deve estar: as mesmas constantes que o replayer usa.
sys.path.insert(0, str(REPO / "shared" / "data-generator"))
from replayer import BASE, BASE_BYTES  # noqa: E402

SEED_BASE = 41
DISCO_MINIMO_GB = 50        # do MENOR entre o disco dos dados e o do repositorio
ESPERA_PRONTO_S = 180       # ate o pipeline dizer que esta no ar
ACOMODACAO_S = 10           # do "no ar" ate o primeiro evento
ESPERA_ENCERRAR_S = 120     # do arquivo de parada ate o processo sair
INTERVALO_FLINK_S = 5       # o checkpoint do Flink e fixo (decisao de 12/10/2026)

# "intervalo": o intervalo que ESTE motor usa numa combinacao de intervalo i.
MOTORES = {
    "spark": {
        "grupo": "tcc-nrt-spark", "conteiner": "spark-run", "pronto": "consulta no ar",
        "checkpoint": DATA_DIR / "checkpoints" / "nrt", "imagem": "tcc-spark:4.0.4",
        "lancador": "pipeline-nrt/run_local.sh",
        "intervalo": lambda i: i,
        "env": lambda s: {"STARTING_OFFSETS": "earliest", "TRIGGER": f"{s} seconds"},
        "particoes_nulas": ("__HIVE_DEFAULT_PARTITION__",),
    },
    "flink": {
        "grupo": "tcc-rt-flink", "conteiner": "flink-run", "pronto": ">> job no ar",
        "checkpoint": DATA_DIR / "checkpoints" / "rt", "imagem": "tcc-flink:1.20.5",
        "lancador": "pipeline-streaming/run_local.sh",
        "intervalo": lambda i: INTERVALO_FLINK_S,
        "env": lambda s: {"STARTUP_MODE": "earliest-offset", "CHECKPOINT_INTERVAL": f"{s} s"},
        "particoes_nulas": ("__DEFAULT_PARTITION__",),
    },
}


class Abortar(Exception):
    """A rodada nao pode comecar, ou nao pode continuar."""


def sh(*cmd: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, cwd=REPO, **kw)


def agora() -> str:
    return time.strftime("%H:%M:%S")


class Rodada:
    def __init__(self) -> None:
        try:
            self.motor = os.environ["MOTOR"]
            self.intervalo = int(os.environ["INTERVALO"])
            self.carga = int(os.environ["CARGA"])
            self.duracao = int(os.environ["DURACAO"])
            self.rodada = int(os.environ["RODADA"])
        except (KeyError, ValueError) as e:
            raise Abortar(f"parametro ausente ou invalido: {e}. Ver o cabecalho deste arquivo.")
        if self.motor not in MOTORES or not 1 <= self.rodada <= 10:
            raise Abortar("MOTOR tem de ser spark ou flink, e RODADA de 1 a 10.")

        self.m = MOTORES[self.motor]
        # O intervalo que o motor usa de fato: o da combinacao no Spark, 5 s no Flink.
        self.intervalo_motor = self.m["intervalo"](self.intervalo)
        self.seed = SEED_BASE + self.rodada
        self.malformados = 0.0      # sem malformados desde o contrato 3.0 (secao 2.6)
        carimbo = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        self.run_id = f"{carimbo}-{self.motor}-i{self.intervalo}-c{self.carga}-r{self.rodada:02d}"
        self.pasta = (REPO / "results" / f"Intervalo{self.intervalo}Carga{self.carga}"
                      / self.motor / f"rodada-{self.rodada:02d}")
        self.parada = CONTROLE / f"stop.{self.run_id}"

        cfg = yaml.safe_load((REPO / "shared/data-generator/config.yaml").read_text("utf-8"))
        self.broker, self.topico = cfg["broker"], cfg["topic"]
        self.admin = AdminClient({"bootstrap.servers": self.broker})

        self.proc_pipeline = self.proc_coletor = None
        self.motivos: list[str] = []      # por que a rodada NAO vale, se nao valer
        self.manifesto: dict = {
            "run_id": self.run_id, "motor": self.motor, "intervalo_s": self.intervalo,
            "intervalo_do_motor_s": self.intervalo_motor, "carga_evt_s": self.carga, "duracao_s": self.duracao, "rodada": self.rodada,
            "seed": self.seed, "malformed_rate": self.malformados, "passos": [],
        }

    # --- utilitarios -------------------------------------------------------
    def passo(self, texto: str) -> None:
        print(f">> [{agora()}] {texto}", flush=True)
        self.manifesto["passos"].append({"quando_ms": int(time.time() * 1000), "passo": texto})

    def gravar_manifesto(self) -> None:
        self.manifesto["valida"] = not self.motivos
        self.manifesto["motivos"] = self.motivos
        (self.pasta / "manifesto.json").write_text(
            json.dumps(self.manifesto, indent=2, ensure_ascii=False), encoding="utf-8")

    def ultimos_offsets(self) -> dict[int, int]:
        meta = self.admin.list_topics(topic=self.topico, timeout=10).topics[self.topico]
        if meta.error is not None:
            raise Abortar(f"topico {self.topico}: {meta.error}")
        futs = self.admin.list_offsets(
            {TopicPartition(self.topico, p): OffsetSpec.latest() for p in meta.partitions},
            request_timeout=10)
        return {tp.partition: f.result().offset for tp, f in futs.items()}

    def lag(self) -> int | None:
        """Lag total do grupo, ou None se alguma particao com dado ainda nao tem commit."""
        ultimos = self.ultimos_offsets()
        pedido = ConsumerGroupTopicPartitions(
            self.m["grupo"], [TopicPartition(self.topico, p) for p in ultimos])
        fut = self.admin.list_consumer_group_offsets([pedido], request_timeout=10)
        total = 0
        for tp in fut[self.m["grupo"]].result().topic_partitions:
            if ultimos[tp.partition] == 0:
                continue
            if tp.offset < 0:
                return None
            total += max(ultimos[tp.partition] - tp.offset, 0)
        return total

    def pipeline_vivo(self) -> bool:
        return self.proc_pipeline is not None and self.proc_pipeline.poll() is None

    # --- 1. conferencias -----------------------------------------------------
    def conferir(self) -> None:
        self.passo("1/12 conferencias")
        # Dois lugares, e vale o MENOR. No WSL o disco de ~/tcc-data e virtual: ele
        # anuncia ~1 TB livre seja qual for a situacao, porque o arquivo que o
        # contem cresce dentro do disco do Windows. O limite real e o do Windows,
        # que e onde mora o repositorio (/mnt/c). Num Linux comum os dois coincidem.
        livres = {str(p): shutil.disk_usage(p).free / 1e9 for p in (DATA_DIR, REPO)}
        livre = min(livres.values())
        self.manifesto["disco_livre_gb"] = {k: round(v, 1) for k, v in livres.items()}
        if livre < DISCO_MINIMO_GB:
            raise Abortar(f"so {livre:.1f} GB livres ({livres}); o minimo e {DISCO_MINIMO_GB} GB.")

        if self.pasta.exists():
            if os.environ.get("REFAZER") != "1":
                raise Abortar(f"esta rodada ja existe: {self.pasta}\n"
                              "   Para refazer, REFAZER=1 — a anterior e guardada ao lado.")
            self.pasta.rename(self.pasta.with_name(
                f"{self.pasta.name}.anterior-{time.strftime('%Y%m%dT%H%M%S')}"))
        self.pasta.mkdir(parents=True)
        CONTROLE.mkdir(parents=True, exist_ok=True)

        relogio = sh(PY, "scripts/check_clock.py")
        (self.pasta / "relogio.txt").write_text(relogio.stdout + relogio.stderr, encoding="utf-8")
        self.manifesto["relogio_ok"] = relogio.returncode == 0
        if relogio.returncode != 0:
            raise Abortar("o relogio reprovou (ver relogio.txt). "
                          "sudo systemctl stop systemd-timesyncd, esperar ~2 min e tentar de novo.")

        # A base real tem de estar pronta; o sha256 dela e conferido pelo
        # preparar_base.sh, e aqui o tamanho basta para pegar arquivo errado.
        if not BASE.is_file() or BASE.stat().st_size != BASE_BYTES:
            raise Abortar(f"a base nao esta pronta em {BASE}: bash scripts/preparar_base.sh")

        saude = sh("docker", "inspect", "-f", "{{.State.Health.Status}}", "kafka").stdout.strip()
        if saude != "healthy":
            raise Abortar(f"o Kafka nao esta saudavel (estado: {saude or 'ausente'}).")
        no_ar = sh("docker", "ps", "--format", "{{.Names}}").stdout.split()
        esquecidos = [n for n in no_ar if "spark-run" in n or "flink-run" in n]
        if esquecidos:
            raise Abortar(f"ha pipeline rodando: {', '.join(esquecidos)}. Pare-o antes.")

        imagens = {}
        for nome in ("tcc-spark:4.0.4", "tcc-flink:1.20.5", "apache/kafka:4.0.2"):
            imagens[nome] = sh("docker", "image", "inspect", "--format", "{{.Id}}", nome).stdout.strip()
        if not imagens[self.m["imagem"]]:
            raise Abortar(f"imagem {self.m['imagem']} ausente: docker compose build {self.motor}")
        self.manifesto["imagens"] = imagens
        # Quantos nucleos o Docker enxerga. A CPU e medida em % de UM nucleo; os
        # arquivos de resultado a mostram em % da maquina, dividindo por este numero.
        nucleos = sh("docker", "info", "--format", "{{.NCPU}}").stdout.strip()
        self.manifesto["nucleos"] = int(nucleos) if nucleos.isdigit() else os.cpu_count()

        # Mudanca nao commitada NAO impede a rodada (decisao de 11/10/2026): o
        # manifesto anota o commit e se a pasta local diferia dele.
        pendentes = sh("git", "status", "--porcelain").stdout.splitlines()
        self.manifesto["git"] = {
            "commit": sh("git", "rev-parse", "HEAD").stdout.strip() or None,
            "pasta_local_igual_ao_commit": not pendentes,
            "arquivos_pendentes": [p.strip() for p in pendentes],
        }
        if pendentes:
            print(f"   aviso: {len(pendentes)} arquivo(s) com mudanca nao commitada — anotado no manifesto")

    # --- 2 a 4. estado limpo ---------------------------------------------------
    def zerar(self) -> None:
        self.passo("2/12 zerando o checkpoint")
        shutil.rmtree(self.m["checkpoint"], ignore_errors=True)
        if self.m["checkpoint"].exists():
            raise Abortar(f"nao consegui apagar {self.m['checkpoint']}")

        self.passo("3/12 recriando o topico")
        k = ("docker", "exec", "kafka", "/opt/kafka/bin")
        topicos = lambda: sh(k[0], k[1], k[2], f"{k[3]}/kafka-topics.sh", "--bootstrap-server",
                             "localhost:9092", "--list").stdout.split()
        if self.topico in topicos():
            sh(k[0], k[1], k[2], f"{k[3]}/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
               "--delete", "--topic", self.topico)
            # A exclusao e assincrona. Criar antes de ela terminar faria o
            # "--if-not-exists" achar o topico e nao criar nada.
            for _ in range(60):
                if self.topico not in topicos():
                    break
                time.sleep(1)
            else:
                raise Abortar("o topico nao foi apagado em 60 s")
        # A posicao antiga do grupo: e ela que fazia a fila comecar com um numero falso.
        sh(k[0], k[1], k[2], f"{k[3]}/kafka-consumer-groups.sh", "--bootstrap-server",
           "localhost:9092", "--delete", "--group", self.m["grupo"])
        criar = sh("bash", "scripts/create_topic.sh")
        if criar.returncode != 0:
            raise Abortar("scripts/create_topic.sh falhou:\n" + criar.stdout[-600:])

        self.passo("4/12 offsets do topico, antes")
        self.antes = self.ultimos_offsets()
        if len(self.antes) != 24 or sum(self.antes.values()) != 0:
            raise Abortar(f"o topico deveria ter 24 particoes vazias; tem {len(self.antes)} "
                          f"com {sum(self.antes.values())} mensagens")

    # --- 5 e 6. pipeline e coletor ----------------------------------------------
    def subir(self) -> None:
        self.passo("5/12 subindo o pipeline")
        self.parada.unlink(missing_ok=True)
        variaveis = {**self.m["env"](self.intervalo_motor),
                     "STOP_FILE": f"/tcc-data/control/{self.parada.name}"}
        comando = " ".join(f'{k}="{v}"' for k, v in variaveis.items()) + f" bash {self.m['lancador']}"
        self.tela = self.pasta / "pipeline.txt"
        # `script` da ao pipeline um pseudo-terminal, como numa aba: e o caminho
        # em que os dois lancadores foram testados. -e devolve o codigo de saida.
        self.proc_pipeline = subprocess.Popen(
            ["script", "-qefc", comando, "/dev/null"], cwd=REPO,
            stdin=subprocess.DEVNULL, stdout=open(self.tela, "wb"), stderr=subprocess.STDOUT)
        limite = time.monotonic() + ESPERA_PRONTO_S
        while self.m["pronto"] not in self.tela.read_text("utf-8", errors="replace"):
            if not self.pipeline_vivo():
                raise Abortar("o pipeline morreu antes de ficar pronto (ver pipeline.txt)")
            if time.monotonic() > limite:
                raise Abortar(f"o pipeline nao ficou pronto em {ESPERA_PRONTO_S} s")
            time.sleep(1)
        time.sleep(ACOMODACAO_S)

        self.passo("6/12 ligando o coletor")
        self.proc_coletor = subprocess.Popen(
            [PY, "-u", "shared/monitoring/collector.py"], cwd=REPO,
            env={**os.environ, "GROUP_ID": self.m["grupo"],
                 "CONTAINERS": f"{self.m['conteiner']},kafka",
                 # Os CSVs do coletor ja nascem na pasta da rodada.
                 "DATA_DIR": str(self.pasta)},
            stdout=open(self.pasta / "coletor.txt", "wb"), stderr=subprocess.STDOUT)
        time.sleep(3)

    # --- 7 a 9. carga -----------------------------------------------------------------
    def gerar(self) -> None:
        self.passo(f"7/12 replayer: {self.carga} msg/s por {self.duracao} s, "
                   f"janela da base pela seed {self.seed}")
        env = {**os.environ, "RATE": str(self.carga), "DURATION": str(self.duracao),
               "SEED": str(self.seed), "RUN_ID": self.run_id}
        # Substituido abaixo pelo instante em que o replayer comecou a ENVIAR,
        # depois de posicionar na janela.
        self.manifesto["gerador_inicio_ms"] = int(time.time() * 1000)
        gerador = subprocess.Popen([PY, "shared/data-generator/replayer.py"], cwd=REPO, env=env,
                                   stdout=open(self.pasta / "replayer.txt", "wb"),
                                   stderr=subprocess.STDOUT)
        while gerador.poll() is None:
            if not self.pipeline_vivo():
                gerador.terminate()
                gerador.wait()
                self.manifesto["quebrou"] = "o pipeline caiu DURANTE a geracao"
                self.motivos.append("o pipeline caiu durante a geracao")
                return
            time.sleep(1)
        self.manifesto["gerador_fim_ms"] = int(time.time() * 1000)
        self.manifesto["gerador_codigo"] = gerador.returncode
        if gerador.returncode != 0:
            self.motivos.append(f"o replayer saiu com codigo {gerador.returncode}")
        # A janela da base e o instante do primeiro envio, do manifesto do replayer.
        gen = METRICS / f"{self.run_id}.gen0.json"
        if gen.is_file():
            g = json.loads(gen.read_text(encoding="utf-8"))
            self.manifesto["janela"] = g.get("janela")
            self.manifesto["base_sha256_gz"] = (g.get("base") or {}).get("sha256_gz")
            self.manifesto["posicionamento_s"] = g.get("posicionamento_s")
            if g.get("envio_inicio_ms"):
                self.manifesto["gerador_inicio_ms"] = g["envio_inicio_ms"]

        self.passo(f"8/12 esperando a fila zerar (ate {self.duracao} s)")
        inicio = time.monotonic()
        restante = None
        while time.monotonic() - inicio < self.duracao:
            if not self.pipeline_vivo():
                self.manifesto["quebrou"] = "o pipeline caiu ENQUANTO esvaziava a fila"
                self.motivos.append("o pipeline caiu enquanto esvaziava a fila")
                break
            restante = self.lag()
            if restante == 0:
                break
            time.sleep(1)
        self.manifesto["fila"] = {"zerou": restante == 0, "restante": restante,
                                  "espera_s": round(time.monotonic() - inicio, 1)}
        self.manifesto["fila_zerou_ms"] = int(time.time() * 1000)
        if restante != 0 and "quebrou" not in self.manifesto:
            self.motivos.append(f"a fila nao zerou em {self.duracao} s (restante: {restante})")

        self.passo("9/12 offsets do topico, depois")
        depois = self.ultimos_offsets()
        mensagens = sum(depois[p] - self.antes.get(p, 0) for p in depois)
        METRICS.mkdir(parents=True, exist_ok=True)
        (METRICS / f"{self.run_id}.topico.json").write_text(json.dumps(
            {"run_id": self.run_id, "mensagens": mensagens, "origem": "run_round.py",
             "antes": self.antes, "depois": depois}, indent=2), encoding="utf-8")
        self.manifesto["mensagens_no_topico"] = mensagens

    # --- 10. encerramento ---------------------------------------------------------
    def encerrar(self) -> None:
        if self.proc_coletor is not None and self.proc_coletor.poll() is None:
            self.proc_coletor.send_signal(signal.SIGINT)
            try:
                self.proc_coletor.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc_coletor.kill()
        if self.proc_pipeline is None:
            return
        if self.pipeline_vivo():
            self.passo("10/12 arquivo de parada")
            self.parada.touch()
            try:
                self.proc_pipeline.wait(timeout=ESPERA_ENCERRAR_S)
            except subprocess.TimeoutExpired:
                nomes = [n for n in sh("docker", "ps", "--format", "{{.Names}}").stdout.split()
                         if self.m["conteiner"] in n]
                for n in nomes:
                    sh("docker", "stop", n)
                try:
                    self.proc_pipeline.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.proc_pipeline.kill()
                self.motivos.append("o pipeline nao obedeceu ao arquivo de parada")
        self.parada.unlink(missing_ok=True)
        codigo = self.proc_pipeline.returncode
        self.manifesto["pipeline_codigo"] = codigo
        if codigo != 0 and not any("pipeline" in m for m in self.motivos):
            self.motivos.append(f"o pipeline saiu com codigo {codigo}")

    # --- 11 e 12. resultado ---------------------------------------------------------
    def reconciliar(self) -> None:
        self.passo("11/12 reconciliador")
        # Aquecimento (decisao de 12/10/2026): ficam fora do regime os eventos
        # gerados nos primeiros segundos da rodada — tantos quantos o intervalo
        # da combinacao: 5 s nas rodadas de 60 s, 30 s nas de 5 min. O MESMO
        # corte para os dois motores, que assim descartam o mesmo trecho da carga.
        self.manifesto["aquecimento_s"] = self.intervalo
        r = sh(PY, "shared/analysis/reconcile.py",
               env={**os.environ, "MOTOR": self.motor, "RUN_ID": self.run_id,
                    "AQUECIMENTO_S": str(self.intervalo)})
        (self.pasta / "reconcile.txt").write_text(r.stdout + r.stderr, encoding="utf-8")
        self.manifesto["reconcile_codigo"] = r.returncode
        if r.returncode != 0:
            self.motivos.append(f"o reconciliador saiu com codigo {r.returncode} (ver reconcile.txt)")
        relatorio = METRICS / f"reconcile.{self.motor}.{self.run_id}.json"
        self.relatorio = json.loads(relatorio.read_text("utf-8")) if relatorio.is_file() else {}

    def recursos(self) -> dict:
        """CPU e memoria do conteiner do motor, do inicio do gerador ate a fila zerar.

        A CPU fica aqui como foi medida: em % de UM nucleo (100 = um nucleo
        inteiro; passa de 100 com mais de um). Junto com `nucleos`, no manifesto,
        da a % da maquina, que e como os arquivos de resultado a mostram.
        """
        arquivos = sorted((self.pasta / "metrics").glob("recursos.*.csv"))
        ini, fim = self.manifesto.get("gerador_inicio_ms"), self.manifesto.get("fila_zerou_ms")
        if not arquivos or ini is None or fim is None:
            return {}
        linhas = [l for l in csv.DictReader(open(arquivos[-1], encoding="utf-8"))
                  if self.m["conteiner"] in l["conteiner"] and ini <= int(l["amostra_ts"]) <= fim]
        if len(linhas) < 2:
            return {}
        segundos = (int(linhas[-1]["amostra_ts"]) - int(linhas[0]["amostra_ts"])) / 1000
        cpu = (int(linhas[-1]["cpu_ns"]) - int(linhas[0]["cpu_ns"])) / 1e9
        picos = [float(l["cpu_pct"]) for l in linhas if l["cpu_pct"]]
        mem = [int(l["mem_sem_cache_bytes"]) / 1048576 for l in linhas]
        return {"cpu_media_pct": round(cpu / segundos * 100, 1), "cpu_pico_pct": round(max(picos), 1),
                "mem_media_mib": round(sum(mem) / len(mem), 1), "mem_pico_mib": round(max(mem), 1),
                "leituras": len(linhas)}

    def fechar(self) -> None:
        self.passo("12/12 manifesto e arquivos da rodada")
        lat = self.relatorio.get("latencia_ms", {})
        integ = self.relatorio.get("integridade", {})
        self.manifesto["resultado"] = {
            "latencia_processamento_ms": lat.get("LAT. PROCESSAMENTO"),
            "latencia_visibilidade_ms": lat.get("LAT. VISIBILIDADE"),
            "throughput_medio_evt_s": (self.relatorio.get("throughput_medio") or {}).get("evt_s"),
            **self.recursos(),
            "perdas": integ.get("perdas"), "duplicatas": integ.get("duplicatas"),
            "atrasados": self.relatorio.get("is_late"),
            "invalidas": integ.get("invalidas"), "nulas": integ.get("nulas"),
            "descartadas": integ.get("descartadas"),
            "arquivos": (self.relatorio.get("destino") or {}).get("arquivos"),
            "bytes": (self.relatorio.get("destino") or {}).get("bytes"),
        }
        inicio = self.manifesto["passos"][0]["quando_ms"]
        for nome in (f"{self.run_id}.gen0.json", f"{self.run_id}.topico.json",
                     f"reconcile.{self.motor}.{self.run_id}.json"):
            if (METRICS / nome).is_file():
                shutil.copy2(METRICS / nome, self.pasta / nome)
        # O log de progresso do motor e nomeado pelo instante de subida.
        for log in METRICS.glob(f"{self.motor}.progresso.*.jsonl"):
            if int(log.name.split(".")[2]) >= inicio:
                shutil.copy2(log, self.pasta / log.name)

        dados = DATA_DIR / "out" / self.motor / "events"
        if self.motivos or os.environ.get("MANTER_PARQUET") == "1":
            self.manifesto["parquet"] = f"mantido em {dados}/run_id={self.run_id}"
        else:
            for pasta in [f"run_id={self.run_id}"] + [f"run_id={p}" for p in self.m["particoes_nulas"]]:
                shutil.rmtree(dados / pasta, ignore_errors=True)
            self.manifesto["parquet"] = "apagado depois do reconciliador (rodada valida)"

    # --- a rodada ---------------------------------------------------------------------
    def rodar(self) -> int:
        print(f"=== rodada {self.rodada:02d} — {self.motor} — intervalo {self.intervalo} s, "
              f"carga {self.carga} msg/s, {self.duracao} s ===")
        if self.intervalo_motor != self.intervalo:
            print(f"    o {self.motor} roda com intervalo de {self.intervalo_motor} s, fixo")
        print(f"    run_id {self.run_id}")
        print(f"    pasta  {self.pasta}")
        try:
            self.conferir()
        except Abortar as e:
            # Uma rodada que nem comecou nao deixa pasta: senao a proxima
            # tentativa esbarraria em "esta rodada ja existe".
            if self.pasta.is_dir() and not (self.pasta / "manifesto.json").exists():
                relogio = self.pasta / "relogio.txt"
                if relogio.is_file() and not self.manifesto.get("relogio_ok", True):
                    print("\n".join(relogio.read_text("utf-8").splitlines()[-8:]))
                shutil.rmtree(self.pasta)
            print(f"\nNAO COMECOU: {e}")
            return 2
        try:
            self.zerar()
            self.subir()
            self.gerar()
        except Abortar as e:
            self.motivos.append(str(e))
            print(f"\n!! {e}")
        finally:
            self.encerrar()
        if "gerador_fim_ms" in self.manifesto or "quebrou" in self.manifesto:
            self.reconciliar()
            self.fechar()
        self.gravar_manifesto()

        print()
        if self.motivos:
            print("RODADA NAO VALE:")
            for m in self.motivos:
                print(f"  - {m}")
        else:
            r = self.manifesto["resultado"]
            print("RODADA VALIDA.")
            print(f"  latencia de processamento P50 {r['latencia_processamento_ms']['p50']} ms   "
                  f"visibilidade P50 {r['latencia_visibilidade_ms']['p50']} ms")
            print(f"  throughput medio {r['throughput_medio_evt_s']} evt/s   "
                  f"CPU media {r.get('cpu_media_pct')} % de um nucleo   "
                  f"memoria pico {r.get('mem_pico_mib')} MiB")
            print(f"  perdas {r['perdas']}  duplicatas {r['duplicatas']}  invalidas {r['invalidas']}  "
                  f"nulas {r['nulas']}  descartadas {r['descartadas']}")
        print(f"manifesto: {self.pasta / 'manifesto.json'}")
        return 1 if self.motivos else 0


if __name__ == "__main__":
    try:
        sys.exit(Rodada().rodar())
    except Abortar as e:
        print(f"NAO COMECOU: {e}", file=sys.stderr)
        sys.exit(2)
