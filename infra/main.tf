# Etapa 10 — a maquina do benchmark na AWS.
#
# Uma maquina so, com tudo dentro, como no local (decisao de 14/10/2026,
# reconfirmada em 16/10): Kafka, gerador, os dois motores e os scripts. A
# preparacao (Docker, repositorio, imagens) NAO e feita aqui: vem depois, por
# script via SSH (peca 3), para a saida aparecer na tela.
#
# No fim de CADA sessao do laboratorio:  terraform destroy
# Uma maquina so parada volta a ligar sozinha na sessao seguinte, e cobra.

# --- Rede propria (decisao de 17/10/2026) -----------------------------------
# VPC, uma sub-rede publica, a saida para a internet e a rota ate ela. Nada
# disso cobra; tudo sai no destroy.
resource "aws_vpc" "tcc" {
  cidr_block           = var.rede_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "tcc-rede" }
}

resource "aws_internet_gateway" "tcc" {
  vpc_id = aws_vpc.tcc.id

  tags = { Name = "tcc-saida" }
}

resource "aws_subnet" "publica" {
  vpc_id                  = aws_vpc.tcc.id
  cidr_block              = var.subrede_cidr
  availability_zone       = var.zona
  map_public_ip_on_launch = true # o IP publico e o que o SSH usa

  tags = { Name = "tcc-subrede-publica" }
}

resource "aws_route_table" "publica" {
  vpc_id = aws_vpc.tcc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.tcc.id
  }

  tags = { Name = "tcc-rotas" }
}

resource "aws_route_table_association" "publica" {
  subnet_id      = aws_subnet.publica.id
  route_table_id = aws_route_table.publica.id
}

# --- Acesso: SSH so do IP de quem roda o apply (decisao de 17/10/2026) ------
# O IP e lido a cada plan/apply. Se a internet de casa trocar de IP, rodar o
# apply de novo atualiza a regra.
data "http" "meu_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  meu_ip = "${chomp(data.http.meu_ip.response_body)}/32"
}

resource "aws_security_group" "ssh" {
  name        = "tcc-ssh"
  description = "SSH so do IP do autor; saida livre"
  vpc_id      = aws_vpc.tcc.id

  ingress {
    description = "SSH do IP de quem rodou o apply"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.meu_ip]
  }

  egress {
    description = "Saida livre: apt, Docker Hub, GitHub, Maven"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "tcc-ssh" }
}

# --- A imagem: Ubuntu 24.04 LTS oficial da Canonical, FIXADA pelo ID ---------
# Decisoes de 17 e 21/10/2026. Toda sessao liga o mesmo "disco de fabrica" — o
# mesmo kernel, o mesmo Python do sistema —, para nenhuma combinacao rodar
# sobre um sistema diferente das outras. A busca confere que o ID e mesmo da
# Canonical; se a imagem deixar de existir, o plan falha antes de criar nada.
data "aws_ami" "ubuntu" {
  owners = ["099720109477"] # Canonical

  filter {
    name   = "image-id"
    values = [var.ami_id]
  }
}

# --- A maquina ---------------------------------------------------------------
resource "aws_instance" "benchmark" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.tipo_instancia
  subnet_id              = aws_subnet.publica.id
  vpc_security_group_ids = [aws_security_group.ssh.id]
  key_name               = var.chave_ssh

  # Perfil que ja existe no Learner Lab (nao da para criar roles la).
  iam_instance_profile = "LabInstanceProfile"

  # Metadados so com token (IMDSv2). Limite de 2 saltos: um conteiner, que
  # e um salto a mais, so enxerga as credenciais da maquina assim — e o que o
  # S3 vai precisar, se entrar.
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.disco_gb
    delete_on_termination = true # o disco sai junto com a maquina
  }

  tags = { Name = "tcc-benchmark" }
}
