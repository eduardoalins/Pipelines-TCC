# Os valores padrao sao as decisoes da Etapa 10 (16 e 17/10/2026). Os limites
# do AWS Academy Learner Lab sao duros: tentar passar de um deles pode desativar
# a conta. Por isso as validacoes abaixo recusam o valor antes de chegar a AWS.

variable "regiao" {
  description = "Regiao da AWS. O Learner Lab so permite us-east-1 e us-west-2; o par de chaves vockey so existe na primeira."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = contains(["us-east-1", "us-west-2"], var.regiao)
    error_message = "O Learner Lab so permite us-east-1 e us-west-2."
  }
}

variable "zona" {
  description = "Zona de disponibilidade da sub-rede (e da maquina)."
  type        = string
  default     = "us-east-1a"
}

variable "tipo_instancia" {
  description = "Tipo da maquina. m5.large: 2 vCPU, 8 GB, CPU sem creditos."
  type        = string
  default     = "m5.large"

  validation {
    # O Learner Lab vai ate o tamanho large. Nada de xlarge para cima.
    condition     = can(regex("\\.(nano|micro|small|medium|large)$", var.tipo_instancia))
    error_message = "O Learner Lab so permite tamanhos ate large."
  }
}

variable "disco_gb" {
  description = "Disco da maquina, em GB (gp3). 80 por causa da guarda de 50 GB livres do run_round.py."
  type        = number
  default     = 80

  validation {
    condition     = var.disco_gb >= 8 && var.disco_gb <= 100
    error_message = "O Learner Lab so permite volumes de ate 100 GB."
  }
}

variable "chave_ssh" {
  description = "Nome do par de chaves na AWS. O vockey vem pronto no Learner Lab."
  type        = string
  default     = "vockey"
}

variable "arquivo_chave" {
  description = "Onde a chave privada (Download PEM do AWS Details) fica no WSL. So entra no comando de SSH impresso no fim."
  type        = string
  default     = "~/.ssh/labsuser.pem"
}

variable "rede_cidr" {
  description = "Faixa de enderecos da VPC propria."
  type        = string
  default     = "10.42.0.0/16"
}

variable "subrede_cidr" {
  description = "Faixa de enderecos da sub-rede publica, dentro da VPC."
  type        = string
  default     = "10.42.1.0/24"
}
