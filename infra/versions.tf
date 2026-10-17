# Versoes travadas (decisao E7). O Terraform e o mesmo instalado pelo
# scripts/setup_aws_tools.sh; a versao exata de cada provider fica no
# .terraform.lock.hcl, que entra no git.
terraform {
  required_version = "= 1.16.4"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    # So para descobrir o IP publico de quem roda o apply (regra do SSH).
    http = {
      source  = "hashicorp/http"
      version = "~> 3.4"
    }
  }
}

# Credenciais: as do ~/.aws/credentials, coladas a cada sessao do Learner Lab.
# Nunca neste arquivo.
provider "aws" {
  region = var.regiao

  default_tags {
    tags = {
      Projeto = "tcc-pipelines"
    }
  }
}
