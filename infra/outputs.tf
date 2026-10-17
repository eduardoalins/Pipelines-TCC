output "ip_publico" {
  description = "IP publico da maquina. Muda a cada apply."
  value       = aws_instance.benchmark.public_ip
}

output "ssh" {
  description = "Comando para entrar na maquina."
  value       = "ssh -i ${var.arquivo_chave} ubuntu@${aws_instance.benchmark.public_ip}"
}

output "ami_id" {
  description = "A imagem exata usada. Vai para o registro da etapa."
  value       = data.aws_ami.ubuntu.id
}

output "ami_nome" {
  description = "O nome da imagem, com a data de publicacao da Canonical."
  value       = data.aws_ami.ubuntu.name
}

output "ssh_liberado_para" {
  description = "O IP de onde o SSH e aceito."
  value       = local.meu_ip
}
