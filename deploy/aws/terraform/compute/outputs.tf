output "cluster_name" { value = aws_eks_cluster.poc.name }
output "state_volume_ids" { value = local.p.state_volume_ids }
output "state_zone" { value = local.p.state_zone }
