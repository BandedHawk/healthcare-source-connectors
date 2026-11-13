output "cluster_role_arn" { value = aws_iam_role.cluster.arn }
output "node_role_arn" { value = aws_iam_role.node.arn }
output "ebs_csi_role_arn" { value = aws_iam_role.ebs_csi.arn }
output "autoscaler_role_arn" { value = aws_iam_role.autoscaler.arn }
output "stage_role_arns" { value = { for k, r in aws_iam_role.stage : k => r.arn } }
output "stage_service_accounts" { value = var.stage_service_accounts }
output "infra_key_arn" { value = aws_kms_key.infra.arn }
output "kek_arns" { value = { for k, v in aws_kms_key.kek : k => v.arn } }
output "signing_key_arns" { value = { for k, v in aws_kms_key.signing : k => v.arn } }
output "buckets" { value = { for k, b in aws_s3_bucket.layer : k => b.bucket } }
output "ecr_repositories" { value = { for k, r in aws_ecr_repository.stage : k => r.repository_url } }
output "state_volume_ids" { value = { for k, v in aws_ebs_volume.state : k => v.id } }
output "state_zone" { value = var.state_zone }
output "guard_state_machine_arn" { value = aws_sfn_state_machine.guard.arn }
output "scheduler_role_arn" { value = aws_iam_role.scheduler.arn }
output "cluster_name" { value = var.cluster_name }
output "gold_reader_role_arn" { value = aws_iam_role.gold_reader.arn }
