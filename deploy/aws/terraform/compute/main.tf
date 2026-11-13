# The compute layer of the AWS proof of concept: the EKS cluster, its node groups, add-ons and Pod Identity
# associations. deploy/aws/poc.sh up creates it, and poc.sh down (or the auto-down guard) destroys it. It costs
# about $0.25 an hour while it exists, and nothing once it's gone.
#
# It holds no Kubernetes objects. poc.sh applies those with kubectl, so that a cluster the guard deleted behind
# Terraform's back is simply created again by the next apply.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {
    bucket       = "sdh-poc-tfstate-493469777520"
    key          = "compute.tfstate"
    region       = "us-east-2"
    use_lockfile = true
  }
}

provider "aws" {
  region              = var.region
  allowed_account_ids = [var.account_id]
  default_tags { tags = { Project = "sdh-poc" } }
}

data "terraform_remote_state" "persistent" {
  backend = "s3"
  config = {
    bucket = "sdh-poc-tfstate-493469777520"
    key    = "persistent.tfstate"
    region = "us-east-2"
  }
}

locals {
  p    = data.terraform_remote_state.persistent.outputs
  name = local.p.cluster_name
}

data "aws_vpc" "default" { default = true }

data "aws_subnet" "zone" {
  for_each          = toset(var.zones)
  vpc_id            = data.aws_vpc.default.id
  availability_zone = each.key
  default_for_az    = true
}

data "aws_subnet" "burst_zone" {
  for_each          = toset(var.burst_zones)
  vpc_id            = data.aws_vpc.default.id
  availability_zone = each.key
  default_for_az    = true
}

data "aws_caller_identity" "me" {}

resource "aws_eks_cluster" "poc" {
  name     = local.name
  version  = var.kubernetes_version
  role_arn = local.p.cluster_role_arn

  vpc_config {
    subnet_ids              = [for s in data.aws_subnet.zone : s.id]
    endpoint_private_access = true # the nodes reach the API inside the VPC
    endpoint_public_access  = true
    public_access_cidrs     = var.api_cidrs # poc.sh passes this machine's address
  }

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  encryption_config {
    resources = ["secrets"]
    provider { key_arn = local.p.infra_key_arn }
  }

  # Standard support only: a version that falls into extended support costs $0.60 an hour instead of $0.10.
  upgrade_policy { support_type = "STANDARD" }

  # Control-plane logs go to CloudWatch at $0.50/GB: off.
  enabled_cluster_log_types = []

  bootstrap_self_managed_addons = false
}

# --- Node groups ---------------------------------------------------------------------------------------------
# base: one on-demand node per zone. They hold the active and standby landing pods (one per node), Postgres and
# Kafka (on the node in the state volumes' zone), and one pod of each scaled stage.
#
# burst: Spot nodes (on-demand for a performance test), 0 to 2, in any of three zones and six instance types,
# tainted so only the KEDA-scaled stages and the gold check run there. Cluster Autoscaler adds one when
# pods can't be placed, and removes it 10 minutes after it empties.

resource "aws_eks_node_group" "base" {
  for_each        = data.aws_subnet.zone
  cluster_name    = aws_eks_cluster.poc.name
  node_group_name = "base-${substr(each.key, -1, 1)}"
  node_role_arn   = local.p.node_role_arn
  subnet_ids      = [each.value.id]
  ami_type        = "AL2023_ARM_64_STANDARD"
  capacity_type   = "ON_DEMAND"
  instance_types  = [var.base_instance_type]
  disk_size       = 20
  labels          = { "sdh-poc/pool" = "base" }

  scaling_config {
    min_size     = 1
    desired_size = 1
    max_size     = 1
  }
  update_config { max_unavailable = 1 }

  # The cluster has no self-managed add-ons: the nodes need the CNI and kube-proxy before they're ready.
  depends_on = [aws_eks_addon.vpc_cni, aws_eks_addon.kube_proxy, aws_eks_addon.pod_identity]
}

resource "aws_eks_node_group" "burst" {
  cluster_name    = aws_eks_cluster.poc.name
  node_group_name = "burst"
  node_role_arn   = local.p.node_role_arn
  subnet_ids      = [for s in data.aws_subnet.burst_zone : s.id]
  ami_type        = "AL2023_ARM_64_STANDARD"
  capacity_type   = var.burst_capacity_type
  instance_types  = var.burst_instance_types
  disk_size       = 20
  labels          = { "sdh-poc/pool" = "burst" }

  taint {
    key    = "sdh-poc/burst"
    value  = "true"
    effect = "NO_SCHEDULE"
  }

  scaling_config {
    min_size     = 0
    desired_size = 0
    max_size     = var.burst_max
  }
  update_config { max_unavailable = 1 }

  # The Cluster Autoscaler owns desired_size.
  lifecycle { ignore_changes = [scaling_config[0].desired_size] }
  depends_on = [aws_eks_addon.vpc_cni, aws_eks_addon.kube_proxy, aws_eks_addon.pod_identity]
}

# --- Add-ons -------------------------------------------------------------------------------------------------

resource "aws_eks_addon" "pod_identity" {
  cluster_name                = aws_eks_cluster.poc.name
  addon_name                  = "eks-pod-identity-agent"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

resource "aws_eks_addon" "vpc_cni" {
  cluster_name                = aws_eks_cluster.poc.name
  addon_name                  = "vpc-cni"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  # NetworkPolicy enforcement, which the sandbox's policies and the landing isolation test need.
  configuration_values = jsonencode({ enableNetworkPolicy = "true" })
}

resource "aws_eks_addon" "kube_proxy" {
  cluster_name                = aws_eks_cluster.poc.name
  addon_name                  = "kube-proxy"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

resource "aws_eks_addon" "coredns" {
  cluster_name                = aws_eks_cluster.poc.name
  addon_name                  = "coredns"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  depends_on                  = [aws_eks_node_group.base]
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name                = aws_eks_cluster.poc.name
  addon_name                  = "aws-ebs-csi-driver"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  pod_identity_association {
    role_arn        = local.p.ebs_csi_role_arn
    service_account = "ebs-csi-controller-sa"
  }
  depends_on = [aws_eks_node_group.base, aws_eks_addon.pod_identity]
}

# --- Pod Identity for the Cluster Autoscaler and the stages ----------------------------------------------------

resource "aws_eks_pod_identity_association" "autoscaler" {
  cluster_name    = aws_eks_cluster.poc.name
  namespace       = "kube-system"
  service_account = "cluster-autoscaler"
  role_arn        = local.p.autoscaler_role_arn
}

resource "aws_eks_pod_identity_association" "stage" {
  for_each        = local.p.stage_service_accounts
  cluster_name    = aws_eks_cluster.poc.name
  namespace       = split("/", each.value)[0]
  service_account = split("/", each.value)[1]
  role_arn        = local.p.stage_role_arns[each.key]
}

# The gold check's reader (deploy/aws/k8s/pipeline/gold-check.sh): read-only, in sdh-fhir. Only once the persistent
# layer has the role (it was added after the first apply).
resource "aws_eks_pod_identity_association" "gold_reader" {
  count           = lookup(local.p, "gold_reader_role_arn", null) == null ? 0 : 1
  cluster_name    = aws_eks_cluster.poc.name
  namespace       = "sdh-fhir"
  service_account = "gold-reader"
  role_arn        = local.p.gold_reader_role_arn
}

# --- Console access ---------------------------------------------------------------------------------------------
#
# The cluster's creator (the deployer) is its only admin. These IAM principals may also see its workloads in the
# EKS console (Resources tab), read-only and without Secrets: AmazonEKSViewPolicy.
resource "aws_eks_access_entry" "viewer" {
  for_each      = toset(var.console_viewers)
  cluster_name  = aws_eks_cluster.poc.name
  principal_arn = each.value
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "viewer" {
  for_each      = aws_eks_access_entry.viewer
  cluster_name  = aws_eks_cluster.poc.name
  principal_arn = each.value.principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy"
  access_scope { type = "cluster" }
}
