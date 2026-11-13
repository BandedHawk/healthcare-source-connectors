variable "region" {
  type    = string
  default = "us-east-2"
}

variable "account_id" {
  type    = string
  default = "493469777520"
}

# The first zone must be the persistent layer's state_zone: Postgres and Kafka run on that zone's base node.
variable "zones" {
  type    = list(string)
  default = ["us-east-2a", "us-east-2b"]
}

# In standard support until 2027-11-30 (aws eks describe-cluster-versions, 2026-11-12).
variable "kubernetes_version" {
  type    = string
  default = "1.37"
}

variable "base_instance_type" {
  type    = string
  default = "t4g.large"
}

# Every arm64 type with 2 vCPUs and 8 GiB, so the Cluster Autoscaler's idea of a node's size holds for any of
# them. Three types in one zone found no Spot capacity on 2026-11-12: more types and zones give Spot more pools
# to choose from.
variable "burst_instance_types" {
  type    = list(string)
  default = ["t4g.large", "m7g.large", "m6g.large", "m8g.large", "m7gd.large", "m6gd.large"]
}

# The burst nodes may be in any of these zones: they hold no volumes. The base nodes stay in var.zones.
variable "burst_zones" {
  type    = list(string)
  default = ["us-east-2a", "us-east-2b", "us-east-2c"]
}

# SPOT, or ON_DEMAND for a performance test, where an interruption would spoil the measurement (poc.sh up
# --on-demand-burst). On-demand needs the account's on-demand vCPU quota to cover the base and burst nodes
# (iam/spot-quota.sh shows it).
variable "burst_capacity_type" {
  type    = string
  default = "SPOT"
  validation {
    condition     = contains(["SPOT", "ON_DEMAND"], var.burst_capacity_type)
    error_message = "burst_capacity_type is SPOT or ON_DEMAND."
  }
}

variable "burst_max" {
  type    = number
  default = 2
}

# Who may reach the Kubernetes API. poc.sh sets this machine's public address (/32).
variable "api_cidrs" {
  type = list(string)
}

variable "console_viewers" {
  description = "IAM principals that may view the cluster's workloads in the EKS console, read-only"
  type        = list(string)
  default     = ["arn:aws:iam::493469777520:user/jonas-admin"]
}
