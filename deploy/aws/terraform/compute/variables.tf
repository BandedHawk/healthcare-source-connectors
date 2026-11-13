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

variable "burst_instance_types" {
  type    = list(string)
  default = ["t4g.large", "m7g.large", "m6g.large"]
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
