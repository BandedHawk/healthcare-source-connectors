# The persistent layer of the AWS proof of concept (docs/AWS-POC.md). It is created once and kept while the PoC
# is shut down, and it costs about $11 a month then:
#   - the S3 buckets;
#   - the KMS keys;
#   - the ECR repositories;
#   - the Postgres and Kafka EBS volumes;
#   - the IAM roles;
#   - the auto-shutdown state machine;
#   - the S3 gateway endpoint.
# The cluster is in ../compute, which deploy/aws/poc.sh creates and destroys each session.
#
# The keys, buckets and volumes have prevent_destroy: data lost here is lost for good (the PHI rule), so removing
# them takes a deliberate edit.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {
    bucket       = "sdh-poc-tfstate-493469777520"
    key          = "persistent.tfstate"
    region       = "us-east-2"
    use_lockfile = true
  }
}

provider "aws" {
  region              = var.region
  allowed_account_ids = [var.account_id]
  default_tags { tags = { Project = "sdh-poc" } }
}

data "aws_vpc" "default" { default = true }

data "aws_route_tables" "default" { vpc_id = data.aws_vpc.default.id }

# --- KMS ---------------------------------------------------------------------------------------------------------
# The same keys as the sandbox's Vault Transit keys (scripts/sandbox.sh setup_vault), one set per pipeline, and one
# key for the infrastructure itself (EKS secrets, EBS volumes).
#
# Rotation is off: each rotation adds $1 a month per key, and this is a PoC.
#
# The default key policy (the account's IAM policies decide who uses a key) is kept on purpose. The deployer's own
# policy denies it decrypting or signing with any of them.

locals {
  keks     = flatten([for p in var.pipelines : [for k in ["landed-items", "records", "fhir"] : "${p}-${k}"]])
  signings = flatten([for p in var.pipelines : [for s in ["landing", "splitter", "convert", "transform"] : "${p}-${s}-signing"]])
}

resource "aws_kms_key" "kek" {
  for_each                = toset(local.keks)
  description             = "sdh-poc KEK ${each.key}"
  deletion_window_in_days = 30
  enable_key_rotation     = false
  lifecycle { prevent_destroy = true }
}

resource "aws_kms_alias" "kek" {
  for_each      = aws_kms_key.kek
  name          = "alias/sdh-poc-${each.key}"
  target_key_id = each.value.key_id
}

resource "aws_kms_key" "signing" {
  for_each                 = toset(local.signings)
  description              = "sdh-poc signing key ${each.key}"
  customer_master_key_spec = "ECC_NIST_P256"
  key_usage                = "SIGN_VERIFY"
  deletion_window_in_days  = 30
  lifecycle { prevent_destroy = true }
}

resource "aws_kms_alias" "signing" {
  for_each      = aws_kms_key.signing
  name          = "alias/sdh-poc-${each.key}"
  target_key_id = each.value.key_id
}

resource "aws_kms_key" "infra" {
  description             = "sdh-poc infrastructure: EKS secrets and EBS volumes"
  deletion_window_in_days = 30
  enable_key_rotation     = false
  lifecycle { prevent_destroy = true }
}

resource "aws_kms_alias" "infra" {
  name          = "alias/sdh-poc-infra"
  target_key_id = aws_kms_key.infra.key_id
}

# --- S3 ----------------------------------------------------------------------------------------------------------
# intake: synthetic test C-CDA that the landing stage reads. It uses SSE-S3, so the deployer can upload to it.
# bronze, silver, gold: the medallion layers, with SSE-KMS under the infra key and a bucket key (fewer KMS calls).
# The pipeline also encrypts every record itself (DESIGN.md §8). tools: test tools, such as the HL7 FHIR validator
# the gold check runs (deploy/aws/k8s/pipeline/gold-check.sh); no data, so SSE-S3.

locals {
  layers = { intake = false, bronze = true, silver = true, gold = true, tools = false } # name => SSE-KMS?
}

resource "aws_s3_bucket" "layer" {
  for_each = local.layers
  bucket   = "sdh-poc-${each.key}-${var.account_id}"
  lifecycle { prevent_destroy = true }
}

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each                = aws_s3_bucket.layer
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = local.layers[each.key] ? "aws:kms" : "AES256"
      kms_master_key_id = local.layers[each.key] ? aws_kms_key.infra.arn : null
    }
    bucket_key_enabled = local.layers[each.key]
  }
}

# Unfinished multipart uploads cost storage while invisible. Old versions aren't kept, because versioning is off:
# Iceberg keeps its own snapshots, and the pipeline's maintenance expires them.
resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
}

resource "aws_s3_bucket_policy" "layer" {
  for_each   = aws_s3_bucket.layer
  bucket     = each.value.id
  depends_on = [aws_s3_bucket_public_access_block.layer]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "TlsOnly"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# S3 traffic from the nodes stays inside AWS and costs nothing; there's no NAT gateway (docs/AWS-POC.md §3).
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = data.aws_route_tables.default.ids
  tags              = { Name = "sdh-poc-s3" }
}

# --- ECR ---------------------------------------------------------------------------------------------------------

resource "aws_ecr_repository" "stage" {
  for_each             = toset(var.images)
  name                 = "sdh-poc/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false
  image_scanning_configuration { scan_on_push = false }
}

resource "aws_ecr_lifecycle_policy" "stage" {
  for_each   = aws_ecr_repository.stage
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

# --- EBS volumes for the stateful services -----------------------------------------------------------------------
# Made here, not by the cluster, so they survive the cluster being destroyed. The compute layer mounts them as
# static PersistentVolumes (deploy/aws/k8s/infra). Both are in one zone, so Postgres and Kafka run on the node in
# that zone.

resource "aws_ebs_volume" "state" {
  for_each          = var.state_volumes
  availability_zone = var.state_zone
  size              = each.value
  type              = "gp3"
  encrypted         = true
  kms_key_id        = aws_kms_key.infra.arn
  tags              = { Name = "sdh-poc-${each.key}", "sdh-poc/volume" = each.key }
  lifecycle { prevent_destroy = true }
}
