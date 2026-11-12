# Every role is under /sdh-poc/ and carries the permissions boundary (deploy/aws/iam), as the deployer's policy
# requires. Roles cost nothing, so they stay while the PoC is shut down.

locals {
  boundary = "arn:aws:iam::${var.account_id}:policy/sdh-poc-admin/sdh-poc-boundary"
  path     = "/sdh-poc/"

  # Pod Identity: the pods' credentials come through the EKS Pod Identity agent.
  pods_trust = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
      Condition = { StringEquals = { "aws:SourceAccount" = var.account_id } }
    }]
  })

  bucket_arn = { for k, b in aws_s3_bucket.layer : k => b.arn }
  kek_arn    = { for k, v in aws_kms_key.kek : k => v.arn }
  sign_arn   = { for k, v in aws_kms_key.signing : k => v.arn }

  # What each stage reads and writes (docs/AWS-POC.md §7), for every pipeline.
  stage_access = {
    landing = {
      rw_buckets = ["intake", "bronze"], ro_buckets = []
      keks       = [for p in var.pipelines : "${p}-landed-items"]
      signing    = [for p in var.pipelines : "${p}-landing-signing"]
    }
    splitter = {
      rw_buckets = ["silver"], ro_buckets = ["bronze"]
      keks       = flatten([for p in var.pipelines : ["${p}-landed-items", "${p}-records"]])
      signing    = [for p in var.pipelines : "${p}-splitter-signing"]
    }
    convert = {
      rw_buckets = ["silver"], ro_buckets = []
      keks       = [for p in var.pipelines : "${p}-records"]
      signing    = [for p in var.pipelines : "${p}-convert-signing"]
    }
    transform = {
      rw_buckets = ["gold"], ro_buckets = ["silver"]
      keks       = flatten([for p in var.pipelines : ["${p}-records", "${p}-fhir"]])
      signing    = [for p in var.pipelines : "${p}-transform-signing"]
    }
  }
}

# --- Cluster and nodes -------------------------------------------------------------------------------------------

resource "aws_iam_role" "cluster" {
  name                 = "${var.cluster_name}-cluster"
  path                 = local.path
  permissions_boundary = local.boundary
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role" "node" {
  name                 = "${var.cluster_name}-node"
  path                 = local.path
  permissions_boundary = local.boundary
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.node.name
  policy_arn = each.value
}

# --- EBS CSI driver: attaches the state volumes, which are encrypted under the infra key --------------------------

resource "aws_iam_role" "ebs_csi" {
  name                 = "${var.cluster_name}-ebs-csi"
  path                 = local.path
  permissions_boundary = local.boundary
  assume_role_policy   = local.pods_trust
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

resource "aws_iam_policy" "ebs_csi_kms" {
  name = "${var.cluster_name}-ebs-csi-kms"
  path = local.path
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Action    = ["kms:CreateGrant", "kms:ListGrants", "kms:RevokeGrant"]
        Resource  = aws_kms_key.infra.arn
        Condition = { Bool = { "kms:GrantIsForAWSResource" = "true" } }
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
        Resource = aws_kms_key.infra.arn
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi_kms" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = aws_iam_policy.ebs_csi_kms.arn
}

# --- Cluster Autoscaler: adds and removes Spot burst nodes --------------------------------------------------------

resource "aws_iam_role" "autoscaler" {
  name                 = "${var.cluster_name}-cluster-autoscaler"
  path                 = local.path
  permissions_boundary = local.boundary
  assume_role_policy   = local.pods_trust
}

resource "aws_iam_policy" "autoscaler" {
  name = "${var.cluster_name}-cluster-autoscaler"
  path = local.path
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "autoscaling:DescribeAutoScalingGroups", "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations", "autoscaling:DescribeScalingActivities",
          "autoscaling:DescribeTags", "ec2:DescribeImages", "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplateVersions", "ec2:GetInstanceTypesFromInstanceRequirements",
          "eks:DescribeNodegroup",
        ]
        Resource = "*"
      },
      {
        Effect    = "Allow"
        Action    = ["autoscaling:SetDesiredCapacity", "autoscaling:TerminateInstanceInAutoScalingGroup"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned" } }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "autoscaler" {
  role       = aws_iam_role.autoscaler.name
  policy_arn = aws_iam_policy.autoscaler.arn
}

# --- The stages ---------------------------------------------------------------------------------------------------

resource "aws_iam_role" "stage" {
  for_each             = local.stage_access
  name                 = "${var.cluster_name}-${each.key}"
  path                 = local.path
  permissions_boundary = local.boundary
  assume_role_policy   = local.pods_trust
}

resource "aws_iam_policy" "stage" {
  for_each = local.stage_access
  name     = "${var.cluster_name}-${each.key}"
  path     = local.path
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [for b in concat(each.value.rw_buckets, each.value.ro_buckets) : local.bucket_arn[b]]
      }],
      length(each.value.ro_buckets) == 0 ? [] : [{
        Sid      = "ReadObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [for b in each.value.ro_buckets : "${local.bucket_arn[b]}/*"]
      }],
      [{
        Sid    = "ReadWriteObjects"
        Effect = "Allow"
        # PutObject with If-None-Match / If-Match is how the bucket intake claims and the table commits fence.
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"]
        Resource = [for b in each.value.rw_buckets : "${local.bucket_arn[b]}/*"]
      }],
      [{
        Sid      = "Keks"
        Effect   = "Allow"
        Action   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = [for k in each.value.keks : local.kek_arn[k]]
      }],
      [{
        Sid      = "OwnSigningKey"
        Effect   = "Allow"
        Action   = ["kms:Sign", "kms:GetPublicKey", "kms:DescribeKey"]
        Resource = [for k in each.value.signing : local.sign_arn[k]]
      }],
      [{
        # Every stage checks the others' signatures.
        Sid      = "OthersPublicKeys"
        Effect   = "Allow"
        Action   = ["kms:GetPublicKey", "kms:Verify"]
        Resource = values(local.sign_arn)
      }],
      [{
        # S3 encrypts the bronze, silver and gold objects under the infra key (SSE-KMS).
        Sid       = "BucketEncryption"
        Effect    = "Allow"
        Action    = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource  = aws_kms_key.infra.arn
        Condition = { StringEquals = { "kms:ViaService" = "s3.${var.region}.amazonaws.com" } }
      }],
    )
  })
}

resource "aws_iam_role_policy_attachment" "stage" {
  for_each   = local.stage_access
  role       = aws_iam_role.stage[each.key].name
  policy_arn = aws_iam_policy.stage[each.key].arn
}
