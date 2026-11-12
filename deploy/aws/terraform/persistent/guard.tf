# The auto-shutdown guard. poc.sh up creates a one-off EventBridge Scheduler schedule, sdh-poc-auto-down, set to
# the session's end (8 hours at most, unless a resilience or performance test asks for more). When it fires, it
# runs this state machine, which deletes the node groups and then the EKS cluster, so a forgotten session stops
# costing money even when this laptop is off or asleep.
#
# What it removes:
#   - the cluster and its nodes, and so everything running in them;
#   - not the state volumes, buckets, keys or images, which belong to this layer.
# Postgres and Kafka are stopped without warning, as a crash would. The pipeline is built to recover from that
# (leases, claims, exactly-once registries). poc.sh down, the normal way, drains first and snapshots the volumes.
#
# A Standard workflow costs $0.025 per 1,000 state transitions, so nothing while it isn't running.

resource "aws_iam_role" "guard" {
  name                 = "${var.cluster_name}-auto-down"
  path                 = local.path
  permissions_boundary = local.boundary
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = var.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "guard" {
  name = "delete-poc-cluster"
  role = aws_iam_role.guard.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["eks:ListNodegroups", "eks:DescribeCluster", "eks:DeleteNodegroup", "eks:DeleteCluster"]
      Resource = [
        "arn:aws:eks:${var.region}:${var.account_id}:cluster/${var.cluster_name}",
        "arn:aws:eks:${var.region}:${var.account_id}:nodegroup/${var.cluster_name}/*/*",
      ]
    }]
  })
}

resource "aws_sfn_state_machine" "guard" {
  name     = "${var.cluster_name}-auto-down"
  role_arn = aws_iam_role.guard.arn
  type     = "STANDARD"
  definition = jsonencode({
    Comment        = "Delete the sdh-poc EKS cluster: its node groups first, then the cluster."
    TimeoutSeconds = 3600
    StartAt        = "ListNodegroups"
    States = {
      ListNodegroups = {
        Type       = "Task"
        Resource   = "arn:aws:states:::aws-sdk:eks:listNodegroups"
        Parameters = { ClusterName = var.cluster_name }
        ResultPath = "$.ng"
        Catch      = [{ ErrorEquals = ["Eks.ResourceNotFoundException"], Next = "Gone" }]
        Next       = "AnyNodegroups"
      }
      AnyNodegroups = {
        Type    = "Choice"
        Choices = [{ Variable = "$.ng.Nodegroups[0]", IsPresent = true, Next = "DeleteNodegroups" }]
        Default = "DeleteCluster"
      }
      DeleteNodegroups = {
        Type       = "Map"
        ItemsPath  = "$.ng.Nodegroups"
        ResultPath = null
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "DeleteNodegroup"
          States = {
            DeleteNodegroup = {
              Type       = "Task"
              Resource   = "arn:aws:states:::aws-sdk:eks:deleteNodegroup"
              Parameters = { ClusterName = var.cluster_name, "NodegroupName.$" = "$" }
              # Already being deleted on an earlier pass.
              Catch = [{ ErrorEquals = ["States.ALL"], Next = "Skip" }]
              End   = true
            }
            Skip = { Type = "Pass", End = true }
          }
        }
        Next = "WaitForNodegroups"
      }
      WaitForNodegroups = { Type = "Wait", Seconds = 60, Next = "ListNodegroups" }
      DeleteCluster = {
        Type       = "Task"
        Resource   = "arn:aws:states:::aws-sdk:eks:deleteCluster"
        Parameters = { Name = var.cluster_name }
        Retry = [{
          ErrorEquals = ["Eks.ResourceInUseException"], IntervalSeconds = 60, MaxAttempts = 10, BackoffRate = 1
        }]
        Catch = [{ ErrorEquals = ["Eks.ResourceNotFoundException"], Next = "Gone" }]
        End   = true
      }
      Gone = { Type = "Succeed" }
    }
  })
}

# EventBridge Scheduler assumes this role to start the state machine.
resource "aws_iam_role" "scheduler" {
  name                 = "${var.cluster_name}-scheduler"
  path                 = local.path
  permissions_boundary = local.boundary
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = var.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "start-auto-down"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.guard.arn
    }]
  })
}
