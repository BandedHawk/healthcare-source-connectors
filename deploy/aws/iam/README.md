# IAM for the AWS proof of concept (docs/AWS-POC.md)

An account administrator applies these with `apply-admin.sh`, and again whenever a policy file changes (the
script adds a new policy version only where a file differs). Claude (as `claude-poc-deployer`) can then create and tear down the
PoC with Terraform.

## Files

| File | What it is |
|---|---|
| `sdh-poc-boundary.json` | The permissions boundary. Every role the deployer creates must carry it, and it caps what those roles can ever do: EC2, ELB and autoscaling in us-east-2, pulling images, `sdh-poc-*` buckets, KMS keys tagged `Project=sdh-poc`, logs and metrics, and (for the auto-down guard) deleting the `sdh-poc` cluster and starting its state machine. There is no IAM write access except service-linked roles. |
| `sdh-poc-deployer-infra.json` | The deployer's rights over the infrastructure: read-only in the region; full rights over `sdh-poc*` EKS clusters, S3 buckets, ECR repositories `sdh-poc/*` and logs; security groups, VPC endpoints, EBS volumes and snapshots it creates (tagged `Project=sdh-poc`); KMS keys it creates (tagged); the auto-down state machine and its schedule; cost and pricing read access; Access Analyzer validation. It is 6,038 of IAM's 6,144 characters: put new statements in a third policy. |
| `sdh-poc-deployer-iam.json` | IAM rights: roles and policies only under the path `/sdh-poc/`, only with the boundary attached, only the listed AWS managed policies, and PassRole only to EKS, EC2, EKS Pod Identity, Step Functions and EventBridge Scheduler. It also holds the guardrails, which are below. |

## Guardrails, all explicit denies

- **Nothing outside us-east-2,** except the global IAM, STS, Budgets, Pricing and Cost Explorer.
- **The deployer can't assume any `sdh-poc` role,** even if a trust policy names it. So creating a role can't give
  it more rights.
- **The deployer can't remove a role's boundary, or edit or delete the boundary policy.** The boundary lives under
  the path `/sdh-poc-admin/`, which the deployer can't write.
- **The deployer can't decrypt, generate data keys or sign with any KMS key,** except through EBS when it creates an
  encrypted volume. It can manage the PoC keys but never read the pipeline's data (the stages wrap every data key
  with KMS directly), even if it wrote itself into a key policy. KMS grants are allowed only when an AWS service
  (EKS, EBS) asks on its behalf.
- **The deployer can't create or change a KMS key policy with the lockout safety check bypassed.**

What this means for the design:

- The Terraform state bucket and the intake bucket use SSE-S3, not SSE-KMS, so the deployer can write state and
  upload synthetic test C-CDA there.
- The bronze, silver and gold buckets use SSE-KMS.
- The Postgres and Kafka EBS volumes are created by Terraform, encrypted with the tagged `sdh-poc-infra` key, not
  the AWS-managed `aws/ebs` key.

## Apply (as an administrator, from this directory)

```bash
ACCT=493469777520
```

1. Check the three files. `aws accessanalyzer validate-policy` should report no `ERROR` or `SECURITY_WARNING`
   findings for any of them.

```bash
for f in sdh-poc-boundary sdh-poc-deployer-infra sdh-poc-deployer-iam; do aws accessanalyzer validate-policy --policy-type IDENTITY_POLICY --policy-document file://$f.json --query 'findings[].[findingType,issueCode,findingDetails]' --output text; done
```

2. Create the boundary under the admin path.

```bash
aws iam create-policy --path /sdh-poc-admin/ --policy-name sdh-poc-boundary --policy-document file://sdh-poc-boundary.json
```

3. Create the two deployer policies.

```bash
aws iam create-policy --path /sdh-poc-admin/ --policy-name sdh-poc-deployer-infra --policy-document file://sdh-poc-deployer-infra.json
```

```bash
aws iam create-policy --path /sdh-poc-admin/ --policy-name sdh-poc-deployer-iam --policy-document file://sdh-poc-deployer-iam.json
```

4. Attach both deployer policies to the deployer user.

```bash
aws iam attach-user-policy --user-name claude-poc-deployer --policy-arn arn:aws:iam::$ACCT:policy/sdh-poc-admin/sdh-poc-deployer-infra
```

```bash
aws iam attach-user-policy --user-name claude-poc-deployer --policy-arn arn:aws:iam::$ACCT:policy/sdh-poc-admin/sdh-poc-deployer-iam
```

5. Recommended, if not already on: turn on S3 Block Public Access for the whole account. The deployer has
   `s3:*` on its own buckets, so this is what stops one being made public.

```bash
aws s3control put-public-access-block --account-id $ACCT --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

6. Recommended: a budget alarm at $60/month, alerting at 50% and 100% of actual spend and 100% of forecast
   (changed from $250 on 2026-11-12, after the PoC began shutting down when idle). Replace `you@example.com` with the address that should get the
   alerts. The deployer can then see the budget, but it is better owned by the admin.

```bash
aws budgets create-budget --account-id $ACCT --budget '{"BudgetName":"sdh-poc-monthly","BudgetLimit":{"Amount":"60","Unit":"USD"},"TimeUnit":"MONTHLY","BudgetType":"COST"}' --notifications-with-subscribers '[{"Notification":{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":50,"ThresholdType":"PERCENTAGE"},"Subscribers":[{"SubscriptionType":"EMAIL","Address":"you@example.com"}]},{"Notification":{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":100,"ThresholdType":"PERCENTAGE"},"Subscribers":[{"SubscriptionType":"EMAIL","Address":"you@example.com"}]},{"Notification":{"NotificationType":"FORECASTED","ComparisonOperator":"GREATER_THAN","Threshold":100,"ThresholdType":"PERCENTAGE"},"Subscribers":[{"SubscriptionType":"EMAIL","Address":"you@example.com"}]}]'
```

## Expected gaps

These policies are written from the AWS documentation and haven't been used yet. A few permissions may turn out
to be missing on the first `terraform apply`, for example:

- whether EKS managed node groups need anything more when created without a launch template;
- the exact ARN forms of EKS access entries.

Each such error names the missing action. It should be added to the right file, narrowly, and the policy
version updated with `aws iam create-policy-version ... --set-as-default`. Removing the boundary or the denies
is never the fix.

## The Spot quota

`spot-quota.sh` (as the admin, like `apply-admin.sh`) shows the account's Spot and on-demand vCPU quotas in
us-east-2 and, only if you answer `y`, requests 8 vCPUs where a quota is smaller. The PoC needs 4 on-demand (the two
base nodes) and up to 4 on Spot (the two burst nodes). A request costs nothing by itself; AWS emails the account's
address when it's decided.

```bash
deploy/aws/iam/spot-quota.sh
```
