#!/usr/bin/env bash
# Applies the sdh-poc IAM policies to claude-poc-deployer, as an admin.
#
# Usage: deploy/aws/iam/apply-admin.sh [profile]
#   profile: an admin profile from `aws login --profile poc-admin` (the default).
#
# Safe to run more than once:
#   - existing policies get a new version only if their file changed;
#   - attachments, Block Public Access and the budget are skipped if already in place.
set -euo pipefail

PROFILE="${1:-poc-admin}"
ACCT=493469777520
DEPLOYER=claude-poc-deployer
PATH_PREFIX=/sdh-poc-admin/
POLICIES=(sdh-poc-boundary sdh-poc-deployer-infra sdh-poc-deployer-iam)
DIR="$(cd "$(dirname "$0")" && pwd)"
export AWS_PROFILE="$PROFILE" AWS_REGION=us-east-2 AWS_PAGER=""

step() { printf '\n== %s\n' "$*"; }
arn()  { echo "arn:aws:iam::$ACCT:policy${PATH_PREFIX}$1"; }

step "1. Who am I"
CALLER=$(aws sts get-caller-identity --query Arn --output text) || {
  echo "No credentials for profile '$PROFILE'. Run: aws login --profile $PROFILE" >&2; exit 1; }
echo "$CALLER"
case "$CALLER" in
  "arn:aws:iam::$ACCT:root")          echo "Refusing to run as root." >&2; exit 1 ;;
  *":user/$DEPLOYER")                 echo "This is the deployer, not an admin." >&2; exit 1 ;;
  "arn:aws:"*":$ACCT:"*)              ;;
  *)                                  echo "Not account $ACCT." >&2; exit 1 ;;
esac

step "2. Validate the policies with Access Analyzer"
BAD=0
for p in "${POLICIES[@]}"; do
  out=$(aws accessanalyzer validate-policy --policy-type IDENTITY_POLICY \
        --policy-document "file://$DIR/$p.json" \
        --query 'findings[].[findingType,issueCode,locations[0].path[0].value]' --output text)
  if [ -z "$out" ]; then echo "$p: no findings"; else echo "$p:"; echo "$out" | sed 's/^/   /'; fi
  if echo "$out" | grep -q -E '^(ERROR|SECURITY_WARNING)'; then BAD=1; fi
done
if [ "$BAD" = 1 ]; then echo "Stopping: errors or security warnings above. Nothing was changed." >&2; exit 1; fi

same_doc() {  # $1 policy arn, $2 file: true if the default version equals the file
  local v; v=$(aws iam get-policy --policy-arn "$1" --query Policy.DefaultVersionId --output text)
  aws iam get-policy-version --policy-arn "$1" --version-id "$v" --query PolicyVersion.Document --output json \
    | python3 -I -c 'import json,sys; a=json.load(sys.stdin); b=json.load(open(sys.argv[1])); sys.exit(0 if a==b else 1)' "$2"
}

step "3. Create or update the policies (boundary first)"
for p in "${POLICIES[@]}"; do
  a=$(arn "$p")
  if aws iam get-policy --policy-arn "$a" >/dev/null 2>&1; then
    if same_doc "$a" "$DIR/$p.json"; then echo "$p: already up to date"; continue; fi
    # A policy keeps at most 5 versions: drop the oldest non-default one if full.
    n=$(aws iam list-policy-versions --policy-arn "$a" --query 'length(Versions)' --output text)
    if [ "$n" -ge 5 ]; then
      old=$(aws iam list-policy-versions --policy-arn "$a" \
            --query 'sort_by(Versions[?IsDefaultVersion==`false`],&CreateDate)[0].VersionId' --output text)
      aws iam delete-policy-version --policy-arn "$a" --version-id "$old"
    fi
    aws iam create-policy-version --policy-arn "$a" --policy-document "file://$DIR/$p.json" --set-as-default \
      --query PolicyVersion.VersionId --output text | sed "s/^/$p: new version /"
  else
    aws iam create-policy --path "$PATH_PREFIX" --policy-name "$p" --policy-document "file://$DIR/$p.json" \
      --tags Key=Project,Value=sdh-poc --query Policy.Arn --output text | sed 's/^/created /'
  fi
done

step "4. Attach the deployer policies to $DEPLOYER"
for p in sdh-poc-deployer-infra sdh-poc-deployer-iam; do
  aws iam attach-user-policy --user-name "$DEPLOYER" --policy-arn "$(arn "$p")"
  echo "$p: attached"
done

step "5. What $DEPLOYER now has"
aws iam list-attached-user-policies --user-name "$DEPLOYER" --output table
aws iam list-user-policies --user-name "$DEPLOYER" --output table
aws iam list-groups-for-user --user-name "$DEPLOYER" --query 'Groups[].GroupName' --output table

step "6. S3 Block Public Access for the account"
bpa=$(aws s3control get-public-access-block --account-id "$ACCT" \
      --query 'PublicAccessBlockConfiguration.[BlockPublicAcls,IgnorePublicAcls,BlockPublicPolicy,RestrictPublicBuckets]' \
      --output text 2>/dev/null || echo none)
if [ "$(echo "$bpa" | tr -s '\t ' '\n' | grep -c -x True)" = 4 ]; then
  echo "already on"
else
  aws s3control put-public-access-block --account-id "$ACCT" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  echo "turned on (was: $bpa)"
fi

step "7. Budget alarm, \$60/month: email at 50% and 100% of actual spend, and 100% of forecast"
if aws budgets describe-budget --account-id "$ACCT" --budget-name sdh-poc-monthly >/dev/null 2>&1; then
  echo "sdh-poc-monthly already exists"
else
  read -r -p "Email for budget alerts (empty to skip): " EMAIL
  if [ -n "$EMAIL" ]; then
    # Another run may have made it while this one waited at the prompt.
    if out=$(aws budgets create-budget --account-id "$ACCT" \
      --budget '{"BudgetName":"sdh-poc-monthly","BudgetLimit":{"Amount":"60","Unit":"USD"},"TimeUnit":"MONTHLY","BudgetType":"COST"}' \
      --notifications-with-subscribers "[{\"Notification\":{\"NotificationType\":\"ACTUAL\",\"ComparisonOperator\":\"GREATER_THAN\",\"Threshold\":50,\"ThresholdType\":\"PERCENTAGE\"},\"Subscribers\":[{\"SubscriptionType\":\"EMAIL\",\"Address\":\"$EMAIL\"}]},{\"Notification\":{\"NotificationType\":\"ACTUAL\",\"ComparisonOperator\":\"GREATER_THAN\",\"Threshold\":100,\"ThresholdType\":\"PERCENTAGE\"},\"Subscribers\":[{\"SubscriptionType\":\"EMAIL\",\"Address\":\"$EMAIL\"}]},{\"Notification\":{\"NotificationType\":\"FORECASTED\",\"ComparisonOperator\":\"GREATER_THAN\",\"Threshold\":100,\"ThresholdType\":\"PERCENTAGE\"},\"Subscribers\":[{\"SubscriptionType\":\"EMAIL\",\"Address\":\"$EMAIL\"}]}]" 2>&1); then
      echo "created"
    elif echo "$out" | grep -q DuplicateRecord; then
      echo "sdh-poc-monthly already exists (made by another run)"
    else
      echo "$out" >&2; exit 1
    fi
  else
    echo "skipped"
  fi
fi

step "Done"
echo "To sign the admin profile out: aws logout --profile $PROFILE"
