#!/usr/bin/env bash
# Checks the account's EC2 vCPU quotas in us-east-2 and, if you agree, asks AWS to raise the Spot one, as an admin.
#
# Usage: deploy/aws/iam/spot-quota.sh [profile]
#   profile: an admin profile from `aws login --profile poc-admin` (the default).
#
# Why: on 2026-11-12 the PoC's Spot burst node group (0 to 2 nodes, each 2 vCPUs: t4g, m7g or m6g .large) failed
# every launch with UnfulfillableCapacity, though Spot was priced in us-east-2a. A new account's Spot vCPU quota
# is often too small for even one node. The deployer can't read Service Quotas, so an admin runs this.
#
# It changes nothing unless you answer y: then it files one quota increase request, which AWS reviews (often
# approved within minutes for small amounts, sometimes a support case). Raising a quota costs nothing by itself;
# the burst nodes are still capped at 2 by the node group and only run while a session is up.
set -euo pipefail

PROFILE="${1:-poc-admin}"
ACCT=493469777520
DEPLOYER=claude-poc-deployer
SPOT=L-34B43A08        # All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests, in vCPUs
ONDEMAND=L-1216C47A    # Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances, in vCPUs
# 2 base nodes on-demand (4 vCPUs) and up to 2 burst nodes on Spot (4 vCPUs). 8 leaves room for a node being
# replaced while its successor starts.
WANT_SPOT=8
WANT_ONDEMAND=8
export AWS_PROFILE="$PROFILE" AWS_REGION=us-east-2 AWS_PAGER=""

step() { printf '\n== %s\n' "$*"; }

step "1. Who am I"
CALLER=$(aws sts get-caller-identity --query Arn --output text) || {
  echo "No credentials for profile '$PROFILE'. Run: aws login --profile $PROFILE" >&2; exit 1; }
echo "$CALLER"
case "$CALLER" in
  "arn:aws:iam::$ACCT:root") echo "Refusing to run as root." >&2; exit 1 ;;
  *":user/$DEPLOYER")        echo "This is the deployer, not an admin." >&2; exit 1 ;;
  "arn:aws:"*":$ACCT:"*)     ;;
  *)                         echo "Not account $ACCT." >&2; exit 1 ;;
esac

quota() {  # <code>: the applied value, or AWS's default if the account has never changed it
  aws service-quotas get-service-quota --service-code ec2 --quota-code "$1" --query Quota.Value --output text \
    2>/dev/null || aws service-quotas get-aws-default-service-quota --service-code ec2 --quota-code "$1" \
    --query Quota.Value --output text
}
open_requests() {  # <code>: requests still being reviewed
  aws service-quotas list-requested-service-quota-change-history-by-quota --service-code ec2 --quota-code "$1" \
    --query "RequestedQuotas[?Status=='PENDING' || Status=='CASE_OPENED'].[Status,DesiredValue,Created]" --output text
}

step "2. The quotas in us-east-2 (vCPUs)"
spot=$(quota "$SPOT"); ondemand=$(quota "$ONDEMAND")
printf 'Spot (standard families):      %s   (the PoC wants %s)\n' "$spot" "$WANT_SPOT"
printf 'On-demand (standard families): %s   (the PoC wants %s)\n' "$ondemand" "$WANT_ONDEMAND"
spot_open=$(open_requests "$SPOT"); ondemand_open=$(open_requests "$ONDEMAND")
[ -z "$spot_open" ] || { echo "Spot request already open:"; echo "$spot_open" | sed 's/^/   /'; }
[ -z "$ondemand_open" ] || { echo "On-demand request already open:"; echo "$ondemand_open" | sed 's/^/   /'; }

ask_raise() {  # <name> <code> <current> <wanted> <open requests>
  if python3 -I -c 'import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)' "$3" "$4"; then
    echo "$1: $3 is enough; nothing to do."; return
  fi
  if [ -n "$5" ]; then echo "$1: a request is already open; wait for it."; return; fi
  read -r -p "$1: ask AWS to raise it from $3 to $4 vCPUs? [y/N] " yes
  if [ "$yes" = y ] || [ "$yes" = Y ]; then
    aws service-quotas request-service-quota-increase --service-code ec2 --quota-code "$2" --desired-value "$4" \
      --query 'RequestedQuota.[Id,Status]' --output text
    echo "$1: requested. AWS emails the account's address when it's decided; run this again to see the status."
  else
    echo "$1: left as it is."
  fi
}

step "3. Raise what's too small"
ask_raise "Spot" "$SPOT" "$spot" "$WANT_SPOT" "$spot_open"
ask_raise "On-demand" "$ONDEMAND" "$ondemand" "$WANT_ONDEMAND" "$ondemand_open"

step "Done"
echo "If the Spot quota was already $WANT_SPOT or more, the cause is elsewhere: tell Claude, with this output."
echo "To sign the admin profile out: aws logout --profile $PROFILE"
