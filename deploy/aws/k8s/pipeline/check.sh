#!/usr/bin/env bash
# The PoC's smoke test (docs/AWS-POC.md §10, step 5), as scripts/sandbox.sh check: puts the synthetic and
# de-identified sample C-CDA documents (connector-core/src/test/resources/ccda) in the intake bucket, under
# <pipeline>/, and waits until each is landed, split, converted and has every record ended at the transform stage.
# Then checks that the intake prefix is empty again, and that no stage's log holds document content or a key
# (filename). Prints counts and IDs only. Needs a session (deploy/aws/poc.sh up).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's
ACCOUNT=493469777520
P="${1:-ccda-push}"
N="sdh-$P"; PREFIX="${P//-/_}"
INTAKE="s3://sdh-poc-intake-$ACCOUNT/$P/"

say() { printf '>> %s\n' "$*"; }
infra() { kubectl -n sdh-infra "$@"; }
db() { infra exec postgres-0 -- psql -U postgres -d sdh -At -c "$1"; }
audited() {  # <namespace> <app> <event pattern>: audit events logged by the app's pods since they started
  kubectl -n "$1" logs -l "app=$2" --tail=-1 --max-log-requests=10 2>/dev/null | grep -cE "(^|[] ])$3( |$)" || true
}
topic_count() {
  infra exec kafka-0 -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" \
    2>/dev/null | awk -F: '{sum += $3} END {print sum + 0}'
}
wait_for() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do ((SECONDS < deadline)) || return 1; sleep 5; done
}

samples=("$ROOT"/connector-core/src/test/resources/ccda/*.xml)
count=${#samples[@]}
# Items ended at the transform stage: every record mapped to a bundle in gold, or kept as it came in the fail
# store. From its registry, so a pod scaled away isn't missed.
ended_sql="FROM transform_$PREFIX.stage_item WHERE expected IS NOT NULL AND NOT upstream_open AND done >= expected"
ended() { db "SELECT count(*) $ended_sql" 2>/dev/null || echo 0; }

declare -A before
for t in landed split records fhir; do before[$t]="$(topic_count "$P.$t")"; done
ended_before="$(ended)"; landed_before="$(audited "$N" landing "item\.landed")"
split_before="$(audited "$N" splitter "item\.completed")"
converted_before="$(audited "$N" convert "stage\.item_completed")"

say "Putting $count synthetic and de-identified C-CDA documents in $INTAKE"
run="smoke-$(date -u +%Y%m%dT%H%M%SZ)"
for i in "${!samples[@]}"; do
  # Named by number, not by the sample's name: a key is never logged, but it is the producer's to choose.
  aws s3 cp --only-show-errors "${samples[$i]}" "$INTAKE$run-$i.xml"
done
failures=0
step() {  # <description> <seconds> <command>...
  local what="$1" secs="$2"; shift 2
  if wait_for "$secs" "$@"; then say "  $what"; else say "  FAIL: $what"; failures=1; fi
}
at_least() { [[ "$(audited "$N" "$1" "$2")" -ge $(($3 + count)) ]]; }
all_ended() { [[ "$(ended)" -ge $((ended_before + count)) ]]; }
intake_empty() { [[ -z "$(aws s3 ls "$INTAKE$run-" 2>/dev/null)" ]]; }
step "all $count landed (item.landed)" 300 at_least landing "item\.landed" "$landed_before"
step "the intake prefix is empty again (each object deleted once landed)" 120 intake_empty
step "all $count split (item.completed)" 300 at_least splitter "item\.completed" "$split_before"
step "all $count converted (stage.item_completed)" 300 at_least convert "stage\.item_completed" "$converted_before"
step "all $count transformed (every record ended)" 600 all_ended
IFS='|' read -r expected done failed <<<"$(db "SELECT coalesce(sum(expected),0), coalesce(sum(done),0),
  coalesce(sum(failed),0) $ended_sql")"
say "  transform stage, all items so far: $expected records, $((done - failed)) mapped to a bundle," \
  "$failed kept as they came"
for t in landed split records fhir; do
  say "  $P.$t: $(($(topic_count "$P.$t") - ${before[$t]})) more messages"
done
for where in "$N" sdh-fhir; do
  for pod in $(kubectl -n "$where" get pods -o name); do
    if kubectl -n "$where" logs "$pod" --all-containers 2>/dev/null | grep -qE "ClinicalDocument|$run|\.xml"; then
      say "  FAIL: $pod's log holds document content or an object key"; failures=1
    fi
  done
done
kubectl -n "$N" get pods -o wide; kubectl -n sdh-fhir get pods -o wide
exit "$failures"
