#!/usr/bin/env bash
# The PoC's load test (docs/AWS-POC.md §10, step 6): pushes a load of C-CDA documents with no PHI through a pipeline,
# with inputs each stage must fail and handle, measures throughput, counts what each stage did and failed, and then
# runs the gold check (gold-check.sh) on the bundles at the end. Needs a session (deploy/aws/poc.sh up).
#
#   deploy/aws/k8s/pipeline/load-test.sh [copies] [error-copies]    # defaults 80 and 10: 2,450 objects, ~300 MB
#
# The inputs (load-inputs.py): mapping's 30 test documents, `copies` of each (made unique by a comment), and the
# file connector's demo error files, `error-copies` of each: a batch of 3, a batch with an unconvertible document
# (convert stage: CONVERT_XML_MALFORMED), a truncated batch (split stage: PARSE_UNBALANCED_ELEMENT), a UTF-16
# document (split stage: PARSE_XML_UNSUPPORTED_ENCODING) and an exact resend (landing: item.duplicate_content; split:
# a copy noted). Half the test documents fail the US Core set's checks (transform stage: TRANSFORM_CHECK_FAILED).
# On top, 3 messages no stage can open go on each of <p>.split and <p>.records (rejected and dropped by the convert
# and transform stages: stage.message_rejected reason=MALFORMED; a dead-letter queue is for messages that open), and the landing pod holding the lease is deleted part-way (its standby takes over).
#
# How: the inputs go to a staging prefix in the intake bucket first, then are copied server-side under <p>/ at t0,
# so the push isn't limited by this machine's upload. Progress every 15 s; done when the intake prefix is empty, no
# consumer group lags and the registries have stopped changing. The report (counts, codes and timings only) is
# printed and kept in $STATE/load/<run>/report.txt. The inputs are deleted at the end, locally and in S3.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
TRANSFORMER="${SDH_TRANSFORMER:-$(cd "$ROOT/.." && pwd)/healthcare-data-transformer}"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's
ACCOUNT=493469777520
COPIES="${1:-80}" ERROR_COPIES="${2:-10}"
P="${SDH_PIPELINES:-ccda-push}"; P="${P%% *}"
N="sdh-$P"; PREFIX="${P//-/_}"
BUCKET="s3://sdh-poc-intake-$ACCOUNT"
INTAKE="$BUCKET/$P/"
TIMEOUT_MIN="${SDH_LOAD_TIMEOUT_MIN:-120}"
FAILOVER_AT="${SDH_LOAD_FAILOVER_AT:-30}"   # % of objects landed when the lease holder is deleted; 0: never

say() { printf '>> %s\n' "$*" | tee -a "${report:-/dev/null}"; }
die() { printf 'load-test.sh: %s\n' "$*" >&2; exit 1; }
infra() { kubectl -n sdh-infra "$@"; }
db() { infra exec -i postgres-0 -- psql -U postgres -d sdh -At -F '|' -c "$1"; }
kafka() { infra exec -i kafka-0 -- "$@"; }
offsets() {  # <topic>: messages ever written
  kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}'
}
lag() {  # the three stages' consumer groups, summed
  kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe \
    --group "splitter-$P" --group "convert-$P" --group "transform-$P" 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ {sum += $6} END {print sum + 0}'
}
replicas() { kubectl -n "$1" get deploy "$2" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0; }
intake_count() { aws s3 ls "$INTAKE" 2>/dev/null | grep -c '\.xml$' || true; }
# Items ended at a stage (a stage registry's items whose records have all ended), and its records ended.
ended() { db "SELECT count(*), coalesce(sum(done),0), coalesce(sum(failed),0) FROM $1.stage_item
  WHERE expected IS NOT NULL AND NOT upstream_open AND done >= expected" 2>/dev/null || echo "0|0|0"; }
split_ended() { db "SELECT count(*) FROM split_$PREFIX.split_item WHERE ended_at IS NOT NULL" 2>/dev/null || echo 0; }

# --- Checks and inputs -------------------------------------------------------------------------------------------

for t in kubectl aws python3 envsubst; do command -v "$t" >/dev/null || die "missing tool: $t"; done
kubectl get ns "$N" >/dev/null || die "no session: run deploy/aws/poc.sh up"
docs="$TRANSFORMER/transform-stage/target/test-documents/documents"
[[ -d "$docs" ]] || die "no test documents at $docs: build the transformer (./mvnw -o package)"
[[ "$(intake_count)" == 0 ]] || die "$INTAKE isn't empty: wait for the pipeline, or look at the landing stage"

run="load-$(date -u +%Y%m%dT%H%M%SZ)"
out="$STATE/load/$run"
mkdir -p "$STATE/load"; chmod 700 "$STATE/load"
report="$out/report.txt"
staging="$BUCKET/staging/$run/"
cleanup() {
  aws s3 rm --recursive --only-show-errors "$staging" 2>/dev/null || true
  rm -rf "$out/inputs"
}
trap cleanup EXIT
python3 -I "$HERE/load-inputs.py" "$out/inputs" "$run" "$COPIES" "$ERROR_COPIES" "$docs" \
  "$ROOT/connector-core/src/test/resources/ccda"
mv "$out/inputs/manifest.json" "$out/manifest.json"
read -r files bytes documents < <(python3 -I -c 'import json, sys; m = json.load(open(sys.argv[1]))
print(m["files"], m["bytes"], len(m["clean"]))' "$out/manifest.json")
: > "$report"
say "Run $run: $files objects, $((bytes / 1048576)) MB ($COPIES copies of each of $documents test documents; $ERROR_COPIES of each of 5 error inputs)"
say "Staging them in $staging"
aws s3 cp --recursive --only-show-errors "$out/inputs/" "$staging"
rm -rf "$out/inputs"

# --- Before ------------------------------------------------------------------------------------------------------

topics=(landed split records fhir convert.dlq fhir.dlq)
declare -A before
for t in "${topics[@]}"; do before[$t]="$(offsets "$P.$t")"; done
IFS='|' read -r t_items0 t_done0 t_failed0 <<<"$(ended "transform_$PREFIX")"
IFS='|' read -r c_items0 c_done0 c_failed0 <<<"$(ended "convert_$PREFIX")"
s_items0="$(split_ended)"
t0_db="$(db "SELECT now()")"
t0_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
t0=$SECONDS

# --- Push --------------------------------------------------------------------------------------------------------

say "t0: copying them under $INTAKE (server-side)"
aws s3 cp --recursive --only-show-errors "$staging" "$INTAKE" &
copier=$!
say "Putting 3 messages no stage can open on each of $P.split and $P.records"
for t in split records; do
  printf "sdh-poc-load $run junk %s\n" 1 2 3 \
    | kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$P.$t" >/dev/null
done

# --- Watch -------------------------------------------------------------------------------------------------------

holder() {  # the running landing pod that has logged lease.acquired (with two, the standby that took over)
  local pod logs
  for pod in $(kubectl -n "$N" get pods -l app=landing --field-selector=status.phase=Running -o name); do
    [[ "$pod" == "${killed:-}" ]] && continue
    # Not `logs | grep -q`: grep's early exit is a SIGPIPE, which pipefail turns into a failure.
    logs="$(kubectl -n "$N" logs "$pod" 2>/dev/null || true)"
    if grep -qE 'lease\.acquired' <<<"$logs"; then echo "$pod"; return 0; fi
  done
  return 0
}
killed="" killed_at="" takeover=""
declare -A max_pods=([landing]=0 [splitter]=0 [convert]=0 ["transform-$P"]=0)
max_nodes=0 max_burst=0 pushed_at="" stable=0 last=""
declare -A last_change seen
printf '%6s %7s %6s %7s %9s %9s %6s %5s %s\n' secs intake landed split converted transform lag nodes pods \
  | tee -a "$report"
while :; do
  sleep 15
  now=$((SECONDS - t0))
  if [[ -z "$pushed_at" ]] && ! kill -0 "$copier" 2>/dev/null; then
    wait "$copier" || die "copying into the intake failed"
    pushed_at=$now; say "  all $files objects in the intake after ${now}s"
  fi
  in="$(intake_count)"
  landed=$(($(offsets "$P.landed") - before[landed]))
  split=$(($(split_ended) - s_items0))
  IFS='|' read -r c_items c_done c_failed <<<"$(ended "convert_$PREFIX")"
  IFS='|' read -r t_items t_done t_failed <<<"$(ended "transform_$PREFIX")"
  converted=$((c_items - c_items0)) transformed=$((t_items - t_items0))
  l="$(lag)"
  pods=""
  for d in landing splitter convert "transform-$P"; do
    ns="$N"; [[ "$d" == transform-* ]] && ns=sdh-fhir
    r="$(replicas "$ns" "$d")"; r="${r:-0}"
    ((r > ${max_pods[$d]})) && max_pods[$d]=$r
    pods+="${d%%-*}=$r "
  done
  nodes="$(kubectl get nodes --no-headers 2>/dev/null | grep -c ' Ready' || true)"
  burst="$(kubectl get nodes -l sdh-poc/pool=burst --no-headers 2>/dev/null | grep -c ' Ready' || true)"
  ((nodes > max_nodes)) && max_nodes=$nodes; ((burst > max_burst)) && max_burst=$burst
  printf '%6s %7s %6s %7s %9s %9s %6s %5s %s\n' "$now" "$in" "$landed" "$split" "$converted" "$transformed" "$l" \
    "$nodes" "$pods" | tee -a "$report"
  for k in landed split converted transformed; do
    [[ "${!k}" != "${seen[$k]:-}" ]] && { last_change[$k]=$now; seen[$k]=${!k}; }
  done

  # The landing failover: delete the lease holder part-way, then time its standby's takeover.
  if [[ -z "$killed" && "$FAILOVER_AT" != 0 ]] && ((landed * 100 >= files * FAILOVER_AT)); then
    killed="$(holder)"
    if [[ -n "$killed" ]]; then
      kubectl -n "$N" delete "$killed" --wait=false >/dev/null; killed_at=$SECONDS
      say "  failover: deleted the lease holder ($killed) at $landed landed"
    fi
  fi
  if [[ -n "$killed" && -z "$takeover" && -n "$(holder)" ]]; then
    takeover=$((SECONDS - killed_at)); say "  failover: $(holder) took the lease ${takeover}s later"
  fi

  state="$in|$landed|$split|$c_done|$t_done|$t_failed|$l"
  if [[ -n "$pushed_at" && "$in" == 0 && "$l" == 0 && "$state" == "$last" ]]; then stable=$((stable + 1))
  else stable=0; fi
  last="$state"
  ((stable >= 3)) && break   # 45 s with nothing moving
  ((now < TIMEOUT_MIN * 60)) || { say "  TIMEOUT after $TIMEOUT_MIN minutes: reporting what's done"; break; }
done
end=$(( ${last_change[transformed]:-$((SECONDS - t0))} ))

# --- Report ------------------------------------------------------------------------------------------------------

per_min() { python3 -I -c 'import sys; n, s = float(sys.argv[1]), float(sys.argv[2]); print("%.1f" % (n * 60 / s) if s else "-")' "$1" "$2"; }
say ""
say "=== Report: run $run (pipeline $P) ==="
say "Pushed: $files objects, $bytes bytes; all in the intake after ${pushed_at:-?}s (server-side copy)"
say "Landed: $landed items for $files objects (exactly once: $([[ "$landed" == "$files" ]] && echo yes || echo NO))"
copies="$(db "SELECT count(*) FROM split_$PREFIX.split_copy WHERE noted_at >= '$t0_db'" 2>/dev/null || echo '?')"
say "  resends noted as copies by the split stage: $copies (expected $ERROR_COPIES)"
say "Items ended: split $split, convert $converted, transform $transformed"
say "Records: convert $((c_done - c_done0)) ended ($((c_failed - c_failed0)) failed);" \
  "transform $((t_done - t_done0)) ended, $((t_done - t_done0 - t_failed + t_failed0)) mapped to a bundle," \
  "$((t_failed - t_failed0)) kept as they came"
say "Failures by stage and code, since t0 (fail stores):"
for s in split convert transform; do
  db "SELECT stage, error_code, count(*) FROM ${s}_$PREFIX.stage_failure WHERE failed_at >= '$t0_db'
      GROUP BY 1, 2 ORDER BY 1, 2" 2>/dev/null | sed 's/^/    /; s/|/  /g' | tee -a "$report" || true
done
say "  expected at least: split PARSE_UNBALANCED_ELEMENT $ERROR_COPIES, PARSE_XML_UNSUPPORTED_ENCODING" \
  "$ERROR_COPIES; convert CONVERT_XML_MALFORMED $ERROR_COPIES; transform TRANSFORM_CHECK_FAILED (the set's)"
say "Split items by state, since t0:"
db "SELECT state, count(*) FROM split_$PREFIX.split_item WHERE updated_at >= '$t0_db' GROUP BY 1 ORDER BY 1" \
  2>/dev/null | sed 's/^/    /; s/|/  /g' | tee -a "$report" || true
say "Messages written since t0:"
for t in "${topics[@]}"; do say "    $P.$t: $(($(offsets "$P.$t") - before[$t]))"; done
say "  expected: $P.convert.dlq and $P.fhir.dlq 0 (the junk is rejected, not dead-lettered)"
say "Junk rejected (stage.message_rejected reason=MALFORMED, since t0; expected 3 each):"
for d in "$N convert" "sdh-fhir transform-$P"; do
  read -r ns app <<<"$d"
  logs="$(kubectl -n "$ns" logs -l "app=$app" --tail=-1 --max-log-requests=10 --since-time="$t0_iso" 2>/dev/null || true)"
  say "    $app: $(grep -c 'stage\.message_rejected.*reason=MALFORMED' <<<"$logs" || true)"
done
say "Throughput (from t0 to each stage's last change):"
say "    end to end: ${end}s; $(per_min "$transformed" "$end") items/min," \
  "$(per_min "$((t_done - t_done0))" "$end") records/min, $(per_min "$((bytes / 1048576))" "$end") MB/min"
for k in landed split converted transformed; do
  say "    $k: ${!k} items by ${last_change[$k]:-?}s, $(per_min "${!k}" "${last_change[$k]:-0}") items/min"
done
say "Pods at most: landing ${max_pods[landing]} (1 active), splitter ${max_pods[splitter]}/2," \
  "convert ${max_pods[convert]}/3, transform ${max_pods[transform-$P]}/4; nodes $max_nodes, burst $max_burst"
if [[ -n "$killed" ]]; then
  say "Landing failover: holder deleted at ${FAILOVER_AT}% landed; standby took the lease after ${takeover:-NEVER}s"
fi
say "Report kept in $report"

say ""
say "=== Gold check ==="
"$HERE/gold-check.sh" 2>&1 | tee -a "$report"
