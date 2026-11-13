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
# SDH_LOAD_OUTAGE_AT, _MIN and _LOSE add an outage part way (below, and docs/PERFORMANCE.md §3.6); the report then
# checks every item ended at every stage, and counts what the reconcilers rebuilt.
#
# At the end it measures the run (measure.sh: time per item to gold, where it went, the rate as the tables grew,
# commits, KMS calls, cost), then runs the gold check; SDH_LOAD_GOLD_CHECK=0 skips the check, whose reading is as
# slow as split's on a large load (docs/PERFORMANCE.md §2.1).
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
# The outage (docs/PERFORMANCE.md §3.6): at OUTAGE_AT % landed, every stage, Postgres and Kafka are killed at once,
# with no graceful stop; they stay down OUTAGE_MIN minutes. OUTAGE_LOSE names topics whose queued messages are
# deleted meanwhile (landed,split,records), as if the outage had outlasted the brokers' retention: the reconcilers
# must then rebuild them, after their backlog wait, so the test waits SETTLE_MIN more before it can end.
OUTAGE_AT="${SDH_LOAD_OUTAGE_AT:-0}"
OUTAGE_MIN="${SDH_LOAD_OUTAGE_MIN:-10}"
OUTAGE_LOSE="${SDH_LOAD_OUTAGE_LOSE:-}"
SETTLE_MIN="${SDH_LOAD_SETTLE_MIN:-$([[ -n "$OUTAGE_LOSE" ]] && echo 45 || echo 0)}"
GOLD_CHECK="${SDH_LOAD_GOLD_CHECK:-1}"

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
  [[ "${outage_state:-}" == down ]] && { outage_restore || true; }
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
echo "$t0_db" > "$out/t0"   # for measure.sh
for t in "${topics[@]}"; do echo "$t ${before[$t]}"; done > "$out/offsets0"   # for measure.sh
db "SELECT catalog_name || ':' || table_namespace || '.' || table_name, metadata_location FROM public.iceberg_tables" \
  | sed -E 's#\|.*/0*([0-9]+)-[^/]*\.metadata\.json$# \1#' > "$out/versions0" || true   # for measure.sh

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

# The outage: KEDA paused at 0 and every pod killed, then Postgres and Kafka killed (their StatefulSets start them
# again at once, as after a crash; the stages stay down until the outage ends).
outage_state="" outage_end=0 outage_landed="" restored_at="" settle_until=0
outage_start() {
  local ns
  for ns in "$N" sdh-fhir; do
    kubectl -n "$ns" annotate scaledobject --all autoscaling.keda.sh/paused-replicas=0 --overwrite >/dev/null
    kubectl -n "$ns" scale deploy --all --replicas=0 >/dev/null
    kubectl -n "$ns" delete pods --all --grace-period=0 --force >/dev/null 2>&1 || true
  done
  infra delete pod postgres-0 kafka-0 --grace-period=0 --force >/dev/null 2>&1 || true
  infra wait --for=condition=Ready pod/postgres-0 pod/kafka-0 --timeout=600s >/dev/null \
    || die "Postgres or Kafka didn't come back after the outage's kill"
}
# Deletes every queued message of a topic (up to its end): what retention would do to a backlog it outlasted.
lose_messages() {
  local topic="$P.$1" parts json
  parts="$(kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$topic" \
    | awk '/PartitionCount/ {for (i = 1; i <= NF; i++) if ($i == "PartitionCount:") print $(i + 1)}')"
  json="$(python3 -I -c 'import json, sys; print(json.dumps({"version": 1, "partitions": [
    {"topic": sys.argv[1], "partition": i, "offset": -1} for i in range(int(sys.argv[2]))]}))' "$topic" "$parts")"
  printf '%s' "$json" | kafka sh -c 'cat > /tmp/sdh-lose.json && /opt/kafka/bin/kafka-delete-records.sh \
    --bootstrap-server localhost:9092 --offset-json-file /tmp/sdh-lose.json >/dev/null && rm /tmp/sdh-lose.json'
}
outage_restore() {
  local ns
  kubectl -n "$N" scale deploy landing --replicas=2 >/dev/null
  for ns in "$N" sdh-fhir; do
    kubectl -n "$ns" annotate scaledobject --all autoscaling.keda.sh/paused-replicas- >/dev/null
  done
}
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

  # The outage: down for OUTAGE_MIN minutes, losing OUTAGE_LOSE's queued messages, then started again.
  if [[ -z "$outage_state" && "$OUTAGE_AT" != 0 ]] && ((landed * 100 >= files * OUTAGE_AT)); then
    outage_landed=$landed
    say "  outage: killing every stage, Postgres and Kafka at $landed landed, for $OUTAGE_MIN minutes"
    outage_start
    for t in ${OUTAGE_LOSE//,/ }; do
      lose_messages "$t" && say "  outage: deleted the queued messages of $P.$t" \
        || say "  outage: couldn't delete the queued messages of $P.$t"
    done
    outage_state=down outage_end=$((SECONDS + OUTAGE_MIN * 60))
  fi
  if [[ "$outage_state" == down ]] && ((SECONDS >= outage_end)); then
    outage_restore; outage_state=restored restored_at=$SECONDS settle_until=$((SECONDS + SETTLE_MIN * 60))
    say "  outage: started again after $OUTAGE_MIN minutes${SETTLE_MIN:+; waiting at least $SETTLE_MIN more for the reconcilers}"
  fi

  state="$in|$landed|$split|$c_done|$t_done|$t_failed|$l"
  if [[ -n "$pushed_at" && "$in" == 0 && "$l" == 0 && "$state" == "$last" ]]; then stable=$((stable + 1))
  else stable=0; fi
  # Nothing moving is the end only when no outage is under way or settling.
  if [[ "$outage_state" == down ]] || ((SECONDS < settle_until)); then stable=0; fi
  last="$state"
  ((stable >= 3)) && break   # 45 s with nothing moving
  ((now < (TIMEOUT_MIN + OUTAGE_MIN * (OUTAGE_AT != 0) + SETTLE_MIN) * 60)) || { say "  TIMEOUT after $TIMEOUT_MIN minutes: reporting what's done"; break; }
done
end=$(( ${last_change[transformed]:-$((SECONDS - t0))} ))
db "SELECT now()" > "$out/t1"; echo "$max_nodes" > "$out/nodes"   # for measure.sh

# --- Report ------------------------------------------------------------------------------------------------------

per_min() { python3 -I -c 'import sys; n, s = float(sys.argv[1]), float(sys.argv[2]); print("%.1f" % (n * 60 / s) if s else "-")' "$1" "$2"; }
say ""
say "=== Report: run $run (pipeline $P) ==="
say "Pushed: $files objects, $bytes bytes; all in the intake after ${pushed_at:-?}s (server-side copy)"
# Items, not messages: a landing node that takes the lease announces the last item landed again (STAGES.md §13.2),
# and the split stage drops the repeat, so a failover adds a .landed message but no item.
items="$(db "SELECT count(DISTINCT item_id) FROM split_$PREFIX.split_item WHERE updated_at >= '$t0_db'" 2>/dev/null || echo '?')"
say "Landed: $items items for $files objects (exactly once: $([[ "$items" == "$files" ]] && echo yes || echo NO));" \
  "$landed $P.landed messages (one more per failover: the last item announced again)"
# Every item split must have ended at convert, and every item convert ended must have ended at transform: what
# an outage could leave behind (docs/PERFORMANCE.md §3.6). 0 and 0 when nothing was. Ended, as in ended(): every
# record ended, failed or not; completed_at is set only when none failed, so it can't be the test. An item with a
# failure open upstream stays open downstream until the failure is retried (STAGES.md, the fail store), so its
# total counts as final there once every record it got has ended.
not_ended() { echo "NOT EXISTS (SELECT 1 FROM $1.stage_item b WHERE b.item_id = a.item_id
  AND b.expected IS NOT NULL AND (NOT b.upstream_open OR $2) AND b.done >= b.expected)"; }
run_items="SELECT item_id FROM split_$PREFIX.split_item WHERE updated_at >= '$t0_db'"
split_behind="$(db "SELECT count(*) FROM split_$PREFIX.split_item a WHERE a.updated_at >= '$t0_db'
  AND a.state = 'COMPLETED' AND $(not_ended "convert_$PREFIX" false)" 2>/dev/null || echo '?')"
convert_behind="$(db "SELECT count(*) FROM convert_$PREFIX.stage_item a WHERE a.item_id IN ($run_items)
  AND a.failed < a.done AND $(not_ended "transform_$PREFIX" "a.failed > 0")" 2>/dev/null || echo '?')"
say "Items left behind: split but not ended at convert $split_behind; converted (with a record sent on)" \
  "but not ended at transform $convert_behind (expected 0 and 0)"
if [[ -n "$outage_state" ]]; then
  say "Outage: at $outage_landed landed, down $OUTAGE_MIN minutes, messages lost: ${OUTAGE_LOSE:-none};" \
    "$((${last_change[transformed]:-0} - (restored_at - t0)))s from the restart to the last item transformed"
  say "  what the reconcilers did (audit events, all pods since the restart):"
  for e in stage.records_recovered stage.item_completion_recovered stage.item_total_missing \
      stage.placement_unknown stage.reconcile_deferral_capped split.recovered; do
    c=0
    for ns in "$N" sdh-fhir; do
      for pod in $(kubectl -n "$ns" get pods -o name 2>/dev/null); do
        c=$((c + $(kubectl -n "$ns" logs "$pod" 2>/dev/null | grep -c "$e" || true)))
      done
    done
    say "    $e: $c"
  done
fi
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
say "Junk rejected (stage.message_rejected reason=MALFORMED, since t0; expected 3 each$([[ -n "$outage_state" ]] \
  && echo ', or fewer: counted from the pods'"'"' logs, and the outage killed the pods that read them')):"
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
say "=== Measured ==="
"$HERE/measure.sh" "$run" 2>&1 | tee -a "$report" || say "measure.sh failed (above); run it again by hand"
say ""
say "=== Gold check ==="
if [[ "$GOLD_CHECK" == 0 ]]; then say "skipped (SDH_LOAD_GOLD_CHECK=0)"; else
  "$HERE/gold-check.sh" 2>&1 | tee -a "$report"
fi
