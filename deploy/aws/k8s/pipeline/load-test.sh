#!/usr/bin/env bash
# The PoC's load test (docs/AWS-POC.md §10, step 6): pushes a load of C-CDA documents with no PHI through a pipeline,
# with inputs each stage must fail and handle, measures throughput, counts what each stage did and failed, and then
# runs the gold check (gold-check.sh) on the bundles at the end. Needs a session (deploy/aws/poc.sh up).
#
#   deploy/aws/k8s/pipeline/load-test.sh [copies] [error-copies]    # defaults 80 and 10: 2,450 objects, ~300 MB
#
# The inputs (load-inputs.py): mapping's 30 test documents, `copies` of each (made unique by an element), and the
# file connector's demo error files, `error-copies` of each: a batch of 3, a batch with an unconvertible document
# (convert stage: CONVERT_XML_MALFORMED), a truncated batch (split stage: PARSE_UNBALANCED_ELEMENT), a UTF-16
# document (split stage: PARSE_XML_UNSUPPORTED_ENCODING) and an exact resend (landing: item.duplicate_content; split:
# a copy noted). Half the test documents fail the US Core set's checks (transform stage: TRANSFORM_CHECK_FAILED).
# On top, 3 messages no stage can open go on each of <p>.split and <p>.records (rejected and dropped by the convert
# and transform stages: stage.message_rejected reason=MALFORMED; a dead-letter queue is for messages that open), and the landing pod holding the lease is deleted part-way (its standby takes over).
#
# SDH_LOAD_OUTAGE_AT, _MIN and _LOSE add an outage part way (below, and docs/PERFORMANCE.md §3.6); the report then
# checks every item ended at every stage, and counts what the reconcilers rebuilt.
# With no outage, items left behind are noted where they are, and waited for up to SDH_LOAD_RECOVER_MIN (40)
# minutes, for the reconcilers to end them (below).
# Every convert and transform pod's warnings, errors and events are kept as the run goes, in pod-logs/ (gzipped at
# the end), so a pod that stops part way leaves its log (STAGES.md §10, 2026-11-18). No content: they log none.
#
# What the run used is sampled every SDH_LOAD_SAMPLE_SEC (60) seconds (resources.py): each pod's and node's CPU and
# memory from the kubelets, Postgres' size, commits, cache hits and waits, and Kafka's bytes on disk by topic; the
# report gives each stage's and service's pods, CPU and memory against their requests and limits, min / avg / max.
#
# SDH_LOAD_KILLS (say "25:force:transform:1,30:graceful:transform:half") stops stage pods part way, at AWS's pod
# counts (docs/PERFORMANCE.md §10, as the sandbox's kill checks of 2026-11-18): each entry is the share of objects
# ended at transform, in %; force (deleted with no grace: killed mid-batch, as a crash) or graceful (deleted with
# their grace, as a Spot drain); the stage (splitter, convert or transform); and how many of its running pods,
# chosen at random, or half. The report lists what was stopped, and the claims and ends the stages took up again;
# items left behind and exactly once are its verdict, as for any run.
#
# SDH_LOAD_BACKLOG (say "1,2,4,6") is the backlog test (docs/PERFORMANCE.md §10): transform is held at no pods
# while the load lands, splits and converts, then cleared at each of those pod counts in turn (below).
#
# At the end it measures the run (measure.sh: time per item to gold, where it went, the rate as the tables grew,
# commits, KMS calls, cost), then runs the gold check; SDH_LOAD_GOLD_CHECK=0 skips the check, whose reading is as
# slow as split's on a large load (docs/PERFORMANCE.md §2.1).
#
# How: the inputs go to a staging prefix in the intake bucket first, then are copied server-side under <p>/ at t0,
# so the push isn't limited by this machine's upload. All at once by default; SDH_LOAD_RATE (objects a second) feeds
# them at that pace instead, a steady load (docs/PERFORMANCE.md §10): staged in slices of 100, one slice copied
# every 100/RATE seconds. A large load's upload can be done first, with no session up: SDH_LOAD_STAGE_ONLY=1 makes
# and stages the inputs and stops (no cluster needed, nothing deleted); SDH_LOAD_RUN=<run> then runs that staged
# load, with the same arguments, and deletes it at the end. Progress every 15 s; done when the intake prefix is empty, no
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
RATE="${SDH_LOAD_RATE:-}"                # objects a second at t0; empty: all at once
STAGE_ONLY="${SDH_LOAD_STAGE_ONLY:-0}"   # 1: make and stage the inputs, then stop
REUSE="${SDH_LOAD_RUN:-}"                # a run staged with SDH_LOAD_STAGE_ONLY=1
SLICE=100                                # objects a slice, with SDH_LOAD_RATE
[[ -z "$RATE" || "$RATE" =~ ^[1-9][0-9]*$ ]] || { echo "SDH_LOAD_RATE: objects a second, as 100" >&2; exit 2; }
P="${SDH_PIPELINES:-ccda-push}"; P="${P%% *}"
N="sdh-$P"; PREFIX="${P//-/_}"
BUCKET="s3://sdh-poc-intake-$ACCOUNT"
INTAKE="$BUCKET/$P/"
TIMEOUT_MIN="${SDH_LOAD_TIMEOUT_MIN:-120}"
KILLS="${SDH_LOAD_KILLS:-}"                # stage pods stopped part way: <% ended>:<force|graceful>:<stage>:<n|half>,...
for k in ${KILLS//,/ }; do
  [[ "$k" =~ ^[0-9]{1,2}:(force|graceful):(splitter|convert|transform):([1-9][0-9]*|half)$ ]] \
    || { echo "SDH_LOAD_KILLS: $k isn't <% ended>:<force|graceful>:<splitter|convert|transform>:<n|half>" >&2; exit 2; }
done
SAMPLE_SEC="${SDH_LOAD_SAMPLE_SEC:-60}"   # seconds between samples of what the run used (resources.py)
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
# Items left behind with no outage (runs 39 and 43 of docs/PERFORMANCE.md §10, as Spot nodes were reclaimed): the
# report notes where each is in the registries, then waits up to RECOVER_MIN minutes for the reconcilers to end them
# (a 10-minute grace, every 15 minutes: STAGES.md §9 and §13.2), and says how long they took; 0 doesn't wait.
RECOVER_MIN="${SDH_LOAD_RECOVER_MIN:-40}"
[[ "$RECOVER_MIN" =~ ^[0-9]+$ ]] || { echo "SDH_LOAD_RECOVER_MIN: minutes, as 40" >&2; exit 2; }
# The backlog test: once split and convert have nothing queued, transform is released at the first pod count of
# BACKLOG, the others held at one pod. Each count is held until its pods have all been ready for BACKLOG_WARM
# seconds and then BACKLOG_STEP more, its window; the next count follows, and the last stays until the backlog
# is clear. Each window's rate is records ended at transform a second, with transform's CPU over the same window.
BACKLOG="${SDH_LOAD_BACKLOG:-}"
BACKLOG_STEP="${SDH_LOAD_BACKLOG_STEP:-120}"
BACKLOG_WARM="${SDH_LOAD_BACKLOG_WARM:-30}"
[[ -z "$BACKLOG" || "$BACKLOG" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] || { echo "SDH_LOAD_BACKLOG: pod counts, as 1,2,4,6" >&2; exit 2; }

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
group_lag() {  # <consumer group>: its lag, summed over partitions
  kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" \
    2>/dev/null | awk '$6 ~ /^[0-9]+$/ {sum += $6} END {print sum + 0}'
}
prom_at() {  # <unix time> <PromQL>: one number then, or "-"
  kubectl -n sdh-infra exec deploy/prometheus -- wget -qO- \
    "http://localhost:9090/api/v1/query?time=$1&query=$(python3 -I -c \
      'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$2")" 2>/dev/null \
    | python3 -I -c 'import json, sys
try:
    r = json.load(sys.stdin)["data"]["result"]; v = float(r[0]["value"][1]); print("-" if v != v else "%.2f" % v)
except Exception:
    print("-")'
}
replicas() { kubectl -n "$1" get deploy "$2" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0; }
intake_count() { aws s3 ls "$INTAKE" 2>/dev/null | grep -c '\.xml$' || true; }
# Items ended at a stage (a stage registry's items whose records have all ended), and its records ended.
ended() { db "SELECT count(*), coalesce(sum(done),0), coalesce(sum(failed),0) FROM $1.stage_item
  WHERE expected IS NOT NULL AND NOT upstream_open AND done >= expected" 2>/dev/null || echo "0|0|0"; }
split_ended() { db "SELECT count(*) FROM split_$PREFIX.split_item WHERE ended_at IS NOT NULL" 2>/dev/null || echo 0; }

# --- Checks and inputs -------------------------------------------------------------------------------------------

for t in kubectl aws python3 envsubst; do command -v "$t" >/dev/null || die "missing tool: $t"; done
if [[ "$STAGE_ONLY" != 1 ]]; then
  kubectl get ns "$N" >/dev/null || die "no session: run deploy/aws/poc.sh up"
fi
docs="$TRANSFORMER/transform-stage/target/test-documents/documents"
[[ -d "$docs" ]] || die "no test documents at $docs: build the transformer (./mvnw -o package)"
if [[ "$STAGE_ONLY" != 1 ]]; then
  [[ "$(intake_count)" == 0 ]] || die "$INTAKE isn't empty: wait for the pipeline, or look at the landing stage"
fi

run="${REUSE:-load-$(date -u +%Y%m%dT%H%M%SZ)}"
out="$STATE/load/$run"
mkdir -p "$STATE/load"; chmod 700 "$STATE/load"
report="$out/report.txt"
staging="$BUCKET/staging/$run/"
cleanup() {
  declare -F stop_logs >/dev/null && stop_logs
  declare -F stop_sampler >/dev/null && stop_sampler
  [[ "${outage_state:-}" == down ]] && { outage_restore || true; }
  [[ -n "$BACKLOG" ]] && { backlog_restore || true; }
  [[ "$STAGE_ONLY" == 1 ]] || aws s3 rm --recursive --only-show-errors "$staging" 2>/dev/null || true
  rm -rf "$out/inputs"
}
trap cleanup EXIT
if [[ -n "$REUSE" ]]; then
  [[ -f "$out/manifest.json" ]] || die "no staged run $REUSE ($out/manifest.json)"
  [[ -z "$RATE" || -f "$out/slices" ]] || die "$REUSE wasn't staged in slices: stage it with SDH_LOAD_RATE"
  [[ -n "$RATE" || ! -f "$out/slices" ]] || die "$REUSE was staged in slices: run it with SDH_LOAD_RATE"
else
  python3 -I "$HERE/load-inputs.py" "$out/inputs" "$run" "$COPIES" "$ERROR_COPIES" "$docs" \
    "$ROOT/connector-core/src/test/resources/ccda"
  mv "$out/inputs/manifest.json" "$out/manifest.json"
  if [[ -n "$RATE" ]]; then
    # Slices of SLICE objects, in name order (a copy of each document, then the next copy), s00000/ on.
    python3 -I -c 'import os, sys
d, n = sys.argv[1], int(sys.argv[2])
names = sorted(f for f in os.listdir(d) if f.endswith(".xml"))
for i, f in enumerate(names):
    s = os.path.join(d, "s%05d" % (i // n))
    os.makedirs(s, exist_ok=True)
    os.rename(os.path.join(d, f), os.path.join(s, f))
print((len(names) + n - 1) // n)' "$out/inputs" "$SLICE" > "$out/slices"
  fi
fi
read -r files bytes documents < <(python3 -I -c 'import json, sys; m = json.load(open(sys.argv[1]))
print(m["files"], m["bytes"], len(m["clean"]))' "$out/manifest.json")
: > "$report"
say "Run $run: $files objects, $((bytes / 1048576)) MB ($COPIES copies of each of $documents test documents; $ERROR_COPIES of each of 5 error inputs)"
if [[ -z "$REUSE" ]]; then
  say "Staging them in $staging"
  aws s3 cp --recursive --only-show-errors "$out/inputs/" "$staging"
  rm -rf "$out/inputs"
fi
if [[ "$STAGE_ONLY" == 1 ]]; then
  say "Staged. Run it with SDH_LOAD_RUN=$run${RATE:+ SDH_LOAD_RATE=$RATE} and the same arguments"
  exit 0
fi

# --- Before ------------------------------------------------------------------------------------------------------

# The objects staged, by name (the test inputs' names, no PHI), so the report can name any the feed never had
# acknowledged (copy_slice). Listed before t0, so the feed isn't held up.
{ aws s3 ls --recursive "$staging" || true; } | awk '{print $4}' | sed 's#.*/##' | sort > "$out/staged"

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

# Each stage pod's log, followed from its start until it goes, warnings, errors and events only (STAGES.md §10,
# 2026-11-18): the only record of what a pod that stopped part way did. A loop looks for new pods every 15 s, and
# takes up only those running: a pod still starting has no log yet, so `logs -f` would end at once and the pod
# would never be looked at again (half of run 47's pods left no log).
mkdir -p "$out/pod-logs"
follow_logs() {
  local -A following=()
  while :; do
    for ns in "$N" sdh-fhir; do
      for pod in $(kubectl -n "$ns" get pods -o name --field-selector=status.phase=Running 2>/dev/null \
          | grep -E 'convert|transform|splitter|landing'); do
        [[ -n "${following[$ns/$pod]:-}" ]] && continue
        following[$ns/$pod]=1
        kubectl -n "$ns" logs -f --timestamps --all-containers "$pod" 2>/dev/null \
          | grep --line-buffered -E ' (WARN|ERROR) |sdh\.audit|stage\.|record\.|broker\.|lease\.' \
          >> "$out/pod-logs/${ns}_${pod#pod/}.log" &
      done
    done
    sleep 15
  done
}
follow_logs &
log_follower=$!
stop_logs() {
  [[ -n "${log_follower:-}" ]] || return 0
  # Each may find nothing left to stop; under set -e a bare failure here would end the EXIT trap, and the run,
  # with 1, and leave the staged copies in S3.
  kill "$log_follower" 2>/dev/null || true
  pkill -P "$log_follower" 2>/dev/null || true
  pkill -f "logs -f --timestamps --all-containers pod/" 2>/dev/null || true
  log_follower=""
  gzip -q "$out"/pod-logs/*.log 2>/dev/null || true
  return 0
}
pod_events() {  # <event>: how many lines name it, across every pod kept
  zcat -f "$out"/pod-logs/*.log* 2>/dev/null | grep -c -- "$1" || true
}
mkdir -p "$out/events"; : > "$out/pg-connections"
# What the run used (resources.py, docs/PERFORMANCE.md §10): every pod's and node's CPU and memory, Postgres' size,
# activity and waits, and Kafka's bytes by topic, every SAMPLE_SEC seconds, for min / avg / max in the report.
t0_epoch="$(date -u +%s)"
rm -f "$out/usage" "$out/pg-stats" "$out/kafka-disk" "$out/pg-schemas" "$out/pg-settings"
sample_resources() {
  while :; do
    python3 -I "$HERE/resources.py" sample "$out" "$t0_epoch" "$N" sdh-fhir sdh-infra 2>/dev/null || true
    sleep "$SAMPLE_SEC"
  done
}
sample_resources &
sampler=$!
stop_sampler() {
  [[ -n "${sampler:-}" ]] || return 0
  kill "$sampler" 2>/dev/null || true   # a sample under way finishes; its lines are whole
  pkill -P "$sampler" -f 'sleep' 2>/dev/null || true
  sampler=""
  return 0
}
for t in "${topics[@]}"; do echo "$t ${before[$t]}"; done > "$out/offsets0"   # for measure.sh
"$HERE/node-cpu.sh" > "$out/nodecpu0" 2>/dev/null || true   # for measure.sh: the T nodes' surplus CPU credits
db "SELECT catalog_name || ':' || table_namespace || '.' || table_name, metadata_location FROM public.iceberg_tables" \
  | sed -E 's#\|.*/0*([0-9]+)-[^/]*\.metadata\.json$# \1#' > "$out/versions0" || true   # for measure.sh

# The backlog test's holds (KEDA's paused-replicas sets a fixed count, past the ScaledObject's maximum too).
hold() {  # <namespace> <scaled object> <pods>
  kubectl -n "$1" annotate scaledobject "$2" autoscaling.keda.sh/paused-replicas="$3" --overwrite >/dev/null
  kubectl -n "$1" scale deploy "$2" --replicas="$3" >/dev/null
}
backlog_restore() {
  kubectl -n sdh-fhir annotate scaledobject "transform-$P" autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1
  kubectl -n "$N" annotate scaledobject splitter convert autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1
  return 0
}
IFS=, read -r -a steps <<<"$BACKLOG"
backlog_state="" step=0 step_at=0 ready_at="" win_at="" win_done=0 win_epoch=0 backlog_at="" backlog_msgs=""
backlog_quiet=0 backlog_lines=() backlog_pod_lines=() step_pods=0 win_closed=0
if [[ -n "$BACKLOG" ]]; then
  hold sdh-fhir "transform-$P" 0
  kubectl -n sdh-fhir wait --for=delete pod -l "app=transform-$P" --timeout=180s >/dev/null 2>&1 || true
  # A group with no offset on a partition shows no lag there, so the backlog would read as nothing queued (the
  # stages read such a partition from its start: ReceiveSettings sets autoOffsetReset=earliest). So the group's
  # offsets are set to the topic's end now, before the push, and the lag counts what queues from here.
  kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group "transform-$P" \
    --topic "$P.records" --reset-offsets --to-latest --execute >/dev/null \
    || die "couldn't set transform-$P's offsets on $P.records"
  backlog_state=held
  say "Backlog test: transform held at 0 pods until split and convert are clear; then ${BACKLOG} pods in turn"
fi
# A step's window: records mapped a second (Prometheus' count of transform's process step, so a record counts when
# it is mapped, not when its item's total has come too), and how they were spread over the pods (the least and the
# most a pod mapped, and how many mapped any); records ended at transform (the registry) as well; transform's CPU
# then (cores, share waiting for one), and its mean seconds a batch and a record, and records a batch.
prom_vec() {  # <unix time> <PromQL>: the values then, one a line
  kubectl -n sdh-infra exec deploy/prometheus -- wget -qO- \
    "http://localhost:9090/api/v1/query?time=$1&query=$(python3 -I -c \
      'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$2")" 2>/dev/null \
    | python3 -I -c 'import json, sys
try:
    for r in json.load(sys.stdin)["data"]["result"]: print(r["value"][1])
except Exception:
    pass'
}
prom_by() {  # <unix time> <PromQL summed by service_instance_id>: "<pod> <value>", one a line
  kubectl -n sdh-infra exec deploy/prometheus -- wget -qO- \
    "http://localhost:9090/api/v1/query?time=$1&query=$(python3 -I -c \
      'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$2")" 2>/dev/null \
    | python3 -I -c 'import json, sys
try:
    for r in json.load(sys.stdin)["data"]["result"]:
        print(r["metric"].get("service_instance_id", "?"), r["value"][1])
except Exception:
    pass'
}
# Each transform pod in a step's window, for telling a slow node from a slow pod: its node's pool, instance type and
# capacity type, the records it mapped a second, its CPU (cores) and the share of the time a thread of it waited
# for a CPU.
pod_lines() {  # <pods> <unix time> <window seconds>
  local at="$2" w="$3" h=sdh_stage_phase_duration_seconds nodes
  nodes="$(kubectl get nodes -o go-template='{{range .items}}{{.metadata.name}} {{index .metadata.labels "sdh-poc/pool"}} {{index .metadata.labels "node.kubernetes.io/instance-type"}} {{index .metadata.labels "eks.amazonaws.com/capacityType"}}{{"\n"}}{{end}}' 2>/dev/null || true)"
  python3 -I -c 'import sys
pods, w = sys.argv[1], float(sys.argv[2])
def table(text):
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            out[parts[0]] = parts[1:]
    return out
nodes, where = table(sys.argv[3]), table(sys.argv[4])
rec, cpu, wait = (dict((k, float(v[0])) for k, v in table(t).items()) for t in sys.argv[5:8])
for pod in sorted(rec, key=lambda p: -rec[p]):
    node = where.get(pod, ["?"])[0]
    pool, kind, cap = (nodes.get(node, []) + ["?", "?", "?"])[:3]
    print("%5s %-34s %-6s %-11s %-9s %7.2f %6.2f %5.2f" % (pods, pod[-34:], pool, kind, cap,
        rec[pod] / w, cpu.get(pod, 0) / w, wait.get(pod, 0) / w))' \
    "$1" "$w" "$nodes" \
    "$(kubectl -n sdh-fhir get pods -l "app=transform-$P" -o go-template='{{range .items}}{{.metadata.name}} {{.spec.nodeName}}{{"\n"}}{{end}}' 2>/dev/null || true)" \
    "$(prom_by "$at" 'sum by (service_instance_id) (increase('"$h"'_count{sdh_stage="transform",sdh_phase="process"}['"$w"'s]))')" \
    "$(prom_by "$at" 'sum by (service_instance_id) (increase(sdh_process_cpu_time_seconds_total{job="transform-stage"}['"$w"'s]))')" \
    "$(prom_by "$at" 'sum by (service_instance_id) (increase(sdh_process_cpu_wait_seconds_total{job="transform-stage"}['"$w"'s]))')"
}
step_line() {  # <pods> <seconds to all ready> <window seconds> <records ended>
  local h=sdh_stage_phase_duration_seconds w="$3" at
  at="$(date -u +%s)"
  local cnt='increase('"$h"'_count{sdh_stage="transform",sdh_phase="process"}['"$w"'s])'
  local m='sum(increase(sdh_process_cpu_time_seconds_total{job="transform-stage"}['"$w"'s])) / '"$w"
  local q='avg(increase(sdh_process_cpu_wait_seconds_total{job="transform-stage"}['"$w"'s])) / '"$w"
  local b='sum(increase('"$h"'_sum{sdh_stage="transform",sdh_phase="batch"}['"$w"'s])) / sum(increase('"$h"'_count{sdh_stage="transform",sdh_phase="batch"}['"$w"'s]))'
  local r='sum(increase('"$h"'_sum{sdh_stage="transform",sdh_phase="process"}['"$w"'s])) / sum('"$cnt"')'
  local c='sum('"$cnt"') / sum(increase('"$h"'_count{sdh_stage="transform",sdh_phase="batch"}['"$w"'s]))'
  local per_pod
  per_pod="$(prom_vec "$at" "sum by (service_instance_id) ($cnt)" | tr '\n' ' ')"
  while IFS= read -r line; do backlog_pod_lines+=("$line"); done < <(pod_lines "$1" "$at" "$w")
  backlog_lines+=("$(python3 -I -c 'import sys
p, ready, w, ended, pods = int(sys.argv[1]), sys.argv[2], float(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
v = [float(x) for x in pods.split()]
n = sum(v)
rate = n / w if w else 0
busy = sum(1 for x in v if x >= 1)
lo, hi = (min(v), max(v)) if v else (0, 0)
print("%5d %6s %6.0f %7.0f %7.2f %7.2f %5d %6.0f %6.0f %7d %6s %5s %6s %6s %5s" % (
    p, ready, w, n, rate, rate / p, busy, lo, hi, ended, *sys.argv[6:]))' \
    "$1" "$2" "$w" "$4" "$per_pod" "$(prom_at "$at" "$m")" "$(prom_at "$at" "$q")" "$(prom_at "$at" "$b")" \
    "$(prom_at "$at" "$r")" "$(prom_at "$at" "$c")")")
}

# --- Push --------------------------------------------------------------------------------------------------------

# The paced feed: slice k is started SLICE*k/RATE seconds after t0, each in its own copier, so a slow one doesn't
# hold the pace; it fails if any slice's copy failed. A slice's copy is tried up to 7 times, 20 s apart (2 minutes):
# this machine's connection to S3 can drop for a minute or more (2026-11-17; 3 tries 10 s apart weren't enough). A copy that failed listing copied nothing, and is
# tried again whole; one that failed part-way tries again only the objects that failed, never one that was copied,
# which landing may have landed and deleted already. An object whose copy "failed" may still have been written, its
# answer lost, and so may one the CLI retried itself: written twice, it is landed twice, as a new version of the
# object (docs/PERFORMANCE.md §10). The report counts the objects tried again; the CLI's own retries it can't see.
# Each object is written once (run 54): with the CLI's own retries a copy whose answer was late (its 60 s read
# timeout) was sent again and wrote the object a second time, a new version the landing node rightly took as a new
# item. So the CLI tries once (AWS_MAX_ATTEMPTS=1), and a copy it reports failed is tried again only if the object
# isn't in the intake already: a failed copy may have been written all the same. A slice that failed with no
# object named is synced, which copies only what isn't there. Each copy S3 answered for is noted (acks/, the CLI's
# "copy:" lines), so the report can name any object staged but never acknowledged.
#
# And a copy that ends well is not taken at its word (run 57): the CLI's recursive copy exited 0 with nothing on
# stderr yet left 30 of 90,000 objects unwritten, which no stage ever saw: the objects runs 55 and 56 lost. So
# once the copy ends, the slice is listed and each object in it neither acknowledged nor found written is copied
# on its own (unreported/), if it isn't in the intake already, until the listing and the acknowledgements agree.
copy_slice() {  # <slice>
  local try src="${staging}$(printf 's%05d' "$1")/" err="$out/copy-errors/$1" ack="$out/acks/$1" left f dst ok=""
  if AWS_MAX_ATTEMPTS=1 aws s3 cp --recursive --no-progress "$src" "$INTAKE" 2> "$err" >> "$ack"; then ok=1; fi
  for try in 2 3 4 5 6 7; do
    [[ -n "$ok" ]] && break
    sleep 20
    left="$(sed -n 's/^copy failed: \(s3:[^ ]*\) to .*/\1/p' "$err")"
    if [[ -z "$left" ]]; then
      if AWS_MAX_ATTEMPTS=1 aws s3 sync --size-only --no-progress "$src" "$INTAKE" 2> "$err" >> "$ack"; then ok=1; fi
      continue
    fi
    : > "$err"
    for f in $left; do
      dst="${INTAKE}${f#"$src"}"; dst="${dst#s3://}"
      if aws s3api head-object --bucket "${dst%%/*}" --key "${dst#*/}" >/dev/null 2>&1; then
        echo "$f" >> "$out/written-anyway"
        continue
      fi
      echo "$f" >> "$out/copied-again"
      AWS_MAX_ATTEMPTS=1 aws s3 cp --no-progress "$f" "$INTAKE" 2>/dev/null >> "$ack" \
        || echo "copy failed: $f to $INTAKE" >> "$err"
    done
    [[ -s "$err" ]] || ok=1
  done
  if [[ -z "$ok" ]]; then
    echo "load-test.sh: slice $1 not copied after 7 tries" >&2
    return 1
  fi
  # The check: the slice's objects, less those acknowledged or found written. Names sorted alike (LC_ALL=C).
  for try in 1 2 3 4 5 6; do
    ((try > 1)) && sleep 20
    left="$(aws s3 ls "$src" 2>/dev/null)" || continue
    left="$(awk '{print $4}' <<< "$left" | LC_ALL=C sort | LC_ALL=C comm -23 - <(
      { sed -n 's#^copy: .* to s3://.*/##p' "$ack"; grep -F "$src" "$out/written-anyway" | sed 's#.*/##'; } \
        | LC_ALL=C sort -u))" || continue
    [[ -z "$left" ]] && return 0
    for f in $left; do
      echo "$src$f" >> "$out/unreported/$1"
      dst="${INTAKE}${f}"; dst="${dst#s3://}"
      if aws s3api head-object --bucket "${dst%%/*}" --key "${dst#*/}" >/dev/null 2>&1; then
        echo "$src$f" >> "$out/written-anyway"
        continue
      fi
      AWS_MAX_ATTEMPTS=1 aws s3 cp --no-progress "$src$f" "$INTAKE" 2>/dev/null >> "$ack" || true
    done
  done
  echo "load-test.sh: slice $1 not all acknowledged after 6 checks" >&2
  return 1
}
paced_copy() {
  local k start now due pids=() fail=0 pid
  start="$(date +%s%3N)"
  for ((k = 0; k < slices; k++)); do
    due=$((start + k * SLICE * 1000 / RATE)); now="$(date +%s%3N)"
    ((due > now)) && sleep "$(printf '%d.%03d' $(((due - now) / 1000)) $(((due - now) % 1000)))"
    copy_slice "$k" &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do wait "$pid" || fail=1; done
  return "$fail"
}
if [[ -n "$RATE" ]]; then
  slices="$(cat "$out/slices")"
  mkdir -p "$out/copy-errors" "$out/acks" "$out/unreported"; : > "$out/copied-again"; : > "$out/written-anyway"
  say "t0: copying them under $INTAKE (server-side), $RATE a second ($slices slices of $SLICE)"
  paced_copy &
else
  say "t0: copying them under $INTAKE (server-side)"
  aws s3 cp --recursive --only-show-errors "$staging" "$INTAKE" &
fi
copier=$!
say "Putting 3 messages no stage can open on each of $P.split and $P.records"
for t in split records; do
  printf "sdh-poc-load $run junk %s\n" 1 2 3 \
    | kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$P.$t" >/dev/null
done

# --- Watch -------------------------------------------------------------------------------------------------------

declare -A kill_done=()
kill_lines=()
kill_pods() {  # <force|graceful> <stage> <n|half>: stops them; prints "<stopped> <running before>"
  local how="$1" stage="$2" count="$3" ns="$N" app="$2" n pods=()
  [[ "$stage" == transform ]] && { ns=sdh-fhir; app="transform-$P"; }
  mapfile -t pods < <(kubectl -n "$ns" get pods -l "app=$app" --field-selector=status.phase=Running -o name \
    2>/dev/null | shuf)
  n="$count"; [[ "$count" == half ]] && n=$(((${#pods[@]} + 1) / 2))
  ((n > ${#pods[@]})) && n=${#pods[@]}
  if ((n > 0)); then
    if [[ "$how" == force ]]; then
      kubectl -n "$ns" delete "${pods[@]:0:n}" --grace-period=0 --force --wait=false >/dev/null 2>&1 || true
    else
      kubectl -n "$ns" delete "${pods[@]:0:n}" --wait=false >/dev/null 2>&1 || true
    fi
  fi
  echo "$n ${#pods[@]}"
}
holder() {  # the running landing pod holding the intake's lease (with two, the standby that took over), from the
  # lease tables (sdh_lease, in whichever schema), not the pod's log: a long run's log has rotated past
  # lease.acquired by then (run 54). An owner is "<pod>/<pid>/<suffix>".
  local union pod
  union="$(db "SELECT string_agg(format('SELECT owner, acquired_at FROM %I.sdh_lease WHERE expires_at > now()',
    schemaname), ' UNION ALL ') FROM pg_tables WHERE tablename = 'sdh_lease'" 2>/dev/null || true)"
  [[ -n "$union" ]] || return 0
  pod="$(db "SELECT split_part(owner, '/', 1) FROM ($union) l WHERE owner LIKE 'landing-%'
    ORDER BY acquired_at DESC LIMIT 1" 2>/dev/null || true)"
  [[ -n "$pod" && "pod/$pod" != "${killed:-}" ]] || return 0
  [[ "$(kubectl -n "$N" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null)" == Running ]] && echo "pod/$pod"
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
  # What went wrong, if anything (docs/PERFORMANCE.md §10, the first steady feed): Postgres' connections by role,
  # "-" when it doesn't answer, and Kubernetes' Warning events, saved as they come (they expire after an hour).
  { printf '%s ' "$now"; db "SELECT coalesce(sum(n), 0), coalesce(string_agg(u || '=' || n, ' ' ORDER BY n DESC), '') FROM (SELECT
      coalesce(usename, '(system)') u, count(*) n FROM pg_stat_activity GROUP BY 1) a" 2>/dev/null \
      | tr '|' ' ' || echo "-"; } >> "$out/pg-connections"
  kubectl get events -A --field-selector type=Warning -o json > "$out/events/$now.json" 2>/dev/null || true
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

  # The kills: at each share ended at transform, stop that many of the stage's running pods (never its duties pod,
  # which has an app label of its own).
  for k in ${KILLS//,/ }; do
    [[ -n "${kill_done[$k]:-}" ]] && continue
    IFS=: read -r pct how stage count <<<"$k"
    ((transformed * 100 >= files * pct)) || continue
    kill_done[$k]=1
    stopped="$(kill_pods "$how" "$stage" "$count")"
    kill_lines+=("$(printf '%3s%% at %5ss (%6s ended): %-8s %-9s %s of %s' "$pct" "$now" "$transformed" "$how" \
      "$stage" "${stopped%% *}" "${stopped#* }")")
    say "  kill: ${kill_lines[-1]}"
  done

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

  # The backlog test: released once split and convert have been clear for 30 s, then a pod count at a time.
  # Records converted (and sent on) that transform hasn't ended: its backlog, from the registries, not the lag.
  remaining=$((c_done - c_done0 - c_failed + c_failed0 - t_done + t_done0))
  if [[ "$backlog_state" == held && -n "$pushed_at" && "$in" == 0 ]]; then
    if ((landed >= files && split >= landed && converted >= split)) \
        && [[ "$(group_lag "splitter-$P")" == 0 && "$(group_lag "convert-$P")" == 0 ]]; then
      backlog_quiet=$((backlog_quiet + 1))
    else backlog_quiet=0; fi
    if ((backlog_quiet >= 2)); then
      backlog_msgs="$(group_lag "transform-$P")" backlog_at=$now backlog_records=$remaining
      hold "$N" splitter 1; hold "$N" convert 1
      hold sdh-fhir "transform-$P" "${steps[0]}"
      backlog_state=running step=0 step_at=$now ready_at="" win_at=""
      say "  backlog: $backlog_records records ($backlog_msgs messages) queued for transform; released at ${steps[0]} pods"
    fi
  elif [[ "$backlog_state" == running ]]; then
    want="${steps[$step]}" r="$(replicas sdh-fhir "transform-$P")"
    if [[ -z "$ready_at" && "${r:-0}" -ge "$want" ]]; then
      ready_at=$now step_pods=$want; say "  backlog: $want pods ready $((now - step_at))s after asking"
    elif [[ -z "$ready_at" ]] && ((now >= step_at + 300)); then
      # No room for them all (the burst nodes, or Spot): the window takes those that are ready.
      ready_at=$now step_pods="${r:-0}"; say "  backlog: only ${r:-0} of $want pods ready after 300s; measuring those"
    fi
    if [[ -n "$ready_at" && -z "$win_at" ]] && ((now >= ready_at + BACKLOG_WARM)); then
      win_at=$now win_done=$t_done
    fi
    last_step=$((step + 1 >= ${#steps[@]}))
    clear_now=$((remaining <= 0))
    # The last step's window closes once a tenth of the backlog is left, before the tail, when the partitions
    # still holding records may be fewer than the pods.
    tail_now=$((last_step && remaining * 10 <= backlog_records))
    if [[ -n "$win_at" && "$win_closed" != 1 ]] && { ((clear_now || tail_now)) \
        || { ((! last_step)) && ((now >= win_at + BACKLOG_STEP)); }; }; then
      step_line "$step_pods" "$((ready_at - step_at))" "$((now - win_at))" "$((t_done - win_done))"
      say "  backlog: $want pods, $((t_done - win_done)) records in $((now - win_at))s"
      if ((clear_now)); then
        backlog_state=cleared; say "  backlog: clear $((now - backlog_at))s after the release"
      elif ((last_step)); then
        win_closed=1; say "  backlog: a tenth left $((now - backlog_at))s after the release"
      else
        step=$((step + 1)) step_at=$now ready_at="" win_at=""
        hold sdh-fhir "transform-$P" "${steps[$step]}"
        say "  backlog: now ${steps[$step]} pods"
      fi
    elif ((clear_now)); then
      backlog_state=cleared
      say "  backlog: clear $((now - backlog_at))s after the release$([[ "$win_closed" == 1 ]] || echo ", during $want pods' warm-up")"
    fi
  fi

  state="$in|$landed|$split|$c_done|$t_done|$t_failed|$l"
  if [[ -n "$pushed_at" && "$in" == 0 && "$l" == 0 && "$state" == "$last" ]]; then stable=$((stable + 1))
  else stable=0; fi
  # Nothing moving is the end only when no outage is under way or settling.
  if [[ "$outage_state" == down ]] || ((SECONDS < settle_until)); then stable=0; fi
  [[ "$backlog_state" == held || "$backlog_state" == running ]] && stable=0
  last="$state"
  ((stable >= 3)) && break   # 45 s with nothing moving
  ((now < (TIMEOUT_MIN + OUTAGE_MIN * (OUTAGE_AT != 0) + SETTLE_MIN) * 60)) || { say "  TIMEOUT after $TIMEOUT_MIN minutes: reporting what's done"; break; }
done
end=$(( ${last_change[transformed]:-$((SECONDS - t0))} ))
db "SELECT now()" > "$out/t1"; echo "$max_nodes" > "$out/nodes"   # for measure.sh
stop_sampler
python3 -I "$HERE/resources.py" sample "$out" "$t0_epoch" "$N" sdh-fhir sdh-infra --final 2>/dev/null || true
[[ -n "$BACKLOG" ]] && backlog_restore

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
# An object written twice is landed twice, as a new version (copy_slice says why). The feed now copies again only
# what isn't there, so the count of items landed twice should be 0.
[[ -f "$out/copied-again" ]] && say "  objects the feed copied again after a failed copy: $(wc -l < "$out/copied-again");" \
  "found written after a failed copy, so not copied again: $(wc -l < "$out/written-anyway" 2>/dev/null || echo 0)"
# Objects a copy that ended well left out, found by the slice check (copy_slice): run 57 lost 30 of 90,000 so.
[[ -d "$out/unreported" ]] && say "  objects a copy that ended well left unacknowledged, copied by the slice check:" \
  "$(sort -u "$out"/unreported/* 2>/dev/null | wc -l) (names in $out/unreported/)"
# Where a lost object went (runs 55 and 56): staged but never acknowledged by S3 nor found written is the feed's;
# acknowledged but never landed is landing's, and landing's own audit says whether it took any object from the
# intake that it hadn't landed: each item.finished_at_source names a sighting, which must be one an item.landed
# names, and item.left_at_source counts objects it found changed or gone. Hashes and counts only: no names.
if [[ -d "$out/acks" ]]; then
  { sed -n 's#^copy: .* to \(s3://.*\)$#\1#p' "$out"/acks/* 2>/dev/null || true; } | sed 's#.*/##' | sort -u \
    > "$out/acknowledged"
  sed 's#.*/##' "$out/acknowledged" "$out/written-anyway" | sort -u > "$out/written"
  comm -23 "$out/staged" "$out/written" > "$out/unacknowledged"
  say "  copies S3 acknowledged: $(wc -l < "$out/acknowledged") distinct, of $(wc -l < "$out/staged") staged;" \
    "staged but never acknowledged nor found written: $(wc -l < "$out/unacknowledged") (expected 0;" \
    "names in $out/unacknowledged)"
fi
python3 -I "$HERE/landing-takes.py" "$out"/pod-logs/"${N}"_landing-*.log* | tee -a "$report" \
  || say "  landing's takes from the intake couldn't be read from its logs"
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
if [[ "$split_behind" =~ ^[0-9]+$ && "$convert_behind" =~ ^[0-9]+$ ]] && ((split_behind + convert_behind > 0)); then
  # Where they are: at the stage they didn't end at, whether its total came, its counts, whether its reconciler asked
  # for the end again, and its records by state. IDs and counts only, kept in the run's directory.
  left_split="SELECT a.item_id FROM split_$PREFIX.split_item a WHERE a.updated_at >= '$t0_db'
    AND a.state = 'COMPLETED' AND $(not_ended "convert_$PREFIX" false)"
  left_convert="SELECT a.item_id FROM convert_$PREFIX.stage_item a WHERE a.item_id IN ($run_items)
    AND a.failed < a.done AND $(not_ended "transform_$PREFIX" "a.failed > 0")"
  for pair in "convert:$left_split" "transform:$left_convert"; do
    s="${pair%%:*}" ids="${pair#*:}"
    db "SELECT '$s', l.item_id, i.expected IS NOT NULL, i.expected, i.done, i.failed, i.upstream_open,
          i.total_wanted_at IS NOT NULL, i.completed_at IS NOT NULL,
          (SELECT string_agg(r.state || '=' || r.n, ',') FROM (SELECT state, count(*) n FROM ${s}_$PREFIX.stage_record
             WHERE item_id = l.item_id GROUP BY state) r)
        FROM (SELECT item_id FROM ($ids) x) l LEFT JOIN ${s}_$PREFIX.stage_item i ON i.item_id = l.item_id" \
      >> "$out/left-items" 2>/dev/null || true
  done
  say "  where they are (stage|item|total came|expected|done|failed|upstream open|end asked again|completed|records):"
  say "    $(cut -d'|' -f1,3- "$out/left-items" 2>/dev/null | sort | uniq -c | sed 's/^ *//' | paste -sd ';' -)"
  say "    (by item: $out/left-items)"
  # Their records still claimed: the pod holding each, its attempts and when the claim expired (STAGES.md §10).
  for pair in "convert:$left_split" "transform:$left_convert"; do
    s="${pair%%:*}" ids="${pair#*:}"
    db "SELECT '$s', r.item_id, r.record_id, split_part(r.owner, '/', 1), r.attempts, r.claim_expires
          FROM ${s}_$PREFIX.stage_record r WHERE r.state = 'CLAIMED' AND r.item_id IN ($ids)" \
      >> "$out/left-claims" 2>/dev/null || true
  done
  if [[ -s "$out/left-claims" ]]; then
    live=" $(kubectl get pods -A -o custom-columns=N:.metadata.name --no-headers 2>/dev/null | paste -sd ' ' -) "
    say "  records left claimed (stage|pod|attempts|claim expired|pod still there): $(wc -l < "$out/left-claims")"
    say "    $(while IFS='|' read -r st _ _ pod att exp; do
        [[ "$live" == *" $pod "* ]] && there=yes || there=no
        echo "$st|$pod|$att|${exp%%.*}|$there"
      done < "$out/left-claims" | sort | uniq -c | sed 's/^ *//' | paste -sd ';' -)"
    say "    (by record: $out/left-claims; the pods' logs: $out/pod-logs/)"
  fi
  if ((RECOVER_MIN > 0)); then
    say "  waiting up to $RECOVER_MIN minutes for the reconcilers to end them"
    waited=0 left=$((split_behind + convert_behind))
    while ((left > 0 && waited < RECOVER_MIN * 60)); do
      sleep 60; waited=$((waited + 60))
      a="$(db "SELECT count(*) FROM ($left_split) x" 2>/dev/null || echo '?')"
      b="$(db "SELECT count(*) FROM ($left_convert) x" 2>/dev/null || echo '?')"
      [[ "$a" =~ ^[0-9]+$ && "$b" =~ ^[0-9]+$ ]] && left=$((a + b))
    done
    say "  after ${waited}s: $left left behind (split but not ended at convert ${a:-?}, not ended at transform ${b:-?})"
    say "  expired claims handed in again (stage.claims_recovered): $(pod_events stage.claims_recovered);" \
      "batches tried again for a record still claimed (stage.claims_left): $(pod_events stage.claims_left)"
    for s in convert transform; do
      say "  ends asked for again at $s: $(db "SELECT count(*) || ', ' || count(expected) || ' came'
        FROM ${s}_$PREFIX.stage_item WHERE item_id IN ($run_items) AND total_wanted_at IS NOT NULL" \
        2>/dev/null || echo '?')"
    done
  fi
fi
if [[ -n "$outage_state" ]]; then
  say "Outage: at $outage_landed landed, down $OUTAGE_MIN minutes, messages lost: ${OUTAGE_LOSE:-none};" \
    "$((${last_change[transformed]:-0} - (restored_at - t0)))s from the restart to the last item transformed"
  say "  what the reconcilers did (audit events, all pods since the restart):"
  for e in stage.records_recovered stage.item_completion_recovered stage.item_total_missing \
      stage.placement_unknown stage.reconcile_deferral_capped split.recovered stage.item_end_resent; do
    c=0
    for ns in "$N" sdh-fhir; do
      for pod in $(kubectl -n "$ns" get pods -o name 2>/dev/null); do
        c=$((c + $(kubectl -n "$ns" logs "$pod" 2>/dev/null | grep -c "$e" || true)))
      done
    done
    say "    $e: $c"
  done
  # From the registries, so it doesn't depend on which pods are still running: the items whose end a stage's
  # reconciler asked the stage before to send again (STAGES.md §13.2), and how many of those ends came.
  for s in convert transform; do
    say "  ends asked for again at $s: $(db "SELECT count(*) || ', ' || count(expected) || ' came'
      FROM ${s}_$PREFIX.stage_item WHERE item_id IN ($run_items) AND total_wanted_at IS NOT NULL" \
      2>/dev/null || echo '?')"
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
# From the logs the run keeps of every pod (pod-logs), not the pods still running: the pods that read the junk
# have often been scaled in by the end (run 53 read 0 though their logs had all three).
for d in "$N convert" "sdh-fhir transform-$P"; do
  read -r ns app <<<"$d"
  say "    $app: $(zcat -f "$out/pod-logs/${ns}_${app}-"*.log* 2>/dev/null \
    | grep -c 'stage\.message_rejected.*reason=MALFORMED' || true)"
done
say "Throughput (from t0 to each stage's last change):"
say "    end to end: ${end}s; $(per_min "$transformed" "$end") items/min," \
  "$(per_min "$((t_done - t_done0))" "$end") records/min, $(per_min "$((bytes / 1048576))" "$end") MB/min"
for k in landed split converted transformed; do
  say "    $k: ${!k} items by ${last_change[$k]:-?}s, $(per_min "${!k}" "${last_change[$k]:-0}") items/min"
done
scaled_max() { kubectl -n "$1" get scaledobject "$2" -o jsonpath='{.spec.maxReplicaCount}' 2>/dev/null || echo '?'; }
say "Pods at most: landing ${max_pods[landing]} (1 active), splitter ${max_pods[splitter]}/$(scaled_max "$N" splitter)," \
  "convert ${max_pods[convert]}/$(scaled_max "$N" convert)," \
  "transform ${max_pods[transform-$P]}/$(scaled_max sdh-fhir "transform-$P"); nodes $max_nodes, burst $max_burst"
# Containers that stopped in the run, of pods still there: why (OOMKilled, Error and its exit code), how often.
say "Containers restarted in the run (pods still there): namespace/pod container, restarts, last reason, when (s)"
kubectl get pods -A -o json 2>/dev/null | python3 -I -c 'import datetime, json, sys
t0 = datetime.datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
n = 0
for pod in json.load(sys.stdin)["items"]:
    for c in pod.get("status", {}).get("containerStatuses", []):
        last = c.get("lastState", {}).get("terminated")
        if not c.get("restartCount") or not last or not last.get("finishedAt"):
            continue
        at = datetime.datetime.fromisoformat(last["finishedAt"].replace("Z", "+00:00"))
        if at < t0:
            continue
        n += 1
        print("    %s/%s %s, %d, %s (exit %s), at %ds" % (pod["metadata"]["namespace"], pod["metadata"]["name"],
            c["name"], c["restartCount"], last.get("reason", "?"), last.get("exitCode", "?"), (at - t0).total_seconds()))
if not n:
    print("    none")' "$t0_iso" | tee -a "$report"
say "Postgres connections (pg_stat_activity), at most: $(awk '$2 ~ /^[0-9]+$/ && $2 > m {m = $2; l = $0}
  END {print (m ? m " at " l : "never read")}' "$out/pg-connections" | cut -c1-300);" \
  "unanswered in $(awk '$2 == "-"' "$out/pg-connections" | wc -l) of $(wc -l < "$out/pg-connections") reads" \
  "(by second and role: $out/pg-connections)"
python3 -I "$HERE/resources.py" report "$out" "$report" | tee -a "$report" || say "resources.py failed (above)"
# The Warning events since t0, by namespace, kind, reason and the object's name without its pod hash; the
# messages of the first of each, cut short (they name pods, nodes and probes, never documents).
say "Warning events since t0: count, namespace kind/name reason: the first one's message"
python3 -I -c 'import datetime, glob, json, re, sys
t0 = datetime.datetime.fromisoformat(sys.argv[2].replace("Z", "+00:00"))
def ts(x):
    return datetime.datetime.fromisoformat(x[:19] + "+00:00")
groups = {}
for f in sorted(glob.glob(sys.argv[1] + "/*.json"), key=lambda f: int(re.sub(r"\D", "", f.rsplit("/", 1)[1]) or 0)):
    try:
        items = json.load(open(f))["items"]
    except Exception:
        continue
    for e in items:
        uid = e["metadata"]["uid"]
        last = e.get("lastTimestamp") or e.get("eventTime") or e["metadata"].get("creationTimestamp")
        if not last or ts(last) < t0:
            continue
        o = e.get("involvedObject", {})
        name = re.sub(r"-[0-9a-f]{8,10}-[0-9a-z]{5}$|-[0-9a-z]{5}$", "-*", o.get("name", "?"))
        if o.get("kind") == "Node":
            name = "*"
        key = (e["metadata"]["namespace"], o.get("kind", "?"), name, e.get("reason", "?"))
        count = e.get("count") or 1
        g = groups.setdefault(key, [0, (e.get("message") or "").replace("\n", " ")[:160], {}])
        g[2][uid] = count
for key, (_, msg, uids) in sorted(groups.items(), key=lambda kv: -sum(kv[1][2].values())):
    print("    %5d  %s %s/%s %s: %s" % (sum(uids.values()), key[0], key[1], key[2], key[3], msg))
if not groups:
    print("    none")' "$out/events" "$t0_iso" | head -40 | tee -a "$report"
if [[ -n "$killed" ]]; then
  say "Landing failover: holder deleted at ${FAILOVER_AT}% landed; standby took the lease after ${takeover:-NEVER}s"
elif [[ "$FAILOVER_AT" != 0 ]]; then
  say "Landing failover: NOT DONE at ${FAILOVER_AT}% landed: no lease holder found, or the run ended first"
fi
if [[ -n "$KILLS" ]]; then
  say "Kills (share ended at transform; when; how; stage; pods stopped of those running):"
  for line in "${kill_lines[@]}"; do say "    $line"; done
  ((${#kill_lines[@]})) || say "    none: the run ended before the first"
  say "  expired claims handed in again (stage.claims_recovered): $(pod_events stage.claims_recovered);" \
    "batches tried again for a record still claimed (stage.claims_left): $(pod_events stage.claims_left)"
  for s in convert transform; do
    say "  ends asked for again at $s: $(db "SELECT count(*) FROM ${s}_$PREFIX.stage_item
      WHERE item_id IN ($run_items) AND total_wanted_at IS NOT NULL" 2>/dev/null || echo '?')"
  done
fi
if [[ -n "$BACKLOG" ]]; then
  say "Backlog: ${backlog_records:-?} records (${backlog_msgs:-?} messages) queued at ${backlog_at:-?}s; ${backlog_state}" \
    "$([[ "$backlog_state" == cleared ]] && echo "after $((${last_change[transformed]:-0} - backlog_at))s")"
  say "  by pod count: seconds to all ready; the window (s); records mapped in it, a second, a pod's a second; pods"
  say "  that mapped any, the least and most a pod mapped; records ended (registry); transform's CPU (cores, share"
  say "  waiting for one); seconds a batch and a record; records a batch"
  say "$(printf '  %5s %6s %6s %7s %7s %7s %5s %6s %6s %7s %6s %5s %6s %6s %5s' pods ready window mapped rec/s \
    per-pod busy least most ended cores wait batch record per)"
  for line in "${backlog_lines[@]}"; do say "  $line"; done
  say "  by pod in each window: its node's pool, instance type and capacity; records a second; cores; share waiting"
  say "$(printf '  %5s %-34s %-6s %-11s %-9s %7s %6s %5s' pods pod pool type capacity rec/s cores wait)"
  for line in "${backlog_pod_lines[@]}"; do say "  $line"; done
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
