#!/usr/bin/env bash
# Measures a load run (docs/PERFORMANCE.md §10): how long each item took from landing to gold, where the time went,
# how the rate held as the tables grew, what was committed, the KMS calls, and an estimated cost per million
# documents. load-test.sh runs it at the end; run it again by hand any time before `poc.sh down` (the stages'
# metrics are kept only while the session is up). Prints counts, durations and table names only.
#
#   deploy/aws/k8s/pipeline/measure.sh [run]    # default the latest run in $STATE/load
#
# Where the figures come from:
#   - Per item, from the registries in Postgres: when it landed (split_landed, noted by the split stage from the
#     landing table), when its splitting ended (split_item.ended_at), and when each stage first and last stored
#     one of its records (stage_record.stored_at; a record that failed is stored too). An item reached gold when
#     the transform stage's last record of it was stored. Queue wait is the gap from one stage's end of an item to
#     the next stage's first record of it.
#   - Inside each stage, from Prometheus (k8s/infra/telemetry.yaml): sdh.stage.phase_duration by stage and phase,
#     and the commit durations by table.
#   - Commits per table, from the metadata version in the catalog.
#   - KMS Sign calls, from the messages written to the topics (each is signed once).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's
P="${SDH_PIPELINES:-ccda-push}"; P="${P%% *}"; PREFIX="${P//-/_}"
# us-east-2 list prices, from the AWS Price List API on 2026-11-13: an on-demand t4g.large hour (a Spot burst node
# costs less), an EKS cluster hour, and one KMS asymmetric (ECC) request.
NODE_HOUR=0.0672 EKS_HOUR=0.10 KMS_ASYMMETRIC=0.000015 KMS_REQUEST=0.000003

say() { printf '>> %s\n' "$*"; }
die() { printf 'measure.sh: %s\n' "$*" >&2; exit 1; }
db() { kubectl -n sdh-infra exec -i postgres-0 -- psql -U postgres -d sdh -At -F '|' -c "$1"; }

run="${1:-$(ls -1 "$STATE/load" 2>/dev/null | grep '^load-' | sort | tail -1)}"
out="$STATE/load/$run"
[[ -n "$run" && -s "$out/t0" ]] || die "no run with a start time in $STATE/load (run load-test.sh)"
kubectl get ns "sdh-$P" >/dev/null 2>&1 || die "no session: run deploy/aws/poc.sh up"
t0="$(cat "$out/t0")"
t1="$(cat "$out/t1" 2>/dev/null || db "SELECT now()")"
secs="$(db "SELECT round(extract(epoch FROM '$t1'::timestamptz - '$t0'::timestamptz))")"
say "Measured: $run, ${secs}s"

# --- Per item, from the registries -------------------------------------------------------------------------------

# Each item landed in the run, with when each stage first and last stored a record of it. Ended at a stage: every
# record of it there has ended (as load-test.sh's ended()).
items="
  WITH landed AS (SELECT l.item_id, l.landed_at, s.ended_at AS split_end, s.state
                    FROM split_$PREFIX.split_landed l JOIN split_$PREFIX.split_item s USING (item_id)
                   WHERE l.landed_at >= '$t0' AND l.landed_at <= '$t1'),
       c AS (SELECT item_id, min(stored_at) AS first, max(stored_at) AS last
               FROM convert_$PREFIX.stage_record WHERE item_id IN (SELECT item_id FROM landed) GROUP BY item_id),
       t AS (SELECT item_id, min(stored_at) AS first, max(stored_at) AS last
               FROM transform_$PREFIX.stage_record WHERE item_id IN (SELECT item_id FROM landed) GROUP BY item_id),
       ended AS (SELECT item_id FROM transform_$PREFIX.stage_item
                  WHERE expected IS NOT NULL AND done >= expected AND expected > 0)
  SELECT landed.*, c.first AS c_first, c.last AS c_last, t.first AS t_first,
         CASE WHEN e.item_id IS NOT NULL THEN t.last END AS t_last
    FROM landed LEFT JOIN c USING (item_id) LEFT JOIN t USING (item_id) LEFT JOIN ended e USING (item_id)"
secs_of() { echo "extract(epoch FROM $1)"; }
pct() {  # <expression in seconds>: p50|p95|p99|max over the items that reached gold
  db "WITH i AS ($items) SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY $1)::numeric, 1),
        round(percentile_cont(0.95) WITHIN GROUP (ORDER BY $1)::numeric, 1),
        round(percentile_cont(0.99) WITHIN GROUP (ORDER BY $1)::numeric, 1), round(max($1)::numeric, 1)
      FROM i WHERE t_last IS NOT NULL"
}
IFS='|' read -r n_landed n_gold n_split_failed <<<"$(db "WITH i AS ($items) SELECT count(*),
  count(t_last), count(*) FILTER (WHERE state = 'FAILED') FROM i")"
say "Items: $n_landed landed in the run, $n_gold reached gold, $n_split_failed failed at split (no records)"
if [[ "$n_gold" -gt 0 ]]; then
  say "Seconds per item, p50 / p95 / p99 / max:"
  printf '    %-36s %s\n' \
    "landed -> gold" "$(pct "$(secs_of "t_last - landed_at")" | tr '|' ' ')" \
    "landed -> split ended" "$(pct "$(secs_of "split_end - landed_at")" | tr '|' ' ')" \
    "split ended -> convert's 1st record" "$(pct "$(secs_of "c_first - split_end")" | tr '|' ' ')" \
    "convert: 1st -> last record" "$(pct "$(secs_of "c_last - c_first")" | tr '|' ' ')" \
    "convert's last -> transform's 1st" "$(pct "$(secs_of "t_first - c_last")" | tr '|' ' ')" \
    "transform: 1st -> last record" "$(pct "$(secs_of "t_last - t_first")" | tr '|' ' ')"
  say "  (a gap from one stage to the next is the queue's wait plus the next stage's work on one record)"
  # The rate as the tables grow: the items in tenths by when they reached gold, and each tenth's rate.
  say "Rate by tenth of the run (items to gold, by when they got there): items/min"
  db "WITH i AS ($items), g AS (SELECT t_last, ntile(10) OVER (ORDER BY t_last) AS tenth FROM i
                                  WHERE t_last IS NOT NULL)
      SELECT tenth, count(*), round((count(*) / greatest(extract(epoch FROM max(t_last) - min(t_last)), 1)
             * 60)::numeric, 1) FROM g GROUP BY tenth ORDER BY tenth" \
    | awk -F'|' '{printf "    %2d: %4d items, %6.1f/min\n", $1, $2, $3}'
  db "WITH i AS ($items) SELECT round((count(t_last) / greatest(extract(epoch FROM max(t_last) - min(landed_at)),
        1))::numeric, 2) FROM i" | awk '{print ">> Overall: " $1 " documents/s, first landed to last in gold"}'
fi

# --- Inside each stage, from Prometheus --------------------------------------------------------------------------

prom() {  # <PromQL>: the instant vector at the run's end, as JSON
  kubectl -n sdh-infra exec deploy/prometheus -- wget -qO- \
    "http://localhost:9090/api/v1/query?time=$(date -u -d "$t1" +%s)&query=$(python3 -I -c \
      'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$1")" 2>/dev/null
}
prom_at() {  # <unix time> <PromQL>: the instant vector then, as JSON
  kubectl -n sdh-infra exec deploy/prometheus -- wget -qO- \
    "http://localhost:9090/api/v1/query?time=$1&query=$(python3 -I -c \
      'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$2")" 2>/dev/null
}
range="${secs}s"
say "Inside the stages (seconds; per batch for open, commit, register, send, complete and batch; per record or"
say "message otherwise): count, mean, p95"
if ! kubectl -n sdh-infra get deploy prometheus >/dev/null 2>&1; then
  say "    no Prometheus (k8s/infra/telemetry.yaml): poc.sh up applies it"
else
  h=sdh_stage_phase_duration_seconds
  python3 -I - "$(prom "sum by (sdh_stage, sdh_phase) (increase(${h}_count[$range]))")" \
    "$(prom "sum by (sdh_stage, sdh_phase) (increase(${h}_sum[$range]))")" \
    "$(prom "histogram_quantile(0.95, sum by (le, sdh_stage, sdh_phase) (increase(${h}_bucket[$range])))")" <<'EOF'
import json, sys
def vec(s, keys):
    try:
        return {tuple(r["metric"].get(k, "") for k in keys): float(r["value"][1])
                for r in json.loads(s)["data"]["result"]}
    except Exception:
        return {}
k = ("sdh_stage", "sdh_phase")
count, total, p95 = (vec(s, k) for s in sys.argv[1:4])
order = ["split", "convert", "transform"]
phases = ["lookup", "open", "split", "failed_rows", "fail_store", "correction_rows", "announce", "checkpoint", "item",
          "claim", "read", "transform", "process", "commit", "seal", "register", "send", "complete", "batch"]
if not count:
    print("    none recorded (were the stages built with the timings, and was the collector up?)")
for key in sorted(count, key=lambda x: (order.index(x[0]) if x[0] in order else 9,
                                         phases.index(x[1]) if x[1] in phases else 99)):
    n = count[key]
    if n < 0.5:
        continue
    print(f"    {key[0]:<10} {key[1]:<15} {n:9.0f} {total.get(key, 0) / n:8.3f} {p95.get(key, float('nan')):8.3f}")
EOF
  say "Commits by table (seconds): count, mean"
  c=sdh_commit_duration_seconds
  python3 -I - "$(prom "sum by (sdh_table) (increase(${c}_count[$range]))")" \
    "$(prom "sum by (sdh_table) (increase(${c}_sum[$range]))")" <<'EOF'
import json, sys
def vec(s):
    try:
        return {r["metric"].get("sdh_table", ""): float(r["value"][1]) for r in json.loads(s)["data"]["result"]}
    except Exception:
        return {}
count, total = vec(sys.argv[1]), vec(sys.argv[2])
for t in sorted(count):
    if count[t] >= 0.5:
        print(f"    {t:<40} {count[t]:7.0f} {total.get(t, 0) / count[t]:8.3f}")
EOF
  # Split's steps by quarter of the run: which of them grows as the tables do (docs/PERFORMANCE.md §10, baseline).
  say "Split's steps by quarter of the run: mean seconds per item"
  quarter=$((secs / 4)); end_s="$(date -u -d "$t1" +%s)"
  cols=()
  for q in 3 2 1 0; do
    at=$((end_s - q * quarter))
    cols+=("$(prom_at "$at" "sum by (sdh_phase) (increase(${h}_sum{sdh_stage=\"split\"}[${quarter}s])) /
      sum by (sdh_phase) (increase(${h}_count{sdh_stage=\"split\"}[${quarter}s]))")")
  done
  python3 -I - "${cols[@]}" <<'EOF2'
import json, sys
order = ["lookup", "open", "split", "failed_rows", "fail_store", "correction_rows", "announce", "checkpoint", "item"]
qs = []
for a in sys.argv[1:]:
    try:
        qs.append({r["metric"].get("sdh_phase", ""): float(r["value"][1]) for r in json.loads(a)["data"]["result"]})
    except Exception:
        qs.append({})
print(f"    {'':<16}" + "".join(f"{'Q' + str(i + 1):>9}" for i in range(4)))
for k in order:
    if any(k in q for q in qs):
        print(f"    {k:<16}" + "".join(f"{q[k]:9.2f}" if k in q and q[k] == q[k] else f"{'-':>9}" for q in qs))
EOF2
fi


# --- The tables, from the catalog -------------------------------------------------------------------------------

# Iceberg writes a new metadata file, numbered, at each commit: the number in the catalog's metadata_location counts
# the table's commits, with no read of the warehouse (the deployer may not read the data buckets).
version() { sed -E 's#.*/([0-9]+)-[^/]*\.metadata\.json$#\1#; s/^0+([0-9])/\1/'; }
say "Commits per table in the run (metadata versions, from the catalog)"
mapfile -t tables < <(db "SELECT catalog_name || ':' || table_namespace || '.' || table_name, metadata_location
  FROM public.iceberg_tables WHERE table_namespace LIKE '%$PREFIX%' ORDER BY 1")
for row in "${tables[@]}"; do
  IFS='|' read -r name location <<<"$row"
  now_v="$(version <<<"$location")"
  then_v="$(awk -v n="$name" '$1 == n {print $2}' "$out/versions0" 2>/dev/null || true)"
  printf '    %-44s %7d  (version %d)\n' "$name" "$((now_v - ${then_v:-0}))" "$now_v"
done

# --- KMS signatures, from the topics ------------------------------------------------------------------------------

# Every message on the pipeline's topics is signed once, with the sending stage's KMS key (an asymmetric Sign, the
# call with the 300/s quota, docs/PERFORMANCE.md §2.3). So the messages written in the run are its Sign calls.
# Unwrapping data keys (Decrypt) is cached per batch and not counted; CloudWatch's KMS usage is closed to the
# deployer.
offsets() {
  kubectl -n sdh-infra exec -i kafka-0 -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic "$1" 2>/dev/null | awk -F: '{sum += $3} END {print sum + 0}'
}
say "KMS Sign calls in the run (messages written, each signed once)"
kms_sign=0 kms_other=0
for t in landed split records fhir; do
  then_o="$(awk -v t="$t" '$1 == t {print $2}' "$out/offsets0" 2>/dev/null || true)"
  n=$(($(offsets "$P.$t") - ${then_o:-0}))
  printf '    %-16s %8d\n' "$P.$t" "$n"
  kms_sign=$((kms_sign + n))
done
[[ -s "$out/offsets0" ]] || say "  (no offsets saved at t0: counted from the topics' start)"
say "  $kms_sign in all; at the ~300/s quota that caps the pipeline at $((300 * ${n_gold:-0} / (kms_sign > 0 ? kms_sign : 1))) documents/s"

# --- Cost ---------------------------------------------------------------------------------------------------------

nodes="$(cat "$out/nodes" 2>/dev/null || kubectl get nodes --no-headers 2>/dev/null | wc -l)"
if [[ "$n_gold" -gt 0 ]]; then
  python3 -I -c 'import sys
secs, docs, nodes, sign, other = (float(a) for a in sys.argv[1:6])
node, eks, asym, req = (float(a) for a in sys.argv[6:10])
compute = secs / 3600 * (nodes * node + eks); kms = sign * asym + other * req
per = 1e6 / docs
print(f">> Estimated cost: ${compute:.2f} compute ({nodes:.0f} nodes at most, on-demand price) + ${kms:.2f} KMS;"
      f" ${(compute + kms) * per:,.0f} per million documents at this rate ({compute * per:,.0f} compute,"
      f" {kms * per:,.0f} KMS); storage and S3 requests not counted")' \
    "$secs" "$n_gold" "$nodes" "$kms_sign" "$kms_other" "$NODE_HOUR" "$EKS_HOUR" "$KMS_ASYMMETRIC" "$KMS_REQUEST"
fi
