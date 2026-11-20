#!/usr/bin/env bash
# Checks that the AWS deployment's settings and the load test's scripts still hold the fixes found on AWS
# (docs/PERFORMANCE.md §10, DESIGN.md revisions (dk) to (dv)), so none creeps back unnoticed. No AWS, cluster or network: it reads the files, and runs the
# load test's own functions against stand-ins for aws and kubectl.
#
#   check-config.sh            every check; exits 1 if any fails
#
# Run it before `poc.sh up`, and after changing anything under deploy/aws. Needs bash, python3 and gzip.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
K8S="$HERE/k8s"
P="$K8S/pipeline"
TF="$HERE/terraform/compute"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failures=0
pass() { echo "  ok    $1"; }
fail() { echo "  FAIL  $1"; failures=$((failures + 1)); }
check() {  # <what> <command>...: passes if the command does
  local what="$1"; shift
  if "$@" >/dev/null 2>&1; then pass "$what"; else fail "$what"; fi
}
func() {  # <file> <name>: a file holding the shell function's definition, to source: not inlined in a bash -c,
  # whose command line a pkill -f in the function would match
  awk -v n="$2" '$0 ~ "^" n "\\(\\) \\{" { on = 1 } on { print } on && /^}/ { exit }' "$1" > "$work/$2.sh"
  echo "$work/$2.sh"
}
setting() {  # <file> <regex with one group>: the group's first match
  sed -nE "s/$2/\1/p" "$1" | head -1
}

echo "Deploy settings"

# Spot gives two minutes' notice; a stage pod needs time to finish its batch and hand back its claims and any
# duties lease (DESIGN.md revision (dv), 2026-11-18). Under 60 s it can't; at 120 s or more the node goes first.
for stage in splitter convert transform; do
  grace="$(setting "$P/$stage-patch.yaml" '^ *terminationGracePeriodSeconds: *([0-9]+).*')"
  check "$stage pods have 60-119 s to stop (have ${grace:-none})" test "${grace:-0}" -ge 60 -a "${grace:-0}" -lt 120
done

# The read-only catalogs hold one Postgres connection, not Iceberg's two (revision (dv)): ~24 at the maxima.
for app in splitter transform; do
  check "$app's read-only catalog holds one connection (clients: 1)" grep -qE '^ +clients: 1$' "$P/$app-application.yml"
done

# One burst node group a zone, which Auto Scaling never rebalances: a group over three zones ended busy nodes
# without a drain (run 46, 2026-11-18).
burst="$(awk '/^resource "aws_eks_node_group" "burst"/ { on = 1 } on { print } on && /^}/ { exit }' "$TF/main.tf")"
check "burst nodes are one node group a zone" grep -qE 'for_each += data\.aws_subnet\.burst_zone' <<<"$burst"
check "each burst group is in one subnet" grep -qE 'subnet_ids += \[each\.value\.id\]' <<<"$burst"

# The Cluster Autoscaler caps the cluster at the base nodes, the duties node and burst_max, over all the burst
# groups.
zones="$(awk '/^variable "zones"/ { on = 1 } on && /default/ { print; exit }' "$TF/variables.tf" \
  | grep -o '"[a-z0-9-]*"' | wc -l)"
burst_max="$(awk '/^variable "burst_max"/ { on = 1 } on && /default/ { print $3; exit }' "$TF/variables.tf")"
cap="$(setting "$K8S/infra/cluster-autoscaler.yaml" '^ *- --max-nodes-total=([0-9]+).*')"
check "the autoscaler's cap is the $zones base nodes + the duties node + burst_max $burst_max (have ${cap:-none})" \
  test "${cap:-0}" -eq $((zones + 1 + burst_max))
check "the autoscaler balances the per-zone groups" \
  grep -qE '^ *- --balance-similar-node-groups' "$K8S/infra/cluster-autoscaler.yaml"

# Each scaled stage's duties are its duties pod's, on the on-demand duties node, never Spot; the stage's own pods
# take them up only as a fallback (DESIGN.md revision (dz), 2026-11-19).
duties="$(awk '/^resource "aws_eks_node_group" "duties"/ { on = 1 } on { print } on && /^}/ { exit }' "$TF/main.tf")"
check "the duties node is on-demand" grep -qE 'capacity_type += "ON_DEMAND"' <<<"$duties"
check "the duties node is tainted for the duties pods alone" grep -qE 'key += "sdh-poc/duties"' <<<"$duties"
check "the duties pod takes no input and needs the duties node" \
  bash -c "grep -qE 'SDH_STAGE_DUTIES_ONLY, value: \"true\"' '$P/duties-patch.yaml' \
    && grep -qE 'requiredDuringScheduling' '$P/duties-patch.yaml' && grep -qE 'values: \[duties\]' '$P/duties-patch.yaml'"
for stage in splitter convert transform; do
  check "$stage pods take the duties only as a fallback" \
    grep -qE 'SDH_STAGE_DUTIES_FALLBACKAFTER, value: [1-9]' "$P/$stage-patch.yaml"
done
check "apply.sh deploys each scaled stage's duties pod" test "$(grep -c 'duties-patch.yaml' "$P/apply.sh")" -ge 4

# The gold check's Job survives a pod disrupted by a node going, and keeps off nodes the autoscaler empties
# (2026-11-16: its burst node was removed part way through the read).
check "the gold check ignores a disrupted pod (podFailurePolicy)" \
  grep -qE 'action: Ignore, onPodConditions: \[\{ type: DisruptionTarget \}\]' "$P/gold-check.yaml"
check "the gold check's pod isn't safe to evict" \
  grep -qE 'cluster-autoscaler\.kubernetes\.io/safe-to-evict: "false"' "$P/gold-check.yaml"

# Landing is single-active: at most a holder and a standby, never autoscaled.
landing="$(setting "$P/landing-patch.yaml" '^  replicas: *([0-9]+).*')"
check "landing runs at most two pods, a holder and a standby (have ${landing:-none})" test "${landing:-9}" -le 2
check "landing isn't autoscaled" bash -c "! grep -qE 'scaleTargetRef: \{ name: landing' '$P/scaling.yaml'"

# Postgres: 2 GiB (512 MiB ran out at 200 backends, 2026-11-17), and connections enough for every stage pod at
# KEDA's maxima. A pod's pools, as measured at the maxima (docs/PERFORMANCE.md §10, runs 46-47): split 8, convert
# 4, transform 8; landing, the system and psql about 15 in all.
mem="$(sed -nE '/name: postgres, namespace/,/^---/ s/^ *limits: \{ memory: ([0-9]+)(Mi|Gi) \}.*/\1 \2/p' \
  "$K8S/infra/state.yaml" | head -1 | awk '{ print $2 == "Gi" ? $1 * 1024 : $1 }')"
check "Postgres has at least 2 GiB (have ${mem:-no} MiB)" test "${mem:-0}" -ge 2048
max_conn="$(setting "$K8S/infra/state.yaml" '.*max_connections=([0-9]+).*')"
read -r split_max convert_max transform_max <<<"$(awk '
  /scaleTargetRef/ { name = $0 }
  /maxReplicaCount/ {
    if (name ~ /splitter/) s = $2; else if (name ~ /convert/) c = $2; else if (name ~ /transform/) t = $2
  }
  END { print s, c, t }' "$P/scaling.yaml")"
needed=$((split_max * 8 + convert_max * 4 + transform_max * 8 + 15))
check "Postgres allows the stages' connections at their maxima ($split_max/$convert_max/$transform_max: ~$needed of\
 ${max_conn:-none})" test "${max_conn:-0}" -ge "$needed"
check "Postgres allows the scaled-out maxima of runs 45-48 (6/12/18: ~$((6 * 8 + 12 * 4 + 18 * 8 + 15)))" \
  test "${max_conn:-0}" -ge $((6 * 8 + 12 * 4 + 18 * 8 + 15))

# Every client comes through PgBouncer (k8s/infra/state.yaml, DESIGN.md revision (ea)), in transaction pooling,
# which pgjdbc's prepared statements need 1.21 or later for, with max_prepared_statements; its connections stay
# under max_connections.
check "no stage connects to Postgres but through PgBouncer" \
  bash -c "! grep -qE 'postgres\.sdh-infra\.svc' '$P'/*-application.yml"
# The gold check's role is made by gold-check.sh, which PgBouncer's users (poc.sh) don't include: it goes to
# Postgres itself, as psql does (run 50, 2026-11-19: through PgBouncer its login failed).
check "the gold check connects to Postgres itself" \
  grep -qE 'CATALOG_URI, value: "jdbc:postgresql://postgres\.sdh-infra\.svc' "$P/gold-check.yaml"
check "PgBouncer pools transactions and keeps pgjdbc's prepared statements" \
  bash -c "grep -qE '^ +pool_mode = transaction$' '$K8S/infra/state.yaml' \
    && grep -qE '^ +max_prepared_statements = [1-9]' '$K8S/infra/state.yaml' \
    && grep -qE 'image: edoburu/pgbouncer:v1\.(2[1-9]|[3-9][0-9])' '$K8S/infra/state.yaml'"
pooled="$(setting "$K8S/infra/state.yaml" '^ +max_db_connections = ([0-9]+)$')"
check "PgBouncer keeps under max_connections, with room for psql (${pooled:-none} of ${max_conn:-none})" \
  test "${pooled:-999}" -le $((${max_conn:-0} - 20))
check "the clients' network policy lets the stages reach PgBouncer" \
  grep -qE 'name: clients-to-pgbouncer' "$K8S/infra/namespace.yaml"

echo "Load test"

for script in "$HERE"/*.sh "$P"/*.sh; do
  check "$(basename "$script") parses" bash -n "$script"
done

# Stand-ins for the CLIs, on the PATH ahead of the real ones.
mkdir -p "$work/bin"
cat > "$work/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
# A cluster with one stage pod running and one still starting, which has no log yet.
case "$*" in
  *"get pods"*)
    echo pod/transform-running
    [[ "$*" == *status.phase=Running* ]] || echo pod/transform-starting ;;
  *"logs -f"*)
    [[ "$*" == *transform-starting* ]] && exit 1
    echo "2026-11-18T00:00:00Z WARN 1 --- sdh.audit : stage.claims_recovered stage=transform count=1" ;;
esac
EOF
cat > "$work/bin/aws" <<'EOF'
#!/usr/bin/env bash
# s3 cp: notes each call; the first whole-slice copy fails as $AWS_MODE says.
echo "$*" >> "$CALLS"
# head-object: the object is in the intake only after a failed copy that was written all the same.
if [[ "$1 $2" == "s3api head-object" ]]; then [[ "$AWS_MODE" == written ]] && exit 0; exit 254; fi
if [[ "$*" == *--recursive* && ! -e "$CALLS.once" ]]; then
  touch "$CALLS.once"
  case "$AWS_MODE" in
    part-way|written) echo "copy failed: s3://staging/s00000/b.xml to s3://intake/" >&2; exit 1 ;;
    listing) echo "An error occurred (RequestTimeout) when calling the ListObjectsV2 operation" >&2; exit 1 ;;
  esac
fi
exit 0
EOF
chmod +x "$work/bin/kubectl" "$work/bin/aws"

# Stopping the log follower, in the EXIT trap under set -e, must not fail when there is nothing left to stop: it
# ended both runs 47 and 48 with 1, the logs not gzipped and the staged copies left in S3 (2026-11-18).
mkdir -p "$work/stop/pod-logs"; echo line > "$work/stop/pod-logs/a.log"
check "stopping the log follower with nothing left to stop doesn't end the run" bash -euo pipefail -c "
  source '$(func "$P/load-test.sh" stop_logs)'
  out='$work/stop'; sleep 30 & log_follower=\$!; kill \$log_follower; wait \$log_follower 2>/dev/null || true
  stop_logs; echo after > '$work/stop/after'"
check "stopping the log follower gzips the logs" test -s "$work/stop/after" -a -f "$work/stop/pod-logs/a.log.gz"

# The follower takes up only running pods: a pod still starting has no log, so `logs -f` ends at once and the pod
# was never looked at again (about half of run 47's pods left no log, 2026-11-18).
mkdir -p "$work/follow/pod-logs"
PATH="$work/bin:$PATH" bash -c "
  source '$(func "$P/load-test.sh" follow_logs)'
  sleep() { exit 0; }   # one pass of the loop
  N=sdh-ccda-push out='$work/follow'; follow_logs" >/dev/null 2>&1
command sleep 1
check "the log follower keeps a running pod's log" test -s "$work/follow/pod-logs/sdh-fhir_transform-running.log"
check "the log follower leaves a starting pod for later" \
  bash -c "! ls '$work/follow/pod-logs' | grep -q starting"

# A slice copy that failed part way copies again only the objects that failed, never one already copied, which
# landing may have landed and deleted (2026-11-17: re-copied objects were landed twice); and of those, only one not
# in the intake already, since a copy reported failed may have been written (run 54: the CLI's own retry wrote an
# object twice). One that failed listing copied nothing, and is synced, which copies only what isn't there.
copy_slice_with() {  # <mode>
  mkdir -p "$work/$1/copy-errors"; : > "$work/$1/copied-again"; : > "$work/$1/written-anyway"
  CALLS="$work/$1/calls" AWS_MODE="$1" PATH="$work/bin:$PATH" bash -c "
    source '$(func "$P/load-test.sh" copy_slice)'
    sleep() { :; }
    staging=s3://staging/ out='$work/$1' INTAKE=s3://intake/; copy_slice 0"
}
check "a slice copy that failed part way ends copied" copy_slice_with part-way
check "it copies again only the object that failed, once it finds it isn't there" bash -c "
  [[ \$(grep -c -- --recursive '$work/part-way/calls') == 1 ]] \
    && grep -qx 's3api head-object --bucket intake --key b.xml' '$work/part-way/calls' \
    && grep -qx 's3 cp --only-show-errors s3://staging/s00000/b.xml s3://intake/' '$work/part-way/calls' \
    && [[ \$(wc -l < '$work/part-way/calls') == 3 ]] \
    && grep -qx 's3://staging/s00000/b.xml' '$work/part-way/copied-again'"
check "a copy reported failed but written ends copied" copy_slice_with written
check "it isn't copied again" bash -c "
  [[ \$(wc -l < '$work/written/calls') == 2 ]] && [[ ! -s '$work/written/copied-again' ]] \
    && grep -qx 's3://staging/s00000/b.xml' '$work/written/written-anyway'"
check "a slice copy whose listing failed ends copied" copy_slice_with listing
check "it syncs the slice, copying only what isn't there" bash -c "
  [[ \$(grep -c -- --recursive '$work/listing/calls') == 1 ]] \
    && grep -qx 's3 sync --size-only --only-show-errors s3://staging/s00000/ s3://intake/' '$work/listing/calls' \
    && [[ ! -s '$work/listing/copied-again' ]]"

# Each copy of a test document differs, by an element of the load test's own namespace, so no stage takes one
# for a resend of another (2026-11-15: comments were stripped, and transform's copies were identical again); and
# each stays well formed. healthcare-data-transformer's GoldCheckTest holds GoldCheck to this marker.
mkdir -p "$work/docs"
printf '<?xml version="1.0"?>\n<ClinicalDocument xmlns="urn:hl7-org:v3"><title>t</title></ClinicalDocument>\n' \
  > "$work/docs/one.xml"
check "the load test makes its inputs" \
  python3 -I "$P/load-inputs.py" "$work/inputs" run1 3 0 "$work/docs" "$ROOT/connector-core/src/test/resources/ccda"
check "each copy of a document differs from the others" bash -c "
  [[ \$(ls '$work'/inputs/c000*-00.xml | wc -l) == 3 ]] \
    && [[ \$(md5sum '$work'/inputs/c000*-00.xml | cut -d' ' -f1 | sort -u | wc -l) == 3 ]]"
check "each copy is well formed, its marker the load test's element before the end tag" python3 -I -c "
import glob, sys, xml.etree.ElementTree as ET
for f in glob.glob(sys.argv[1] + '/c*.xml'):
    root = ET.parse(f).getroot()
    assert root[-1].tag == '{urn:sdh-poc:load}sdhLoadCopy' and root[-1].get('run') == 'run1', f
" "$work/inputs"

echo
if ((failures)); then echo "check-config.sh: $failures check(s) failed"; exit 1; fi
echo "check-config.sh: all checks passed"
