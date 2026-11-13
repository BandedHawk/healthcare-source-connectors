#!/usr/bin/env bash
# Starts and stops the AWS proof of concept (docs/AWS-POC.md), so it costs money only while in use.
#
#   poc.sh bootstrap                  once: the Terraform state bucket and the persistent layer
#   poc.sh up [--hours N]             create the cluster and start the services (default: what's left of today's 8 h)
#   poc.sh up --long-test "reason" --hours N
#                                     beyond the 8-hour day, for a resilience or performance test only
#   poc.sh up ... --on-demand-burst   burst nodes on demand, not Spot, so no interruption spoils a measurement
#   poc.sh extend N [--long-test "reason"]
#                                     move the automatic shutdown to N hours from now
#   poc.sh down [--no-drain]          drain the pipeline, stop the services, snapshot the state volumes, destroy the cluster
#   poc.sh status                     what's running, when it stops, today's hours, this month's cost
#   poc.sh reset [--yes]              with a session up: delete every pipeline's data (tables, topics, offsets,
#                                     registries, leases, intake), then start the pipeline again on nothing
#
# Every `up` first schedules an automatic shutdown on AWS (EventBridge Scheduler, then the sdh-poc-auto-down
# state machine). So a session ends on time even if this machine is off or asleep, or the script fails half way.
#
# The cluster runs at most 8 hours in a local calendar day, counting every session that day. --long-test lifts the
# limit for one session. The reason is logged, and the session still ends at its deadline.
#
# Runs as the claude-poc profile (claude-poc-deployer). Needs: aws, terraform >= 1.10, kubectl, python3, curl,
# envsubst, openssl.
#
# State: $SDH_POC_STATE (default ~/.local/state/sdh-poc). Never delete it: it holds the database passwords (and
# later the stages' keys). Without it the kept Postgres volume can't be logged into.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TF="$HERE/terraform"
K8S="$HERE/k8s"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
export KUBECONFIG="$STATE/kubeconfig"

ACCOUNT=493469777520
CLUSTER=sdh-poc
TFSTATE_BUCKET="sdh-poc-tfstate-$ACCOUNT"
SCHEDULE=sdh-poc-auto-down
DAILY_HOURS=8
LONG_TEST_MAX_HOURS=24
SNAPSHOTS_KEPT=3
PIPELINES="${SDH_PIPELINES:-ccda-push}"
KEDA_VERSION="2.21.0"
KEDA_SHA256="b43c89ffeef81722d7e2dd2c079d74789767a0f89cae1336cff784994814f6d7"

say() { printf '>> %s\n' "$*"; }
die() { printf 'poc.sh: %s\n' "$*" >&2; exit 1; }
infra() { kubectl -n sdh-infra "$@"; }
kafka() { infra exec kafka-0 -- "$@"; }
prefix() { echo "${1//-/_}"; }

need_tools() {
  local t missing=()
  for t in aws terraform kubectl python3 curl envsubst openssl; do command -v "$t" >/dev/null || missing+=("$t"); done
  [[ ${#missing[@]} -eq 0 ]] || die "missing tools: ${missing[*]} (deploy/aws/README.md, Tools)"
  terraform version -json | python3 -I -c 'import json,sys; v=json.load(sys.stdin)["terraform_version"].split("."); sys.exit(0 if (int(v[0]),int(v[1]))>=(1,10) else 1)' \
    || die "terraform 1.10 or later is needed (S3 state locking)"
}

state_dir() {
  mkdir -p "$STATE/downloads"
  chmod 700 "$STATE"
  touch "$STATE/sessions"
}

# secret <name>: a random secret made once, kept in the state directory.
secret() {
  local f="$STATE/secrets/$1"
  mkdir -p "$STATE/secrets"; chmod 700 "$STATE/secrets"
  [[ -s "$f" ]] || (umask 077; openssl rand -base64 24 | tr -d '/+=\n' > "$f")
  cat "$f"
}

tf() { local layer="$1"; shift; terraform -chdir="$TF/$layer" "$@"; }
tf_init() { tf "$1" init -input=false -upgrade=false >/dev/null; }
persistent_output() {
  tf persistent output -json | python3 -I -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]]["value"])' "$1"
}
volume_id() {
  tf persistent output -json | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["state_volume_ids"]["value"][sys.argv[1]])' "$1"
}

cluster_exists() { aws eks describe-cluster --name "$CLUSTER" >/dev/null 2>&1; }

# --- The 8-hour day ------------------------------------------------------------------------------------------
# $STATE/sessions holds one line per session: start epoch, planned end epoch, actual end epoch (or -), reason.
# A session that ended without `down` (the guard stopped it) counts up to its planned end.

used_today() {
  python3 -I - "$STATE/sessions" <<'EOF'
import sys, time, datetime
now = time.time()
midnight = datetime.datetime.combine(datetime.date.today(), datetime.time()).timestamp()
used = 0
for line in open(sys.argv[1]):
    f = line.split("\t")
    if len(f) < 3: continue
    start, planned = float(f[0]), float(f[1])
    end = float(f[2]) if f[2] != "-" else min(planned, now)
    used += max(0, min(end, now) - max(start, midnight))
print(int(used))
EOF
}

open_session() { awk -F'\t' '$3 == "-"' "$STATE/sessions" | tail -n 1; }

close_sessions() {  # marks every open session ended now
  python3 -I - "$STATE/sessions" <<'EOF'
import sys, time
p = sys.argv[1]; now = str(int(time.time()))
lines = open(p).read().splitlines()
out = []
for l in lines:
    f = l.split("\t")
    if len(f) >= 3 and f[2] == "-":
        f[2] = str(min(int(float(f[1])), int(now)))
    out.append("\t".join(f))
open(p, "w").write("".join(x + "\n" for x in out))
EOF
}

# deadline_for <hours> <long-test reason>: prints the epoch the session must end by.
deadline_for() {
  local hours="$1" reason="$2" now used left
  now="$(date +%s)"
  if [[ -n "$reason" ]]; then
    python3 -I -c "import sys; h=float(sys.argv[1]); sys.exit(0 if 0 < h <= $LONG_TEST_MAX_HOURS else 1)" "$hours" \
      || die "--hours must be more than 0 and at most $LONG_TEST_MAX_HOURS"
    echo $(( now + $(python3 -I -c 'import sys; print(int(float(sys.argv[1])*3600))' "$hours") ))
    return
  fi
  used="$(used_today)"
  left=$(( DAILY_HOURS * 3600 - used ))
  (( left > 600 )) || die "today's $DAILY_HOURS hours are used up ($(( used / 60 )) min). For a resilience or performance test: --long-test \"reason\" --hours N"
  if [[ -n "$hours" ]]; then
    local want; want="$(python3 -I -c 'import sys; print(int(float(sys.argv[1])*3600))' "$hours")"
    (( want <= left )) || die "only $(( left / 60 )) min of today's $DAILY_HOURS hours are left; ask for less, or use --long-test"
    left="$want"
  fi
  echo $(( now + left ))
}

schedule_auto_down() {  # <epoch>
  local at sfn role target
  at="$(date -u -d "@$1" +%Y-%m-%dT%H:%M:%S)"
  sfn="$(persistent_output guard_state_machine_arn)"
  role="$(persistent_output scheduler_role_arn)"
  target="{\"Arn\":\"$sfn\",\"RoleArn\":\"$role\",\"Input\":\"{}\",\"RetryPolicy\":{\"MaximumRetryAttempts\":10,\"MaximumEventAgeInSeconds\":3600}}"
  local args=(--name "$SCHEDULE" --schedule-expression "at($at)" --schedule-expression-timezone UTC
              --flexible-time-window Mode=OFF --target "$target" --action-after-completion DELETE
              --description "Destroys the sdh-poc cluster at the session's end (deploy/aws/poc.sh)")
  if aws scheduler get-schedule --name "$SCHEDULE" >/dev/null 2>&1; then
    aws scheduler update-schedule "${args[@]}" >/dev/null
  else
    aws scheduler create-schedule "${args[@]}" >/dev/null
  fi
  say "automatic shutdown at $(date -d "@$1" '+%a %H:%M %Z') ($at UTC)"
}

# --- bootstrap -----------------------------------------------------------------------------------------------

cmd_bootstrap() {
  need_tools; state_dir
  if ! aws s3api head-bucket --bucket "$TFSTATE_BUCKET" >/dev/null 2>&1; then
    say "Terraform state bucket $TFSTATE_BUCKET"
    aws s3api create-bucket --bucket "$TFSTATE_BUCKET" --create-bucket-configuration LocationConstraint=us-east-2 >/dev/null
    aws s3api put-public-access-block --bucket "$TFSTATE_BUCKET" --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    aws s3api put-bucket-versioning --bucket "$TFSTATE_BUCKET" --versioning-configuration Status=Enabled
    aws s3api put-bucket-encryption --bucket "$TFSTATE_BUCKET" --server-side-encryption-configuration \
      '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
    aws s3api put-bucket-tagging --bucket "$TFSTATE_BUCKET" --tagging 'TagSet=[{Key=Project,Value=sdh-poc}]'
    aws s3api put-bucket-lifecycle-configuration --bucket "$TFSTATE_BUCKET" --lifecycle-configuration \
      '{"Rules":[{"ID":"old-state-versions","Status":"Enabled","Filter":{},"NoncurrentVersionExpiration":{"NoncurrentDays":30}}]}'
  fi
  # EKS checks with iam:GetRole that its service-linked roles exist before it makes a node group. The deployer may
  # only read them once they exist, because a missing role's ARN has no /aws-service-role/ path. So they're made
  # here first (found on the first trial, 2026-11-12).
  local svc
  for svc in eks.amazonaws.com eks-nodegroup.amazonaws.com autoscaling.amazonaws.com spot.amazonaws.com; do
    aws iam create-service-linked-role --aws-service-name "$svc" >/dev/null 2>&1 || true
  done
  say "Persistent layer (keys, buckets, volumes, roles, images, guard)"
  tf_init persistent
  tf persistent apply -input=false
}

# --- up ------------------------------------------------------------------------------------------------------

cmd_up() {
  local hours="" reason="" burst=SPOT
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --hours) hours="${2:?}"; shift 2 ;;
      --long-test) reason="${2:?--long-test needs a reason}"; shift 2 ;;
      --on-demand-burst) burst=ON_DEMAND; shift ;;
      *) die "up: unknown option $1" ;;
    esac
  done
  need_tools; state_dir
  tf_init persistent; tf_init compute

  local deadline now
  if [[ -n "$(open_session)" ]] && cluster_exists; then
    say "a session is already open; use extend to change its end"
  else
    close_sessions
    deadline="$(deadline_for "$hours" "$reason")"
    now="$(date +%s)"
    # First, so even an up that fails half way ends on time.
    schedule_auto_down "$deadline"
    printf '%s\t%s\t-\t%s\n' "$now" "$deadline" "${reason:-}" >> "$STATE/sessions"
    [[ -z "$reason" ]] || say "long test: $reason"
  fi

  local ip; ip="$(curl -s -m 10 https://checkip.amazonaws.com | tr -d '[:space:]')"
  [[ "$ip" =~ ^[0-9.]+$ ]] || die "couldn't find this machine's public address"
  say "Cluster $CLUSTER (API open to $ip/32 only; about 15 minutes the first time)"
  [[ "$burst" == SPOT ]] || say "burst nodes on demand"
  tf compute apply -input=false -auto-approve -var "api_cidrs=[\"$ip/32\"]" -var "burst_capacity_type=$burst"
  aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null

  start_services
  setup_keda
  setup_topics
  setup_database
  if [[ -x "$K8S/pipeline/apply.sh" ]]; then
    "$K8S/pipeline/apply.sh"
  else
    say "the pipeline itself isn't configured for AWS yet (docs/AWS-POC.md §10, step 4)"
  fi
  # Stages paused by the last `down` start again.
  resume_stages
  cmd_status
}

start_services() {
  say "Postgres and Kafka on the kept volumes"
  export POSTGRES_VOLUME_ID KAFKA_VOLUME_ID STATE_ZONE
  POSTGRES_VOLUME_ID="$(volume_id postgres)"
  KAFKA_VOLUME_ID="$(volume_id kafka)"
  STATE_ZONE="$(persistent_output state_zone)"
  kubectl apply -f "$K8S/infra/namespace.yaml" >/dev/null
  kubectl -n sdh-infra create secret generic postgres --from-literal=password="$(secret postgres)" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  # shellcheck disable=SC2016
  envsubst '$POSTGRES_VOLUME_ID $KAFKA_VOLUME_ID $STATE_ZONE' < "$K8S/infra/state.yaml" | kubectl apply -f - >/dev/null
  infra scale statefulset/postgres statefulset/kafka --replicas=1 >/dev/null
  kubectl apply -f "$K8S/infra/cluster-autoscaler.yaml" >/dev/null
  infra rollout status statefulset/postgres --timeout=600s >/dev/null
  infra rollout status statefulset/kafka --timeout=600s >/dev/null
}

setup_keda() {
  local manifest="$STATE/downloads/keda-$KEDA_VERSION.yaml" work
  if [[ ! -s "$manifest" ]]; then
    curl -sSfL --max-time 120 -o "$manifest.part" \
      "https://github.com/kedacore/keda/releases/download/v$KEDA_VERSION/keda-$KEDA_VERSION.yaml"
    mv "$manifest.part" "$manifest"
  fi
  [[ "$(sha256sum "$manifest" | cut -d' ' -f1)" == "$KEDA_SHA256" ]] \
    || { rm -f "$manifest"; die "KEDA $KEDA_VERSION's manifest doesn't match its pinned checksum"; }
  say "KEDA $KEDA_VERSION"
  # As in the sandbox: its three pods held to 128 MiB each (they use under 50), on the base nodes.
  work="$(mktemp -d)"
  cp "$manifest" "$work/keda.yaml"
  cat > "$work/kustomization.yaml" <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: [keda.yaml]
patches:
  - target: { kind: Deployment, namespace: keda }
    patch: |
      - { op: replace, path: /spec/template/spec/containers/0/resources,
          value: { requests: { cpu: 50m, memory: 128Mi }, limits: { memory: 128Mi } } }
      - { op: add, path: /spec/template/spec/nodeSelector, value: { "sdh-poc/pool": base } }
EOF
  kubectl kustomize "$work" | kubectl apply --server-side --force-conflicts -f - >/dev/null
  rm -rf "$work"
  local w
  for w in keda-operator keda-metrics-apiserver keda-admission; do
    kubectl -n keda rollout status "deploy/$w" --timeout=300s >/dev/null
  done
}

# The stages' inputs get a partition for each pod KEDA may run times its concurrency (STAGES.md §10):
#   - split: up to 2 pods, so 4 partitions;
#   - convert: up to 3, so 6;
#   - transform: up to 4 pods at concurrency 2, so 8.
# Partitions can only be added later, with the topic drained. Everything else gets 1.
setup_topics() {
  say "Kafka topics"
  local p topic parts
  for p in $PIPELINES; do
    for topic in "$p.landed:4" "$p.split:6" "$p.records:8" "$p.fhir:1" "$p.convert.dlq:1" "$p.fhir.dlq:1"; do
      parts="${topic##*:}"; topic="${topic%:*}"
      kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
        --topic "$topic" --partitions "$parts" --replication-factor 1 >/dev/null
    done
  done
}

# A login role per stage, all acting as sdh_catalog, as in the sandbox (scripts/sandbox.sh setup_database).
setup_database() {
  say "Database roles"
  local p role name sql
  sql="SELECT 'CREATE ROLE sdh_catalog NOLOGIN' WHERE NOT EXISTS
         (SELECT FROM pg_roles WHERE rolname = 'sdh_catalog')\\gexec
       GRANT ALL ON SCHEMA public TO sdh_catalog;
       GRANT CREATE ON DATABASE sdh TO sdh_catalog;"
  for p in $PIPELINES; do
    for role in landing splitter convert fhir; do
      name="${role}_$(prefix "$p")"
      sql+="
       SELECT 'CREATE ROLE $name LOGIN IN ROLE sdh_catalog' WHERE NOT EXISTS
         (SELECT FROM pg_roles WHERE rolname = '$name')\\gexec
       ALTER ROLE $name PASSWORD '$(secret "db-$role-$p")';
       ALTER ROLE $name SET role = 'sdh_catalog';"
    done
  done
  printf '%s\n' "$sql" | infra exec -i postgres-0 -- psql -q -v ON_ERROR_STOP=1 -U postgres -d sdh >/dev/null
}

pipeline_namespaces() {
  kubectl get ns -l sdh.smiledigitalhealth.com/infra-client=true -o name 2>/dev/null | sed 's|namespace/||'
}

resume_stages() {
  local n
  for n in $(pipeline_namespaces); do
    kubectl -n "$n" annotate scaledobject --all autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1 || true
    kubectl -n "$n" annotate deploy --all sdh-poc/paused- >/dev/null 2>&1 || true
  done
}

# --- extend --------------------------------------------------------------------------------------------------

cmd_extend() {
  local hours="${1:?extend <hours>}" reason=""; shift
  # A number, or the deadline would be now: `extend --hours 2` once scheduled the shutdown for that moment.
  [[ "$hours" =~ ^[0-9]+(\.[0-9]+)?$ ]] && python3 -I -c 'import sys; sys.exit(float(sys.argv[1]) <= 0)' "$hours" \
    || die "extend takes a number of hours: poc.sh extend N [--long-test \"reason\"]"
  [[ "${1:-}" == "--long-test" ]] && reason="${2:?--long-test needs a reason}"
  need_tools; state_dir; tf_init persistent
  [[ -n "$(open_session)" ]] || die "no open session"
  local deadline now
  now="$(date +%s)"
  # The open session's time so far counts as used; the new end must fit today's remaining hours.
  python3 -I - "$STATE/sessions" "$now" <<'EOF'
import sys
p, now = sys.argv[1], sys.argv[2]
lines = open(p).read().splitlines()
for i in range(len(lines) - 1, -1, -1):
    f = lines[i].split("\t")
    if len(f) >= 3 and f[2] == "-":
        f[1] = now; lines[i] = "\t".join(f); break
open(p, "w").write("".join(x + "\n" for x in lines))
EOF
  deadline="$(deadline_for "$hours" "$reason")"
  schedule_auto_down "$deadline"
  python3 -I - "$STATE/sessions" "$deadline" "$reason" <<'EOF'
import sys
p, d, r = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(p).read().splitlines()
for i in range(len(lines) - 1, -1, -1):
    f = lines[i].split("\t")
    if len(f) >= 3 and f[2] == "-":
        f[1] = d
        if r: f[3:] = [((f[3] + "; ") if len(f) > 3 and f[3] else "") + r]
        lines[i] = "\t".join(f); break
open(p, "w").write("".join(x + "\n" for x in lines))
EOF
}

# --- down ----------------------------------------------------------------------------------------------------

cmd_down() {
  local drain=1
  [[ "${1:-}" == "--no-drain" ]] && drain=0
  need_tools; state_dir
  tf_init persistent; tf_init compute

  if cluster_exists; then
    aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
    if (( drain )); then drain_pipeline; fi
    stop_services
  else
    say "no cluster (already down, or stopped by the guard)"
  fi

  snapshot_volumes
  say "Destroying the cluster"
  tf compute destroy -input=false -auto-approve -var 'api_cidrs=["127.0.0.1/32"]'
  aws scheduler delete-schedule --name "$SCHEDULE" >/dev/null 2>&1 || true
  close_sessions
  prune_snapshots
  leftovers
  say "down. Kept: buckets, keys, images, state volumes and snapshots (about \$11 a month)"
}

# Stops intake, then waits until every stage has caught up: no consumer group has lag. Up to 20 minutes, then it
# goes on, as a crash would. Nothing is lost either way: the stages pick up where they stopped.
drain_pipeline() {
  local n
  for n in $(pipeline_namespaces); do
    if kubectl -n "$n" get deploy landing >/dev/null 2>&1; then
      say "pausing intake ($n/landing)"
      kubectl -n "$n" annotate deploy landing sdh-poc/paused=true --overwrite >/dev/null
      kubectl -n "$n" scale deploy landing --replicas=0 >/dev/null
    fi
  done
  infra get pod kafka-0 >/dev/null 2>&1 || return 0
  say "draining (waiting for every consumer group's lag to reach 0, up to 20 min)"
  local i lag
  for i in $(seq 1 80); do
    lag="$(kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null \
      | awk '$1 != "GROUP" && $6 ~ /^[0-9]+$/ { s += $6 } END { print s + 0 }')"
    [[ "$lag" == 0 ]] && { say "drained"; return 0; }
    (( i % 4 == 1 )) && say "  lag $lag"
    sleep 15
  done
  say "still lag $lag after 20 min: stopping anyway"
}

# Scales every stage to 0 (KEDA paused at 0, so it doesn't start them again), then stops Postgres and Kafka
# cleanly so the snapshots are consistent.
stop_services() {
  local n
  for n in $(pipeline_namespaces); do
    kubectl -n "$n" annotate scaledobject --all autoscaling.keda.sh/paused-replicas=0 --overwrite >/dev/null 2>&1 || true
    kubectl -n "$n" scale deploy --all --replicas=0 >/dev/null 2>&1 || true
  done
  for n in $(pipeline_namespaces); do
    kubectl -n "$n" wait --for=delete pod --all --timeout=180s >/dev/null 2>&1 || true
  done
  if infra get statefulset postgres >/dev/null 2>&1; then
    say "stopping Kafka and Postgres"
    infra scale statefulset/kafka --replicas=0 >/dev/null
    infra wait --for=delete pod/kafka-0 --timeout=180s >/dev/null 2>&1 || true
    infra scale statefulset/postgres --replicas=0 >/dev/null
    infra wait --for=delete pod/postgres-0 --timeout=180s >/dev/null 2>&1 || true
  fi
}

snapshot_volumes() {
  local v id
  for v in postgres kafka; do
    id="$(volume_id "$v")"
    aws ec2 create-snapshot --volume-id "$id" --description "sdh-poc $v $(date -u +%FT%TZ)" \
      --tag-specifications "ResourceType=snapshot,Tags=[{Key=Project,Value=sdh-poc},{Key=sdh-poc/volume,Value=$v}]" \
      --query SnapshotId --output text | sed "s/^/>> snapshot of $v: /"
  done
}

prune_snapshots() {
  local v old
  for v in postgres kafka; do
    for old in $(aws ec2 describe-snapshots --owner-ids self \
        --filters "Name=tag:sdh-poc/volume,Values=$v" "Name=tag:Project,Values=sdh-poc" \
        --query "reverse(sort_by(Snapshots,&StartTime))[$SNAPSHOTS_KEPT:].SnapshotId" --output text); do
      aws ec2 delete-snapshot --snapshot-id "$old" && say "removed old snapshot $old ($v)"
    done
  done
}

# Anything the cluster made that its deletion doesn't take with it would go on costing money.
leftovers() {
  local vols enis
  vols="$(aws ec2 describe-volumes --filters Name=tag-key,Values=kubernetes.io/created-for/pvc/name \
          --query 'Volumes[].VolumeId' --output text)"
  enis="$(aws ec2 describe-network-interfaces --filters "Name=tag:cluster.k8s.amazonaws.com/name,Values=$CLUSTER" \
          Name=status,Values=available --query 'NetworkInterfaces[].NetworkInterfaceId' --output text)"
  [[ -z "$vols" ]] || say "WARNING: volumes made by the cluster are left over: $vols"
  [[ -z "$enis" ]] || say "note: unattached network interfaces left by the CNI (free; AWS removes them): $enis"
}

# --- reset ---------------------------------------------------------------------------------------------------
# Puts the pipeline back as it was before anything was landed, for a clean test. With a session up, it:
#   1. deletes every stage's workloads (deployments, KEDA scaling, any gold check) and waits for the pods to go;
#   2. deletes each pipeline's topics and every consumer group, then makes the topics again;
#   3. drops and recreates the database sdh: the Iceberg catalog, leases, registries, checkpoints, fail store;
#   4. empties the bronze, silver and gold buckets, and each pipeline's prefix of the intake bucket (and its claim);
#   5. deploys the pipeline again (apply.sh), which makes its tables afresh as it starts.
# Kept: the KMS keys, the state directory (passwords, key pairs, images, load reports), the tools bucket, the
# images, the roles. Synthetic data only (docs/AWS-POC.md): what this deletes can't be got back.
cmd_reset() {
  local yes=0
  [[ "${1:-}" == "--yes" ]] && yes=1
  need_tools; state_dir
  cluster_exists || die "reset needs a session: poc.sh up"
  aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
  local buckets=("sdh-poc-bronze-$ACCOUNT" "sdh-poc-silver-$ACCOUNT" "sdh-poc-gold-$ACCOUNT") p b n answer
  if (( ! yes )); then
    printf 'This deletes all PoC pipeline data for good: the database sdh, the topics of %s, every consumer\n' "$PIPELINES"
    printf 'group, the buckets %s, and the intake prefixes. Type reset to go on: ' "${buckets[*]}"
    read -r answer
    [[ "$answer" == reset ]] || die "not reset"
  fi

  say "Stopping the stages"
  # The workloads go, not just their pods: the stages leave replicas to KEDA, so a deployment scaled to 0 would
  # stay at 0 when apply.sh applies it again. apply.sh then creates them as on a new cluster.
  for n in $(pipeline_namespaces); do
    kubectl -n "$n" delete scaledobject --all --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "$n" delete job,deploy --all --ignore-not-found >/dev/null 2>&1 || true
  done
  for n in $(pipeline_namespaces); do
    kubectl -n "$n" wait --for=delete pod --all --timeout=300s >/dev/null 2>&1 \
      || die "the pods in $n didn't stop; nothing has been deleted yet"
  done

  say "Kafka: deleting the topics and consumer groups"
  local kt=/opt/kafka/bin/kafka-topics.sh kg=/opt/kafka/bin/kafka-consumer-groups.sh bs="--bootstrap-server localhost:9092"
  for p in $PIPELINES; do
    # shellcheck disable=SC2086
    if kafka $kt $bs --list | grep -q "^$p\."; then
      kafka $kt $bs --delete --topic "${p//./\\.}\\..*" >/dev/null
    fi
  done
  local i left
  for i in $(seq 1 60); do
    # shellcheck disable=SC2086
    left="$(kafka $kt $bs --list | grep -cE "^($(echo "$PIPELINES" | tr ' ' '|'))\." || true)"
    [[ "$left" == 0 ]] && break
    sleep 2
  done
  [[ "$left" == 0 ]] || die "Kafka still lists $left of the pipeline's topics"
  # shellcheck disable=SC2086
  if [[ -n "$(kafka $kg $bs --list 2>/dev/null)" ]]; then kafka $kg $bs --delete --all-groups >/dev/null; fi
  setup_topics

  say "Postgres: dropping and recreating the database sdh"
  infra exec -i postgres-0 -- psql -q -v ON_ERROR_STOP=1 -U postgres -d postgres >/dev/null <<'SQL'
DROP DATABASE IF EXISTS sdh WITH (FORCE);
CREATE DATABASE sdh;
SQL
  setup_database
  # The marker that shows the database survives a new cluster (docs/AWS-POC.md, Status), made again.
  infra exec -i postgres-0 -- psql -q -v ON_ERROR_STOP=1 -U postgres -d sdh >/dev/null <<'SQL'
CREATE TABLE sdh_poc_marker (made_at timestamptz NOT NULL DEFAULT now(), note text NOT NULL);
INSERT INTO sdh_poc_marker (note) VALUES ('made by poc.sh reset');
SQL

  say "S3: emptying bronze, silver and gold, and the intake prefixes"
  for b in "${buckets[@]}"; do aws s3 rm "s3://$b" --recursive --only-show-errors; done
  for p in $PIPELINES; do aws s3 rm "s3://sdh-poc-intake-$ACCOUNT/$p/" --recursive --only-show-errors; done

  say "Deploying the pipeline again"
  "$K8S/pipeline/apply.sh"
  say "reset: the pipeline runs on empty tables"
}

# --- status --------------------------------------------------------------------------------------------------

cmd_status() {
  state_dir
  local used at
  used="$(used_today)"
  if cluster_exists; then
    say "cluster $CLUSTER: $(aws eks describe-cluster --name "$CLUSTER" --query cluster.status --output text)"
    aws eks list-nodegroups --cluster-name "$CLUSTER" --query nodegroups --output text \
      | tr '\t' '\n' | while read -r g; do
        [[ -n "$g" ]] && say "  node group $g: $(aws eks describe-nodegroup --cluster-name "$CLUSTER" --nodegroup-name "$g" \
          --query 'nodegroup.[status,scalingConfig.desiredSize]' --output text)"
      done
  else
    say "cluster $CLUSTER: none (down)"
  fi
  at="$(aws scheduler get-schedule --name "$SCHEDULE" --query ScheduleExpression --output text 2>/dev/null || true)"
  [[ -n "$at" ]] && say "automatic shutdown: $at UTC" || say "automatic shutdown: none scheduled"
  say "today: $(( used / 3600 ))h $(( used % 3600 / 60 ))m of $DAILY_HOURS h used"
  local month; month="$(date +%Y-%m-01)"
  if [[ "$month" != "$(date +%F)" ]]; then
    say "this month so far: \$$(aws ce get-cost-and-usage --region us-east-1 --time-period "Start=$month,End=$(date +%F)" \
      --granularity MONTHLY --metrics UnblendedCost --query 'ResultsByTime[0].Total.UnblendedCost.Amount' --output text \
      | xargs printf '%.2f') (Cost Explorer lags by up to a day)"
  fi
}

case "${1:-}" in
  bootstrap) shift; cmd_bootstrap "$@" ;;
  up)        shift; cmd_up "$@" ;;
  extend)    shift; cmd_extend "$@" ;;
  down)      shift; cmd_down "$@" ;;
  status)    shift; cmd_status "$@" ;;
  reset)     shift; cmd_reset "$@" ;;
  *) sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
