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
#   poc.sh harness-up                 with a session up: hold the pipelines and deploy the control harness's own,
#                                     harness (k8s/harness/apply.sh), for scripts/harness.sh aws
#   poc.sh harness-down               delete the harness pipeline and everything it stored, and start the
#                                     pipelines again
#   poc.sh db-backup                  with a session up: a base backup of Postgres to the backup bucket (also made
#                                     at every up); WAL is archived there all the time
#   poc.sh db-restore --to <time> [--yes]
#                                     with a session up: stop the stages and take Postgres back to <time> (as
#                                     date -d reads it; UTC if no zone) from the backup bucket. The stages stay
#                                     stopped for their checks and repairs (HIPAA-OPERATIONS.md §7)
#   poc.sh db-resume                  after a restore and its repairs: start the stages again
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
BACKUP_BUCKET="sdh-poc-backup-$ACCOUNT"
# Where the Postgres volume is mounted; the data directory ($PGDATA) and the backup outbox are under it.
PG_HOME=/var/lib/postgresql
PIPELINES="${SDH_PIPELINES:-ccda-push}"
KEDA_VERSION="2.21.0"
# Partitions of each stage's input topic (setup_topics): 48 since 2026-11-15, for ~24 transform pods of 2 consumers
# each at 100 documents/s (docs/PERFORMANCE.md §10).
STAGE_PARTITIONS=48
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
  db_backup
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
  infra_tls
  export BACKUP_BUCKET
  # shellcheck disable=SC2016
  envsubst '$POSTGRES_VOLUME_ID $KAFKA_VOLUME_ID $STATE_ZONE $BACKUP_BUCKET $AWS_REGION' < "$K8S/infra/state.yaml" \
    | kubectl apply -f - >/dev/null
  infra scale statefulset/postgres statefulset/kafka --replicas=1 >/dev/null
  kubectl apply -f "$K8S/infra/cluster-autoscaler.yaml" >/dev/null
  # The stages' metrics, for measure.sh (docs/PERFORMANCE.md §10); kept only while the session is up.
  kubectl apply -f "$K8S/infra/telemetry.yaml" >/dev/null
  infra rollout status statefulset/postgres --timeout=600s >/dev/null
  infra rollout status statefulset/kafka --timeout=600s >/dev/null
}

# The PoC's own CA (state directory, pki/) and the certificates it issues to Postgres and Kafka (HIPAA.md §11.4,
# L5), as the secrets postgres-tls and kafka-tls. A pipeline deployed from deploy/reference/ trusts the CA
# (pki/ca.crt) and checks the server's name (verify-full; Kafka's default endpoint identification).
infra_issue() {  # <stem> <common name> <DNS names, comma-separated>
  local d="$STATE/pki" san="" name
  [[ -s "$d/$1.crt" ]] && return
  for name in ${3//,/ }; do san+="DNS:$name,"; done
  (umask 077; openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/O=SDH PoC/CN=$2" \
     -keyout "$d/$1.key.tmp" -out "$d/$1.csr" 2>/dev/null
   openssl pkcs8 -topk8 -nocrypt -in "$d/$1.key.tmp" -out "$d/$1.key"
   rm -f "$d/$1.key.tmp")
  openssl x509 -req -in "$d/$1.csr" -CA "$d/ca.crt" -CAkey "$d/ca.key" -CAcreateserial -days 825 \
    -extfile <(printf 'extendedKeyUsage=serverAuth\nsubjectAltName=%s\n' "${san%,}") -out "$d/$1.crt" 2>/dev/null
  rm -f "$d/$1.csr"
}

infra_tls() {
  local d="$STATE/pki" n=sdh-infra.svc
  mkdir -p "$d"; chmod 700 "$d"
  if [[ ! -s "$d/ca.crt" ]]; then
    (umask 077; openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj "/O=SDH PoC/CN=SDH PoC Infra CA" -keyout "$d/ca.key" -out "$d/ca.crt" 2>/dev/null)
  fi
  infra_issue postgres "postgres.$n" "postgres.$n,postgres.$n.cluster.local"
  infra_issue kafka "kafka-0.kafka.$n" "kafka-0.kafka.$n,kafka-0.kafka.$n.cluster.local"
  if [[ ! -s "$d/kafka.p12" ]]; then
    (umask 077; openssl pkcs12 -export -name kafka -inkey "$d/kafka.key" -in "$d/kafka.crt" -certfile "$d/ca.crt" \
      -passout "pass:$(secret kafka-keystore)" -out "$d/kafka.p12")
  fi
  infra create secret tls postgres-tls --cert="$d/postgres.crt" --key="$d/postgres.key" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  infra create secret generic kafka-tls --from-file=server.p12="$d/kafka.p12" \
    --from-literal=keystore-password="$(secret kafka-keystore)" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# --- Postgres's backups (k8s/infra/state.yaml; HIPAA.md §11.4, L4) ------------------------------------------------

pg() { infra exec -i postgres-0 -c postgres -- "$@"; }
pg_sql() { pg psql -qAt -v ON_ERROR_STOP=1 -U postgres -d "${2:-postgres}" -c "$1"; }

# db_backup: a base backup (pg_basebackup, tar, compressed, with the WAL it needs), made on the volume and then
# moved whole into the outbox, which the wal-shipper sends to s3://$BACKUP_BUCKET/base/<UTC time>/. Skipped, with a
# warning, if the volume hasn't room for it beside the database.
db_backup() {
  local stamp; stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  say "Base backup of Postgres ($stamp) to $BACKUP_BUCKET"
  pg env STAMP="$stamp" HOME_DIR="$PG_HOME" bash -s < "$K8S/infra/base-backup.sh" \
    || say "WARNING: no base backup made"
}

# db_flush: closes the WAL segment being written, and waits (up to 3 min) until it is archived and the outbox is
# sent, so the backup bucket holds everything up to now. Before Postgres stops, and before a restore.
db_flush() {
  infra get pod postgres-0 >/dev/null 2>&1 || return 0
  local seg i
  seg="$(pg_sql "SELECT pg_walfile_name(pg_switch_wal())" 2>/dev/null)" || { say "WARNING: WAL not flushed"; return 0; }
  for i in $(seq 1 36); do
    if [[ "$(pg_sql "SELECT coalesce(last_archived_wal >= '$seg', false) FROM pg_stat_archiver")" == t ]] \
        && [[ -z "$(pg find "$PG_HOME/backup/outbox" -type f 2>/dev/null | head -n 1)" ]]; then
      return 0
    fi
    sleep 5
  done
  say "WARNING: WAL up to $seg not all sent to $BACKUP_BUCKET after 3 min (kubectl -n sdh-infra logs postgres-0 -c wal-shipper)"
}

cmd_db_backup() {
  need_tools; state_dir
  cluster_exists || die "db-backup needs a session: poc.sh up"
  aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
  db_backup
  db_flush
}

# pause_stages: every stage at none, KEDA held there, and their pods gone (as stop_services begins).
pause_stages() {
  local n
  for n in $(pipeline_namespaces); do
    kubectl -n "$n" annotate scaledobject --all autoscaling.keda.sh/paused-replicas=0 --overwrite >/dev/null 2>&1 || true
    kubectl -n "$n" scale deploy --all --replicas=0 >/dev/null 2>&1 || true
  done
  for n in $(pipeline_namespaces); do
    kubectl -n "$n" wait --for=delete pod --all --timeout=180s >/dev/null 2>&1 || true
  done
}

# base_before <stamp>: the newest whole base backup (its three files all sent) from before the UTC time <stamp>.
base_before() {
  aws s3api list-objects-v2 --bucket "$BACKUP_BUCKET" --prefix base/ --query 'Contents[].Key' --output text \
    | tr '\t' '\n' | python3 -I -c '
import collections, sys
have = collections.defaultdict(set)
for key in sys.stdin.read().split():
    parts = key.split("/")
    if len(parts) == 3:
        have[parts[1]].add(parts[2])
needed = {"base.tar.gz", "pg_wal.tar.gz", "backup_manifest"}
whole = [s for s, f in have.items() if needed <= f and s <= sys.argv[1]]
print(max(whole) if whole else "")' "$1"
}

# db-restore --to <time>: the latest base backup from before <time>, then the archived WAL up to it (Postgres's
# recovery_target_time), promoted. What the volume held is kept beside it ($PGDATA.before-<UTC time>): delete it by
# hand once the restore is checked. The stages stay stopped: each stage's check and repair come next
# (HIPAA-OPERATIONS.md §7), then db-resume. Synthetic data only.
cmd_db_restore() {
  local to="" yes=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --to) to="${2:?--to needs a time}"; shift 2 ;;
      --yes) yes=1; shift ;;
      *) die "db-restore: unknown option $1" ;;
    esac
  done
  [[ -n "$to" ]] || die "db-restore needs --to <time>"
  need_tools; state_dir
  cluster_exists || die "db-restore needs a session: poc.sh up"
  aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
  ! kubectl get ns "sdh-$HARNESS" >/dev/null 2>&1 || die "the harness pipeline is up: poc.sh harness-down first"
  local target stamp now base answer
  target="$(TZ=UTC date -d "$to" +'%Y-%m-%d %H:%M:%S+00')" || die "not a time: $to"
  stamp="$(TZ=UTC date -d "$to" +%Y%m%dT%H%M%SZ)"
  now="$(date -u +%Y%m%dT%H%M%SZ)"
  [[ "$stamp" < "$now" ]] || die "$target is not in the past"
  base="$(base_before "$stamp")"
  [[ -n "$base" ]] || die "no whole base backup from before $target in $BACKUP_BUCKET"
  if (( ! yes )); then
    printf 'This stops every stage and takes Postgres (every database) back to %s, from the base backup\n' "$target"
    printf '%s and the WAL after it. Type restore to go on: ' "$base"
    read -r answer
    [[ "$answer" == restore ]] || die "not restored"
  fi

  say "Stopping the stages"
  pause_stages
  db_flush
  say "Stopping Postgres"
  infra scale statefulset/postgres --replicas=0 >/dev/null
  infra wait --for=delete pod/postgres-0 --timeout=180s >/dev/null 2>&1 || die "Postgres didn't stop"

  say "Restoring $base, with the WAL to $target, onto the volume"
  local image shipper
  image="$(infra get statefulset postgres -o jsonpath='{.spec.template.spec.containers[?(@.name=="postgres")].image}')"
  shipper="$(infra get statefulset postgres -o jsonpath='{.spec.template.spec.containers[?(@.name=="wal-shipper")].image}')"
  infra delete pod postgres-restore --ignore-not-found --wait=true >/dev/null
  # shellcheck disable=SC2016
  RESTORE_BASE="$base" RESTORE_TARGET="$target" RESTORE_NOW="$now" POSTGRES_IMAGE="$image" SHIPPER_IMAGE="$shipper" \
    STATE_ZONE="$(persistent_output state_zone)" BACKUP_BUCKET="$BACKUP_BUCKET" PG_HOME="$PG_HOME" \
    envsubst '$RESTORE_BASE $RESTORE_TARGET $RESTORE_NOW $POSTGRES_IMAGE $SHIPPER_IMAGE $STATE_ZONE $BACKUP_BUCKET $PG_HOME $AWS_REGION' \
    < "$K8S/infra/restore.yaml" | kubectl apply -f - >/dev/null
  if ! infra wait pod/postgres-restore --for=jsonpath='{.status.phase}'=Succeeded --timeout=1800s >/dev/null; then
    infra logs postgres-restore --all-containers --tail=20 >&2 || true
    die "the restore pod didn't finish; Postgres is stopped. If prepare ran, the volume's data is at PGDATA.before-$now"
  fi
  infra delete pod postgres-restore --wait=true >/dev/null

  say "Starting Postgres: it replays the WAL to $target, then takes writes"
  infra scale statefulset/postgres --replicas=1 >/dev/null
  local i state=""
  for i in $(seq 1 360); do
    state="$(pg_sql "SELECT pg_is_in_recovery()" 2>/dev/null || true)"
    [[ "$state" == f ]] && break
    sleep 5
  done
  [[ "$state" == f ]] || die "Postgres hadn't finished recovering after 30 min (kubectl -n sdh-infra logs postgres-0 -c postgres)"
  pg_sql "ALTER SYSTEM RESET restore_command" >/dev/null
  pg_sql "ALTER SYSTEM RESET recovery_target_time" >/dev/null
  pg_sql "ALTER SYSTEM RESET recovery_target_action" >/dev/null
  pg_sql "SELECT pg_reload_conf()" >/dev/null
  pg rm -rf "$PG_HOME/restore"
  # A new timeline: a base backup on it, so a later restore needn't replay through this one.
  db_backup
  say "Restored to $target. The stages are stopped: check and repair each (HIPAA-OPERATIONS.md §7), then poc.sh db-resume"
  say "The volume's data from before is kept at PGDATA.before-$now on the volume: delete it once this is checked"
}

cmd_db_resume() {
  need_tools; state_dir
  cluster_exists || die "db-resume needs a session: poc.sh up"
  aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
  "$K8S/pipeline/apply.sh"
  resume_stages
  say "the stages are running again"
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

# The stages' inputs get STAGE_PARTITIONS each (docs/PERFORMANCE.md §3.4): enough for the most pods any stage could
# be given, so a stage scales without the topic being changed. Messages are keyed by item or record, so each one's
# messages stay in order on their partition; a pod takes a batch of as many as it works on at once. Gold's output and
# the dead-letter topics get 1.
setup_topics() {
  say "Kafka topics"
  local p topic parts have
  for p in $PIPELINES; do
    for topic in "$p.landed:$STAGE_PARTITIONS" "$p.split:$STAGE_PARTITIONS" "$p.records:$STAGE_PARTITIONS" \
        "$p.fhir:1" "$p.convert.dlq:1" "$p.fhir.dlq:1"; do
      parts="${topic##*:}"; topic="${topic%:*}"
      kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
        --topic "$topic" --partitions "$parts" --replication-factor 1 >/dev/null
      # A topic made with fewer, before this. Partitions are only ever added; a key's messages already queued stay
      # on their old partition, so this is done at "up", before any stage starts.
      have="$(kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$topic" \
        | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -n 1)"
      if [[ -n "$have" && "$have" -lt "$parts" ]]; then
        say "  $topic: $have partitions to $parts"
        kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic "$topic" \
          --partitions "$parts" >/dev/null
      fi
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

  # The same roles for PgBouncer (k8s/infra/state.yaml, DESIGN.md revision (ea)), which the stages connect
  # through: it checks each client's password and logs in to Postgres as that role. A change restarts it.
  local users="" digest
  for p in $PIPELINES; do
    for role in landing splitter convert fhir; do
      users+="\"${role}_$(prefix "$p")\" \"$(secret "db-$role-$p")\""$'\n'
    done
  done
  printf '%s' "$users" | infra create secret generic pgbouncer-users --from-file=userlist.txt=/dev/stdin \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  digest="$(printf '%s' "$users" | sha256sum | cut -c1-16)"
  infra patch deployment/pgbouncer --type merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"sdh.poc/users\":\"$digest\"}}}}}" >/dev/null
  infra rollout status deployment/pgbouncer --timeout=300s >/dev/null || die "PgBouncer didn't start"
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
  say "down. Kept: buckets, keys, images, state volumes and snapshots (about \$18 a month)"
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

# Scales every stage to 0 (KEDA paused at 0, so it doesn't start them again), sends the last WAL to the backup
# bucket, then stops Postgres and Kafka cleanly so the snapshots are consistent.
stop_services() {
  pause_stages
  # Everything written so far into the backup bucket before Postgres stops.
  db_flush
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

# --- The control harness's pipeline -------------------------------------------------------------------------
# harness (docs/CONTROL-API-DESIGN.md §15.12): disposable, since its scenarios erase it, with keys of its own
# (terraform/persistent: harness_keks), in namespace sdh-harness. While it runs, the PoC's own pipelines are held
# (their landing nodes and duties pods scaled to none, KEDA told to hold their stages at none), so that the
# harness's pods fit the base nodes, and so that its cuts of Postgres and Kafka touch nothing else. Their items
# wait in the bucket intake, Kafka and the registries. Nothing of ccda-push's is deleted.
#
# The harness pipeline is deployed from deploy/reference/ (HIPAA.md §11.4, L5): its own database, sdh_harness, made
# by deploy/reference/postgres.sql, reached over TLS only; Kafka's SASL_SSL listener, with a SCRAM user per stage
# and the rights deploy/reference/kafka-acls.sh grants.
HARNESS=harness
REFERENCE="$HERE/../reference"

# held_workloads <pipeline>: "<namespace> <kind/name> <replicas>" for each workload harness-up holds.
held_workloads() {
  local n="sdh-$1" w
  for w in "$n deploy/landing" "$n deploy/splitter-duties" "$n deploy/convert-duties" \
           "sdh-fhir deploy/transform-$1-duties"; do
    kubectl -n "${w% *}" get "${w#* }" >/dev/null 2>&1 || continue
    echo "$w $(kubectl -n "${w% *}" get "${w#* }" -o jsonpath='{.spec.replicas}')"
  done
}

hold_pipelines() {
  local p w so held="$STATE/harness/held"
  mkdir -p "$STATE/harness"
  # An earlier harness-up's record, not yet released, is kept: it has the replicas from before.
  [[ -s "$held" ]] || for p in $PIPELINES; do held_workloads "$p"; done > "$held"
  for p in $PIPELINES; do
    kubectl get ns "sdh-$p" >/dev/null 2>&1 || continue
    say "Holding pipeline $p while the harness runs"
    for so in "sdh-$p scaledobject/splitter" "sdh-$p scaledobject/convert" "sdh-fhir scaledobject/transform-$p"; do
      kubectl -n "${so% *}" annotate "${so#* }" autoscaling.keda.sh/paused-replicas=0 --overwrite >/dev/null \
        2>&1 || true
    done
  done
  while read -r w; do
    [[ -n "$w" ]] || continue
    read -r n name _ <<<"$w"
    kubectl -n "$n" scale "$name" --replicas=0 >/dev/null
  done < "$held"
  for p in $PIPELINES; do
    kubectl -n "sdh-$p" wait --for=delete pod -l 'app in (landing,splitter,convert,splitter-duties,convert-duties)' \
      --timeout=300s >/dev/null 2>&1 || die "pipeline $p's pods didn't stop"
    kubectl -n sdh-fhir wait --for=delete pod -l "app in (transform-$p,transform-$p-duties)" --timeout=300s \
      >/dev/null 2>&1 || die "pipeline $p's transform pods didn't stop"
  done
}

release_pipelines() {
  local p w so n name replicas held="$STATE/harness/held"
  for p in $PIPELINES; do
    for so in "sdh-$p scaledobject/splitter" "sdh-$p scaledobject/convert" "sdh-fhir scaledobject/transform-$p"; do
      kubectl -n "${so% *}" annotate "${so#* }" autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1 || true
    done
  done
  if [[ -s "$held" ]]; then
    while read -r n name replicas; do
      [[ -n "$name" ]] || continue
      kubectl -n "$n" scale "$name" --replicas="${replicas:-1}" >/dev/null 2>&1 || true
    done < "$held"
    rm -f "$held"
  fi
  for p in $PIPELINES; do kubectl get ns "sdh-$p" >/dev/null 2>&1 && say "Pipeline $p started again"; done
  return 0
}

harness_sql() {  # <sql> [database]
  printf '%s\n' "$1" | infra exec -i postgres-0 -c postgres -- psql -q -v ON_ERROR_STOP=1 -U postgres -d "${2:-sdh}"
}

# harness_database: the harness pipeline's database and roles from deploy/reference/postgres.sql, each login role's
# password from the state directory (given on psql's input, never in a file or an argument), and the landing node's
# own schema.
harness_database() {
  local x role sql=""
  x="$(prefix "$HARNESS")"
  infra exec -i postgres-0 -c postgres -- psql -q -v ON_ERROR_STOP=1 -v pipeline="$x" -U postgres -d postgres \
    < "$REFERENCE/postgres.sql" >/dev/null
  for role in landing splitter convert fhir; do
    sql+="ALTER ROLE ${role}_$x PASSWORD '$(secret "db-$role-$HARNESS")';"$'\n'
  done
  harness_sql "$sql" postgres >/dev/null
  harness_sql "CREATE SCHEMA IF NOT EXISTS landing_$x AUTHORIZATION catalog_$x;" "sdh_$x" >/dev/null
}

# harness_kafka: a SCRAM user per stage, with its password from the state directory, and the rights
# deploy/reference/kafka-acls.sh grants, run in the broker's pod over its plaintext listener (a super user's). The
# password is an argument of kafka-configs.sh in that pod, which no one else uses (synthetic data only).
harness_kafka() {
  local role
  for role in landing splitter convert; do
    kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type users \
      --entity-name "$role-$HARNESS" --add-config "SCRAM-SHA-512=[password=$(secret "kafka-$role-$HARNESS")]" \
      >/dev/null
  done
  infra exec -i kafka-0 -- env PIPELINE="$HARNESS" KAFKA_BOOTSTRAP=localhost:9092 KAFKA_ADMIN_CONFIG=/dev/null \
    bash -s < "$REFERENCE/kafka-acls.sh" >/dev/null
}

# harness_clean: deletes the harness pipeline and everything it stored: its namespace, its database and roles (and
# what it kept in sdh before it had a database of its own), its topics and groups, and its objects (under its own
# prefixes only; the buckets keep versions, so these stay as noncurrent versions for 7 days). Its keys, Kafka users
# and ACLs, and state files are kept; harness-up uses them again. Synthetic data only.
harness_clean() {
  local x; x="$(prefix "$HARNESS")"
  kubectl delete ns "sdh-$HARNESS" --ignore-not-found --wait=true --timeout=300s >/dev/null
  harness_sql "SET client_min_messages TO warning;
    DROP SCHEMA IF EXISTS landing_$x, split_$x, convert_$x CASCADE;
    DO \$\$ BEGIN
      IF to_regclass('public.sdh_lease') IS NOT NULL THEN
        DELETE FROM public.sdh_lease WHERE name LIKE '%split-$HARNESS%' OR name LIKE '%convert-$HARNESS%';
      END IF;
      IF to_regclass('public.iceberg_tables') IS NOT NULL THEN
        DELETE FROM public.iceberg_tables WHERE table_namespace IN ('$x', '${x}_landing');
      END IF;
      IF to_regclass('public.iceberg_namespace_properties') IS NOT NULL THEN
        DELETE FROM public.iceberg_namespace_properties WHERE namespace IN ('$x', '${x}_landing');
      END IF;
    END \$\$;" >/dev/null
  harness_sql "SET client_min_messages TO warning;
    DROP DATABASE IF EXISTS sdh_$x WITH (FORCE);
    DROP ROLE IF EXISTS landing_$x, splitter_$x, convert_$x, fhir_$x, catalog_$x;" postgres >/dev/null
  kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists \
    --topic "$HARNESS\\..*" >/dev/null
  local group
  for group in "splitter-$HARNESS" "convert-$HARNESS"; do
    kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group "$group" \
      >/dev/null 2>&1 || true
  done
  aws s3 rm "s3://sdh-poc-bronze-$ACCOUNT/warehouse/${x}_landing/" --recursive --only-show-errors
  aws s3 rm "s3://sdh-poc-silver-$ACCOUNT/warehouse/$x/" --recursive --only-show-errors
}

# Restores what the harness may have left cut off (control-harness AwsTarget): Postgres and Kafka stopped.
harness_services() {
  infra scale statefulset/postgres --replicas=1 >/dev/null
  infra scale statefulset/kafka --replicas=1 >/dev/null
  infra rollout status statefulset/postgres --timeout=600s >/dev/null || die "Postgres didn't start"
  infra rollout status statefulset/kafka --timeout=600s >/dev/null || die "Kafka didn't start"
}

cmd_harness_up() {
  need_tools; state_dir
  cluster_exists || die "harness-up needs a session: poc.sh up"
  aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
  harness_services
  # Whatever an earlier run left: its namespace, database and roles (also those it had in sdh before L5, which
  # would clash with postgres.sql's), topics and objects.
  kubectl get ns "sdh-$HARNESS" >/dev/null 2>&1 && say "The harness pipeline is there from an earlier run: removing it first"
  harness_clean
  hold_pipelines
  say "Harness pipeline $HARNESS: topics, Kafka users and rights, its database"
  PIPELINES="$HARNESS" setup_topics
  harness_kafka
  harness_database
  "$K8S/harness/apply.sh"
}

cmd_harness_down() {
  need_tools; state_dir
  cluster_exists || { say "no cluster: nothing to remove"; return 0; }
  aws eks update-kubeconfig --name "$CLUSTER" --alias "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
  harness_services
  say "Removing the harness pipeline and what it stored"
  harness_clean
  release_pipelines
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
  harness-up)   shift; cmd_harness_up "$@" ;;
  harness-down) shift; cmd_harness_down "$@" ;;
  db-backup)    shift; cmd_db_backup "$@" ;;
  db-restore)   shift; cmd_db_restore "$@" ;;
  db-resume)    shift; cmd_db_resume "$@" ;;
  *) sed -n '2,37p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
