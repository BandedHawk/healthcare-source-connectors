#!/usr/bin/env bash
# Deploys each pipeline's four stages on the PoC's EKS cluster (docs/AWS-POC.md §5, §10 step 4). Run by
# deploy/aws/poc.sh up, after Postgres, Kafka, KEDA, the topics and the database roles; it can be run again on its
# own while the cluster is up, to apply a change (it is idempotent).
#
# As scripts/sandbox.sh deploy_pipeline, with:
#   - AWS KMS (aliases alias/sdh-poc-<pipeline>-*) in place of Vault, and S3 in place of SeaweedFS, each through
#     the stage's own role (EKS Pod Identity), so no token or access key is in a secret;
#   - the bucket intake (s3://sdh-poc-intake-<account>/<pipeline>/) in place of HTTP push;
#   - the landing node as an active pod and a hot standby;
#   - KEDA maxima of 2, 3 and 4 pods, the scaled stages allowed onto the Spot burst nodes;
#   - beside each scaled stage, its duties pod (<stage>-duties, duties-patch.yaml) on the on-demand duties node;
#   - the hipaa profile on, as deploy/reference/ sets it (DESIGN.md revision (fk)): Kafka over SASL_SSL on 9094 with
#     a SCRAM login per stage (made by poc.sh, pipeline_kafka; SDH_KAFKA_JAAS in the stage's env secret), Postgres
#     through PgBouncer with verify-full, both servers' CA ($SDH_POC_STATE/pki/ca.crt) in the stage's keys secret.
#
# Needs: $SDH_POC_STATE/images.env (deploy/aws/build-images.sh), the transformer's US Core set
# (healthcare-data-transformer/transform-stage/target/test-sets/us-core.tar.gz, made by its Maven build), and
# kms:GetPublicKey (the deployer has it), to export the signing keys' public halves once (kept in the state dir).
#
# Keys made here, once, and kept in $SDH_POC_STATE/keys (never delete it: what was sealed to them can't be opened
# without them): each stage's inbox key pair, the transform stage's (fhir-recipient), and gold's readers'
# (consumer), each private key encrypted with a passphrase from $SDH_POC_STATE/secrets.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
TRANSFORMER="${SDH_TRANSFORMER:-$(cd "$ROOT/.." && pwd)/healthcare-data-transformer}"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's
ACCOUNT=493469777520
PIPELINES="${SDH_PIPELINES:-ccda-push}"
# How messages are sealed (sdh.crypto.jose.provider: jdk, accp or conscrypt) and how long the audit journal waits
# to fill a batch (sdh.telemetry.journal.linger), for every stage (docs/PERFORMANCE.md §13).
export JOSE_PROVIDER="${SDH_JOSE_PROVIDER:-accp}" JOURNAL_LINGER="${SDH_JOURNAL_LINGER:-100ms}"
# The split reconciler's grace (how long a landed item may wait for its message) and how often it runs
# (sdh.landing.reconcile.*; the product's defaults). Shorter, a load test sees its passes under a backlog.
export SPLIT_RECONCILE_GRACE="${SDH_SPLIT_RECONCILE_GRACE:-10m}"
export SPLIT_RECONCILE_INTERVAL="${SDH_SPLIT_RECONCILE_INTERVAL:-15m}"
# What the split stage splits (sdh.split.format, and sdh.split.options / sdh.convert.options as JSON-style flow
# maps): C-CDA, the pipeline's own, unless a load test of another format asks (docs/PERFORMANCE.md §13.2). The
# transform stage's set reads C-CDA only, so it keeps another format's records as they came (TRANSFORM_UNSUPPORTED).
# The empty map is a variable: in "${X:-{}}" bash ends the default at the first }, so a set X gained a stray }.
no_options='{}'
export SPLIT_FORMAT="${SDH_SPLIT_FORMAT:-ccda}" SPLIT_OPTIONS="${SDH_SPLIT_OPTIONS:-$no_options}"
export CONVERT_OPTIONS="${SDH_CONVERT_OPTIONS:-$no_options}"
# The split pods' pool for their registry, leases and checkpoints (sdh.lease.jdbc.pool-size; 2, the product's
# default), each wait for one of its connections timed as split's pool_wait (docs/PERFORMANCE.md §13.3). PgBouncer
# gives a role 40 server connections (k8s/infra/state.yaml), so 6 pods of more than 6 can queue there instead.
export SPLIT_POOL_SIZE="${SDH_SPLIT_POOL_SIZE:-2}"
[[ "$SPLIT_POOL_SIZE" =~ ^[1-9][0-9]*$ ]] || { echo "apply.sh: SDH_SPLIT_POOL_SIZE is a count: $SPLIT_POOL_SIZE" >&2; exit 2; }
# How many items' registry statements of one kind a split pod gathers into one (sdh.split.registry-batch; 64, the
# product's default; 1 runs each alone, as before STAGES.md §18).
export SPLIT_REGISTRY_BATCH="${SDH_SPLIT_REGISTRY_BATCH:-64}"
[[ "$SPLIT_REGISTRY_BATCH" =~ ^[1-9][0-9]*$ ]] \
  || { echo "apply.sh: SDH_SPLIT_REGISTRY_BATCH is a count: $SPLIT_REGISTRY_BATCH" >&2; exit 2; }
# Registry retention (sdh.stage.retention.*, the convert and transform stages; docs/DEPLOYMENT.md "Registry
# retention"): unset, every row is kept, the product's default. SDH_STAGE_RETENTION_AFTER turns it on. The stage
# refuses an after shorter than its input queue's retention or its reconciler's lookback, each plus a day, so a
# test of it shortens both: SDH_STAGE_QUEUE_RETENTION, which also sets the stage inputs' Kafka retention.ms to it
# (unset: the broker's default, 7 days, and any such override removed), and SDH_RECONCILE_LOOKBACK (2d unset). A
# queue kept so briefly drops a backlog older than that; the reconcilers would rebuild its records.
RETENTION_AFTER="${SDH_STAGE_RETENTION_AFTER:-}" QUEUE_RETENTION="${SDH_STAGE_QUEUE_RETENTION:-}"
RECONCILE_LOOKBACK="${SDH_RECONCILE_LOOKBACK:-}" RETENTION_INTERVAL="${SDH_STAGE_RETENTION_INTERVAL:-1h}"
for d in "$QUEUE_RETENTION" "$RECONCILE_LOOKBACK" "$RETENTION_INTERVAL" "${RETENTION_AFTER:-1h}"; do
  [[ -z "$d" || "$d" =~ ^[1-9][0-9]*[smhd]$ ]] || { echo "apply.sh: a retention duration is <n><s|m|h|d>: $d" >&2; exit 2; }
done
export KAFKA_BROKERS="kafka-0.kafka.sdh-infra.svc:9094"   # SASL_SSL, the only listener on the network
export INTAKE_BUCKET="sdh-poc-intake-$ACCOUNT" BRONZE_BUCKET="sdh-poc-bronze-$ACCOUNT"
export SILVER_BUCKET="sdh-poc-silver-$ACCOUNT" GOLD_BUCKET="sdh-poc-gold-$ACCOUNT"

say() { printf '>> %s\n' "$*"; }
die() { printf 'apply.sh: %s\n' "$*" >&2; exit 1; }
kc() { kubectl "$@"; }
ns() { echo "sdh-$1"; }
prefix() { echo "${1//-/_}"; }

# secret <name>: as poc.sh's, a random secret made once, kept in the state directory.
secret() {
  local f="$STATE/secrets/$1"
  mkdir -p "$STATE/secrets"; chmod 700 "$STATE/secrets"
  [[ -s "$f" ]] || (umask 077; openssl rand -base64 24 | tr -d '/+=\n' > "$f")
  cat "$f"
}

# ec_key <file stem> <passphrase>: an EC P-256 key pair, made once: <stem>.pem (encrypted PKCS#8) and
# <stem>-public.pem. As scripts/sandbox.sh's.
ec_key() {
  [[ -s "$1.pem" ]] && return
  (umask 077
   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 2>/dev/null \
     | openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -passout "pass:$2" -out "$1.pem")
  openssl pkey -in "$1.pem" -passin "pass:$2" -pubout -out "$1-public.pem"
}

fingerprint() { openssl pkey -pubin -in "$1" -outform DER | sha256sum | cut -d' ' -f1; }

# kafka_jaas <user>: a Kafka user's SCRAM login, for SDH_KAFKA_JAAS, with its password from the state directory
# (the user is made by poc.sh pipeline_kafka, or harness_kafka, from the same file).
kafka_jaas() {
  printf 'org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";' \
    "$1" "$(secret "kafka-$1")"
}

# signing_public <pipeline> <stage>: the stage's KMS signing key's public half, as PEM, exported once. The next
# stage verifies its messages with it (sdh.crypto.keys.landing-signer).
signing_public() {
  local f="$STATE/keys/$1-$2-signing-public.pem" der
  if [[ ! -s "$f" ]]; then
    der="$(aws kms get-public-key --key-id "alias/sdh-poc-$1-$2-signing" --query PublicKey --output text)" \
      || die "couldn't read alias/sdh-poc-$1-$2-signing's public key: the deployer needs kms:GetPublicKey" \
             "(deploy/aws/iam/sdh-poc-deployer-infra.json)"
    base64 -d <<<"$der" | openssl pkey -pubin -inform DER -out "$f.tmp"
    mv "$f.tmp" "$f"
  fi
  printf '%s\n' "$f"
}

apply_secret() {  # <namespace> <name> <kubectl create secret args>...
  local n="$1" name="$2"; shift 2
  kc -n "$n" create secret generic "$name" "$@" --dry-run=client -o yaml | kc apply -f - >/dev/null
}
apply_config() {  # <namespace> <name> <kubectl create configmap args>...
  local n="$1" name="$2"; shift 2
  kc -n "$n" create configmap "$name" "$@" --dry-run=client -o yaml | kc apply -f - >/dev/null
}

# wstl_digest <set.tar.gz>: the set's digest, as the transform stage computes it (its set-digest command, from the
# transformer's jar), kept beside the set as <set>.digest until the set changes. The set's ConfigMap is named by
# its first 12 hex digits, immutable, and the stage pinned to it (sdh.transform.wstl-digest), so a new set is a new
# ConfigMap and a change of the Deployment, never an edit under running pods.
wstl_digest() {
  local set="$1" jar
  if [[ ! -s "$set.digest" || "$set.digest" -ot "$set" ]]; then
    jar="$(ls "$TRANSFORMER"/transform-stage/target/transform-stage-*.jar 2>/dev/null \
      | grep -v -e '-sources' -e '-javadoc' | head -n 1 || true)"
    [[ -n "$jar" ]] || die "no transformer jar in $TRANSFORMER/transform-stage/target:" \
      "build the transformer (deploy/aws/build-images.sh)"
    java -jar "$jar" set-digest "$set" 2>/dev/null | awk '{print $1}' > "$set.digest.tmp" || true
    grep -qE '^[0-9a-f]{64}$' "$set.digest.tmp" \
      || { rm -f "$set.digest.tmp"; die "set-digest could not read the WSTL set $set"; }
    mv "$set.digest.tmp" "$set.digest"
  fi
  cat "$set.digest"
}

# queue_retention <pipeline>: the convert and transform stages' input topics kept QUEUE_RETENTION, or the
# broker's default (any override removed) when it is unset.
queue_retention() {
  local topic ms="" n="${QUEUE_RETENTION%?}"
  case "$QUEUE_RETENTION" in
    *s) ms=$((n * 1000)) ;; *m) ms=$((n * 60000)) ;; *h) ms=$((n * 3600000)) ;; *d) ms=$((n * 86400000)) ;;
  esac
  for topic in "$1.split" "$1.records"; do
    if [[ -n "$ms" ]]; then
      kc -n sdh-infra exec kafka-0 -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --alter \
        --entity-type topics --entity-name "$topic" --add-config "retention.ms=$ms" >/dev/null
    else
      kc -n sdh-infra exec kafka-0 -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --alter \
        --entity-type topics --entity-name "$topic" --delete-config retention.ms >/dev/null 2>&1 || true
    fi
  done
  [[ -z "$ms" ]] || say "  $1.split and $1.records kept $QUEUE_RETENTION (retention.ms)"
}

readme_yaml() { awk '/^```yaml$/ {on=1; next} /^```$/ {on=0} on' "$1"; }
settings_digest() { cat "$@" | sha256sum | cut -c1-16; }

# stage_kustomization <stage> <resources> <patch> [scaled [duties patch]]: as scripts/sandbox.sh's. With
# "scaled", replicas are left to KEDA; $CONFIG_SHA goes on the pods, so a change to the settings restarts them.
# A duties patch is for the stage's duties pod, among the resources.
stage_kustomization() {
  local patches="{ path: $3 }"
  if [[ "${4:-}" == "scaled" ]]; then
    patches+=", { target: { kind: Deployment, name: $1 }, patch: '[{\"op\": \"remove\", \"path\": \"/spec/replicas\"}]' }"
  fi
  if [[ -n "${5:-}" ]]; then patches+=", { path: $5 }"; fi
  cat <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $K8S_NS
resources: [$2]
patches: [$patches]
commonAnnotations: { sdh.poc/settings: "$CONFIG_SHA" }
EOF
}

wait_for() {  # <seconds> <command>...
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 5
  done
}

logged() {  # <namespace> <pod or deploy/x> <pattern>
  local logs
  logs="$(kc -n "$1" logs "$2" 2>/dev/null)"
  grep -qE "$3" <<<"$logs"
}

# landing_active <namespace>: both landing pods run, and exactly one has taken the lease (the other is the
# standby). A bucket intake has no port to be ready on, so the lease's audit event is what tells them apart.
landing_active() {
  local pods pod running=0 holders=0
  pods="$(kc -n "$1" get pods -l app=landing --field-selector=status.phase=Running -o name)"
  for pod in $pods; do
    running=$((running + 1))
    if logged "$1" "$pod" 'lease\.acquired'; then holders=$((holders + 1)); fi
  done
  [[ "$running" == 2 && "$holders" == 1 ]]
}

# failed <namespace> <deploy/x>: what an operator needs to see. Logs carry no document content (design §12).
failed() {
  local pod
  kc -n "$1" get pods -o wide
  # The pods that aren't ready, each with its last crashed container's log if it has one: "logs deploy/<name>"
  # picks a pod of its own, often an old one still running (2026-11-23, the first CSV apply).
  for pod in $(kc -n "$1" get pods -o name \
      | grep "^pod/${2#deploy/}-" || true); do
    [[ "$(kc -n "$1" get "$pod" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)" == true ]] && continue
    say "  $pod, not ready:"
    # Its containers' last state (an exit code and reason, which outlast a log the runtime has dropped).
    kc -n "$1" describe "$pod" 2>/dev/null | sed -n '/^Containers:/,/^Conditions:/p' \
      | grep -E '^  [a-z]|State:|Reason:|Exit Code:|Started:|Finished:|Restart Count:' || true
    kc -n "$1" logs "$pod" --previous --tail=60 2>/dev/null || kc -n "$1" logs "$pod" --tail=60 || true
  done
  say "  warning events in $1:"
  kc -n "$1" get events --field-selector type=Warning --sort-by=.lastTimestamp 2>/dev/null | tail -n 20 || true
  die "$2 in $1 didn't start"
}

# --- Checks ------------------------------------------------------------------------------------------------------

[[ -s "$STATE/images.env" ]] || die "no $STATE/images.env: build the images first (deploy/aws/build-images.sh)"
# shellcheck disable=SC1091
source "$STATE/images.env"
for v in landing_connector split_stage convert_stage transform_stage; do
  [[ -n "${!v:-}" ]] || die "$STATE/images.env has no $v: build the images again"
done
wstl="$STATE/wstl-us-core.tar.gz"
set_src="$TRANSFORMER/transform-stage/target/test-sets/us-core.tar.gz"
if [[ -s "$set_src" ]] && ! cmp -s "$set_src" "$wstl"; then cp "$set_src" "$wstl"; fi
[[ -s "$wstl" ]] || die "no US Core set at $set_src: build the transformer (deploy/aws/build-images.sh)"
for t in kubectl aws openssl envsubst java; do command -v "$t" >/dev/null || die "missing tool: $t"; done
wstl_sha="$(wstl_digest "$wstl")"
kc get ns sdh-infra >/dev/null || die "no cluster, or no sdh-infra: run deploy/aws/poc.sh up"
mkdir -p "$STATE/keys"; chmod 700 "$STATE/keys"

# --- Each pipeline -----------------------------------------------------------------------------------------------

deploy_pipeline() {
  local p="$1" n work
  n="$(ns "$p")"
  work="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '$work'" RETURN
  say "Pipeline $p (namespaces $n and sdh-fhir)"
  export PIPELINE="$p" K8S_NS="$n" ICEBERG_PREFIX
  ICEBERG_PREFIX="$(prefix "$p")"
  envsubst '$PIPELINE $K8S_NS' < "$HERE/namespace.yaml" | kc apply -f - >/dev/null

  local k="$STATE/keys/$p"
  ec_key "$k-inbox" "$(secret "inbox-passphrase-$p")"
  ec_key "$k-convert-inbox" "$(secret "convert-inbox-passphrase-$p")"
  ec_key "$k-fhir-recipient" "$(secret "fhir-recipient-passphrase-$p")"
  # Gold's readers' key pair: the transform stage seals to its public half; no reader is deployed.
  ec_key "$k-consumer" "$(secret "consumer-passphrase-$p")"
  local landing_signer split_signer convert_signer
  landing_signer="$(signing_public "$p" landing)"
  split_signer="$(signing_public "$p" splitter)"
  convert_signer="$(signing_public "$p" convert)"
  export INBOX_SHA256 CONVERT_INBOX_SHA256 RECIPIENT_SHA256 CONSUMER_SHA256
  INBOX_SHA256="$(fingerprint "$k-inbox-public.pem")"
  CONVERT_INBOX_SHA256="$(fingerprint "$k-convert-inbox-public.pem")"
  RECIPIENT_SHA256="$(fingerprint "$k-fhir-recipient-public.pem")"
  CONSUMER_SHA256="$(fingerprint "$k-consumer-public.pem")"

  local vars='$PIPELINE $ICEBERG_PREFIX $INBOX_SHA256 $CONVERT_INBOX_SHA256 $RECIPIENT_SHA256 $CONSUMER_SHA256'
  vars+=' $KAFKA_BROKERS $AWS_REGION $INTAKE_BUCKET $BRONZE_BUCKET $SILVER_BUCKET $GOLD_BUCKET'
  vars+=' $JOSE_PROVIDER $JOURNAL_LINGER $SPLIT_RECONCILE_GRACE $SPLIT_RECONCILE_INTERVAL'
  vars+=' $SPLIT_FORMAT $SPLIT_OPTIONS $CONVERT_OPTIONS $SPLIT_POOL_SIZE $SPLIT_REGISTRY_BATCH'
  local stage
  for stage in landing splitter convert transform; do
    envsubst "$vars" < "$HERE/$stage-application.yml" > "$work/$stage-application.yml"
  done
  # Retention, when asked for, as a second YAML document, which Spring reads over the first.
  if [[ -n "$RETENTION_AFTER" ]]; then
    for stage in convert transform; do
      { echo "---"; echo "sdh:"
        echo "  stage:"
        printf '    retention: { after: %s, queue-retention: %s, interval: %s }\n' \
          "$RETENTION_AFTER" "${QUEUE_RETENTION:-7d}" "$RETENTION_INTERVAL"
        [[ -z "$RECONCILE_LOOKBACK" ]] || printf '  reconcile: { lookback: %s }\n' "$RECONCILE_LOOKBACK"
      } >> "$work/$stage-application.yml"
    done
  fi
  queue_retention "$p"

  # Secrets: keys, the servers' CA, and passwords and Kafka logins only. AWS credentials come from Pod Identity.
  local ca="$STATE/pki/ca.crt"
  [[ -s "$ca" ]] || die "no CA at $ca: poc.sh up makes it (infra_tls)"
  apply_secret "$n" landing-keys --from-file=splitter-inbox.pem="$k-inbox-public.pem" \
    --from-file=kafka-ca.crt="$ca" --from-file=postgres-ca.crt="$ca"
  apply_secret "$n" landing-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-landing-$p")" \
    --from-literal=SDH_KAFKA_JAAS="$(kafka_jaas "landing-$p")"
  apply_config "$n" landing-config --from-file=application.yml="$work/landing-application.yml"
  apply_secret "$n" splitter-keys --from-file=inbox.pem="$k-inbox.pem" \
    --from-file=inbox-public.pem="$k-inbox-public.pem" --from-file=recipient.pem="$k-convert-inbox-public.pem" \
    --from-file=landing-signer.pem="$landing_signer" --from-file=kafka-ca.crt="$ca" --from-file=postgres-ca.crt="$ca"
  apply_secret "$n" splitter-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-splitter-$p")" \
    --from-literal=SDH_KAFKA_JAAS="$(kafka_jaas "splitter-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "inbox-passphrase-$p")"
  apply_config "$n" splitter-config --from-file=application.yml="$work/splitter-application.yml"
  apply_secret "$n" convert-keys --from-file=inbox.pem="$k-convert-inbox.pem" \
    --from-file=inbox-public.pem="$k-convert-inbox-public.pem" \
    --from-file=recipient.pem="$k-fhir-recipient-public.pem" --from-file=split-signer.pem="$split_signer" \
    --from-file=kafka-ca.crt="$ca" --from-file=postgres-ca.crt="$ca"
  apply_secret "$n" convert-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-convert-$p")" \
    --from-literal=SDH_KAFKA_JAAS="$(kafka_jaas "convert-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "convert-inbox-passphrase-$p")"
  apply_config "$n" convert-config --from-file=application.yml="$work/convert-application.yml"
  local t="transform-$p"
  apply_secret sdh-fhir "$t-keys" --from-file=inbox.pem="$k-fhir-recipient.pem" \
    --from-file=inbox-public.pem="$k-fhir-recipient-public.pem" --from-file=recipient.pem="$k-consumer-public.pem" \
    --from-file=convert-signer.pem="$convert_signer" --from-file=kafka-ca.crt="$ca" --from-file=postgres-ca.crt="$ca"
  apply_secret sdh-fhir "$t-env" --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-fhir-$p")" \
    --from-literal=SDH_KAFKA_JAAS="$(kafka_jaas "transform-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "fhir-recipient-passphrase-$p")"
  apply_config sdh-fhir "$t-config" --from-file=application.yml="$work/transform-application.yml"
  # The set as binary data (a ConfigMap holds no folders), immutable, named for the set: the Deployment below
  # refers to it and pins its digest. Re-applying the same set changes nothing.
  kc -n sdh-fhir create configmap "$t-wstl-${wstl_sha:0:12}" --from-file=set.tar.gz="$wstl" --dry-run=client \
    -o yaml | sed '1a immutable: true' | kc apply -f - >/dev/null

  # Each stage from the Deployment in its README, renamed as in the sandbox, by digest.
  mkdir -p "$work/landing" "$work/splitter" "$work/convert" "$work/transform"
  readme_yaml "$ROOT/landing-connector/src/dist/README.md" \
    | sed -e "s#<registry>/landing-connector:<version>#$landing_connector#" -e 's/landing-claims/landing/g' \
    > "$work/landing/landing.yaml"
  readme_yaml "$ROOT/file-connector/src/dist/README.md" \
    | sed -e "s#<registry>/split-stage:<version>#$split_stage#" \
          -e 's/file-connector-\(env\|keys\|config\)/splitter-\1/g' \
          -e 's/name: file-connector }/name: splitter }/' -e 's/app: file-connector }/app: splitter }/g' \
    > "$work/splitter/splitter.yaml"
  readme_yaml "$ROOT/convert-stage/src/dist/README.md" \
    | sed -e "s#<registry>/convert-stage:<version>#$convert_stage#" \
          -e 's/convert-stage-\(env\|keys\|config\)/convert-\1/g' \
          -e 's/name: convert-stage }/name: convert }/' -e 's/app: convert-stage }/app: convert }/g' \
    > "$work/convert/convert.yaml"
  readme_yaml "$TRANSFORMER/transform-stage/src/dist/README.md" \
    | sed -e "s#<registry>/transform-stage:<version>#$transform_stage#" \
          -e "s/transform-stage-\(env\|keys\|config\|wstl\)/$t-\1/g" \
          -e "s/<set id>/${wstl_sha:0:12}/g" -e "s/<set digest>/$wstl_sha/g" \
          -e "s/name: transform-stage }/name: $t }/" -e "s/app: transform-stage }/app: $t }/g" \
    > "$work/transform/transform.yaml"
  local f
  for f in landing.yaml splitter.yaml convert.yaml transform.yaml; do
    grep -q '@sha256:' "$work/${f%.yaml}/$f" || die "the ${f%.yaml} README's Deployment no longer matches (image)"
  done
  # Each scaled stage's duties pod: its Deployment again, as <stage>-duties (duties-patch.yaml).
  local d name container account memory cpu
  for d in "splitter splitter file-connector splitter 1536Mi 250m" "convert convert convert-stage convert 1024Mi 250m" \
           "transform $t transform-stage transform 2048Mi 250m"; do
    read -r stage name container account memory cpu <<<"$d"
    sed -e "s/name: $name }/name: $name-duties }/" -e "s/app: $name }/app: $name-duties }/g" \
      "$work/$stage/$stage.yaml" > "$work/$stage/duties.yaml"
    grep -q "name: $name-duties }" "$work/$stage/duties.yaml" || die "the $stage README's Deployment no longer matches (name)"
    DUTIES_NAME="$name-duties" CONTAINER="$container" SERVICE_ACCOUNT="$account" DUTIES_MEMORY="$memory" \
      DUTIES_CPU="$cpu" envsubst '$DUTIES_NAME $CONTAINER $SERVICE_ACCOUNT $DUTIES_MEMORY $DUTIES_CPU' \
      < "$HERE/duties-patch.yaml" > "$work/$stage/duties-patch.yaml"
  done
  cp "$HERE/landing-patch.yaml" "$HERE/landing-pdb.yaml" "$work/landing/"
  cp "$HERE/splitter-patch.yaml" "$work/splitter/"
  cp "$HERE/convert-patch.yaml" "$work/convert/"
  envsubst '$PIPELINE' < "$HERE/transform-patch.yaml" > "$work/transform/transform-patch.yaml"
  CONFIG_SHA="$(settings_digest "$work/landing-application.yml" "$k-inbox-public.pem")" \
    stage_kustomization landing "landing.yaml, landing-pdb.yaml" landing-patch.yaml > "$work/landing/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/splitter-application.yml" "$landing_signer")" \
    stage_kustomization splitter "splitter.yaml, duties.yaml" splitter-patch.yaml scaled duties-patch.yaml \
    > "$work/splitter/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/convert-application.yml" "$split_signer")" \
    stage_kustomization convert "convert.yaml, duties.yaml" convert-patch.yaml scaled duties-patch.yaml \
    > "$work/convert/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/transform-application.yml" "$convert_signer" "$wstl")" \
    K8S_NS=sdh-fhir stage_kustomization "$t" "transform.yaml, duties.yaml" transform-patch.yaml scaled duties-patch.yaml \
    > "$work/transform/kustomization.yaml"

  if [[ "${SDH_POC_RENDER_ONLY:-0}" == "1" ]]; then   # for checking offline: print what would be applied
    for stage in landing splitter convert transform; do kc kustomize "$work/$stage"; echo "---"; done
    envsubst '$PIPELINE $K8S_NS $KAFKA_BROKERS' < "$HERE/scaling.yaml"
    return
  fi

  # In the sandbox's order: landing makes the landing tables, the split stage split_records, the convert stage
  # records, each of which the next stage only reads.
  kc apply -k "$work/landing" >/dev/null
  wait_for 420 landing_active "$n" || failed "$n" deploy/landing
  say "  landing: one pod holds the lease, one waits"
  kc apply -k "$work/splitter" >/dev/null
  { kc -n "$n" rollout status deploy/splitter --timeout=420s >/dev/null \
      && wait_for 300 logged "$n" deploy/splitter "Started FileConnectorApplication"; } || failed "$n" deploy/splitter
  kc apply -k "$work/convert" >/dev/null
  { kc -n "$n" rollout status deploy/convert --timeout=420s >/dev/null \
      && wait_for 300 logged "$n" deploy/convert "Started ConvertStageApplication"; } || failed "$n" deploy/convert
  kc apply -k "$work/transform" >/dev/null
  { kc -n sdh-fhir rollout status "deploy/$t" --timeout=420s >/dev/null \
      && wait_for 300 logged sdh-fhir "deploy/$t" "Started TransformStageApplication"; } || failed sdh-fhir "deploy/$t"
  # The set's ConfigMap from before sets had their own (named $t-wstl, edited in place): nothing refers to it now.
  kc -n sdh-fhir delete configmap "$t-wstl" --ignore-not-found >/dev/null

  local duties
  for duties in "$n splitter-duties" "$n convert-duties" "sdh-fhir $t-duties"; do
    kc -n "${duties% *}" rollout status "deploy/${duties#* }" --timeout=420s >/dev/null \
      && wait_for 300 logged "${duties% *}" "deploy/${duties#* }" 'duties_only=true' \
      || failed "${duties% *}" "deploy/${duties#* }"
  done
  say "  duties pods running on the duties node, one a stage"

  # KEDA's Kafka login (poc.sh pipeline_kafka: the user keda, Describe only), for the TriggerAuthentication in
  # scaling.yaml, in each namespace with a ScaledObject.
  local keda_ns
  for keda_ns in "$n" sdh-fhir; do
    apply_secret "$keda_ns" keda-kafka --from-literal=sasl=scram_sha512 --from-literal=username=keda \
      --from-literal=password="$(secret kafka-keda)" --from-literal=tls=enable --from-file=ca="$ca"
  done
  envsubst '$PIPELINE $K8S_NS $KAFKA_BROKERS' < "$HERE/scaling.yaml" | kc apply -f - >/dev/null
  local scaled
  for scaled in "$n splitter" "$n convert" "sdh-fhir $t"; do
    kc -n "${scaled% *}" wait --for=condition=Ready "scaledobject/${scaled#* }" --timeout=120s >/dev/null \
      || { kc -n "${scaled% *}" describe "scaledobject/${scaled#* }" | tail -20; die "KEDA isn't scaling ${scaled#* }"; }
  done
  say "  split, convert and transform running, one pod each; KEDA scales them to 3, 3 and 4"
  say "  intake: aws s3 cp <file> s3://$INTAKE_BUCKET/$p/"
}

# Sourced by ../harness/apply.sh for the helpers above; run, it deploys the pipelines.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  for p in $PIPELINES; do deploy_pipeline "$p"; done
fi
