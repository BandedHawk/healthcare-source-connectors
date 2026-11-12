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
#   - KEDA maxima of 2, 3 and 4 pods, the scaled stages allowed onto the Spot burst nodes.
#
# Needs: $SDH_POC_STATE/images.env (deploy/aws/build-images.sh), the transformer's US Core set
# (healthcare-data-transformer/transform-stage/target/test-sets/us-core.tar.gz, made by its Maven build), and
# kms:GetPublicKey (the deployer has it), to export the signing keys' public halves once (kept in the state dir).
#
# Keys made here, once, and kept in $SDH_POC_STATE/keys (never delete it: what was sealed to them can't be opened
# without them): each stage's inbox key pair, the transform stage's (fhir-recipient), and gold's readers'
# (consumer), each private key encrypted with a passphrase from $SDH_POC_STATE/secrets.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
TRANSFORMER="${SDH_TRANSFORMER:-$(cd "$ROOT/.." && pwd)/healthcare-data-transformer}"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's
ACCOUNT=493469777520
PIPELINES="${SDH_PIPELINES:-ccda-push}"
export KAFKA_BROKERS="kafka-0.kafka.sdh-infra.svc:9092"
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

readme_yaml() { awk '/^```yaml$/ {on=1; next} /^```$/ {on=0} on' "$1"; }
settings_digest() { cat "$@" | sha256sum | cut -c1-16; }

# stage_kustomization <stage> <resources> <patch> [scaled]: as scripts/sandbox.sh's. With "scaled", replicas are
# left to KEDA; $CONFIG_SHA goes on the pods, so a change to the settings restarts them.
stage_kustomization() {
  local patches="{ path: $3 }"
  if [[ "${4:-}" == "scaled" ]]; then
    patches+=", { target: { kind: Deployment, name: $1 }, patch: '[{\"op\": \"remove\", \"path\": \"/spec/replicas\"}]' }"
  fi
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
  kc -n "$1" get pods -o wide
  kc -n "$1" logs "$2" --tail=40 || true
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
for t in kubectl aws openssl envsubst; do command -v "$t" >/dev/null || die "missing tool: $t"; done
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
  local stage
  for stage in landing splitter convert transform; do
    envsubst "$vars" < "$HERE/$stage-application.yml" > "$work/$stage-application.yml"
  done

  # Secrets: keys and database passwords only. AWS credentials come from Pod Identity.
  apply_secret "$n" landing-keys --from-file=splitter-inbox.pem="$k-inbox-public.pem"
  apply_secret "$n" landing-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-landing-$p")"
  apply_config "$n" landing-config --from-file=application.yml="$work/landing-application.yml"
  apply_secret "$n" splitter-keys --from-file=inbox.pem="$k-inbox.pem" \
    --from-file=inbox-public.pem="$k-inbox-public.pem" --from-file=recipient.pem="$k-convert-inbox-public.pem" \
    --from-file=landing-signer.pem="$landing_signer"
  apply_secret "$n" splitter-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-splitter-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "inbox-passphrase-$p")"
  apply_config "$n" splitter-config --from-file=application.yml="$work/splitter-application.yml"
  apply_secret "$n" convert-keys --from-file=inbox.pem="$k-convert-inbox.pem" \
    --from-file=inbox-public.pem="$k-convert-inbox-public.pem" \
    --from-file=recipient.pem="$k-fhir-recipient-public.pem" --from-file=split-signer.pem="$split_signer"
  apply_secret "$n" convert-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-convert-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "convert-inbox-passphrase-$p")"
  apply_config "$n" convert-config --from-file=application.yml="$work/convert-application.yml"
  local t="transform-$p"
  apply_secret sdh-fhir "$t-keys" --from-file=inbox.pem="$k-fhir-recipient.pem" \
    --from-file=inbox-public.pem="$k-fhir-recipient-public.pem" --from-file=recipient.pem="$k-consumer-public.pem" \
    --from-file=convert-signer.pem="$convert_signer"
  apply_secret sdh-fhir "$t-env" --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-fhir-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "fhir-recipient-passphrase-$p")"
  apply_config sdh-fhir "$t-config" --from-file=application.yml="$work/transform-application.yml"
  # The set as binary data: a ConfigMap holds no folders.
  apply_config sdh-fhir "$t-wstl" --from-file=set.tar.gz="$wstl"

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
          -e "s/name: transform-stage }/name: $t }/" -e "s/app: transform-stage }/app: $t }/g" \
    > "$work/transform/transform.yaml"
  local f
  for f in landing.yaml splitter.yaml convert.yaml transform.yaml; do
    grep -q '@sha256:' "$work/${f%.yaml}/$f" || die "the ${f%.yaml} README's Deployment no longer matches (image)"
  done
  cp "$HERE/landing-patch.yaml" "$HERE/landing-pdb.yaml" "$work/landing/"
  cp "$HERE/splitter-patch.yaml" "$work/splitter/"
  cp "$HERE/convert-patch.yaml" "$work/convert/"
  envsubst '$PIPELINE' < "$HERE/transform-patch.yaml" > "$work/transform/transform-patch.yaml"
  CONFIG_SHA="$(settings_digest "$work/landing-application.yml" "$k-inbox-public.pem")" \
    stage_kustomization landing "landing.yaml, landing-pdb.yaml" landing-patch.yaml > "$work/landing/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/splitter-application.yml" "$landing_signer")" \
    stage_kustomization splitter "splitter.yaml" splitter-patch.yaml scaled > "$work/splitter/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/convert-application.yml" "$split_signer")" \
    stage_kustomization convert "convert.yaml" convert-patch.yaml scaled > "$work/convert/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/transform-application.yml" "$convert_signer" "$wstl")" \
    K8S_NS=sdh-fhir stage_kustomization "$t" "transform.yaml" transform-patch.yaml scaled \
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

  envsubst '$PIPELINE $K8S_NS $KAFKA_BROKERS' < "$HERE/scaling.yaml" | kc apply -f - >/dev/null
  local scaled
  for scaled in "$n splitter" "$n convert" "sdh-fhir $t"; do
    kc -n "${scaled% *}" wait --for=condition=Ready "scaledobject/${scaled#* }" --timeout=120s >/dev/null \
      || { kc -n "${scaled% *}" describe "scaledobject/${scaled#* }" | tail -20; die "KEDA isn't scaling ${scaled#* }"; }
  done
  say "  split, convert and transform running, one pod each; KEDA scales them to 2, 3 and 4"
  say "  intake: aws s3 cp <file> s3://$INTAKE_BUCKET/$p/"
}

for p in $PIPELINES; do deploy_pipeline "$p"; done
