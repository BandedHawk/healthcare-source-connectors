#!/usr/bin/env bash
# Deploys the control harness's own pipeline, harness, on the PoC's EKS cluster (scripts/harness.sh aws,
# docs/CONTROL-API-DESIGN.md §15.12), as scripts/sandbox.sh harness-up deploys it in the sandbox: in namespace
# sdh-harness, its landing node (an HTTP push intake, so that the harness reads each receipt), split and convert
# stages, one pod each and not scaled, and each stage's control API from its README, with a pod serving the
# harness issuer's key set. No transform stage. Run by deploy/aws/poc.sh harness-up, after the pipeline's
# topics, database roles and landing schema; poc.sh harness-down removes it and everything it stored.
#
# As ../pipeline/apply.sh deploys a pipeline (its helpers are sourced from it), with:
#   - the pipeline's own KMS keys (terraform/persistent: harness_keks), the stages' roles through Pod Identity
#     links for sdh-harness (terraform/compute: aws_eks_pod_identity_association.harness), the control APIs on the
#     same service accounts;
#   - each stage doing maintenance (re-keys, purges, retention) every 20 seconds, the landing node's tables in a
#     schema of their own, and the split stage storing each record as it is split, as in the sandbox;
#   - no duties pods: each stage's one pod does its own duties.
#
# Made here once and kept in $SDH_POC_STATE/harness: a CA, the landing node's and the control APIs' certificates
# and the producer's client certificate (pki/), the issuer's key and key set, S3's address ranges in the region
# (s3-cidrs, for the harness's cuts of the store and KMS), and target.properties (the buckets and region), which
# the harness reads (control-harness: AwsTarget). Synthetic data only.
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../pipeline/apply.sh
source "$HARNESS_DIR/../pipeline/apply.sh"
SANDBOX_HARNESS="$ROOT/deploy/sandbox/harness"
P=harness
H="$STATE/harness"
mkdir -p "$H/pki"; chmod 700 "$H"

# issue <stem> <subject> <extensions>: a certificate from the harness CA, made once. As scripts/sandbox.sh's.
issue() {
  [[ -s "$1.crt" ]] && return
  (umask 077; openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "$2" -keyout "$1.key.tmp" \
    -out "$1.csr" 2>/dev/null
   openssl pkcs8 -topk8 -nocrypt -in "$1.key.tmp" -out "$1.key"
   rm -f "$1.key.tmp")
  openssl x509 -req -in "$1.csr" -CA "$H/pki/ca.crt" -CAkey "$H/pki/ca.key" -CAcreateserial \
    -days 825 -extfile <(printf '%s\n' "$3") -out "$1.crt" 2>/dev/null
  rm -f "$1.csr"
}

# The CA, the landing node's and the control APIs' certificates (by their Services' names, and by 127.0.0.1, as
# the harness reaches them through port-forwards), and the producer's.
harness_pki() {
  local d="$H/pki" n=sdh-harness
  if [[ ! -s "$d/ca.crt" ]]; then
    (umask 077; openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj "/O=SDH PoC Harness/CN=SDH PoC Harness CA" -keyout "$d/ca.key" -out "$d/ca.crt" 2>/dev/null)
  fi
  issue "$d/producer" "/O=SDH PoC Harness/CN=harness-producer" "extendedKeyUsage=clientAuth"
  issue "$d/landing" "/O=SDH PoC Harness/CN=landing.$n.svc" "extendedKeyUsage=serverAuth
subjectAltName=DNS:landing.$n.svc,DNS:landing.$n.svc.cluster.local,DNS:localhost,IP:127.0.0.1"
  issue "$d/control" "/O=SDH PoC Harness/CN=sdh-control.$n.svc" "extendedKeyUsage=serverAuth
subjectAltName=DNS:sdh-control-landing.$n.svc,DNS:sdh-control-split.$n.svc,DNS:sdh-control-convert.$n.svc,\
DNS:localhost,IP:127.0.0.1"
}

# The harness's token-signing key (EC P-256, PKCS#8) and its key set, as scripts/sandbox.sh harness_issuer.
harness_issuer() {
  local key="$H/issuer.pem" jwks="$H/issuer-jwks.json"
  if [[ ! -s "$key" ]]; then
    (umask 077; openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 2>/dev/null \
      | openssl pkcs8 -topk8 -nocrypt -out "$key")
  fi
  openssl pkey -in "$key" -pubout -outform DER | python3 -I -c '
import base64, json, sys
point = sys.stdin.buffer.read()[-65:]
assert point[0] == 4
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
print(json.dumps({"keys": [{"kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": sys.argv[1],
                            "x": b64(point[1:33]), "y": b64(point[33:])}]}))' "harness-issuer" > "$jwks"
}

# S3's address ranges in the region (its prefix list, which the gateway endpoint routes), and what else the
# harness needs to know.
harness_target() {
  local list
  list="$(aws ec2 describe-managed-prefix-lists --filters "Name=prefix-list-name,Values=com.amazonaws.$AWS_REGION.s3" \
    --query 'PrefixLists[0].PrefixListId' --output text)"
  [[ "$list" == pl-* ]] || die "no S3 prefix list in $AWS_REGION"
  aws ec2 get-managed-prefix-list-entries --prefix-list-id "$list" --query 'Entries[].Cidr' --output text \
    | tr '\t' '\n' | sed '/^$/d' > "$H/s3-cidrs.tmp"
  [[ -s "$H/s3-cidrs.tmp" ]] || die "S3's prefix list has no entries"
  mv "$H/s3-cidrs.tmp" "$H/s3-cidrs"
  printf 'bronze=%s\nsilver=%s\nregion=%s\n' "$BRONZE_BUCKET" "$SILVER_BUCKET" "$AWS_REGION" \
    > "$H/target.properties"
}

deploy_harness() {
  local n work
  n="$(ns "$P")"
  work="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '$work'" RETURN
  say "Harness pipeline $P (namespace $n)"
  export PIPELINE="$P" K8S_NS="$n" ICEBERG_PREFIX PRODUCER_SUBJECT="CN=harness-producer,O=SDH PoC Harness"
  ICEBERG_PREFIX="$(prefix "$P")"
  envsubst '$PIPELINE $K8S_NS' < "$HERE/namespace.yaml" | kc apply -f - >/dev/null

  local k="$STATE/keys/$P"
  ec_key "$k-inbox" "$(secret "inbox-passphrase-$P")"
  ec_key "$k-convert-inbox" "$(secret "convert-inbox-passphrase-$P")"
  # The convert stage seals to the transform stage's inbox; there is none, so nothing opens what it seals.
  ec_key "$k-fhir-recipient" "$(secret "fhir-recipient-passphrase-$P")"
  local landing_signer split_signer
  landing_signer="$(signing_public "$P" landing)"
  split_signer="$(signing_public "$P" splitter)"
  export INBOX_SHA256 CONVERT_INBOX_SHA256 RECIPIENT_SHA256
  INBOX_SHA256="$(fingerprint "$k-inbox-public.pem")"
  CONVERT_INBOX_SHA256="$(fingerprint "$k-convert-inbox-public.pem")"
  RECIPIENT_SHA256="$(fingerprint "$k-fhir-recipient-public.pem")"

  local vars='$PIPELINE $ICEBERG_PREFIX $PRODUCER_SUBJECT $INBOX_SHA256 $CONVERT_INBOX_SHA256 $RECIPIENT_SHA256'
  vars+=' $KAFKA_BROKERS $AWS_REGION $INTAKE_BUCKET $BRONZE_BUCKET $SILVER_BUCKET $GOLD_BUCKET'
  envsubst "$vars" < "$HARNESS_DIR/landing-application.yml" > "$work/landing-application.yml"
  envsubst "$vars" < "$HERE/splitter-application.yml" > "$work/splitter-application.yml"
  # No transform stage, so no registry for the convert stage to mark items in.
  envsubst "$vars" < "$HERE/convert-application.yml" | sed '/next-registry-schema:/d' \
    > "$work/convert-application.yml"

  local d="$H/pki"
  apply_secret "$n" landing-keys --from-file=tls.crt="$d/landing.crt" --from-file=tls.key="$d/landing.key" \
    --from-file=producers-ca.pem="$d/ca.crt" --from-file=splitter-inbox.pem="$k-inbox-public.pem"
  apply_secret "$n" landing-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-landing-$P")" \
    --from-literal=SDH_MAINTENANCE_INTERVAL=20s --from-literal=SDH_LEASE_JDBC_TABLE="landing_$ICEBERG_PREFIX.sdh_lease"
  apply_config "$n" landing-config --from-file=application.yml="$work/landing-application.yml"
  apply_secret "$n" splitter-keys --from-file=inbox.pem="$k-inbox.pem" \
    --from-file=inbox-public.pem="$k-inbox-public.pem" --from-file=recipient.pem="$k-convert-inbox-public.pem" \
    --from-file=landing-signer.pem="$landing_signer"
  apply_secret "$n" splitter-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-splitter-$P")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "inbox-passphrase-$P")" \
    --from-literal=SDH_MAINTENANCE_INTERVAL=20s --from-literal=SDH_STORAGE_COMMIT_MAX_RECORDS=1
  apply_config "$n" splitter-config --from-file=application.yml="$work/splitter-application.yml"
  apply_secret "$n" convert-keys --from-file=inbox.pem="$k-convert-inbox.pem" \
    --from-file=inbox-public.pem="$k-convert-inbox-public.pem" \
    --from-file=recipient.pem="$k-fhir-recipient-public.pem" --from-file=split-signer.pem="$split_signer"
  apply_secret "$n" convert-env --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-convert-$P")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "convert-inbox-passphrase-$P")" \
    --from-literal=SDH_MAINTENANCE_INTERVAL=20s
  apply_config "$n" convert-config --from-file=application.yml="$work/convert-application.yml"

  # Each stage from the Deployment in its README, renamed as ../pipeline/apply.sh renames them, by digest.
  mkdir -p "$work/landing" "$work/splitter" "$work/convert" "$work/control"
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
  local f
  for f in landing splitter convert; do
    grep -q '@sha256:' "$work/$f/$f.yaml" || die "the $f README's Deployment no longer matches (image)"
  done
  cp "$HARNESS_DIR/landing-patch.yaml" "$ROOT/deploy/sandbox/pipeline/landing-service.yaml" "$work/landing/"
  cp "$HARNESS_DIR/splitter-patch.yaml" "$work/splitter/"
  cp "$HARNESS_DIR/convert-patch.yaml" "$work/convert/"
  CONFIG_SHA="$(settings_digest "$work/landing-application.yml" "$d/landing.crt")" \
    stage_kustomization landing "landing.yaml, landing-service.yaml" landing-patch.yaml \
    > "$work/landing/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/splitter-application.yml" "$landing_signer")" \
    stage_kustomization splitter "splitter.yaml" splitter-patch.yaml > "$work/splitter/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/convert-application.yml" "$split_signer")" \
    stage_kustomization convert "convert.yaml" convert-patch.yaml > "$work/convert/kustomization.yaml"

  # The control APIs: from each README's "Control API", one replica each, on the stage's service account (so its
  # role), with the settings and issuer the sandbox's harness pipeline has (deploy/sandbox/harness), the key set
  # served from ECR Public's copy of the same busybox. No ingress: the harness reaches them through port-forwards.
  envsubst '$K8S_NS' < "$SANDBOX_HARNESS/issuer.yaml" \
    | sed 's#docker.io/library/busybox:#public.ecr.aws/docker/library/busybox:#' > "$work/control/issuer.yaml"
  apply_config "$n" jwks --from-file=jwks.json="$H/issuer-jwks.json"
  envsubst '$K8S_NS' < "$SANDBOX_HARNESS/control-application.yml" > "$work/control-application.yml"
  local stage
  for stage in landing split convert; do
    apply_config "$n" "sdh-control-$stage" --from-file=application.yml="$work/control-application.yml"
    kc -n "$n" create secret tls "sdh-control-$stage-tls" --cert="$d/control.crt" --key="$d/control.key" \
      --dry-run=client -o yaml | kc apply -f - >/dev/null
  done
  local common=(-e 's/replicas: 2 /replicas: 1 /' -e 's/<ingress-namespace>/sdh-ingress/')
  readme_control_yaml() { awk '/^```yaml control$/ {on=1; next} /^```$/ {on=0} on' "$1"; }
  {
    readme_control_yaml "$ROOT/landing-connector/src/dist/README.md" \
      | sed "${common[@]}" -e "s#<registry>/landing-connector:<version>#$landing_connector#" \
            -e 's/landing-claims-\(env\|keys\|config\)/landing-\1/g'
    echo "---"
    readme_control_yaml "$ROOT/file-connector/src/dist/README.md" \
      | sed "${common[@]}" -e "s#<registry>/split-stage:<version>#$split_stage#" \
            -e 's/file-connector-\(env\|keys\|config\)/splitter-\1/g'
    echo "---"
    readme_control_yaml "$ROOT/convert-stage/src/dist/README.md" \
      | sed "${common[@]}" -e "s#<registry>/convert-stage:<version>#$convert_stage#" \
            -e 's/convert-stage-\(env\|keys\|config\)/convert-\1/g'
  } > "$work/control/controls.yaml"
  local patches="" pair
  for pair in landing:landing split:splitter convert:convert; do
    patches+="${patches:+, }{ target: { kind: Deployment, name: sdh-control-${pair%%:*} }, patch: '[
      {\"op\": \"add\", \"path\": \"/spec/template/spec/serviceAccountName\", \"value\": \"${pair#*:}\"},
      {\"op\": \"add\", \"path\": \"/spec/template/spec/automountServiceAccountToken\", \"value\": false}]' }"
  done
  cat > "$work/control/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $n
resources: [issuer.yaml, controls.yaml]
patches: [$patches]
EOF

  if [[ "${SDH_POC_RENDER_ONLY:-0}" == "1" ]]; then   # for checking offline: print what would be applied
    for stage in landing splitter convert control; do kc kustomize "$work/$stage"; echo "---"; done
    return
  fi

  kc apply -k "$work/landing" >/dev/null
  kc -n "$n" rollout status deploy/landing --timeout=420s >/dev/null || failed "$n" deploy/landing
  kc apply -k "$work/splitter" >/dev/null
  { kc -n "$n" rollout status deploy/splitter --timeout=420s >/dev/null \
      && wait_for 300 logged "$n" deploy/splitter "Started FileConnectorApplication"; } || failed "$n" deploy/splitter
  kc apply -k "$work/convert" >/dev/null
  { kc -n "$n" rollout status deploy/convert --timeout=420s >/dev/null \
      && wait_for 300 logged "$n" deploy/convert "Started ConvertStageApplication"; } || failed "$n" deploy/convert
  kc apply -k "$work/control" >/dev/null
  kc -n "$n" rollout status deploy/jwks --timeout=180s >/dev/null || failed "$n" deploy/jwks
  for stage in landing split convert; do
    kc -n "$n" rollout status "deploy/sdh-control-$stage" --timeout=420s >/dev/null \
      || failed "$n" "deploy/sdh-control-$stage"
  done
  say "  landing node, split and convert stages (one pod each), and their control APIs, running"
}

harness_pki
harness_issuer
[[ "${SDH_POC_RENDER_ONLY:-0}" == "1" ]] || harness_target
deploy_harness
