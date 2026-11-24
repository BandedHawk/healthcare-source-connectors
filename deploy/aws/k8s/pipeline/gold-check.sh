#!/usr/bin/env bash
# The gold check (docs/AWS-POC.md §10, step 5): reads every bundle in gold as a downstream reader would, checks each
# against mapping's baselines, and runs the HL7 FHIR validator (US Core 6.1.0) on a few of each document. Run after a
# load (load-test.sh runs it), with a session up. Prints counts, document names, error codes and fact names only.
#
#   deploy/aws/k8s/pipeline/gold-check.sh [keep-per-document]    # default 2 bundles of each document validated
#
# What it does:
#   1. Builds the check's classes (GoldCheck, beside GoldReaderExample and CcdaCorrespondence in the transformer's
#      tests; ./mvnw -o) into a small jar, and gathers mapping's baselines for the set deployed: expected issues and
#      validator errors, resource counts, and each test document's SHA-256 (to name a bundle's source).
#   2. Gives the reader what a downstream reader is given: the consumer key pair's private half (the transform stage
#      seals its messages to it), the transform stage's signing public key, a read-only database role on the
#      catalog (over TLS), its own Kafka login (made by poc.sh), the servers' CA, and the read-only AWS role
#      gold-reader (Pod Identity: gold and silver, unwrap-only on the fhir and records KEKs).
#   3. Runs the Job (gold-check.yaml) on the transform stage's image, and prints its two containers' summaries.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
TRANSFORMER="${SDH_TRANSFORMER:-$(cd "$ROOT/.." && pwd)/healthcare-data-transformer}"
MAPPING="${SDH_MAPPING:-$(cd "$ROOT/.." && pwd)/mapping}"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's
ACCOUNT=493469777520
export PIPELINE="${SDH_PIPELINES:-ccda-push}"; PIPELINE="${PIPELINE%% *}"
export ICEBERG_PREFIX="${PIPELINE//-/_}" KAFKA_BROKERS="kafka-0.kafka.sdh-infra.svc:9094"
export GOLD_BUCKET="sdh-poc-gold-$ACCOUNT" SILVER_BUCKET="sdh-poc-silver-$ACCOUNT"
export KEEP_PER_DOCUMENT="${1:-2}"
export VALIDATOR_SHA256=57aa43fb40cec5de41def81c927725fd85908eeb71509b7f9313e04d9b8ec79b   # 7.0.3, as mapping pins
VALIDATOR_OBJECT="s3://sdh-poc-tools-$ACCOUNT/validator_cli-7.0.3.jar"
DOCUMENTS="$TRANSFORMER/transform-stage/target/test-documents"
BASELINE="$MAPPING/us-core-6.1/src/test/resources/baseline"

say() { printf '>> %s\n' "$*"; }
die() { printf 'gold-check.sh: %s\n' "$*" >&2; exit 1; }
secret() {  # as poc.sh's
  local f="$STATE/secrets/$1"
  mkdir -p "$STATE/secrets"; chmod 700 "$STATE/secrets"
  [[ -s "$f" ]] || (umask 077; openssl rand -base64 24 | tr -d '/+=\n' > "$f")
  cat "$f"
}

# shellcheck disable=SC1091
source "$STATE/images.env"
export TRANSFORM_IMAGE="$transform_stage"
[[ -d "$DOCUMENTS/documents" ]] || die "no test documents at $DOCUMENTS: build the transformer (./mvnw -o package)"
# The baselines must be the set deployed's: mapping's working copy against the test documents the transformer was
# built with.
diff -rq "$BASELINE/output" "$DOCUMENTS/baseline/output" >/dev/null \
  || die "mapping's baseline ($BASELINE) isn't the deployed set's: rebuild the transformer, or check out mapping's"
kubectl get ns sdh-fhir >/dev/null || die "no session: run deploy/aws/poc.sh up"
kubectl -n sdh-fhir delete job gold-check --ignore-not-found --wait=true >/dev/null

work="$(mktemp -d)"
trap 'rm -r "$work"' EXIT

say "Building the check (GoldCheck, offline)"
(cd "$TRANSFORMER" && ./mvnw -o -q -pl transform-stage test-compile)
(cd "$TRANSFORMER/transform-stage/target/test-classes" && jar --create --file "$work/gold-check.jar" \
  com/sdh/healthcare/transform/GoldCheck*.class com/sdh/healthcare/transform/GoldReaderExample*.class \
  com/sdh/healthcare/transform/CcdaCorrespondence.class 'com/sdh/healthcare/transform/CcdaCorrespondence$'*.class \
  com/sdh/healthcare/transform/TestDocuments*.class)
python3 -I - "$DOCUMENTS/documents" "$work/xml-sha256.json" <<'EOF'
import hashlib, json, os, sys
# The root element only, as GoldCheck digests a split record: the split stage drops what's outside it.
def root(b):
    s, e = b.find(b"<ClinicalDocument"), b.rfind(b"</ClinicalDocument>")
    return b if s < 0 or e < s else b[s:e + len(b"</ClinicalDocument>")]
d = sys.argv[1]
json.dump({n[:-4]: hashlib.sha256(root(open(os.path.join(d, n), "rb").read())).hexdigest()
           for n in sorted(os.listdir(d)) if n.endswith(".xml")}, open(sys.argv[2], "w"), indent=1)
EOF

say "The reader's keys, database role and service account"
k="$STATE/keys/$PIPELINE"
[[ -s "$k-consumer.pem" ]] || die "no consumer key in $STATE/keys: deploy the pipeline (apply.sh)"
# The transform stage's KMS signing key's public half, as apply.sh exports the other stages' (no stage after the
# transform stage verifies it, so apply.sh doesn't).
if [[ ! -s "$k-transform-signing-public.pem" ]]; then
  aws kms get-public-key --key-id "alias/sdh-poc-$PIPELINE-transform-signing" --query PublicKey --output text \
    | base64 -d | openssl pkey -pubin -inform DER -out "$k-transform-signing-public.pem.tmp"
  mv "$k-transform-signing-public.pem.tmp" "$k-transform-signing-public.pem"
fi
kubectl -n sdh-fhir create configmap gold-check --from-file=gold-check.jar="$work/gold-check.jar" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n sdh-fhir create configmap gold-check-docs \
  --from-file=expected-issues.json="$DOCUMENTS/baseline/expected-issues.json" \
  --from-file=expected-errors.json="$BASELINE/expected-errors.json" \
  --from-file=resource-counts.json="$BASELINE/resource-counts.json" \
  --from-file=xml-sha256.json="$work/xml-sha256.json" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n sdh-fhir create secret generic gold-check-keys --from-file=consumer.pem="$k-consumer.pem" \
  --from-file=transform-signer.pem="$k-transform-signing-public.pem" \
  --from-file=kafka-ca.crt="$STATE/pki/ca.crt" --from-file=postgres-ca.crt="$STATE/pki/ca.crt" --dry-run=client -o yaml \
  | kubectl apply -f - >/dev/null
url="$(aws s3 presign "$VALIDATOR_OBJECT" --expires-in 7200)"
# Its Kafka login: the user gold-check-<pipeline>, made by poc.sh (pipeline_kafka), which may only read the topic.
jaas="$(printf 'org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";' \
  "gold-check-$PIPELINE" "$(secret "kafka-gold-check-$PIPELINE")")"
kubectl -n sdh-fhir create secret generic gold-check-env \
  --from-literal=CONSUMER_PASSPHRASE="$(secret "consumer-passphrase-$PIPELINE")" \
  --from-literal=CATALOG_PASSWORD="$(secret "db-gold-reader-$PIPELINE")" \
  --from-literal=KAFKA_JAAS="$jaas" \
  --from-literal=VALIDATOR_URL="$url" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
role="gold_reader_$ICEBERG_PREFIX"
kubectl -n sdh-infra exec -i postgres-0 -- psql -q -v ON_ERROR_STOP=1 -U postgres -d sdh >/dev/null <<EOF
SELECT 'CREATE ROLE $role LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$role')\\gexec
ALTER ROLE $role PASSWORD '$(secret "db-gold-reader-$PIPELINE")';
GRANT SELECT ON public.iceberg_tables, public.iceberg_namespace_properties TO $role;
EOF
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ServiceAccount
metadata: { name: gold-reader, namespace: sdh-fhir }
automountServiceAccountToken: false
EOF

say "Running the check (read, then the HL7 validator on $KEEP_PER_DOCUMENT bundle(s) of each document)"
# shellcheck disable=SC2016
envsubst '$TRANSFORM_IMAGE $KEEP_PER_DOCUMENT $AWS_REGION $ICEBERG_PREFIX $GOLD_BUCKET $SILVER_BUCKET $KAFKA_BROKERS
  $PIPELINE $VALIDATOR_SHA256' < "$HERE/gold-check.yaml" | kubectl apply -f - >/dev/null
# Each container's summary, followed live: a pod the Job gives up on is deleted, and its logs with it. Only the
# check's own lines are kept, and the names of any exceptions (never their messages).
# A stream with no output for minutes can be closed by the API server (2026-11-16: the read, which prints only at
# its end, was given up on while it ran), so it is followed again, from where it stopped, until the container ends.
# A pod lost with its node is replaced by the Job (gold-check.yaml), so each new pod is followed in turn.
deadline=$((SECONDS + 5400))
follow() {  # <pod> <container>: once it has started, until it ends; false if the pod ends first
  local since=""
  while ((SECONDS < deadline)); do
    if kubectl -n sdh-fhir logs "$1" -c "$2" --tail=1 >/dev/null 2>&1; then
      local from="$since"
      since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      kubectl -n sdh-fhir logs -f "$1" -c "$2" ${from:+--since-time="$from"} 2>/dev/null \
        | grep --line-buffered -oE '^gold-check: .*|[A-Za-z.]+(Exception|Error)\b' || true
      ended "$1" "$2" && return 0
    fi
    gone "$1" && return 1
    sleep 5
  done
  return 1
}
ended() {  # <pod> <container>: true once the container has terminated
  local c="[?(@.name==\"$2\")].state.terminated.reason"
  [[ -n "$(kubectl -n sdh-fhir get "$1" \
    -o jsonpath="{.status.initContainerStatuses$c}{.status.containerStatuses$c}" 2>/dev/null)" ]]
}
gone() {  # <pod>: true once it has ended, or is no more
  local phase
  phase="$(kubectl -n sdh-fhir get "$1" -o jsonpath='{.status.phase}' 2>/dev/null)"
  [[ -z "$phase" || "$phase" == Failed || "$phase" == Succeeded ]]
}
job_ended() {
  kubectl -n sdh-fhir get job gold-check -o jsonpath='{.status.conditions[?(@.status=="True")].type}' 2>/dev/null \
    | grep -qwE 'Complete|Failed'
}
newest() {
  kubectl -n sdh-fhir get pods -l app=gold-check --sort-by=.metadata.creationTimestamp -o name 2>/dev/null | tail -1
}
# Where each pod runs, and how each container ended, so a failure that takes the pod with it is still explained.
seen="" where=""
until job_ended || ((SECONDS >= deadline)); do
  pod="$(newest)"
  if [[ -z "$pod" || " $seen " == *" $pod "* ]]; then sleep 5; continue; fi
  [[ -z "$seen" ]] || say "  the last pod ended with the Job still running (its node was taken): following the next"
  seen+=" $pod"
  for _ in $(seq 60); do
    where="$(kubectl -n sdh-fhir get "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
    [[ -n "$where" ]] && break
    sleep 5
  done
  [[ -z "$where" ]] || say "  on node $where, pool $(kubectl get node "$where" \
    -o jsonpath='{.metadata.labels.sdh-poc/pool}' 2>/dev/null), $(kubectl get node "$where" \
    -o jsonpath='{.metadata.labels.eks\.amazonaws\.com/capacityType}' 2>/dev/null)"
  follow "$pod" read && follow "$pod" validate || true
  # The Job takes a moment to mark itself ended, or to start the next pod.
  for _ in $(seq 12); do job_ended || [[ "$(newest)" != "$pod" ]] && break; sleep 5; done
done
status="$(kubectl -n sdh-fhir get job gold-check -o jsonpath='{.status.succeeded}')"
if [[ "$status" == 1 ]]; then say "gold check passed"; else
  kubectl -n sdh-fhir get pods -l app=gold-check
  kubectl -n sdh-fhir get events --field-selector involvedObject.kind=Pod --sort-by=.lastTimestamp 2>/dev/null \
    | grep gold-check | tail -5 || true
  [[ -z "$where" ]] || kubectl get events -A --field-selector "involvedObject.kind=Node,involvedObject.name=$where" \
    --sort-by=.lastTimestamp 2>/dev/null | tail -5 || true
  say "gold check FAILED (above); the Job is kept for 2 hours: kubectl -n sdh-fhir describe job gold-check"
  exit 1
fi
