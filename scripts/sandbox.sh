#!/usr/bin/env bash
# The demo sandbox (docs/SANDBOX.md): a three-node Kubernetes cluster in minikube (KVM2), laid out as a customer
# would run the pipeline, with every VM disk and all of its state on the sandbox disk (SDH_SANDBOX, see
# minikube-home.sh).
#
#   scripts/sandbox.sh up          create or start the cluster, and the services every pipeline uses
#   scripts/sandbox.sh images      build the landing node's and both stages' customer images (in the sdh-it VM)
#                                  and push them to the cluster's registry
#   scripts/sandbox.sh deploy      deploy each pipeline's landing node, split stage and convert stage (after up
#                                  and images)
#   scripts/sandbox.sh push <pipeline> <file>...
#                                  push files to a pipeline's landing node, as its producer
#   scripts/sandbox.sh check       push the sample C-CDA documents and check they are landed, split, converted and
#                                  sent on
#   scripts/sandbox.sh status      pods, nodes, topics
#   scripts/sandbox.sh grafana     port-forward Grafana to http://localhost:3000 (until interrupted)
#   scripts/sandbox.sh stop        stop the cluster's VMs; `up` starts them again, with everything kept
#   scripts/sandbox.sh delete --yes
#                                  delete the cluster and its volumes (the state directory is kept)
#
# Environment (defaults): SDH_DEMO_PROFILE=sdh-demo SDH_DEMO_NODES=3 SDH_DEMO_CPUS=6 SDH_DEMO_MEMORY=6g
#   SDH_DEMO_DISK=120g SDH_PIPELINES=ccda-push (each pushed C-CDA; one namespace sdh-<pipeline> each)
#
# Kept in $SDH_SANDBOX/sdh-demo/state (mode 700), made once and reused, since data encrypted or stored under them
# can't be read without them: the database and S3 credentials, Vault's unseal key and root token, the stages'
# Vault tokens, the stages' inbox key pairs and the FHIR stage's, and the sandbox CA. Never delete it while the
# cluster's volumes hold data.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/minikube-home.sh" || exit 1
[[ -n "${SDH_SANDBOX:-}" ]] || { echo "Set SDH_SANDBOX (see scripts/minikube-home.sh)" >&2; exit 1; }
command -v minikube >/dev/null || { echo "minikube is not installed" >&2; exit 1; }
for tool in openssl python3 envsubst; do
  command -v "$tool" >/dev/null || { echo "$tool is needed" >&2; exit 1; }
done

PROFILE="${SDH_DEMO_PROFILE:-sdh-demo}"
PIPELINES="${SDH_PIPELINES:-ccda-push}"
DEPLOY="$ROOT/deploy/sandbox"
STATE="$SDH_SANDBOX/sdh-demo/state"
export KUBECONFIG="$SDH_KUBE_DIR/$PROFILE"
KAFKA_BROKERS="kafka-0.kafka.sdh-infra.svc:9092,kafka-1.kafka.sdh-infra.svc:9092,kafka-2.kafka.sdh-infra.svc:9092"
PRODUCER_SUBJECT="CN=producer-1,O=SDH Sandbox"
REGISTRY="localhost:5000"

say() { echo ">> $*"; }
kc() { minikube -p "$PROFILE" kubectl -- "$@"; }
infra() { kc -n sdh-infra "$@"; }

mkdir -p "$STATE"/{secrets,pki,keys}
chmod 700 "$STATE"

# secret <name>: a generated secret, made on first use and kept.
secret() {
  local file="$STATE/secrets/$1"
  [[ -s "$file" ]] || (umask 077; openssl rand -hex 24 > "$file")
  cat "$file"
}

# Pipeline names: ns <pipeline> is its namespace; prefix <pipeline> its Iceberg namespace and role prefix.
ns() { echo "sdh-$1"; }
prefix() { echo "${1//-/_}"; }

# --- Cluster ---------------------------------------------------------------------------------------------------

start_cluster() {
  local state
  state="$(minikube status -p "$PROFILE" --format '{{.Host}}' 2>/dev/null | head -n 1 || true)"
  if [[ "$state" == "Running" ]]; then
    return
  fi
  mkdir -p "$(dirname "$KUBECONFIG")"
  if minikube profile list -o json 2>/dev/null | python3 -c '
import json, sys
names = [p["Name"] for p in json.load(sys.stdin).get("valid", [])]
sys.exit(0 if sys.argv[1] in names else 1)' "$PROFILE"; then
    say "Starting minikube profile '$PROFILE'"
    minikube start -p "$PROFILE"
  else
    say "Creating minikube profile '$PROFILE': ${SDH_DEMO_NODES:-3} nodes × ${SDH_DEMO_CPUS:-6} CPUs," \
      "${SDH_DEMO_MEMORY:-6g}, ${SDH_DEMO_DISK:-120g} disk, Calico, under $MINIKUBE_HOME"
    minikube start -p "$PROFILE" --driver=kvm2 --container-runtime=docker --kubernetes-version=v1.37.0 \
      --nodes="${SDH_DEMO_NODES:-3}" --cpus="${SDH_DEMO_CPUS:-6}" --memory="${SDH_DEMO_MEMORY:-6g}" \
      --disk-size="${SDH_DEMO_DISK:-120g}" --cni=calico --addons=registry,metrics-server
    # Node-local volumes (local-path) instead of minikube's single-node hostpath provisioner.
    minikube -p "$PROFILE" addons disable storage-provisioner
    minikube -p "$PROFILE" addons disable default-storageclass
  fi
  kc apply -f "$DEPLOY/cluster/local-path-storage.yaml" >/dev/null
  kc -n local-path-storage rollout status deploy/local-path-provisioner --timeout=300s >/dev/null
}

# --- Shared services -------------------------------------------------------------------------------------------

# s3_config: every stage's access key, and the buckets each may use (the medallion layers).
s3_config() {
  local identities=() p
  identities+=("$(s3_identity sandbox-admin "$(secret s3-sandbox-admin)" Admin Read Write List Tagging)")
  for p in $PIPELINES; do
    identities+=("$(s3_identity "landing-$p" "$(secret "s3-landing-$p")" \
      Read:sdh-bronze Write:sdh-bronze List:sdh-bronze Tagging:sdh-bronze)")
    identities+=("$(s3_identity "splitter-$p" "$(secret "s3-splitter-$p")" Read:sdh-bronze List:sdh-bronze \
      Read:sdh-silver Write:sdh-silver List:sdh-silver Tagging:sdh-silver)")
    identities+=("$(s3_identity "convert-$p" "$(secret "s3-convert-$p")" \
      Read:sdh-silver Write:sdh-silver List:sdh-silver Tagging:sdh-silver)")
    identities+=("$(s3_identity "fhir-$p" "$(secret "s3-fhir-$p")" Read:sdh-silver List:sdh-silver \
      Read:sdh-gold Write:sdh-gold List:sdh-gold Tagging:sdh-gold)")
  done
  local IFS=,
  echo "{\"identities\":[${identities[*]}]}"
}

# s3_identity <name> <secret key> <action>...
s3_identity() {
  python3 -c '
import json, sys
name, key, actions = sys.argv[1], sys.argv[2], sys.argv[3:]
print(json.dumps({"name": name, "credentials": [{"accessKey": name, "secretKey": key}], "actions": actions}))
' "$@"
}

start_services() {
  say "Shared services (namespace sdh-infra)"
  kc apply -f "$DEPLOY/infra/namespace.yaml" >/dev/null
  infra create secret generic postgres --from-literal=password="$(secret postgres)" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  infra create secret generic grafana --from-literal=admin-password="$(secret grafana)" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  infra create secret generic seaweedfs --from-file=s3.json=<(s3_config) \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  local manifest
  for manifest in postgres kafka seaweedfs vault telemetry; do
    kc apply -f "$DEPLOY/infra/$manifest.yaml" >/dev/null
  done
  # SeaweedFS reads its identities only at start: restart it if they changed.
  local digest
  digest="$(s3_config | sha256sum | cut -c1-16)"
  if [[ "$(infra get statefulset seaweedfs -o jsonpath='{.spec.template.metadata.annotations.sdh/s3-config}')" \
      != "$digest" ]]; then
    infra patch statefulset seaweedfs --type merge \
      -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"sdh/s3-config\":\"$digest\"}}}}}" >/dev/null
  fi
  for workload in statefulset/postgres statefulset/kafka statefulset/seaweedfs deploy/otel-collector \
      statefulset/lgtm; do
    infra rollout status "$workload" --timeout=600s >/dev/null || { infra get pods; exit 1; }
  done
  infra wait --for=jsonpath='{.status.phase}'=Running pod/vault-0 --timeout=300s >/dev/null
  setup_vault
  setup_buckets
  setup_topics
  setup_database
}

# vault_sh <script>: runs a shell script in the Vault pod, logged in as root. The token goes over stdin, so it
# isn't in any command line.
vault_sh() {
  printf '%s\n' "$(cat "$STATE/secrets/vault-root-token")" \
    | infra exec -i vault-0 -- sh -c 'read -r VAULT_TOKEN; export VAULT_TOKEN; '"$1"
}

setup_vault() {
  local status
  status="$(infra exec vault-0 -- vault status -format=json 2>/dev/null || true)"
  if [[ "$(json_field "$status" initialized)" != "True" ]]; then
    [[ ! -s "$STATE/vault-init.json" ]] || { echo "Vault isn't initialised, but $STATE/vault-init.json" \
      "exists: is this the same Vault? Not initialising it again." >&2; exit 1; }
    say "Initialising Vault (unseal key and root token kept in $STATE)"
    (umask 077; infra exec vault-0 -- vault operator init -key-shares=1 -key-threshold=1 -format=json \
      > "$STATE/vault-init.json")
    (umask 077; python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["root_token"])' \
      "$STATE/vault-init.json" > "$STATE/secrets/vault-root-token")
    status="$(infra exec vault-0 -- vault status -format=json 2>/dev/null || true)"
  fi
  if [[ "$(json_field "$status" sealed)" == "True" ]]; then
    say "Unsealing Vault"
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["unseal_keys_b64"][0])' \
      "$STATE/vault-init.json" | infra exec -i vault-0 -- sh -c 'read -r KEY; vault operator unseal "$KEY"' \
      >/dev/null
  fi
  infra wait --for=condition=Ready pod/vault-0 --timeout=120s >/dev/null
  vault_sh 'vault secrets list -format=json | grep -q "\"transit/\"" || vault secrets enable transit' >/dev/null
  local p
  for p in $PIPELINES; do
    # The KEKs and signing keys never leave Vault, and can't be deleted (deletion_allowed is false).
    vault_sh "
      for k in $p-landed-items $p-records $p-fhir; do
        vault read transit/keys/\$k >/dev/null 2>&1 || vault write -f transit/keys/\$k type=aes256-gcm96
      done
      for k in $p-landing-signing $p-splitter-signing $p-convert-signing; do
        vault read transit/keys/\$k >/dev/null 2>&1 || vault write -f transit/keys/\$k type=ecdsa-p256
      done" >/dev/null
    vault_policy "landing-$p" "
      path \"transit/encrypt/$p-landed-items\" { capabilities = [\"update\"] }
      path \"transit/decrypt/$p-landed-items\" { capabilities = [\"update\"] }
      path \"transit/sign/$p-landing-signing/*\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-landing-signing\" { capabilities = [\"read\"] }"
    vault_policy "splitter-$p" "
      path \"transit/encrypt/$p-landed-items\" { capabilities = [\"update\"] }
      path \"transit/decrypt/$p-landed-items\" { capabilities = [\"update\"] }
      path \"transit/encrypt/$p-records\" { capabilities = [\"update\"] }
      path \"transit/decrypt/$p-records\" { capabilities = [\"update\"] }
      path \"transit/sign/$p-splitter-signing/*\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-splitter-signing\" { capabilities = [\"read\"] }"
    vault_policy "convert-$p" "
      path \"transit/encrypt/$p-records\" { capabilities = [\"update\"] }
      path \"transit/decrypt/$p-records\" { capabilities = [\"update\"] }
      path \"transit/sign/$p-convert-signing/*\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-convert-signing\" { capabilities = [\"read\"] }"
    vault_policy "fhir-$p" "
      path \"transit/decrypt/$p-records\" { capabilities = [\"update\"] }
      path \"transit/encrypt/$p-fhir\" { capabilities = [\"update\"] }
      path \"transit/decrypt/$p-fhir\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-convert-signing\" { capabilities = [\"read\"] }"
    local role
    for role in landing splitter convert fhir; do
      vault_token "$role-$p"
    done
    vault_public_key "$p-landing-signing" > "$STATE/keys/$p-landing-signing-public.pem"
    vault_public_key "$p-splitter-signing" > "$STATE/keys/$p-splitter-signing-public.pem"
    vault_public_key "$p-convert-signing" > "$STATE/keys/$p-convert-signing-public.pem"
  done
}

# json_field <json> <field>: a top-level field, as Python prints it.
json_field() {
  python3 -c 'import json,sys
try: print(json.loads(sys.argv[1]).get(sys.argv[2]))
except ValueError: print("")' "$1" "$2"
}

# vault_policy <name> <policy>
vault_policy() {
  vault_sh "printf '%s' '$2' | vault policy write $1 -" >/dev/null
}

# vault_token <policy>: a periodic token for a stage, renewed if it is still valid, made again if not. Unused for
# 32 days, it expires; `up` renews it.
vault_token() {
  local file="$STATE/secrets/vault-token-$1"
  if [[ -s "$file" ]] && vault_sh "vault token renew $(cat "$file")" >/dev/null 2>&1; then
    return
  fi
  vault_sh "vault token create -orphan -no-default-policy -policy=$1 -period=768h -display-name=$1 \
    -field=token" | (umask 077; cat > "$file")
}

# vault_public_key <key>: a Transit signing key's public half, as PEM.
vault_public_key() {
  vault_sh "vault read -format=json transit/keys/$1" | python3 -c '
import json, sys
data = json.load(sys.stdin)["data"]
print(data["keys"][str(data["latest_version"])]["public_key"].strip())'
}

setup_buckets() {
  local bucket out
  for bucket in sdh-bronze sdh-silver sdh-gold; do
    # Until it is made, or is there already: the S3 gateway takes a while to find its master after a restart.
    local deadline=$((SECONDS + 300))
    until out="$(infra exec statefulset/seaweedfs -- sh -c \
        "echo 's3.bucket.create -name $bucket' | weed shell -master=localhost:9333" 2>&1)" \
        && ! grep -q error <<<"$out" || grep -q "already exists" <<<"$out"; do
      ((SECONDS < deadline)) || { echo "Could not create bucket $bucket: $out" >&2; exit 1; }
      sleep 5
    done
  done
}

setup_topics() {
  local p topic
  for p in $PIPELINES; do
    # One partition each: a pipeline's items and records stay in the order they were landed.
    for topic in "$p.landed" "$p.split" "$p.records" "$p.convert.dlq" "$p.fhir.dlq"; do
      infra exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
        --if-not-exists --topic "$topic" --partitions 1 --replication-factor 3 \
        --config min.insync.replicas=2 >/dev/null
    done
  done
}

# setup_database: a login role per stage, all acting as sdh_catalog, which owns the catalog's and leases' tables
# and the stages' registries (a schema per stage and pipeline, which the stage makes).
setup_database() {
  local p role sql
  sql="SELECT 'CREATE ROLE sdh_catalog NOLOGIN' WHERE NOT EXISTS
         (SELECT FROM pg_roles WHERE rolname = 'sdh_catalog')\\gexec
       GRANT ALL ON SCHEMA public TO sdh_catalog;
       GRANT CREATE ON DATABASE sdh TO sdh_catalog;"
  for p in $PIPELINES; do
    for role in landing splitter convert fhir; do
      local name="${role}_$(prefix "$p")"
      sql+="
       SELECT 'CREATE ROLE $name LOGIN IN ROLE sdh_catalog' WHERE NOT EXISTS
         (SELECT FROM pg_roles WHERE rolname = '$name')\\gexec
       ALTER ROLE $name PASSWORD '$(secret "db-$role-$p")';
       ALTER ROLE $name SET role = 'sdh_catalog';"
    done
  done
  printf '%s\n' "$sql" | infra exec -i postgres-0 -- env PGPASSWORD="" \
    psql -q -v ON_ERROR_STOP=1 -U postgres -d sdh >/dev/null
}

# --- Keys and certificates -------------------------------------------------------------------------------------

# ec_key <file stem> <passphrase>: an EC P-256 key pair, made once: <stem>.pem (encrypted PKCS#8) and
# <stem>-public.pem.
ec_key() {
  [[ -s "$1.pem" ]] && return
  (umask 077
   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 2>/dev/null \
     | openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -passout "pass:$2" -out "$1.pem")
  openssl pkey -in "$1.pem" -passin "pass:$2" -pubout -out "$1-public.pem"
}

fingerprint() { openssl pkey -pubin -in "$1" -outform DER | sha256sum | cut -d' ' -f1; }

# pki: the sandbox CA, the producer's client certificate, and each landing node's server certificate.
pki() {
  local dir="$STATE/pki"
  if [[ ! -s "$dir/ca.crt" ]]; then
    (umask 077; openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj "/O=SDH Sandbox/CN=SDH Sandbox CA" -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null)
  fi
  issue "$dir/producer" "/O=SDH Sandbox/CN=producer-1" "extendedKeyUsage=clientAuth"
  local p n
  for p in $PIPELINES; do
    n="$(ns "$p")"
    issue "$dir/landing-$p" "/O=SDH Sandbox/CN=landing.$n.svc" \
      "extendedKeyUsage=serverAuth
subjectAltName=DNS:landing.$n.svc,DNS:landing.$n.svc.cluster.local,DNS:landing.$n"
  done
}

# issue <stem> <subject> <extensions>: a certificate from the sandbox CA, made once.
issue() {
  [[ -s "$1.crt" ]] && return
  (umask 077; openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "$2" -keyout "$1.key.tmp" \
    -out "$1.csr" 2>/dev/null
   openssl pkcs8 -topk8 -nocrypt -in "$1.key.tmp" -out "$1.key"
   rm -f "$1.key.tmp")
  openssl x509 -req -in "$1.csr" -CA "$STATE/pki/ca.crt" -CAkey "$STATE/pki/ca.key" -CAcreateserial \
    -days 825 -extfile <(printf '%s\n' "$3") -out "$1.crt" 2>/dev/null
  rm -f "$1.csr"
}

# --- Images ----------------------------------------------------------------------------------------------------

build_images() {
  local version
  version="$(cd "$ROOT" && ./mvnw -q -N help:evaluate -Dexpression=project.version -DforceStdout)"
  say "Building the customer images ($version) in the sdh-it VM"
  (cd "$ROOT" && ./mvnw -q -pl landing-connector,file-connector,convert-stage -am package -DskipTests)
  # module=image: the split stage kept its module's name, file-connector.
  local apps=(landing-connector=landing-connector file-connector=split-stage convert-stage=convert-stage) app
  for app in "${apps[@]}"; do
    SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 SDH_IMAGE="sdh/${app#*=}:$version" "$ROOT/scripts/build-image.sh" \
      "${app%%=*}" customer
  done
  start_cluster
  say "Copying them to the cluster's registry ($REGISTRY on every node)"
  # Loaded into the first node's Docker and pushed from there: the registry addon answers on localhost:5000 on
  # every node, and the VMs' networks can't reach each other.
  (source "$ROOT/scripts/minikube-env.sh" >/dev/null
   docker save "sdh/landing-connector:$version" "sdh/split-stage:$version" "sdh/convert-stage:$version") \
    | minikube -p "$PROFILE" ssh --native-ssh=false -- docker load >/dev/null
  : > "$STATE/images.env.tmp"
  for app in "${apps[@]}"; do
    app="${app#*=}"
    minikube -p "$PROFILE" ssh -- "docker tag sdh/$app:$version $REGISTRY/sdh/$app:$version \
      && docker push -q $REGISTRY/sdh/$app:$version >/dev/null"
    # Deployed by digest, so a rebuild under the same SNAPSHOT version is a new image to Kubernetes.
    local digest
    digest="$(minikube -p "$PROFILE" ssh -- docker inspect --format "'{{index .RepoDigests 0}}'" \
      "$REGISTRY/sdh/$app:$version" | tr -d '\r')"
    echo "${app//-/_}=$digest" >> "$STATE/images.env.tmp"
    say "  $digest"
  done
  mv "$STATE/images.env.tmp" "$STATE/images.env"
  if [[ "${SDH_IT_STOP:-1}" == "1" || "${SDH_IT_STOP:-}" == "true" ]]; then
    say "Stopping the sdh-it VM"
    MINIKUBE_HOME="$MINIKUBE_HOME" minikube stop -p "${SDH_IT_PROFILE:-sdh-it}" >/dev/null
  fi
}

# --- Pipelines -------------------------------------------------------------------------------------------------

deploy_pipelines() {
  [[ -s "$STATE/images.env" ]] || { echo "No images yet: run scripts/sandbox.sh images" >&2; exit 1; }
  # shellcheck source=/dev/null
  source "$STATE/images.env"
  [[ -n "${split_stage:-}" && -n "${convert_stage:-}" ]] || { echo "No split and convert stage images (made" \
    "before the stages): run scripts/sandbox.sh images" >&2; exit 1; }
  pki
  kc apply -f "$DEPLOY/producers/producer.yaml" >/dev/null
  kc -n sdh-producers create secret generic producer --from-file=ca.crt="$STATE/pki/ca.crt" \
    --from-file=tls.crt="$STATE/pki/producer.crt" --from-file=tls.key="$STATE/pki/producer.key" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  kc apply -f "$DEPLOY/fhir/namespace.yaml" >/dev/null
  local p
  for p in $PIPELINES; do
    deploy_pipeline "$p"
  done
  kc -n sdh-producers rollout status deploy/producer --timeout=300s >/dev/null
}

# deploy_pipeline <pipeline>
deploy_pipeline() {
  local p="$1" n work
  n="$(ns "$p")"
  work="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '$work'" RETURN
  say "Pipeline $p (namespace $n)"
  export PIPELINE="$p" K8S_NS="$n" ICEBERG_PREFIX="$(prefix "$p")" PRODUCER_SUBJECT KAFKA_BROKERS \
    SPLIT_FORMAT="ccda"
  envsubst '$PIPELINE $K8S_NS' < "$DEPLOY/pipeline/namespace.yaml" | kc apply -f - >/dev/null

  # Each stage's inbox key pair, which the stage before it seals its messages to, and the FHIR stage's, which the
  # convert stage seals records to.
  ec_key "$STATE/keys/$p-inbox" "$(secret "inbox-passphrase-$p")"
  ec_key "$STATE/keys/$p-convert-inbox" "$(secret "convert-inbox-passphrase-$p")"
  ec_key "$STATE/keys/$p-fhir-recipient" "$(secret "fhir-recipient-passphrase-$p")"
  export INBOX_SHA256 CONVERT_INBOX_SHA256 RECIPIENT_SHA256
  INBOX_SHA256="$(fingerprint "$STATE/keys/$p-inbox-public.pem")"
  CONVERT_INBOX_SHA256="$(fingerprint "$STATE/keys/$p-convert-inbox-public.pem")"
  RECIPIENT_SHA256="$(fingerprint "$STATE/keys/$p-fhir-recipient-public.pem")"
  local vars='$PIPELINE $ICEBERG_PREFIX $PRODUCER_SUBJECT $INBOX_SHA256 $CONVERT_INBOX_SHA256 $RECIPIENT_SHA256'
  vars+=' $KAFKA_BROKERS $SPLIT_FORMAT'
  local stage
  for stage in landing splitter convert; do
    envsubst "$vars" < "$DEPLOY/pipeline/$stage-application.yml" > "$work/$stage-application.yml"
  done

  apply_secret "$n" landing-keys --from-file=tls.crt="$STATE/pki/landing-$p.crt" \
    --from-file=tls.key="$STATE/pki/landing-$p.key" --from-file=producers-ca.pem="$STATE/pki/ca.crt" \
    --from-file=splitter-inbox.pem="$STATE/keys/$p-inbox-public.pem" \
    --from-file=vault-token="$STATE/secrets/vault-token-landing-$p"
  apply_secret "$n" landing-env --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-landing-$p")" \
    --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-landing-$p")"
  kc -n "$n" create configmap landing-config --from-file=application.yml="$work/landing-application.yml" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  apply_secret "$n" splitter-keys --from-file=inbox.pem="$STATE/keys/$p-inbox.pem" \
    --from-file=recipient.pem="$STATE/keys/$p-convert-inbox-public.pem" \
    --from-file=landing-signer.pem="$STATE/keys/$p-landing-signing-public.pem" \
    --from-file=vault-token="$STATE/secrets/vault-token-splitter-$p"
  apply_secret "$n" splitter-env --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-splitter-$p")" \
    --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-splitter-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "inbox-passphrase-$p")"
  kc -n "$n" create configmap splitter-config --from-file=application.yml="$work/splitter-application.yml" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  apply_secret "$n" convert-keys --from-file=inbox.pem="$STATE/keys/$p-convert-inbox.pem" \
    --from-file=recipient.pem="$STATE/keys/$p-fhir-recipient-public.pem" \
    --from-file=split-signer.pem="$STATE/keys/$p-splitter-signing-public.pem" \
    --from-file=vault-token="$STATE/secrets/vault-token-convert-$p"
  apply_secret "$n" convert-env --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-convert-$p")" \
    --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-convert-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "convert-inbox-passphrase-$p")"
  kc -n "$n" create configmap convert-config --from-file=application.yml="$work/convert-application.yml" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  fhir_handover "$p"

  # Each stage from the Deployment in its README, renamed for the pipeline's namespace, by digest.
  mkdir -p "$work/landing" "$work/splitter" "$work/convert"
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
  cp "$DEPLOY/pipeline/landing-patch.yaml" "$DEPLOY/pipeline/landing-service.yaml" "$work/landing/"
  cp "$DEPLOY/pipeline/splitter-patch.yaml" "$work/splitter/"
  cp "$DEPLOY/pipeline/convert-patch.yaml" "$work/convert/"
  stage_kustomization landing "landing.yaml, landing-service.yaml" landing-patch.yaml \
    > "$work/landing/kustomization.yaml"
  stage_kustomization splitter "splitter.yaml" splitter-patch.yaml > "$work/splitter/kustomization.yaml"
  stage_kustomization convert "convert.yaml" convert-patch.yaml > "$work/convert/kustomization.yaml"

  # The landing node first: it makes the landing tables, which the split stage may only read. The split stage
  # before the convert stage: it makes split_records.
  kc apply -k "$work/landing" >/dev/null
  if ! wait_for 300 landing_active "$n"; then
    kc -n "$n" get pods -o wide; kc -n "$n" logs deploy/landing --tail=40; exit 1
  fi
  kc apply -k "$work/splitter" >/dev/null
  if ! kc -n "$n" rollout status deploy/splitter --timeout=300s >/dev/null \
      || ! wait_for 300 logged "$n" deploy/splitter "Started FileConnectorApplication"; then
    kc -n "$n" get pods -o wide; kc -n "$n" logs deploy/splitter --tail=60; exit 1
  fi
  kc apply -k "$work/convert" >/dev/null
  if ! kc -n "$n" rollout status deploy/convert --timeout=300s >/dev/null \
      || ! wait_for 300 logged "$n" deploy/convert "Started ConvertStageApplication"; then
    kc -n "$n" get pods -o wide; kc -n "$n" logs deploy/convert --tail=60; exit 1
  fi
  say "  landing (active and standby), split stage and convert stage running"
}

# apply_secret <namespace> <name> <kubectl create secret args>...
apply_secret() {
  local n="$1" name="$2"; shift 2
  kc -n "$n" create secret generic "$name" "$@" --dry-run=client -o yaml | kc apply -f - >/dev/null
}

# fhir_handover <pipeline>: what the FHIR stage will need, ready in its namespace.
fhir_handover() {
  local p="$1"
  apply_secret sdh-fhir "fhir-$p" --from-file=recipient.pem="$STATE/keys/$p-fhir-recipient.pem" \
    --from-literal=RECIPIENT_PASSPHRASE="$(secret "fhir-recipient-passphrase-$p")" \
    --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-fhir-$p")" \
    --from-literal=JDBC_PASSWORD="$(secret "db-fhir-$p")" \
    --from-file=vault-token="$STATE/secrets/vault-token-fhir-$p" \
    --from-file=convert-signer.pem="$STATE/keys/$p-convert-signing-public.pem"
  kc -n sdh-fhir create configmap "fhir-$p" \
    --from-literal=pipeline="$p" \
    --from-literal=records-topic="$p.records" \
    --from-literal=dlq-topic="$p.fhir.dlq" \
    --from-literal=kafka-brokers="$KAFKA_BROKERS" \
    --from-literal=catalog-uri="jdbc:postgresql://postgres.sdh-infra.svc:5432/sdh" \
    --from-literal=jdbc-user="fhir_$(prefix "$p")" \
    --from-literal=records-namespace="$(prefix "$p")" \
    --from-literal=fhir-namespace="$(prefix "$p")_fhir" \
    --from-literal=warehouse="s3://sdh-gold/warehouse" \
    --from-literal=s3-endpoint="http://seaweedfs.sdh-infra.svc:8333" \
    --from-literal=s3-access-key-id="fhir-$p" \
    --from-literal=vault-address="http://vault.sdh-infra.svc:8200" \
    --from-literal=records-kek="$p-records" \
    --from-literal=fhir-kek="$p-fhir" \
    --from-literal=recipient-kid="fhir-$p" \
    --from-literal=convert-signer-kid="convert-$p" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
}

# readme_yaml <readme>: its YAML block, the Deployment customers are given.
readme_yaml() {
  awk '/^```yaml$/ {on=1; next} /^```$/ {on=0} on' "$1"
}

# stage_kustomization <stage> <resources> <patch>
stage_kustomization() {
  cat <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $K8S_NS
resources: [$2]
patches: [{ path: $3 }]
EOF
}

# wait_for <seconds> <command>...: until the command succeeds.
wait_for() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 3
  done
}

# logged <namespace> <pod or deploy/x> <pattern>
logged() {
  local logs
  logs="$(kc -n "$1" logs "$2" 2>/dev/null)"
  grep -qE "$3" <<<"$logs"
}

# landing_active <namespace>: one landing pod holds the lease (is ready), and the other waits as its standby.
landing_active() {
  local ready
  ready="$(kc -n "$1" get pods -l app=landing \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}')"
  [[ "$(grep -c true <<<"$ready")" == "1" && "$(grep -c false <<<"$ready")" == "1" ]]
}

# --- Using it --------------------------------------------------------------------------------------------------

# push <pipeline> <file>...: each file as an item, from the producer pod; prints each receipt.
push() {
  local p="$1"; shift
  local n file
  n="$(ns "$p")"
  for file in "$@"; do
    kc -n sdh-producers exec -i deploy/producer -- curl -sS --fail-with-body --cacert /etc/producer/ca.crt \
      --cert /etc/producer/tls.crt --key /etc/producer/tls.key -H "Content-Type: application/octet-stream" \
      --data-binary @- "https://landing.$n.svc:8443/intakes/$p/items" < "$file"
    echo
  done
}

# audited <namespace> <app> <event pattern>: how many times the app's pods, all of them, have logged an audit
# event since they started.
audited() {
  kc -n "$1" logs -l "app=$2" --tail=-1 --max-log-requests=10 2>/dev/null | grep -cE "(^|[] ])$3( |$)" || true
}

# topic_count <topic>: messages on a topic.
topic_count() {
  infra exec kafka-0 -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" \
    2>/dev/null | awk -F: '{sum += $3} END {print sum + 0}'
}

check() {
  local p="${PIPELINES%% *}" n samples=() file
  n="$(ns "$p")"
  for file in "$ROOT"/connector-core/src/test/resources/ccda/*.xml; do
    samples+=("$file")
  done
  local landed_before split_before records_before split_done converted
  landed_before="$(topic_count "$p.landed")"
  split_before="$(topic_count "$p.split")"
  records_before="$(topic_count "$p.records")"
  split_done="$(audited "$n" splitter "item\.completed")"
  converted="$(audited "$n" convert "stage\.item_completed")"
  say "Pushing ${#samples[@]} synthetic and de-identified C-CDA documents to $p"
  push "$p" "${samples[@]}" | sed 's/^/   /'
  done_by() { [[ "$(audited "$n" "$1" "$2")" -ge $(($3 + ${#samples[@]})) ]]; }
  local failures=0
  if wait_for 300 done_by splitter "item\.completed" "$split_done"; then
    say "  all ${#samples[@]} split (item.completed)"
  else
    say "  FAIL: not all split"; failures=1
  fi
  if wait_for 300 done_by convert "stage\.item_completed" "$converted"; then
    say "  all ${#samples[@]} converted (stage.item_completed)"
  else
    say "  FAIL: not all converted"; failures=1
  fi
  say "  item.landed messages: $(($(topic_count "$p.landed") - landed_before)) more on $p.landed"
  say "  split stage messages: $(($(topic_count "$p.split") - split_before)) more on $p.split"
  say "  convert stage messages: $(($(topic_count "$p.records") - records_before)) more on $p.records"
  local pod
  for pod in $(kc -n "$n" get pods -o name); do
    if kc -n "$n" logs "$pod" --all-containers 2>/dev/null | grep -qE "ClinicalDocument|Newman|\.xml"; then
      say "  FAIL: $pod's log holds document content or a filename"; failures=1
    fi
  done
  return "$failures"
}

status() {
  kc get nodes -o wide
  echo
  kc top nodes 2>/dev/null || true
  echo
  kc get pods -A -o wide -l 'app' --field-selector=metadata.namespace!=kube-system 2>/dev/null \
    | grep -E "^(NAMESPACE|sdh-)" || true
  local p
  for p in $PIPELINES; do
    echo
    echo "$p: $(topic_count "$p.landed") on $p.landed, $(topic_count "$p.split") on $p.split," \
      "$(topic_count "$p.records") on $p.records, $(topic_count "$p.convert.dlq") on $p.convert.dlq," \
      "$(topic_count "$p.fhir.dlq") on $p.fhir.dlq"
  done
}

case "${1:-}" in
  up)
    start_cluster
    start_services
    if [[ -s "$STATE/images.env" ]]; then
      say "Ready. Deployed pipelines restart by themselves; scripts/sandbox.sh deploy deploys changes"
    else
      say "Ready. Next: scripts/sandbox.sh images, then deploy"
    fi
    ;;
  images) build_images ;;
  deploy)
    start_cluster
    start_services
    deploy_pipelines
    ;;
  push)
    [[ $# -ge 3 ]] || { echo "usage: $0 push <pipeline> <file>..." >&2; exit 2; }
    shift; push "$@"
    ;;
  check) check ;;
  status) status ;;
  grafana)
    say "Grafana at http://localhost:3000, user admin, password in $STATE/secrets/grafana (Ctrl-C to stop)"
    infra port-forward svc/lgtm 3000:3000
    ;;
  stop) minikube stop -p "$PROFILE" ;;
  delete)
    [[ "${2:-}" == "--yes" ]] || { echo "This deletes the cluster and every volume in it: $0 delete --yes" >&2
      exit 2; }
    minikube delete -p "$PROFILE"
    ;;
  *) sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
