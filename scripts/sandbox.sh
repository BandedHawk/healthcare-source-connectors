#!/usr/bin/env bash
# The demo sandbox (docs/SANDBOX.md): a three-node Kubernetes cluster in minikube (KVM2), laid out as a customer
# would run the pipeline, with every VM disk and all of its state on the sandbox disk (SDH_SANDBOX, see
# minikube-home.sh).
#
#   scripts/sandbox.sh up          create or start the cluster, and the services every pipeline uses
#   scripts/sandbox.sh images      build the landing node's and the three stages' customer images (in the sdh-it
#                                  VM, with the cluster stopped meanwhile) and push them to the cluster's registry
#   scripts/sandbox.sh deploy      deploy each pipeline's landing node, split, convert and transform stages
#                                  (after up and images); restarts a stage whose image or settings changed
#   scripts/sandbox.sh push <pipeline> <file>...
#                                  push files to a pipeline's landing node, as its producer
#   scripts/sandbox.sh check       push the sample C-CDA documents and check they are landed, split, converted,
#                                  mapped to FHIR or kept as failures, and sent on
#   scripts/sandbox.sh scale-demo [count]
#                                  push count copies of the samples (300 by default), and follow KEDA scaling the
#                                  transform stage out to share them, and back
#   scripts/sandbox.sh status      pods, nodes, topics, scaling
#   scripts/sandbox.sh harness-up  stop the demo pipelines and deploy the control harness's own pipeline, harness
#                                  (namespace sdh-harness: landing node, split and convert stages, one pod each,
#                                  each with its control API), for scripts/harness.sh sandbox
#   scripts/sandbox.sh harness-down
#                                  delete the harness pipeline and everything it stored, and start the demo
#                                  pipelines again
#   scripts/sandbox.sh grafana     port-forward Grafana to http://localhost:3000 (until interrupted)
#   scripts/sandbox.sh stop        stop the cluster's VMs; `up` starts them again, with everything kept
#   scripts/sandbox.sh delete --yes
#                                  delete the cluster and its volumes (the state directory is kept)
#
# Environment (defaults): SDH_DEMO_PROFILE=sdh-demo SDH_DEMO_NODES=3 SDH_DEMO_CPUS=6 SDH_DEMO_CP_MEMORY=7680
#   SDH_DEMO_MEMORY=6912 (MiB, the control-plane node and each other node; docs/SANDBOX.md "Memory")
#   SDH_DEMO_DISK=120g SDH_PIPELINES=ccda-push (each pushed C-CDA; one namespace sdh-<pipeline> each)
#   SDH_TRANSFORMER=../healthcare-data-transformer (the transform stage's project, beside this one)
#
# Kept in $SDH_SANDBOX/sdh-demo/state (mode 700), made once and reused, since data encrypted or stored under them
# can't be read without them: the database and S3 credentials, Vault's unseal key and root token, the stages'
# Vault tokens, the stages' inbox key pairs and the FHIR stage's, gold's consumer key pair, the sandbox CA, and
# the WSTL set last built. Never delete it while the cluster's volumes hold data.
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
# The control harness's pipeline (harness-up): disposable, since its scenarios erase it.
HARNESS="harness"
DEPLOY="$ROOT/deploy/sandbox"
TRANSFORMER="${SDH_TRANSFORMER:-$ROOT/../healthcare-data-transformer}"
STATE="$SDH_SANDBOX/sdh-demo/state"
export KUBECONFIG="$SDH_KUBE_DIR/$PROFILE"
KAFKA_BROKERS="kafka-0.kafka.sdh-infra.svc:9092,kafka-1.kafka.sdh-infra.svc:9092,kafka-2.kafka.sdh-infra.svc:9092"
PRODUCER_SUBJECT="CN=producer-1,O=SDH Sandbox"
REGISTRY="localhost:5000"
# KEDA, which scales the stages (docs/STAGES.md §10). Its release publishes no checksum, so the
# manifest's was taken when it was pinned, and every download is checked against it.
KEDA_VERSION="2.21.0"
KEDA_SHA256="b43c89ffeef81722d7e2dd2c079d74789767a0f89cae1336cff784994814f6d7"
# Partitions of each stage's input topic: the most pods KEDA could give a stage.
STAGE_PARTITIONS=6
# Memory (MiB; docs/SANDBOX.md "Memory"): each VM's size; what the kubelet keeps from pods on each node, measured
# 2026-11-07 (the VM's own use, about 1.5 GiB with its in-memory root filesystem, plus on the control-plane node
# Kubernetes' own pods, which reserve nothing); and the memory left free below which the kubelet evicts pods.
CP_MEMORY="${SDH_DEMO_CP_MEMORY:-7680}"
NODE_MEMORY="${SDH_DEMO_MEMORY:-6912}"
CP_RESERVED=2304
NODE_RESERVED=1664
EVICTION_MIB=256

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
# all_pipelines: the demo pipelines and the harness's, once each.
all_pipelines() { printf '%s\n' $PIPELINES "$HARNESS" | awk '!seen[$0]++'; }
# pipeline_database <pipeline>: its database. The harness pipeline has one of its own, which its restore scenario
# backs up and restores whole (HIPAA.md §11.4, L3); the demo pipelines share sdh.
pipeline_database() { if [[ "$1" == "$HARNESS" ]]; then echo "sdh_$(prefix "$1")"; else echo sdh; fi; }
# pipeline_bucket <pipeline> <layer>: its bucket for a medallion layer. The harness pipeline has its own, which
# keep object versions, as a restore needs (the harness turns versioning on); the demo pipelines share theirs,
# without versions, so that what their maintenance deletes is gone.
pipeline_bucket() { if [[ "$1" == "$HARNESS" ]]; then echo "sdh-$1-$2"; else echo "sdh-$2"; fi; }

# --- Cluster ---------------------------------------------------------------------------------------------------

start_cluster() {
  local state
  state="$(minikube status -p "$PROFILE" --format '{{.Host}}' 2>/dev/null | head -n 1 || true)"
  [[ "$state" == "Running" ]] || boot_cluster
  # Each harmless to repeat, and needed after any start (minikube's or the script's).
  reserve_node_memory
  restore_registry
  keep_control_plane
  kc apply -f "$DEPLOY/cluster/local-path-storage.yaml" >/dev/null
  kc -n local-path-storage rollout status deploy/local-path-provisioner --timeout=300s >/dev/null
}

# boot_cluster: starts the cluster's VMs, sized and checked against the host's memory, or makes the cluster.
boot_cluster() {
  mkdir -p "$(dirname "$KUBECONFIG")"
  if minikube profile list -o json 2>/dev/null | python3 -c '
import json, sys
names = [p["Name"] for p in json.load(sys.stdin).get("valid", [])]
sys.exit(0 if sys.argv[1] in names else 1)' "$PROFILE"; then
    size_vms
    host_guard_cluster
    say "Starting minikube profile '$PROFILE'"
    minikube start -p "$PROFILE"
  else
    local n
    n=${SDH_DEMO_NODES:-3}
    host_guard_cluster "$n"
    say "Creating minikube profile '$PROFILE': $n nodes × ${SDH_DEMO_CPUS:-6} CPUs, ${CP_MEMORY} MiB for the" \
      "control-plane node and ${NODE_MEMORY} MiB for each other, ${SDH_DEMO_DISK:-120g} disk, Calico, under" \
      "$MINIKUBE_HOME"
    minikube start -p "$PROFILE" --driver=kvm2 --container-runtime=docker --kubernetes-version=v1.37.0 \
      --nodes="$n" --cpus="${SDH_DEMO_CPUS:-6}" --memory="${NODE_MEMORY}m" \
      --disk-size="${SDH_DEMO_DISK:-120g}" --cni=calico --addons=registry,metrics-server
    # Node-local volumes (local-path) instead of minikube's single-node hostpath provisioner.
    minikube -p "$PROFILE" addons disable storage-provisioner
    minikube -p "$PROFILE" addons disable default-storageclass
    # minikube gives every node the same memory: the control-plane node is given its own while stopped.
    if ((CP_MEMORY != NODE_MEMORY)); then
      minikube stop -p "$PROFILE" >/dev/null
      size_vms
      minikube start -p "$PROFILE"
    fi
  fi
}

# demo_vms: the cluster's VMs (libvirt domains), the control-plane node first.
demo_vms() {
  virsh -c qemu:///system list --all --name 2>/dev/null | grep -E "^$PROFILE(-m[0-9]+)?\$" | sort
}

# vm_target <domain>: the memory it should have, in MiB.
vm_target() {
  if [[ "$1" == "$PROFILE" ]]; then echo "$CP_MEMORY"; else echo "$NODE_MEMORY"; fi
}

# size_vms: gives each stopped VM its memory (docs/SANDBOX.md "Memory"). Everything on the VMs' disks is kept; a
# running VM is changed only once stopped.
size_vms() {
  local vm target
  for vm in $(demo_vms); do
    target=$(vm_target "$vm")
    [[ "$(sdh_vm_mib "$vm")" == "$target" ]] && continue
    if [[ "$(virsh -c qemu:///system domstate "$vm" 2>/dev/null)" != "shut off" ]]; then
      say "  $vm has $(sdh_vm_mib "$vm") MiB rather than $target; stop the cluster and run up to change it"
      continue
    fi
    say "  Giving $vm $target MiB (it had $(sdh_vm_mib "$vm"))"
    virsh -c qemu:///system setmaxmem "$vm" "${target}M" --config >/dev/null
    virsh -c qemu:///system setmem "$vm" "${target}M" --config >/dev/null
  done
}

# host_guard_cluster [nodes]: refuses to start the cluster's stopped VMs, or the given number of new ones, if the
# host couldn't spare their memory (minikube-home.sh).
host_guard_cluster() {
  local vms=() vm i
  if [[ -n "${1:-}" ]]; then
    vms+=("$CP_MEMORY:$PROFILE")
    for ((i = 2; i <= $1; i++)); do vms+=("$NODE_MEMORY:$PROFILE-m0$i"); done
  else
    for vm in $(demo_vms); do
      [[ "$(virsh -c qemu:///system domstate "$vm" 2>/dev/null)" == "running" ]] && continue
      vms+=("$(vm_target "$vm"):$vm")
    done
  fi
  ((${#vms[@]} == 0)) || sdh_host_guard "${vms[@]}" || exit 1
}

# reserve_node_memory: tells each node's kubelet what the VM itself uses, so pods are only given what is really
# there, and to evict pods before the node runs out (minikube sets neither). Without it, every node offered pods
# its whole memory, and under load the control-plane node ran out (docs/SANDBOX.md "Checked"). minikube writes
# the kubelet's configuration at every start, so this runs at every start.
reserve_node_memory() {
  local node reserved changed
  for node in $(kc get nodes -o name | cut -d/ -f2); do
    if [[ "$node" == "$PROFILE" ]]; then reserved=$CP_RESERVED; else reserved=$NODE_RESERVED; fi
    changed="$(minikube -p "$PROFILE" ssh --native-ssh=false -n "$node" -- \
      "sudo sh -s ${reserved}Mi ${EVICTION_MIB}Mi" < <(kubelet_memory_script) | tr -d '\r')"
    [[ "$changed" == *changed* ]] \
      && say "  $node: ${reserved} MiB kept from pods, which are evicted below ${EVICTION_MIB} MiB free"
  done
  wait_for 120 kc wait --for=condition=Ready nodes --all --timeout=10s || { kc get nodes; exit 1; }
}

# kubelet_memory_script: run on a node as root with <reserved> <eviction>: sets systemReserved memory and
# evictionHard memory.available in the kubelet's configuration, keeping minikube's other thresholds, and restarts
# the kubelet if that changed anything (its pods keep running). Prints "changed" or "same".
kubelet_memory_script() {
  cat <<'SCRIPT'
config=/var/lib/kubelet/config.yaml
awk -v r="$1" -v e="$2" '
  /^systemReserved:/ { skip = 1; next }
  /^evictionHard:/ {
    print "evictionHard:"; print "  memory.available: \"" e "\""; hard = 1; skip = 0
    rest = $0; sub(/^evictionHard:[ ]*/, "", rest)
    if (rest != "" && rest != "{}") print "  " rest
    next
  }
  skip && /^[ ]/ { next }
  hard && /^  memory\.available:/ { next }
  { skip = 0; if (!/^[ ]/) hard = 0; print }
  END { print "systemReserved:"; print "  memory: " r }
' "$config" > /tmp/kubelet-config.yaml || exit 1
grep -q '^evictionHard:' /tmp/kubelet-config.yaml \
  || printf 'evictionHard:\n  memory.available: "%s"\n' "$2" >> /tmp/kubelet-config.yaml
if cmp -s /tmp/kubelet-config.yaml "$config"; then
  echo same
else
  cp /tmp/kubelet-config.yaml "$config" && systemctl restart kubelet && echo changed
fi
SCRIPT
}

# restore_registry: the cluster's registry keeps images only in its container, so a restart empties it, and a pod
# then can't start on a node that hasn't the image yet. Pushed again from the first node's Docker, which keeps
# them on its disk; each must come back under the digest it is deployed by.
restore_registry() {
  [[ -s "$STATE/images.env" ]] || return 0
  local line ref repo digest tag
  # Read on its own descriptor: minikube ssh reads stdin.
  while IFS= read -r line <&3; do
    ref="${line#*=}"; repo="${ref%@*}"; digest="${ref#*@}"
    minikube -p "$PROFILE" ssh -- "curl -sf -o /dev/null \
      -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
      http://localhost:5000/v2/${repo#*/}/manifests/$digest" 2>/dev/null && continue
    tag="$(minikube -p "$PROFILE" ssh -- "docker images --digests --format '{{.Repository}}:{{.Tag}} {{.Digest}}' \
      | awk -v d=$digest -v r=$repo '\$2 == d && index(\$1, r \":\") == 1 && \$1 !~ /<none>/ {print \$1; exit}'" \
      | tr -d '\r')"
    if [[ -z "$tag" ]]; then
      say "  $repo@$digest is in neither the registry nor the first node's Docker: run images again"
      continue
    fi
    minikube -p "$PROFILE" ssh -- "docker push -q $tag >/dev/null" \
      && minikube -p "$PROFILE" ssh -- "curl -sf -o /dev/null \
        -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
        http://localhost:5000/v2/${repo#*/}/manifests/$digest" \
      && say "  Pushed $tag to the registry again" \
      || say "  $tag pushed again under another digest than $digest: run images again"
  done 3< "$STATE/images.env"
}

# keep_control_plane: keeps the control-plane node for the services whose volumes are there (Kafka's first
# broker, Postgres, SeaweedFS, Vault, LGTM: they tolerate the taint) and Kubernetes' own pods, and moves
# anything else running there to another node. A pod that had found room there would otherwise hold memory a
# service needs when it restarts. Not on a single-node cluster.
keep_control_plane() {
  (($(kc get nodes --no-headers | wc -l) > 1)) || return 0
  kc taint nodes "$PROFILE" node-role.kubernetes.io/control-plane=:NoSchedule --overwrite >/dev/null
  local n p namespaces="sdh-producers"
  for p in $PIPELINES; do namespaces+=" $(ns "$p")"; done
  for n in $namespaces; do
    kc get ns "$n" >/dev/null 2>&1 || continue
    kc -n "$n" delete pods --field-selector "spec.nodeName=$PROFILE" --wait=false >/dev/null
  done
  infra delete pods -l app=otel-collector --field-selector "spec.nodeName=$PROFILE" --wait=false \
    >/dev/null 2>&1 || true
}

# --- Shared services -------------------------------------------------------------------------------------------

# s3_config: every stage's access key, and the buckets each may use (the medallion layers).
s3_config() {
  local identities=() p
  identities+=("$(s3_identity sandbox-admin "$(secret s3-sandbox-admin)" Admin Read Write List Tagging)")
  # The harness pipeline's too, always, so that starting and ending it doesn't restart SeaweedFS.
  local b s
  for p in $(all_pipelines); do
    b="$(pipeline_bucket "$p" bronze)"; s="$(pipeline_bucket "$p" silver)"
    identities+=("$(s3_identity "landing-$p" "$(secret "s3-landing-$p")" \
      "Read:$b" "Write:$b" "List:$b" "Tagging:$b")")
    identities+=("$(s3_identity "splitter-$p" "$(secret "s3-splitter-$p")" "Read:$b" "List:$b" \
      "Read:$s" "Write:$s" "List:$s" "Tagging:$s")")
    identities+=("$(s3_identity "convert-$p" "$(secret "s3-convert-$p")" \
      "Read:$s" "Write:$s" "List:$s" "Tagging:$s")")
    identities+=("$(s3_identity "fhir-$p" "$(secret "s3-fhir-$p")" "Read:$s" "List:$s" \
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
  # A changed Vault StatefulSet replaces its pod, which starts sealed: unsealed only once the new pod is there.
  wait_for 300 vault_current || { infra get pods; exit 1; }
  setup_vault
  setup_buckets
  setup_topics
  setup_database
  setup_keda
}

# vault_current: vault-0 runs from its StatefulSet's latest revision.
vault_current() {
  [[ "$(infra get pod vault-0 -o jsonpath='{.metadata.labels.controller-revision-hash}')" \
      == "$(infra get statefulset vault -o jsonpath='{.status.updateRevision}')" \
    && "$(infra get pod vault-0 -o jsonpath='{.status.phase}')" == Running ]]
}

# setup_keda: the KEDA operator, from its pinned release manifest, kept under $SDH_SANDBOX once downloaded.
setup_keda() {
  local cache="$SDH_SANDBOX/sdh-demo/downloads" manifest
  manifest="$cache/keda-$KEDA_VERSION.yaml"
  mkdir -p "$cache"
  if [[ ! -s "$manifest" ]]; then
    say "Downloading KEDA $KEDA_VERSION"
    curl -sSfL --max-time 120 -o "$manifest.part" \
      "https://github.com/kedacore/keda/releases/download/v$KEDA_VERSION/keda-$KEDA_VERSION.yaml"
    mv "$manifest.part" "$manifest"
  fi
  if [[ "$(sha256sum "$manifest" | cut -d' ' -f1)" != "$KEDA_SHA256" ]]; then
    rm -f "$manifest"
    echo "KEDA $KEDA_VERSION's manifest doesn't match its pinned checksum" >&2
    exit 1
  fi
  say "KEDA $KEDA_VERSION (namespace keda)"
  # Its three pods ask for 100 MiB and may grow to 1000 MiB each, which the nodes are not sized for: held to
  # 128 MiB each (they use under 50), and run on the control-plane node, so the other two keep room for the
  # stages to scale (docs/SANDBOX.md "Memory").
  local work
  work="$(mktemp -d)"
  cp "$manifest" "$work/keda.yaml"
  printf '%s\n' 'apiVersion: kustomize.config.k8s.io/v1beta1' 'kind: Kustomization' 'resources: [keda.yaml]' \
    'patches:' "  - { target: { kind: Deployment, namespace: keda }, patch: '[{\"op\": \"replace\"," \
    "      \"path\": \"/spec/template/spec/containers/0/resources\", \"value\": {\"requests\":" \
    "      {\"cpu\": \"100m\", \"memory\": \"128Mi\"}, \"limits\": {\"memory\": \"128Mi\"}}}," \
    "      {\"op\": \"add\", \"path\": \"/spec/template/spec/tolerations\", \"value\":" \
    "      [{\"key\": \"node-role.kubernetes.io/control-plane\", \"operator\": \"Exists\"," \
    "      \"effect\": \"NoSchedule\"}]}, {\"op\": \"add\", \"path\": \"/spec/template/spec/affinity\"," \
    "      \"value\": {\"nodeAffinity\": {\"requiredDuringSchedulingIgnoredDuringExecution\":" \
    "      {\"nodeSelectorTerms\": [{\"matchExpressions\": [{\"key\":" \
    "      \"node-role.kubernetes.io/control-plane\", \"operator\": \"Exists\"}]}]}}}}]' }" \
    > "$work/kustomization.yaml"
  # Server-side: its CRDs are too large for the annotation a client-side apply keeps.
  kc kustomize "$work" | kc apply --server-side --force-conflicts -f - >/dev/null
  rm -rf "$work"
  local workload
  for workload in keda-operator keda-metrics-apiserver keda-admission; do
    kc -n keda rollout status "deploy/$workload" --timeout=300s >/dev/null || { kc -n keda get pods; exit 1; }
  done
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
  # The harness pipeline's keys and tokens too, made once, so that harness-up has them (its scenarios rotate
  # its stages to the second KEKs).
  for p in $(all_pipelines); do
    # The KEKs and signing keys never leave Vault, and can't be deleted (deletion_allowed is false).
    vault_sh "
      for k in $p-landed-items $p-records $p-fhir $(harness_keks "$p"); do
        vault read transit/keys/\$k >/dev/null 2>&1 || vault write -f transit/keys/\$k type=aes256-gcm96
      done
      for k in $p-landing-signing $p-splitter-signing $p-convert-signing $p-transform-signing; do
        vault read transit/keys/\$k >/dev/null 2>&1 || vault write -f transit/keys/\$k type=ecdsa-p256
      done" >/dev/null
    # The harness pipeline's stages may use a second KEK each too, which its keys scenario rotates them to.
    local landed="$p-landed-items" records="$p-records"
    [[ "$p" != "$HARNESS" ]] || { landed+=" $p-landed-items-2"; records+=" $p-records-2"; }
    vault_policy "landing-$p" "$(kek_paths $landed)
      path \"transit/sign/$p-landing-signing/*\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-landing-signing\" { capabilities = [\"read\"] }"
    vault_policy "splitter-$p" "$(kek_paths $landed $records)
      path \"transit/sign/$p-splitter-signing/*\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-splitter-signing\" { capabilities = [\"read\"] }"
    vault_policy "convert-$p" "$(kek_paths $records)
      path \"transit/sign/$p-convert-signing/*\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-convert-signing\" { capabilities = [\"read\"] }"
    # Encrypt as well as decrypt with the records KEK: a stage checks each KEK at startup with a round trip.
    vault_policy "fhir-$p" "
      path \"transit/encrypt/$p-records\" { capabilities = [\"update\"] }
      path \"transit/decrypt/$p-records\" { capabilities = [\"update\"] }
      path \"transit/encrypt/$p-fhir\" { capabilities = [\"update\"] }
      path \"transit/decrypt/$p-fhir\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-convert-signing\" { capabilities = [\"read\"] }
      path \"transit/sign/$p-transform-signing/*\" { capabilities = [\"update\"] }
      path \"transit/keys/$p-transform-signing\" { capabilities = [\"read\"] }"
    local role
    for role in landing splitter convert fhir; do
      vault_token "$role-$p"
    done
    vault_public_key "$p-landing-signing" > "$STATE/keys/$p-landing-signing-public.pem"
    vault_public_key "$p-splitter-signing" > "$STATE/keys/$p-splitter-signing-public.pem"
    vault_public_key "$p-convert-signing" > "$STATE/keys/$p-convert-signing-public.pem"
  done
}

# harness_keks <pipeline>: the harness pipeline's second KEKs, which its keys scenario rotates its stages to;
# none for any other pipeline.
harness_keks() {
  [[ "$1" != "$HARNESS" ]] || echo "$1-landed-items-2 $1-records-2"
}

# kek_paths <kek>...: a policy's lines letting a stage wrap and unwrap with each KEK.
kek_paths() {
  local k
  for k in "$@"; do
    printf '      path "transit/encrypt/%s" { capabilities = ["update"] }\n' "$k"
    printf '      path "transit/decrypt/%s" { capabilities = ["update"] }\n' "$k"
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
  local bucket
  for bucket in sdh-bronze sdh-silver sdh-gold "$(pipeline_bucket "$HARNESS" bronze)" \
      "$(pipeline_bucket "$HARNESS" silver)"; do
    make_bucket "$bucket"
  done
}

# make_bucket <bucket>: made, or there already.
make_bucket() {
  local out deadline=$((SECONDS + 300))
  # Until it is made, or is there already: the S3 gateway takes a while to find its master after a restart.
  until out="$(infra exec statefulset/seaweedfs -- sh -c \
      "echo 's3.bucket.create -name $1' | weed shell -master=localhost:9333" 2>&1)" \
      && ! grep -q error <<<"$out" || grep -q "already exists" <<<"$out"; do
    ((SECONDS < deadline)) || { echo "Could not create bucket $1: $out" >&2; exit 1; }
    sleep 5
  done
}

setup_topics() {
  local p topic
  for p in $PIPELINES; do
    # The stages' inputs have a partition for each pod KEDA may run: messages are keyed by item or record, so
    # each item's and each record's messages stay in order on their partition, and nothing needs more. The rest,
    # gold's output and the dead-letter topics, have one.
    for topic in "$p.landed" "$p.split" "$p.records" "$p.fhir" "$p.convert.dlq" "$p.fhir.dlq"; do
      local partitions=1
      [[ "$topic" == "$p.landed" || "$topic" == "$p.split" || "$topic" == "$p.records" ]] \
        && partitions="$STAGE_PARTITIONS"
      infra exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
        --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor 3 \
        --config min.insync.replicas=2 >/dev/null
      # A topic made with fewer, before the stages scaled. Partitions are only ever added; a key's messages
      # already queued stay on their old partition, so do this with the topic drained.
      local have
      have="$(infra exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --describe --topic "$topic" | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -n 1)"
      if [[ -n "$have" && "$have" -lt "$partitions" ]]; then
        say "  $topic: $have partitions to $partitions"
        infra exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter \
          --topic "$topic" --partitions "$partitions" >/dev/null
      fi
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
  for p in $(all_pipelines); do
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
  for p in $(all_pipelines); do
    n="$(ns "$p")"
    issue "$dir/landing-$p" "/O=SDH Sandbox/CN=landing.$n.svc" \
      "extendedKeyUsage=serverAuth
subjectAltName=DNS:landing.$n.svc,DNS:landing.$n.svc.cluster.local,DNS:landing.$n"
  done
  # The harness's control APIs, one certificate for the three: by their Services' names, and by localhost, as
  # the harness reaches them through port-forwards.
  n="$(ns "$HARNESS")"
  issue "$dir/control-$HARNESS" "/O=SDH Sandbox/CN=sdh-control.$n.svc" \
    "extendedKeyUsage=serverAuth
subjectAltName=DNS:sdh-control-landing.$n.svc,DNS:sdh-control-split.$n.svc,DNS:sdh-control-convert.$n.svc,\
DNS:localhost,IP:127.0.0.1"
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
  # The sdh-it VM and the cluster don't both fit in the host's memory (docs/SANDBOX.md "Memory"): the images are
  # built with the cluster stopped, saved to the sandbox disk, and loaded once sdh-it is stopped again.
  local state archive="$SDH_SANDBOX/sdh-demo/images.tar"
  state="$(minikube status -p "$PROFILE" --format '{{.Host}}' 2>/dev/null | head -n 1 || true)"
  if [[ "$state" == "Running" ]]; then
    say "Stopping the cluster while sdh-it builds (everything on it is kept)"
    minikube stop -p "$PROFILE" >/dev/null
  fi
  [[ -f "$TRANSFORMER/transform-stage/pom.xml" ]] || { echo "No transform stage at $TRANSFORMER: set" \
    "SDH_TRANSFORMER to the healthcare-data-transformer folder" >&2; exit 1; }
  local transform_version
  transform_version="$(cd "$TRANSFORMER" && ./mvnw -q -N help:evaluate -Dexpression=project.version \
    -DforceStdout)"
  say "Building the customer images ($version, the transform stage $transform_version) in the sdh-it VM"
  # Installed, not only packaged: the transformer is built on this repository's libraries and build kit from the
  # local Maven repository, so it is built on them as they are here. Its set is mapping's, from the same
  # repository (mapping installed); its build copies it to target/test-sets.
  (cd "$ROOT" && ./mvnw -q install -DskipTests)
  (cd "$TRANSFORMER" && ./mvnw -q package -DskipTests)
  cp "$TRANSFORMER/transform-stage/target/test-sets/us-core.tar.gz" "$STATE/wstl-us-core.tar.gz.tmp"
  mv "$STATE/wstl-us-core.tar.gz.tmp" "$STATE/wstl-us-core.tar.gz"
  # module=image: the split stage kept its module's name, file-connector. The transform stage is a folder of
  # another project, which build-image.sh takes as it is.
  local apps=(landing-connector=landing-connector file-connector=split-stage convert-stage=convert-stage) app
  for app in "${apps[@]}"; do
    SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 SDH_IMAGE="sdh/${app#*=}:$version" "$ROOT/scripts/build-image.sh" \
      "${app%%=*}" customer
  done
  SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 SDH_IMAGE="sdh/transform-stage:$transform_version" \
    "$ROOT/scripts/build-image.sh" "$TRANSFORMER/transform-stage" customer
  (source "$ROOT/scripts/minikube-env.sh" >/dev/null
   docker save "sdh/landing-connector:$version" "sdh/split-stage:$version" "sdh/convert-stage:$version" \
     "sdh/transform-stage:$transform_version") > "$archive.part"
  mv "$archive.part" "$archive"
  say "Stopping the sdh-it VM"
  MINIKUBE_HOME="$MINIKUBE_HOME" minikube stop -p "${SDH_IT_PROFILE:-sdh-it}" >/dev/null
  start_cluster
  say "Copying them to the cluster's registry ($REGISTRY on every node)"
  # Loaded into the first node's Docker and pushed from there: the registry addon answers on localhost:5000 on
  # every node, and the VMs' networks can't reach each other.
  minikube -p "$PROFILE" ssh --native-ssh=false -- docker load < "$archive" >/dev/null
  rm -f "$archive"
  : > "$STATE/images.env.tmp"
  for app in "${apps[@]}" transform-stage="transform-stage:$transform_version"; do
    app="${app#*=}"
    [[ "$app" == *:* ]] || app="$app:$version"
    minikube -p "$PROFILE" ssh -- "docker tag sdh/$app $REGISTRY/sdh/$app \
      && docker push -q $REGISTRY/sdh/$app >/dev/null"
    # Deployed by digest, so a rebuild under the same SNAPSHOT version is a new image to Kubernetes.
    local digest
    digest="$(minikube -p "$PROFILE" ssh -- docker inspect --format "'{{index .RepoDigests 0}}'" \
      "$REGISTRY/sdh/$app" | tr -d '\r')"
    app="${app%%:*}"
    echo "${app//-/_}=$digest" >> "$STATE/images.env.tmp"
    say "  $digest"
  done
  mv "$STATE/images.env.tmp" "$STATE/images.env"
}

# --- Pipelines -------------------------------------------------------------------------------------------------

deploy_pipelines() {
  need_images
  deploy_producer
  kc apply -f "$DEPLOY/fhir/namespace.yaml" >/dev/null
  local p
  for p in $PIPELINES; do
    deploy_pipeline "$p"
  done
  kc -n sdh-producers rollout status deploy/producer --timeout=300s >/dev/null
}

# need_images: the images' digests, from the last `images`, and the WSTL set.
need_images() {
  [[ -s "$STATE/images.env" ]] || { echo "No images yet: run scripts/sandbox.sh images" >&2; exit 1; }
  # shellcheck source=/dev/null
  source "$STATE/images.env"
  [[ -n "${split_stage:-}" && -n "${convert_stage:-}" && -n "${transform_stage:-}" ]] || { echo "No split," \
    "convert and transform stage images (made before the stages): run scripts/sandbox.sh images" >&2; exit 1; }
  [[ -s "$STATE/wstl-us-core.tar.gz" ]] || { echo "No WSTL set: run scripts/sandbox.sh images" >&2; exit 1; }
}

# deploy_producer: the certificates, and the producer that pushes to the landing nodes.
deploy_producer() {
  pki
  kc apply -f "$DEPLOY/producers/producer.yaml" >/dev/null
  kc -n sdh-producers create secret generic producer --from-file=ca.crt="$STATE/pki/ca.crt" \
    --from-file=tls.crt="$STATE/pki/producer.crt" --from-file=tls.key="$STATE/pki/producer.key" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
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
    SPLIT_FORMAT="ccda" SDH_DATABASE BRONZE_BUCKET SILVER_BUCKET
  SDH_DATABASE="$(pipeline_database "$p")"
  BRONZE_BUCKET="$(pipeline_bucket "$p" bronze)"; SILVER_BUCKET="$(pipeline_bucket "$p" silver)"
  envsubst '$PIPELINE $K8S_NS' < "$DEPLOY/pipeline/namespace.yaml" | kc apply -f - >/dev/null

  # Each stage's inbox key pair, which the stage before it seals its messages to, and the FHIR stage's, which the
  # convert stage seals records to.
  ec_key "$STATE/keys/$p-inbox" "$(secret "inbox-passphrase-$p")"
  ec_key "$STATE/keys/$p-convert-inbox" "$(secret "convert-inbox-passphrase-$p")"
  ec_key "$STATE/keys/$p-fhir-recipient" "$(secret "fhir-recipient-passphrase-$p")"
  # Gold's readers' key pair (STAGES.md §17.10.10, point 4): the transform stage seals to its public half. No
  # reader is deployed; one added later is given the private half, which stays here.
  ec_key "$STATE/keys/$p-consumer" "$(secret "consumer-passphrase-$p")"
  export INBOX_SHA256 CONVERT_INBOX_SHA256 RECIPIENT_SHA256 CONSUMER_SHA256
  INBOX_SHA256="$(fingerprint "$STATE/keys/$p-inbox-public.pem")"
  CONVERT_INBOX_SHA256="$(fingerprint "$STATE/keys/$p-convert-inbox-public.pem")"
  RECIPIENT_SHA256="$(fingerprint "$STATE/keys/$p-fhir-recipient-public.pem")"
  CONSUMER_SHA256="$(fingerprint "$STATE/keys/$p-consumer-public.pem")"
  local vars='$PIPELINE $ICEBERG_PREFIX $PRODUCER_SUBJECT $INBOX_SHA256 $CONVERT_INBOX_SHA256 $RECIPIENT_SHA256'
  vars+=' $CONSUMER_SHA256 $KAFKA_BROKERS $SPLIT_FORMAT $SDH_DATABASE $BRONZE_BUCKET $SILVER_BUCKET'
  local stage
  for stage in landing splitter convert; do
    envsubst "$vars" < "$DEPLOY/pipeline/$stage-application.yml" > "$work/$stage-application.yml"
  done
  envsubst "$vars" < "$DEPLOY/fhir/transform-application.yml" > "$work/transform-application.yml"
  # The harness pipeline (harness-up) has no transform stage, so no registry for the convert stage to mark items
  # in; and each stage carries out maintenance (re-keys, purges, retention) every 20 seconds, as the harness's
  # first level does, its landing node with its tables in a schema of its own, which harness-down drops; and
  # the split stage stores each record as it is split; and the split and convert stages' reconcilers send again,
  # within minutes, what a restored registry lacks (the defaults wait 10 minutes and look every 15), as the
  # harness's first level has them.
  local landing_extra=() splitter_extra=() convert_extra=()
  if [[ "$p" == "$HARNESS" ]]; then
    sed -i '/next-registry-schema:/d' "$work/convert-application.yml"
    landing_extra=(--from-literal=SDH_MAINTENANCE_INTERVAL=20s
      --from-literal=SDH_LEASE_JDBC_TABLE="landing_$(prefix "$p").sdh_lease")
    splitter_extra=(--from-literal=SDH_MAINTENANCE_INTERVAL=20s --from-literal=SDH_STORAGE_COMMIT_MAX_RECORDS=1
      --from-literal=SDH_LANDING_RECONCILE_GRACE=30s --from-literal=SDH_LANDING_RECONCILE_INTERVAL=1m)
    convert_extra=(--from-literal=SDH_MAINTENANCE_INTERVAL=20s --from-literal=SDH_RECONCILE_GRACE=30s
      --from-literal=SDH_RECONCILE_INTERVAL=1m --from-literal=SDH_RECONCILE_BACKLOGQUIET=0s)
  fi

  apply_secret "$n" landing-keys --from-file=tls.crt="$STATE/pki/landing-$p.crt" \
    --from-file=tls.key="$STATE/pki/landing-$p.key" --from-file=producers-ca.pem="$STATE/pki/ca.crt" \
    --from-file=splitter-inbox.pem="$STATE/keys/$p-inbox-public.pem" \
    --from-file=vault-token="$STATE/secrets/vault-token-landing-$p"
  apply_secret "$n" landing-env --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-landing-$p")" \
    --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-landing-$p")" "${landing_extra[@]}"
  kc -n "$n" create configmap landing-config --from-file=application.yml="$work/landing-application.yml" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  apply_secret "$n" splitter-keys --from-file=inbox.pem="$STATE/keys/$p-inbox.pem" \
    --from-file=inbox-public.pem="$STATE/keys/$p-inbox-public.pem" \
    --from-file=recipient.pem="$STATE/keys/$p-convert-inbox-public.pem" \
    --from-file=landing-signer.pem="$STATE/keys/$p-landing-signing-public.pem" \
    --from-file=vault-token="$STATE/secrets/vault-token-splitter-$p"
  apply_secret "$n" splitter-env --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-splitter-$p")" \
    --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-splitter-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "inbox-passphrase-$p")" "${splitter_extra[@]}"
  kc -n "$n" create configmap splitter-config --from-file=application.yml="$work/splitter-application.yml" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  apply_secret "$n" convert-keys --from-file=inbox.pem="$STATE/keys/$p-convert-inbox.pem" \
    --from-file=inbox-public.pem="$STATE/keys/$p-convert-inbox-public.pem" \
    --from-file=recipient.pem="$STATE/keys/$p-fhir-recipient-public.pem" \
    --from-file=split-signer.pem="$STATE/keys/$p-splitter-signing-public.pem" \
    --from-file=vault-token="$STATE/secrets/vault-token-convert-$p"
  apply_secret "$n" convert-env --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-convert-$p")" \
    --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-convert-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "convert-inbox-passphrase-$p")" \
    "${convert_extra[@]}"
  kc -n "$n" create configmap convert-config --from-file=application.yml="$work/convert-application.yml" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  [[ "$p" == "$HARNESS" ]] || transform_settings "$p" "$work/transform-application.yml"

  # Each stage from the Deployment in its README, renamed for the pipeline's namespace, by digest.
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
          -e "s/transform-stage-\(env\|keys\|config\|wstl\)/transform-$p-\1/g" \
          -e "s/name: transform-stage }/name: transform-$p }/" -e "s/app: transform-stage }/app: transform-$p }/g" \
    > "$work/transform/transform.yaml"
  cp "$DEPLOY/pipeline/landing-patch.yaml" "$DEPLOY/pipeline/landing-service.yaml" "$work/landing/"
  cp "$DEPLOY/pipeline/splitter-patch.yaml" "$work/splitter/"
  cp "$DEPLOY/pipeline/convert-patch.yaml" "$work/convert/"
  envsubst '$PIPELINE' < "$DEPLOY/fhir/transform-patch.yaml" > "$work/transform/transform-patch.yaml"
  # Each with a digest of its settings on its pods, so a deploy that changes only the settings restarts them:
  # a pod reads its settings once, at startup.
  CONFIG_SHA="$(settings_digest "$work/landing-application.yml")" \
    stage_kustomization landing "landing.yaml, landing-service.yaml" landing-patch.yaml \
    > "$work/landing/kustomization.yaml"
  # The harness's stages aren't scaled: one pod each, so a scenario knows which pod it stops.
  local scaling=scaled
  [[ "$p" != "$HARNESS" ]] || scaling=one
  CONFIG_SHA="$(settings_digest "$work/splitter-application.yml")" \
    stage_kustomization splitter "splitter.yaml" splitter-patch.yaml "$scaling" \
    > "$work/splitter/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/convert-application.yml")" \
    stage_kustomization convert "convert.yaml" convert-patch.yaml "$scaling" > "$work/convert/kustomization.yaml"
  CONFIG_SHA="$(settings_digest "$work/transform-application.yml" "$STATE/wstl-us-core.tar.gz")" \
    K8S_NS=sdh-fhir stage_kustomization "transform-$p" "transform.yaml" transform-patch.yaml scaled \
    > "$work/transform/kustomization.yaml"

  # The landing node first: it makes the landing tables, which the split stage may only read. The split stage
  # before the convert stage: it makes split_records; the convert stage before the transform stage: it makes
  # records.
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
  if [[ "$p" == "$HARNESS" ]]; then
    deploy_controls "$p" "$work"
    say "  landing node, split and convert stages (one pod each), and their control APIs, running"
    return
  fi
  kc apply -k "$work/transform" >/dev/null
  if ! kc -n sdh-fhir rollout status "deploy/transform-$p" --timeout=300s >/dev/null \
      || ! wait_for 300 logged sdh-fhir "deploy/transform-$p" "Started TransformStageApplication"; then
    kc -n sdh-fhir get pods -o wide; kc -n sdh-fhir logs "deploy/transform-$p" --tail=60; exit 1
  fi
  envsubst '$PIPELINE $K8S_NS $KAFKA_BROKERS' < "$DEPLOY/pipeline/scaling.yaml" | kc apply -f - >/dev/null
  envsubst '$PIPELINE $KAFKA_BROKERS' < "$DEPLOY/fhir/scaling.yaml" | kc apply -f - >/dev/null
  local scaled
  for scaled in "$n splitter" "$n convert" "sdh-fhir transform-$p"; do
    if ! kc -n "${scaled% *}" wait --for=condition=Ready "scaledobject/${scaled#* }" --timeout=120s >/dev/null; then
      kc -n "${scaled% *}" describe "scaledobject/${scaled#* }" | tail -20; exit 1
    fi
  done
  say "  landing (one pod), split and convert stages (one pod each) and transform stage running; KEDA scales the" \
    "transform stage to 2 pods"
}

# apply_secret <namespace> <name> <kubectl create secret args>...
apply_secret() {
  local n="$1" name="$2"; shift 2
  kc -n "$n" create secret generic "$name" "$@" --dry-run=client -o yaml | kc apply -f - >/dev/null
}

# transform_settings <pipeline> <application.yml>: the transform stage's secrets and ConfigMaps in sdh-fhir, as
# its README names them, transform-<pipeline>-*. They replace the handover the namespace was given before the
# stage was built (fhir-<pipeline>), which nothing reads.
transform_settings() {
  local p="$1" t="transform-$1"
  apply_secret sdh-fhir "$t-keys" --from-file=inbox.pem="$STATE/keys/$p-fhir-recipient.pem" \
    --from-file=inbox-public.pem="$STATE/keys/$p-fhir-recipient-public.pem" \
    --from-file=recipient.pem="$STATE/keys/$p-consumer-public.pem" \
    --from-file=convert-signer.pem="$STATE/keys/$p-convert-signing-public.pem" \
    --from-file=vault-token="$STATE/secrets/vault-token-fhir-$p"
  apply_secret sdh-fhir "$t-env" --from-literal=S3_SECRET_ACCESS_KEY="$(secret "s3-fhir-$p")" \
    --from-literal=SDH_LEASE_JDBC_PASSWORD="$(secret "db-fhir-$p")" \
    --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$(secret "fhir-recipient-passphrase-$p")"
  kc -n sdh-fhir create configmap "$t-config" --from-file=application.yml="$2" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  # The set as binary data: a ConfigMap holds no folders, so the .tar.gz as mapping packs it.
  kc -n sdh-fhir create configmap "$t-wstl" --from-file=set.tar.gz="$STATE/wstl-us-core.tar.gz" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  kc -n sdh-fhir delete secret,configmap "fhir-$p" --ignore-not-found >/dev/null
}

# readme_yaml <readme>: its YAML block, the Deployment customers are given.
readme_yaml() {
  awk '/^```yaml$/ {on=1; next} /^```$/ {on=0} on' "$1"
}

# settings_digest <file>...: a short digest of a stage's settings files, for its pods' annotation.
settings_digest() {
  cat "$@" | sha256sum | cut -c1-16
}

# stage_kustomization <stage> <resources> <patch> [scaled|one]: with "scaled", the Deployment's replicas are left
# to KEDA, so a deploy doesn't reset what it has scaled to; with "one", it has one pod. With $CONFIG_SHA, it is
# put on the pods (and the Deployment) as sdh.sandbox/settings, so a change to it restarts them.
stage_kustomization() {
  local patches="{ path: $3 }" annotations=""
  [[ -z "${CONFIG_SHA:-}" ]] || annotations="commonAnnotations: { sdh.sandbox/settings: \"$CONFIG_SHA\" }"
  if [[ "${4:-}" == "scaled" ]]; then
    local remove='[{"op": "remove", "path": "/spec/replicas"}]'
    patches+=", { target: { kind: Deployment, name: $1 }, patch: '$remove' }"
  elif [[ "${4:-}" == "one" ]]; then
    local one='[{"op": "add", "path": "/spec/replicas", "value": 1}]'
    patches+=", { target: { kind: Deployment, name: $1 }, patch: '$one' }"
  fi
  cat <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $K8S_NS
resources: [$2]
patches: [$patches]
$annotations
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

# landing_active <namespace>: the one landing pod holds the lease (is ready).
landing_active() {
  local ready
  ready="$(kc -n "$1" get pods -l app=landing \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}')"
  [[ "$ready" == "true" ]]
}

# --- The control harness's pipeline ----------------------------------------------------------------------------

# readme_control_yaml <readme>: its control API block, the control Deployment, Service and policy customers are
# given (each stage README's "Control API").
readme_control_yaml() {
  awk '/^```yaml control$/ {on=1; next} /^```$/ {on=0} on' "$1"
}

# deploy_controls <pipeline> <work dir>: each stage's control API, from its README, by digest, with one replica,
# the harness's issuer and its key set, and the control settings.
deploy_controls() {
  local p="$1" work="$2" n stage
  n="$(ns "$p")"
  export K8S_NS="$n"
  envsubst '$K8S_NS' < "$DEPLOY/harness/issuer.yaml" | kc apply -f - >/dev/null
  kc -n "$n" create configmap jwks --from-file=jwks.json="$STATE/keys/$p-issuer-jwks.json" \
    --dry-run=client -o yaml | kc apply -f - >/dev/null
  envsubst '$K8S_NS' < "$DEPLOY/harness/control-application.yml" > "$work/control-application.yml"
  for stage in landing split convert; do
    kc -n "$n" create configmap "sdh-control-$stage" --from-file=application.yml="$work/control-application.yml" \
      --dry-run=client -o yaml | kc apply -f - >/dev/null
    kc -n "$n" create secret tls "sdh-control-$stage-tls" --cert="$STATE/pki/control-$p.crt" \
      --key="$STATE/pki/control-$p.key" --dry-run=client -o yaml | kc apply -f - >/dev/null
  done
  # One replica each, not the README's two (docs/SANDBOX.md "Memory"); no ingress here: the harness reaches
  # them through port-forwards, which no network policy applies to.
  local common=(-e 's/replicas: 2 /replicas: 1 /' -e 's/<ingress-namespace>/sdh-ingress/')
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
  } > "$work/controls.yaml"
  kc -n "$n" apply -f "$work/controls.yaml" >/dev/null
  kc -n "$n" rollout status deploy/jwks --timeout=120s >/dev/null || { kc -n "$n" get pods -o wide; exit 1; }
  for stage in landing split convert; do
    if ! kc -n "$n" rollout status "deploy/sdh-control-$stage" --timeout=300s >/dev/null; then
      kc -n "$n" get pods -o wide; kc -n "$n" logs "deploy/sdh-control-$stage" --tail=60; exit 1
    fi
  done
}

# harness_issuer: the harness's token-signing key (EC P-256, PKCS#8, kept in the state directory, made once) and
# its key set, which the control APIs are given. The harness reads the key from the state directory.
harness_issuer() {
  local key="$STATE/keys/$HARNESS-issuer.pem" jwks="$STATE/keys/$HARNESS-issuer-jwks.json"
  if [[ ! -s "$key" ]]; then
    (umask 077; openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 2>/dev/null \
      | openssl pkcs8 -topk8 -nocrypt -out "$key")
  fi
  openssl pkey -in "$key" -pubout -outform DER | python3 -c '
import base64, json, sys
point = sys.stdin.buffer.read()[-65:]
assert point[0] == 4
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
print(json.dumps({"keys": [{"kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": sys.argv[1],
                            "x": b64(point[1:33]), "y": b64(point[33:])}]}))' "$HARNESS-issuer" > "$jwks"
}

# demo_scaled <pipeline>: the demo pipeline's ScaledObjects, as "<namespace> <name>", those that exist.
demo_scaled() {
  local n so
  n="$(ns "$1")"
  for so in "$n splitter" "$n convert" "sdh-fhir transform-$1"; do
    kc -n "${so% *}" get "scaledobject/${so#* }" >/dev/null 2>&1 && echo "$so"
  done
  return 0
}

# hold_demo: stops the demo pipelines, so the harness's fits in memory (docs/SANDBOX.md "Memory"): the landing
# node scaled to none, and KEDA told to hold each stage at none. Their items wait in Kafka and the registries.
hold_demo() {
  local p so
  for p in $PIPELINES; do
    kc get ns "$(ns "$p")" >/dev/null 2>&1 || continue
    say "Stopping pipeline $p while the harness runs"
    kc -n "$(ns "$p")" scale deploy/landing --replicas=0 >/dev/null 2>&1 || true
    while read -r so; do
      [[ -n "$so" ]] || continue
      kc -n "${so% *}" annotate "scaledobject/${so#* }" autoscaling.keda.sh/paused-replicas=0 --overwrite \
        >/dev/null
    done < <(demo_scaled "$p")
    wait_for 300 demo_stopped "$p" || { echo "Pipeline $p's pods didn't stop" >&2; exit 1; }
  done
}

# demo_stopped <pipeline>: none of its pods left.
demo_stopped() {
  [[ -z "$(kc -n "$(ns "$1")" get pods -l 'app in (landing,splitter,convert)' -o name)" \
    && -z "$(kc -n sdh-fhir get pods -l "app=transform-$1" -o name)" ]]
}

# release_demo: the demo pipelines started again, as they were.
release_demo() {
  local p so
  for p in $PIPELINES; do
    kc get ns "$(ns "$p")" >/dev/null 2>&1 || continue
    while read -r so; do
      [[ -n "$so" ]] || continue
      kc -n "${so% *}" annotate "scaledobject/${so#* }" autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1 || true
    done < <(demo_scaled "$p")
    kc -n "$(ns "$p")" scale deploy/landing --replicas=1 >/dev/null 2>&1 || true
    say "Pipeline $p started again"
  done
}

# harness_sql <database> <sql>: runs it in the database as postgres.
harness_sql() {
  printf '%s\n' "$2" | infra exec -i postgres-0 -- env PGPASSWORD="" psql -q -v ON_ERROR_STOP=1 -U postgres -d "$1"
}

# harness_store: the harness pipeline's database and buckets, made anew (harness_clean removes them): the
# database for its stages' roles, as sdh is for the demo's (setup_database), the buckets empty.
harness_store() {
  local db
  db="$(pipeline_database "$HARNESS")"
  harness_sql postgres "SELECT 'CREATE DATABASE $db' WHERE NOT EXISTS
      (SELECT FROM pg_database WHERE datname = '$db')\\gexec
    GRANT CREATE ON DATABASE $db TO sdh_catalog;" >/dev/null
  harness_sql "$db" "GRANT ALL ON SCHEMA public TO sdh_catalog;" >/dev/null
  make_bucket "$(pipeline_bucket "$HARNESS" bronze)"
  make_bucket "$(pipeline_bucket "$HARNESS" silver)"
}

harness_up() {
  start_cluster
  start_services
  need_images
  if kc get ns "$(ns "$HARNESS")" >/dev/null 2>&1; then
    say "The harness pipeline is there from an earlier run: removing it first"
    harness_clean
  fi
  hold_demo
  say "Harness pipeline $HARNESS"
  PIPELINES="$HARNESS" setup_topics
  harness_store
  # The landing node's lease, control and audit tables, in a schema of its own (the others are in the stages'
  # registries).
  harness_sql "$(pipeline_database "$HARNESS")" \
    "CREATE SCHEMA IF NOT EXISTS landing_$(prefix "$HARNESS") AUTHORIZATION sdh_catalog;" >/dev/null
  harness_issuer
  deploy_producer
  deploy_pipeline "$HARNESS"
  kc -n sdh-producers rollout status deploy/producer --timeout=300s >/dev/null
}

# harness_clean: deletes the harness pipeline and everything it stored: its namespace, its database, its topics
# and groups, and its buckets, every object version in them. Its Vault keys are kept (they can't be deleted;
# harness-up uses them again). Synthetic data only. What it stored in sdh and the shared buckets before it had a
# database and buckets of its own (2026-11-24) is removed too.
harness_clean() {
  local n x
  n="$(ns "$HARNESS")"; x="$(prefix "$HARNESS")"
  kc delete ns "$n" --ignore-not-found --wait=true --timeout=300s >/dev/null
  harness_sql postgres "DROP DATABASE IF EXISTS $(pipeline_database "$HARNESS") WITH (FORCE);" >/dev/null
  harness_sql sdh "SET client_min_messages TO warning;
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
  infra exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists \
    --topic "$HARNESS\\..*" >/dev/null
  local group
  for group in "splitter-$HARNESS" "convert-$HARNESS"; do
    infra exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete \
      --group "$group" >/dev/null 2>&1 || true
  done
  infra exec statefulset/seaweedfs -- sh -c "printf '%s\n' 'fs.rm -r /buckets/sdh-bronze/warehouse/${x}_landing' \
    'fs.rm -r /buckets/sdh-silver/warehouse/$x' \
    's3.bucket.delete -name $(pipeline_bucket "$HARNESS" bronze)' \
    's3.bucket.delete -name $(pipeline_bucket "$HARNESS" silver)' | weed shell -master=localhost:9333" \
    >/dev/null 2>&1 || true
}

harness_down() {
  start_cluster
  say "Removing the harness pipeline and what it stored"
  harness_clean
  release_demo
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
  local landed_before split_before records_before fhir_before split_done converted prefix
  prefix="$(prefix "$p")"
  db() { infra exec postgres-0 -- psql -U postgres -d sdh -At -c "$1"; }
  landed_before="$(topic_count "$p.landed")"
  split_before="$(topic_count "$p.split")"
  records_before="$(topic_count "$p.records")"
  fhir_before="$(topic_count "$p.fhir")"
  split_done="$(audited "$n" splitter "item\.completed")"
  converted="$(audited "$n" convert "stage\.item_completed")"
  say "Pushing ${#samples[@]} synthetic and de-identified C-CDA documents to $p"
  local pushed ids
  pushed="$(push "$p" "${samples[@]}")"
  sed 's/^/   /' <<<"$pushed"
  ids="$(grep -o '"itemId":"[0-9a-f-]*"' <<<"$pushed" | cut -d'"' -f4 | sed "s/.*/'&'/" | paste -sd,)"
  # The pushed items whose every record has ended at the transform stage, from its registry: mapped to a bundle
  # or kept in the fail store. An item with a failure stays open until an operator settles it (STAGES.md §15.6),
  # so completion isn't waited for. The pods' logs would miss a pod scaled away.
  local ended_sql="FROM transform_$prefix.stage_item WHERE item_id IN (${ids:-NULL}) AND expected IS NOT NULL
    AND NOT upstream_open AND done >= expected"
  transformed() { db "SELECT count(*) $ended_sql"; }
  transformed_all() { [[ "$(transformed)" -ge ${#samples[@]} ]]; }
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
  if wait_for 300 transformed_all; then
    local counts expected done failed
    counts="$(db "SELECT coalesce(sum(expected), 0), coalesce(sum(done), 0), coalesce(sum(failed), 0)
      $ended_sql")"
    IFS='|' read -r expected done failed <<<"$counts"
    if ((expected > 0 && done == expected)); then
      say "  all ${#samples[@]} transformed (every record ended): $expected records, $((done - failed)) mapped to" \
        "a FHIR bundle in gold, $failed kept as they came in the fail store"
    else
      say "  FAIL: $done of $expected records ended at the transform stage"; failures=1
    fi
  else
    say "  FAIL: not all transformed ($(transformed) of ${#samples[@]} with every record ended)"; failures=1
  fi
  say "  item.landed messages: $(($(topic_count "$p.landed") - landed_before)) more on $p.landed"
  say "  split stage messages: $(($(topic_count "$p.split") - split_before)) more on $p.split"
  say "  convert stage messages: $(($(topic_count "$p.records") - records_before)) more on $p.records"
  say "  transform stage messages: $(($(topic_count "$p.fhir") - fhir_before)) more on $p.fhir"
  local pod where
  for where in "$n" "sdh-fhir -l app=transform-$p"; do
    # shellcheck disable=SC2086
    for pod in $(kc -n $where get pods -o name); do
      if kc -n "${where%% *}" logs "$pod" --all-containers 2>/dev/null \
          | grep -qE "ClinicalDocument|Newman|\.xml"; then
        say "  FAIL: $pod's log holds document content or a filename"; failures=1
      fi
    done
  done
  return "$failures"
}

# scale_demo [count]: pushes count copies of the sample documents, each made an item and a record of its own by a
# numbered comment before its root element's end tag (the content is otherwise the same: a comment before the
# root is outside the record, so its copies would share a record ID, the key its messages are partitioned by),
# and follows KEDA scaling the transform stage out to share them, and back. The split and convert stages are held
# to one pod each here (docs/SANDBOX.md "Memory"). What is done is counted in the stages' registries, so a pod
# scaled away still counts; how the work was shared is from the transform stage's records (who stored each).
scale_demo() {
  local p="${PIPELINES%% *}" count="${1:-300}" n run prefix samples=()
  [[ "$count" =~ ^[0-9]+$ && "$count" -gt 0 ]] || { echo "usage: $0 scale-demo [count]" >&2; exit 2; }
  n="$(ns "$p")"
  prefix="$(prefix "$p")"
  run="$(date -u +%Y%m%dT%H%M%SZ)"
  local file
  for file in "$ROOT"/connector-core/src/test/resources/ccda/*.xml; do
    samples+=("$(basename "$file")")
  done
  db() { infra exec postgres-0 -- psql -U postgres -d sdh -At -c "$1"; }
  local since
  since="$(db "SELECT now()")"
  split_done() { db "SELECT count(*) FROM split_$prefix.split_item WHERE ended_at >= '$since'"; }
  converted() { db "SELECT count(*) FROM convert_$prefix.stage_item WHERE completed_at >= '$since'"; }
  # Items whose every record has ended at the transform stage, mapped or failed: one with a failure stays open
  # until an operator settles it (STAGES.md §15.6), so completion isn't waited for.
  transformed() { db "SELECT count(*) FROM transform_$prefix.stage_item i WHERE i.expected IS NOT NULL
      AND NOT i.upstream_open AND i.done >= i.expected AND i.item_id IN (SELECT item_id
      FROM transform_$prefix.stage_record WHERE stored_at >= '$since')"; }
  # Pods ready, not only asked for: one with no room to run doesn't count.
  replicas() { kc -n sdh-fhir get deploy "transform-$p" -o jsonpath='{.status.readyReplicas}'; }

  say "Pushing $count copies of ${#samples[@]} sample C-CDA documents to $p (run $run), one at a time"
  local url="https://landing.$n.svc:8443/intakes/$p/items"
  (cd "$ROOT/connector-core/src/test/resources/ccda" && tar -c "${samples[@]}") \
    | kc -n sdh-producers exec -i deploy/producer -- sh -c '
        d="$(mktemp -d)"; tar -x -C "$d"; set -- "$d"/*.xml; i=0
        while [ "$i" -lt "'"$count"'" ]; do
          for f in "$@"; do
            [ "$i" -lt "'"$count"'" ] || break
            i=$((i + 1))
            sed "s#</ClinicalDocument>#<!-- sandbox scale-demo '"$run"' $i --></ClinicalDocument>#" "$f" \
              | curl -sS --fail -o /dev/null \
              --cacert /etc/producer/ca.crt --cert /etc/producer/tls.crt --key /etc/producer/tls.key \
              -H "Content-Type: application/octet-stream" --data-binary @- "'"$url"'" || echo "push $i failed"
          done
        done
        rm -rf "$d"' &
  local pusher=$!

  local started=$SECONDS most=0 split_now=0 convert_now=0 transform_now=0 t
  while ((SECONDS - started < 1800)); do
    sleep 15
    split_now="$(split_done)"
    convert_now="$(converted)"
    transform_now="$(transformed)"
    t="$(replicas)"; t="${t:-0}"; ((t > most)) && most=$t
    say "  $((SECONDS - started))s: split $split_now/$count, converted $convert_now/$count, transformed" \
      "$transform_now/$count on $t pod(s)"
    ((transform_now >= count)) && break
  done
  wait "$pusher" || true

  local failures=0 worked again
  say "  transform stage: up to $most pods; records each stored (from its registry):"
  db "SELECT split_part(owner, '/', 1), count(*) FROM transform_$prefix.stage_record WHERE stored_at >= '$since'
      GROUP BY 1 ORDER BY 1" | while IFS='|' read -r pod did; do say "    $pod: $did"; done
  worked="$(db "SELECT count(DISTINCT split_part(owner, '/', 1)) FROM transform_$prefix.stage_record
      WHERE stored_at >= '$since'")"
  again="$(db "SELECT count(*) FROM transform_$prefix.stage_record WHERE stored_at >= '$since' AND attempts > 1")"
  ((again == 0)) || say "  $again record(s) mapped on a second claim: their first pod stopped holding them"
  if ((transform_now < count)); then
    say "  FAIL: $split_now split, $convert_now converted and $transform_now transformed of $count in 30 minutes"
    failures=1
  fi
  if ((most < 2 || worked < 2)); then
    say "  FAIL: the transform stage didn't scale out (its backlog may not have passed 5 records)"; failures=1
  fi
  ((failures)) || say "  The transform stage scaled out and shared the work. KEDA scales it back to one pod" \
    "about two minutes after its backlog is gone (scripts/sandbox.sh status)."
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
      "$(topic_count "$p.records") on $p.records, $(topic_count "$p.fhir") on $p.fhir," \
      "$(topic_count "$p.convert.dlq") on $p.convert.dlq, $(topic_count "$p.fhir.dlq") on $p.fhir.dlq"
    kc -n "$(ns "$p")" get scaledobject,hpa 2>/dev/null || true
    kc -n sdh-fhir get "scaledobject/transform-$p" "hpa/keda-hpa-transform-$p" 2>/dev/null || true
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
  scale-demo) scale_demo "${2:-300}" ;;
  status) status ;;
  harness-up) harness_up ;;
  harness-down) harness_down ;;
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
  *) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
