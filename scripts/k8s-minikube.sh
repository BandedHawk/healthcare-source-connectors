#!/usr/bin/env bash
# Deploys both stages on a real Kubernetes cluster, the landing node and the splitter (file connector), each from the
# manifest customers get (the README in its distribution kit), and checks that they work there (design §15.2).
#
#   scripts/k8s-minikube.sh                 # customer images (distroless), on the minikube profile sdh-k8s
#   scripts/k8s-minikube.sh service         # the service images instead
#   SDH_IT_STOP=1 scripts/k8s-minikube.sh   # stop the VM afterwards
#   SDH_K8S_SKIP_BUILD=1 ...                # reuse the images already in the cluster
#
# Environment (defaults): SDH_K8S_PROFILE=sdh-k8s SDH_K8S_CPUS=4 SDH_K8S_MEMORY=8g SDH_K8S_DISK=40g
#   SDH_K8S_NAMESPACE=sdh-k8s-test (the services the stages need go in <namespace>-services)
#
# The profile is a single-node cluster of its own (KVM2, Docker runtime), separate from the sdh-it profile, which
# runs no Kubernetes, and is kept where minikube-home.sh says (SDH_SANDBOX). Its kubeconfig is $SDH_KUBE_DIR/sdh-k8s
# (minikube-home.sh), so the current kubectl context elsewhere doesn't change.
# kubectl comes from `minikube kubectl`. The namespace is recreated on every run and left in place afterwards
# for inspection:  KUBECONFIG=$SDH_KUBE_DIR/sdh-k8s minikube -p sdh-k8s kubectl -- -n sdh-k8s-test get pods
# Remove everything with: minikube delete -p sdh-k8s
#
# What it checks, in order: both READMEs' Deployments start under the restricted security settings, landing as an
# active node and a standby; a good and a bad file delivered by another pod are landed (moved to landed/), announced
# and split; every message reaches Kafka, encrypted; no pod log holds PHI or filenames; operator commands run
# through `kubectl exec` (no shell in the customer image), and a restored item is read out with `kubectl debug`;
# when the active landing pod is deleted the standby takes over and lands the next file; a deleted splitter pod is
# replaced and takes the lease over at once, a killed one once the lease's TTL has passed; a second copy of the
# splitter and another landing intake on the same directory are both refused.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VARIANT="${1:-customer}"
PROFILE="${SDH_K8S_PROFILE:-sdh-k8s}"
NS="${SDH_K8S_NAMESPACE:-sdh-k8s-test}"
SVC_NS="$NS-services"
K8S="$ROOT/file-connector/src/test/k8s"
README="$ROOT/file-connector/src/dist/README.md"
LANDING_README="$ROOT/landing-connector/src/dist/README.md"
MARKER="PHI-MARKER-k8s"
KS_PASSWORD="ks-secret"
SIGNING_PASSPHRASE="sign-secret"
INBOX_PASSPHRASE="inbox-secret"

command -v minikube >/dev/null || { echo "minikube is not installed" >&2; exit 1; }
source "$ROOT/scripts/minikube-home.sh" || exit 1
export KUBECONFIG="$SDH_KUBE_DIR/$PROFILE"
for tool in keytool openssl python3; do
  command -v "$tool" >/dev/null || { echo "$tool is needed" >&2; exit 1; }
done

state="$(minikube status -p "$PROFILE" --format '{{.Host}}' 2>/dev/null | head -n 1 || true)"
if [[ "$state" != "Running" ]]; then
  echo ">> Starting minikube profile '$PROFILE' (kvm2, docker runtime, Kubernetes)"
  mkdir -p "$(dirname "$KUBECONFIG")"
  minikube start -p "$PROFILE" --driver=kvm2 --container-runtime=docker \
    --cpus="${SDH_K8S_CPUS:-4}" --memory="${SDH_K8S_MEMORY:-8g}" --disk-size="${SDH_K8S_DISK:-40g}" || exit 1
fi

stop_vm() {
  if [[ "${SDH_IT_STOP:-0}" == "1" ]]; then
    echo ">> Stopping minikube profile '$PROFILE'"
    minikube stop -p "$PROFILE"
  fi
}

k() { minikube -p "$PROFILE" kubectl -- -n "$NS" "$@"; }

VERSION="$(cd "$ROOT" && ./mvnw -q -N help:evaluate -Dexpression=project.version -DforceStdout)"
if [[ "$VARIANT" == "service" ]]; then
  IMAGE="sdh/file-connector-service:$VERSION"
  LANDING_IMAGE="sdh/landing-connector-service:$VERSION"
else
  IMAGE="sdh/file-connector:$VERSION"
  LANDING_IMAGE="sdh/landing-connector:$VERSION"
fi
if [[ "${SDH_K8S_SKIP_BUILD:-0}" != "1" ]]; then
  # Built straight into the node's Docker daemon, so no registry is needed.
  (cd "$ROOT" && ./mvnw -q -pl file-connector,landing-connector -am package -DskipTests) || { stop_vm; exit 1; }
  SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 SDH_IT_PROFILE="$PROFILE" SDH_IMAGE="$IMAGE" \
    "$ROOT/scripts/build-image.sh" file-connector "$VARIANT" || { stop_vm; exit 1; }
  SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 SDH_IT_PROFILE="$PROFILE" SDH_IMAGE="$LANDING_IMAGE" \
    "$ROOT/scripts/build-image.sh" landing-connector "$VARIANT" || { stop_vm; exit 1; }
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
failures=0
pass() { echo "   ok    $1"; }
fail() { echo "   FAIL  $1"; failures=$((failures + 1)); }
check() {  # check <description> <expected> <actual>
  if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (expected '$2', got '$3')"; fi
}
# Waits up to $1 seconds for a command to succeed.
wait_for() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

echo ">> Keys (local mode), as an operator would provide them"
for alias in records-kek/1 landed-items-kek/1; do
  keytool -genseckey -alias "$alias" -keyalg AES -keysize 256 -storetype PKCS12 -keystore "$WORK/keks.p12" \
    -storepass "$KS_PASSWORD" -keypass "$KS_PASSWORD" 2>/dev/null || exit 1
done
# ec_key <name> <passphrase>: an EC P-256 key pair, the private half as an encrypted PKCS#8 PEM (<name>.pem) and the
# public half as <name>-public.pem.
ec_key() {
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$WORK/$1.key" 2>/dev/null
  openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -in "$WORK/$1.key" -passout "pass:$2" -out "$WORK/$1.pem"
  openssl pkey -in "$WORK/$1.key" -pubout -out "$WORK/$1-public.pem"
}
ec_key signing "$SIGNING_PASSPHRASE"            # the splitter's message signing key
ec_key landing-signing "$SIGNING_PASSPHRASE"    # the landing node's
ec_key inbox "$INBOX_PASSPHRASE"                # the splitter's inbox key: item.landed is encrypted to it
ec_key recipient unused                         # downstream's; only the public half is used
fingerprint() { openssl pkey -pubin -in "$1" -outform DER | sha256sum | cut -d' ' -f1; }
sed -e "s/RECIPIENT_SHA256/$(fingerprint "$WORK/recipient-public.pem")/" -e "s/SERVICES_NS/$SVC_NS/g" \
  "$K8S/application.yml" > "$WORK/application.yml"
sed -e "s/INBOX_SHA256/$(fingerprint "$WORK/inbox-public.pem")/" -e "s/SERVICES_NS/$SVC_NS/g" \
  "$K8S/landing-application.yml" > "$WORK/landing-application.yml"

echo ">> Namespaces $NS (both stages) and $SVC_NS (services), recreated"
for ns in "$NS" "$SVC_NS"; do
  minikube -p "$PROFILE" kubectl -- delete namespace "$ns" --ignore-not-found --wait=true >/dev/null
  minikube -p "$PROFILE" kubectl -- create namespace "$ns" >/dev/null || exit 1
done
# The stages' namespace enforces the restricted Pod Security Standard, so anything the READMEs' manifests (or the
# kubectl debug command) leave out is refused by the API server rather than passing unnoticed.
minikube -p "$PROFILE" kubectl -- label namespace "$NS" pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/enforce-version=latest >/dev/null

echo ">> Services the stages need"
svc() { minikube -p "$PROFILE" kubectl -- -n "$SVC_NS" "$@"; }
svc apply -f "$K8S/services.yaml" >/dev/null || exit 1
k apply -f "$K8S/upstream.yaml" >/dev/null || exit 1
for d in postgres seaweedfs kafka; do
  svc rollout status "deploy/$d" --timeout=300s >/dev/null || { svc describe "deploy/$d" | tail; exit 1; }
done
k wait --for=condition=Ready pod/upstream --timeout=120s >/dev/null || exit 1
# minikube's hostpath provisioner reuses the directory for the same namespace and claim name, contents and all,
# even after the namespace is deleted; start from an empty intake directory.
k exec upstream -- sh -c 'rm -rf /data/intake/* /data/intake/.[!.]*' || exit 1
bucket() { svc exec deploy/seaweedfs -- sh -c "echo 's3.bucket.create -name sdh-warehouse' | weed shell -master=localhost:9333" 2>&1 \
  && svc exec deploy/seaweedfs -- sh -c "echo 's3.bucket.list' | weed shell -master=localhost:9333" 2>&1 | grep sdh-warehouse >/dev/null; }
wait_for 90 bucket || { echo "Could not create the bucket" >&2; exit 1; }

# yaml_block <readme> <image placeholder> <image>: the README's YAML block, unchanged apart from the image name.
yaml_block() {
  awk '/^```yaml$/ {on=1; next} /^```$/ {on=0} on' "$1" | sed "s#$2#$3#"
}

echo ">> Landing node, from the Deployment in its README (two replicas: active and standby)"
k create secret generic landing-claims-keys --from-file=keks.p12="$WORK/keks.p12" \
  --from-file=signing.pem="$WORK/landing-signing.pem" \
  --from-file=splitter-inbox.pem="$WORK/inbox-public.pem" >/dev/null
k create secret generic landing-claims-env \
  --from-literal=SDH_CRYPTO_KEYSTORE_PASSWORD="$KS_PASSWORD" \
  --from-literal=SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE="$SIGNING_PASSPHRASE" \
  --from-literal=SDH_LEASE_JDBC_PASSWORD="sdh-k8s-postgres" \
  --from-literal=S3_SECRET_ACCESS_KEY="sdh-k8s-secret-key-0123456789" >/dev/null
k create configmap landing-claims-config --from-file=application.yml="$WORK/landing-application.yml" >/dev/null
yaml_block "$LANDING_README" "<registry>/landing-connector:<version>" "$LANDING_IMAGE" > "$WORK/landing.yaml"
[[ -s "$WORK/landing.yaml" ]] || { echo "No yaml block in $LANDING_README" >&2; exit 1; }
if ! k apply -f "$WORK/landing.yaml"; then
  fail "landing README Deployment accepted under the restricted Pod Security Standard"
  stop_vm; exit 1
fi
pass "landing README Deployment accepted under the restricted Pod Security Standard"

echo ">> Splitter, from the Deployment in its README"
k create secret generic file-connector-keys --from-file=keks.p12="$WORK/keks.p12" \
  --from-file=signing.pem="$WORK/signing.pem" --from-file=recipient.pem="$WORK/recipient-public.pem" \
  --from-file=inbox.pem="$WORK/inbox.pem" --from-file=landing-signer.pem="$WORK/landing-signing-public.pem" >/dev/null
k create secret generic file-connector-env \
  --from-literal=SDH_CRYPTO_KEYSTORE_PASSWORD="$KS_PASSWORD" \
  --from-literal=SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE="$SIGNING_PASSPHRASE" \
  --from-literal=SDH_CRYPTO_KEYS_INBOX_PASSPHRASE="$INBOX_PASSPHRASE" \
  --from-literal=SDH_LEASE_JDBC_PASSWORD="sdh-k8s-postgres" \
  --from-literal=S3_SECRET_ACCESS_KEY="sdh-k8s-secret-key-0123456789" >/dev/null
k create configmap file-connector-config --from-file=application.yml="$WORK/application.yml" >/dev/null
yaml_block "$README" "<registry>/file-connector:<version>" "$IMAGE" > "$WORK/deployment.yaml"
[[ -s "$WORK/deployment.yaml" ]] || { echo "No yaml block in $README" >&2; exit 1; }
if ! k apply -f "$WORK/deployment.yaml"; then
  fail "splitter README Deployment accepted under the restricted Pod Security Standard"
  stop_vm; exit 1
fi
pass "splitter README Deployment accepted under the restricted Pod Security Standard"

# landing_pods: the landing pods' names, one per line.
landing_pods() { k get pod -l app=landing-claims -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'; }
# landing_pod <pattern>: the first landing pod whose log matches.
landing_pod() {
  local p
  for p in $(landing_pods); do
    if k logs "$p" 2>/dev/null | grep -E "$1" >/dev/null; then echo "$p"; return 0; fi
  done
  return 1
}
landing_ready() { landing_pod "Started LandingConnectorApplication" && landing_pod "lease\.standby"; }
if wait_for 240 landing_ready; then
  pass "landing started: one active node, one standby"
else
  fail "landing started: one active node, one standby"
  k get pods -o wide; for p in $(landing_pods); do k logs "$p" --tail=40; done
  stop_vm; exit 1
fi
ACTIVE="$(landing_pod "Started LandingConnectorApplication")"
STANDBY="$(landing_pod "lease\.standby")"
[[ "$ACTIVE" != "$STANDBY" ]] && pass "active and standby are different pods" || fail "active and standby are different pods"

started() { k logs deploy/file-connector 2>/dev/null | grep "Started FileConnectorApplication" >/dev/null; }
if wait_for 240 started; then
  pass "splitter started"
else
  fail "splitter started"
  k get pods -o wide; k describe deploy/file-connector | tail -20; k logs deploy/file-connector --tail=60
  stop_vm; exit 1
fi
POD="$(k get pod -l app=file-connector -o jsonpath='{.items[0].metadata.name}')"
for p in "$POD" "$ACTIVE"; do
  check "$p runs as uid 10001" "10001" "$(k get pod "$p" -o jsonpath='{.spec.securityContext.runAsUser}')"
  check "$p has a read-only root filesystem" "true" \
    "$(k get pod "$p" -o jsonpath='{.spec.containers[0].securityContext.readOnlyRootFilesystem}')"
done
# heap_mib <pod>: the heap the JVM picks from the pod's memory limit (container.options, design §14.1).
heap_mib() {
  local heap
  heap="$(k exec "$1" -- java @/app/jvm.options @/app/container.options -XX:+PrintFlagsFinal -version 2>/dev/null \
    | awk '$2 == "MaxHeapSize" {print $4}')"
  echo "$((${heap:-0} / 1048576)) MiB"
}
check "splitter heap is half its 2Gi memory limit" "1024 MiB" "$(heap_mib "$POD")"
check "landing heap is half its 1Gi memory limit" "512 MiB" "$(heap_mib "$ACTIVE")"

echo ">> Files delivered by another pod"
{
  printf 'id,name,note\n'
  for i in $(seq 0 19); do printf '%s,%s-%s,"multi\nline"\n' "$i" "$MARKER" "$i"; done
} > "$WORK/good.csv"
printf 'id,name,note\n1,%s,"never closed\n' "$MARKER" > "$WORK/bad.csv"
deliver() { k exec -i upstream -- sh -c "cat > /data/intake/.$1.part && mv /data/intake/.$1.part /data/intake/$1" < "$2"; }
deliver good.csv "$WORK/good.csv"
deliver bad.csv "$WORK/bad.csv"
# landed <n>: no file left in the intake, and n moved to landed/.
landed() {
  [[ "$(k exec upstream -- sh -c 'echo $(ls /data/intake | grep -c "\.csv$") $(ls /data/intake/landed 2>/dev/null | grep -c "\.csv$")')" == "0 $1" ]]
}
if wait_for 180 landed 2; then pass "both files landed and moved to landed/"; else fail "both files landed and moved to landed/"; fi

# topic_count <topic> [pattern]: messages on a topic (matching the pattern, if given).
topic_count() {
  svc exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$1" --from-beginning --timeout-ms 10000 2>/dev/null | grep -c "${2:-.}"
}
# 2 × file.started, 20 × record.stored, 1 × file.completed (the bad file stores nothing).
records_at() { [[ "$(topic_count sdh-records)" -ge "$1" ]]; }
wait_for 120 records_at 23
check "messages on the records topic" "23" "$(topic_count sdh-records)"
check "records topic encrypted (no PHI marker)" "0" "$(topic_count sdh-records "$MARKER")"
check "item.landed messages on the landing topic" "2" "$(topic_count sdh-landing-claims)"
check "landing topic encrypted (no PHI marker)" "0" "$(topic_count sdh-landing-claims "$MARKER")"

# no_phi <pod> [container]: the pod's log holds no PHI marker and no delivered filename.
no_phi() {
  check "$1 log holds no PHI or filenames" "0" "$(k logs "$1" --all-containers 2>/dev/null | grep -cE "$MARKER|\.csv")"
}
for p in "$POD" $(landing_pods); do no_phi "$p"; done

echo ">> Operator commands through kubectl exec (README)"
ADMIN=(java @/app/jvm.options @/app/container.options @/app/admin.args)
status="$(k exec "$POD" -- "${ADMIN[@]}" status --operator k8s-operator 2>&1)"
if grep -qE "lease: +file-connector held by .+, epoch 1," <<<"$status"; then
  pass "admin status (lease held, epoch 1)"
else
  fail "admin status"; echo "$status" | tail -20
fi
bad_id="$(k logs "$POD" | sed -n 's/.*item\.failed item_id=\([0-9a-f-]*\).*/\1/p' | head -n 1)"
[[ -n "$bad_id" ]] || bad_id="none"
restore="$(k exec "$POD" -- "${ADMIN[@]}" restore --operator k8s-operator --file-id "$bad_id" --to /tmp 2>&1)"
if grep -q "digest verified" <<<"$restore"; then pass "admin restore from landing"; else
  fail "admin restore from landing"; echo "$restore" | tail -20; fi
k logs "$POD" | grep "admin.status operator=k8s-operator" >/dev/null \
  && fail "admin audit event in the connector's log (expected only in the exec's output)" || true
grep -q "admin.status operator=k8s-operator" <<<"$status" && pass "audit event names the operator" \
  || fail "audit event names the operator"

echo ">> kubectl debug (README troubleshooting): inspect the pod and read the restored item out"
# As the README has it, plus what this script needs to run it unattended (--container, --attach, --quiet, a
# command instead of -it).
DEBUG_ARGS=($(sed -n 's/^kubectl debug -it <pod> \(.*\)$/\1/p' "$README" | sed 's/--image=busybox /--image=busybox:1.37.0 /'))
debug="$(k debug "$POD" "${DEBUG_ARGS[@]}" --container=debugger --attach=true --quiet \
  -- sh -c 'id -u; ls /proc/1/root/app/legal' 2>&1)"
if grep -q "^10001$" <<<"$debug" && grep -q "THIRD-PARTY.txt" <<<"$debug"; then
  pass "ephemeral debug container runs as 10001 and sees the app's files"
else
  fail "kubectl debug"; echo "$debug" | tail -10
fi
# stdout only: kubectl may warn on stderr that the container finished before it could attach, and then shows its
# output as logs instead.
restored="$(k debug "$POD" "${DEBUG_ARGS[@]}" --container=reader --attach=true --quiet \
  -- sh -c "cat /proc/1/root/tmp/$bad_id && rm /proc/1/root/tmp/$bad_id" 2>"$WORK/reader.err")"
check "restored item matches what was delivered" "$(cat "$WORK/bad.csv")" "$restored"
[[ "$restored" == "$(cat "$WORK/bad.csv")" ]] || cat "$WORK/reader.err"

echo ">> Active landing pod deleted: the standby takes over"
k delete pod "$ACTIVE" --wait=true >/dev/null
t0=$SECONDS
took_over() { k logs "$STANDBY" 2>/dev/null | grep "Started LandingConnectorApplication" >/dev/null; }
if wait_for 180 took_over; then pass "standby took over ($((SECONDS - t0)) s after the delete)"; else
  fail "standby took over"; k logs "$STANDBY" --tail=30; fi
printf 'id,name,note\n1,after-failover,x\n' > "$WORK/again.csv"
deliver again.csv "$WORK/again.csv"
if wait_for 120 landed 3; then pass "the new active node lands"; else fail "the new active node lands"; fi
if wait_for 120 records_at 26; then pass "and the splitter splits it"; else fail "and the splitter splits it"; fi
new_standby() {
  local p
  for p in $(landing_pods); do
    [[ "$p" != "$STANDBY" ]] && k logs "$p" 2>/dev/null | grep "lease\.standby" >/dev/null && return 0
  done
  return 1
}
if wait_for 180 new_standby; then pass "the replacement pod waits as the new standby"; else
  fail "the replacement pod waits as the new standby"; fi

echo ">> Splitter pod deleted: the replacement takes the lease over"
k delete pod "$POD" --wait=true >/dev/null
t0=$SECONDS
new_pod() { [[ "$(k get pod -l app=file-connector --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)" =~ ^file-connector- ]]; }
wait_for 120 new_pod
POD2="$(k get pod -l app=file-connector -o jsonpath='{.items[0].metadata.name}')"
started2() { k logs "$POD2" 2>/dev/null | grep "Started FileConnectorApplication" >/dev/null; }
if wait_for 180 started2; then pass "replacement started ($((SECONDS - t0)) s after the delete)"; else fail "replacement started"; fi
status2="$(k exec "$POD2" -- "${ADMIN[@]}" status --operator k8s-operator 2>&1)"
if grep -qE "held by .+, epoch 2," <<<"$status2"; then pass "lease taken over (epoch 2)"; else
  fail "lease taken over (epoch 2)"; grep lease <<<"$status2"; fi
printf 'id,name,note\n1,after-restart,x\n' > "$WORK/later.csv"
deliver later.csv "$WORK/later.csv"
if wait_for 120 records_at 29; then pass "replacement splits"; else fail "replacement splits"; fi

echo ">> Splitter JVM killed (no clean shutdown, as an OOM kill or crash): the restart waits for the lease to lapse"
# SIGKILL from the node, as the OOM killer would send it; the lease isn't released. A forced pod delete isn't the
# same (the kubelet still sends SIGTERM first, and the JVM shuts down cleanly), and a debug container can't do it:
# the kernel ignores signals sent to a namespace's PID 1 from inside that namespace.
cid="$(k get pod "$POD2" -o jsonpath='{.status.containerStatuses[?(@.name=="file-connector")].containerID}')"
minikube -p "$PROFILE" ssh -- "sudo kill -9 \$(docker inspect --format '{{.State.Pid}}' ${cid#docker://})" \
  || fail "kill the JVM from the node"
t0=$SECONDS
restarted() {  # the same pod, restarted by the kubelet, has started the connector again
  local n; n="$(k get pod "$POD2" -o jsonpath='{.status.containerStatuses[?(@.name=="file-connector")].restartCount}')"
  [[ "${n:-0}" -ge 1 ]] && k logs "$POD2" -c file-connector 2>/dev/null | grep "Started FileConnectorApplication" >/dev/null
}
if wait_for 400 restarted; then
  restarts="$(k get pod "$POD2" -o jsonpath='{.status.containerStatuses[?(@.name=="file-connector")].restartCount}')"
  # Startup waits for the dead holder's lease to lapse rather than exiting (design §9.3), so the kill is the only
  # restart: no LEASE_HELD exits and no CrashLoopBackOff delays.
  check "splitter running again after the kill ($((SECONDS - t0)) s), without exiting on LEASE_HELD" "1" "$restarts"
else
  fail "splitter running again after the kill"; k get pod "$POD2"
fi
status3="$(k exec "$POD2" -- "${ADMIN[@]}" status --operator k8s-operator 2>&1)"
if grep -qE "held by .+, epoch 3," <<<"$status3"; then pass "lease taken over (epoch 3)"; else
  fail "lease taken over (epoch 3)"; grep lease <<<"$status3"; fi

echo ">> A second instance is refused"
# The same splitter deployment under another name: same lease, so LEASE_HELD.
sed -e 's/metadata: { name: file-connector }/metadata: { name: file-connector-copy }/' -e 's/app: file-connector }/app: file-connector-copy }/g' \
  "$WORK/deployment.yaml" > "$WORK/copy.yaml"
# Another landing intake (its own ID and lease) pointed at the same directory: INBOUND_DIR_CLAIMED.
sed -e 's/metadata: { name: landing-claims }/metadata: { name: landing-other }/' -e 's/app: landing-claims }/app: landing-other }/g' \
  -e 's/replicas: 2 .*/replicas: 1/' \
  -e 's#^            - { name: SPRING_CONFIG_ADDITIONAL_LOCATION, value: "file:/config/" }#&\n            - { name: SDH_CONNECTOR_ID, value: landing-other }\n            - { name: SDH_LANDING_INTAKE, value: other }#' \
  "$WORK/landing.yaml" > "$WORK/other.yaml"
k apply -f "$WORK/copy.yaml" -f "$WORK/other.yaml" >/dev/null
refused() {  # refused <app label> <pattern>
  local p; p="$(k get pod -l "app=$1" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [[ -n "$p" ]] || return 1
  # Read whole, then searched: with pipefail, grep -q closing the pipe early would fail kubectl logs.
  local logs; logs="$(k logs "$p" 2>/dev/null; k logs "$p" --previous 2>/dev/null)"
  grep -qE "$2" <<<"$logs"
}
if wait_for 180 refused file-connector-copy LEASE_HELD; then pass "copy of the splitter refused (LEASE_HELD)"; else
  fail "copy of the splitter refused (LEASE_HELD)"; k logs deploy/file-connector-copy --tail=20; fi
if wait_for 180 refused landing-other 'ClaimedException|directory_claimed|INBOUND_DIR_CLAIMED'; then
  pass "another landing intake on the same directory refused (INBOUND_DIR_CLAIMED)"; else
  fail "another landing intake on the same directory refused (INBOUND_DIR_CLAIMED)"; k logs deploy/landing-other --tail=20; fi
check "the running splitter kept the lease" "1" \
  "$(k exec "$POD2" -- "${ADMIN[@]}" status --operator k8s-operator 2>&1 | grep -cE "held by .+, epoch 3,")"
k delete -f "$WORK/copy.yaml" -f "$WORK/other.yaml" --wait=false >/dev/null
for p in "$POD2" $(landing_pods); do no_phi "$p"; done

mkdir -p "$ROOT/file-connector/target"
k logs "$POD2" -c file-connector > "$ROOT/file-connector/target/k8s-minikube.log" 2>&1
for p in $(landing_pods); do
  echo "---- $p"; k logs "$p" -c landing-connector 2>&1
done > "$ROOT/file-connector/target/k8s-minikube-landing.log"
echo
if ((failures == 0)); then
  echo ">> All checks passed ($IMAGE and $LANDING_IMAGE on Kubernetes $(minikube -p "$PROFILE" kubectl -- version 2>/dev/null \
    | sed -n 's/^Server Version: //p'))"
else
  echo ">> $failures check(s) failed"
fi
stop_vm
exit $((failures > 0))
