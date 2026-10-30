#!/usr/bin/env bash
# Deploys the file connector on a real Kubernetes cluster, from the manifest customers get (the README in the
# distribution kit), and checks that it works there (design §15.2).
#
#   scripts/k8s-minikube.sh                 # customer image (distroless), on the minikube profile sdh-k8s
#   scripts/k8s-minikube.sh service         # the service image instead
#   SDH_IT_STOP=1 scripts/k8s-minikube.sh   # stop the VM afterwards
#   SDH_K8S_SKIP_BUILD=1 ...                # reuse the image already in the cluster
#
# Environment (defaults): SDH_K8S_PROFILE=sdh-k8s SDH_K8S_CPUS=4 SDH_K8S_MEMORY=8g SDH_K8S_DISK=40g
#   SDH_K8S_NAMESPACE=sdh-k8s-test (the services the connector needs go in <namespace>-services)
#
# The profile is a single-node cluster of its own (KVM2, Docker runtime), separate from the sdh-it profile, which
# runs no Kubernetes. Its kubeconfig is ~/.kube/sdh-k8s, so the current context in ~/.kube/config doesn't change.
# kubectl comes from `minikube kubectl`. The namespace is recreated on every run and left in place afterwards
# for inspection:  KUBECONFIG=~/.kube/sdh-k8s minikube -p sdh-k8s kubectl -- -n sdh-k8s-test get pods
# Remove everything with: minikube delete -p sdh-k8s
#
# What it checks, in order: the README's Deployment starts under the restricted security settings; a good and a
# bad file delivered by another pod end in completed/ and failed/; every message reaches Kafka; the pod log holds
# no PHI or filenames; operator commands run through `kubectl exec` (no shell in the customer image); `kubectl
# debug` can inspect the pod; a deleted pod is replaced and takes the lease over at once, a killed one once the
# lease's TTL has passed; a second copy of the deployment and a different connector on the same directory are both
# refused.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VARIANT="${1:-customer}"
PROFILE="${SDH_K8S_PROFILE:-sdh-k8s}"
NS="${SDH_K8S_NAMESPACE:-sdh-k8s-test}"
SVC_NS="$NS-services"
export KUBECONFIG="${HOME}/.kube/${PROFILE}"
K8S="$ROOT/file-connector/src/test/k8s"
README="$ROOT/file-connector/src/dist/README.md"
MARKER="PHI-MARKER-k8s"
KS_PASSWORD="ks-secret"
SIGNING_PASSPHRASE="sign-secret"

command -v minikube >/dev/null || { echo "minikube is not installed" >&2; exit 1; }
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
else
  IMAGE="sdh/file-connector:$VERSION"
fi
if [[ "${SDH_K8S_SKIP_BUILD:-0}" != "1" ]]; then
  # Built straight into the node's Docker daemon, so no registry is needed.
  SDH_IT_STOP=0 SDH_IT_PROFILE="$PROFILE" SDH_IMAGE="$IMAGE" "$ROOT/scripts/build-image.sh" "$VARIANT" || { stop_vm; exit 1; }
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
for alias in sections-kek/1 source-files-kek/1 failed-files-kek/1; do
  keytool -genseckey -alias "$alias" -keyalg AES -keysize 256 -storetype PKCS12 -keystore "$WORK/keks.p12" \
    -storepass "$KS_PASSWORD" -keypass "$KS_PASSWORD" 2>/dev/null || exit 1
done
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$WORK/signing.key" 2>/dev/null
openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -in "$WORK/signing.key" \
  -passout "pass:$SIGNING_PASSPHRASE" -out "$WORK/signing.pem"
for k in recipient recovery; do
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$WORK/$k.key" 2>/dev/null
  openssl pkey -in "$WORK/$k.key" -pubout -out "$WORK/$k-public.pem"
done
RECIPIENT_SHA256="$(openssl pkey -pubin -in "$WORK/recipient-public.pem" -outform DER | sha256sum | cut -d' ' -f1)"
sed -e "s/RECIPIENT_SHA256/$RECIPIENT_SHA256/" -e "s/SERVICES_NS/$SVC_NS/g" "$K8S/application.yml" > "$WORK/application.yml"

echo ">> Namespaces $NS (connector) and $SVC_NS (services), recreated"
for ns in "$NS" "$SVC_NS"; do
  minikube -p "$PROFILE" kubectl -- delete namespace "$ns" --ignore-not-found --wait=true >/dev/null
  minikube -p "$PROFILE" kubectl -- create namespace "$ns" >/dev/null || exit 1
done
# The connector's namespace enforces the restricted Pod Security Standard, so anything the README's manifest (or
# its kubectl debug command) leaves out is refused by the API server rather than passing unnoticed.
minikube -p "$PROFILE" kubectl -- label namespace "$NS" pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/enforce-version=latest >/dev/null

echo ">> Services the connector needs"
svc() { minikube -p "$PROFILE" kubectl -- -n "$SVC_NS" "$@"; }
svc apply -f "$K8S/services.yaml" >/dev/null || exit 1
k apply -f "$K8S/upstream.yaml" >/dev/null || exit 1
for d in postgres seaweedfs kafka; do
  svc rollout status "deploy/$d" --timeout=300s >/dev/null || { svc describe "deploy/$d" | tail; exit 1; }
done
k wait --for=condition=Ready pod/upstream --timeout=120s >/dev/null || exit 1
# minikube's hostpath provisioner reuses the directory for the same namespace and claim name, contents and all,
# even after the namespace is deleted; start from an empty inbound directory.
k exec upstream -- sh -c 'rm -rf /data/inbound/* /data/inbound/.[!.]*' || exit 1
bucket() { svc exec deploy/seaweedfs -- sh -c "echo 's3.bucket.create -name sdh-warehouse' | weed shell -master=localhost:9333" 2>&1 \
  && svc exec deploy/seaweedfs -- sh -c "echo 's3.bucket.list' | weed shell -master=localhost:9333" 2>&1 | grep sdh-warehouse >/dev/null; }
wait_for 90 bucket || { echo "Could not create the bucket" >&2; exit 1; }

echo ">> Connector, from the Deployment in the README"
k create secret generic file-connector-keys --from-file=keks.p12="$WORK/keks.p12" \
  --from-file=signing.pem="$WORK/signing.pem" --from-file=recipient.pem="$WORK/recipient-public.pem" \
  --from-file=recovery-public.pem="$WORK/recovery-public.pem" >/dev/null
k create secret generic file-connector-env \
  --from-literal=SDH_CRYPTO_KEYSTORE_PASSWORD="$KS_PASSWORD" \
  --from-literal=SDH_CRYPTO_KEYS_SIGNING_PASSPHRASE="$SIGNING_PASSPHRASE" \
  --from-literal=SDH_LEASE_JDBC_PASSWORD="sdh-k8s-postgres" \
  --from-literal=S3_SECRET_ACCESS_KEY="sdh-k8s-secret-key-0123456789" >/dev/null
k create configmap file-connector-config --from-file=application.yml="$WORK/application.yml" >/dev/null
# The README's YAML block, unchanged apart from the image name.
awk '/^```yaml$/ {on=1; next} /^```$/ {on=0} on' "$README" | sed "s#<registry>/file-connector:<version>#$IMAGE#" \
  > "$WORK/deployment.yaml"
[[ -s "$WORK/deployment.yaml" ]] || { echo "No yaml block in $README" >&2; exit 1; }
if ! k apply -f "$WORK/deployment.yaml"; then
  fail "README Deployment accepted under the restricted Pod Security Standard"
  stop_vm; exit 1
fi
pass "README Deployment accepted under the restricted Pod Security Standard"

started() { k logs deploy/file-connector 2>/dev/null | grep "Started FileConnectorApplication" >/dev/null; }
if wait_for 240 started; then
  pass "connector started"
else
  fail "connector started"
  k get pods -o wide; k describe deploy/file-connector | tail -20; k logs deploy/file-connector --tail=60
  stop_vm; exit 1
fi
POD="$(k get pod -l app=file-connector -o jsonpath='{.items[0].metadata.name}')"
check "runs as uid 10001" "10001" "$(k get pod "$POD" -o jsonpath='{.spec.securityContext.runAsUser}')"
check "read-only root filesystem" "true" \
  "$(k get pod "$POD" -o jsonpath='{.spec.containers[0].securityContext.readOnlyRootFilesystem}')"
# The heap the JVM picks from the README's 2Gi limit: half of it (container.options, design §14.1).
heap="$(k exec "$POD" -- java @/app/jvm.options @/app/container.options -XX:+PrintFlagsFinal -version 2>/dev/null \
  | awk '$2 == "MaxHeapSize" {print $4}')"
check "heap is half the 2Gi memory limit" "1024 MiB" "$((${heap:-0} / 1048576)) MiB"

echo ">> Files delivered by another pod"
{
  printf 'id,name,note\n'
  for i in $(seq 0 19); do printf '%s,%s-%s,"multi\nline"\n' "$i" "$MARKER" "$i"; done
} > "$WORK/good.csv"
printf 'id,name,note\n1,%s,"never closed\n' "$MARKER" > "$WORK/bad.csv"
k exec -i upstream -- sh -c 'cat > /data/inbound/good.csv' < "$WORK/good.csv"
k exec -i upstream -- sh -c 'cat > /data/inbound/bad.csv' < "$WORK/bad.csv"
picked_up() { ! k exec upstream -- ls /data/inbound | grep '\.csv$' >/dev/null; }
if wait_for 120 picked_up; then pass "both files picked up"; else fail "both files picked up"; fi
check "good file in completed/" "1" "$(k exec upstream -- sh -c 'ls /data/inbound/completed | grep -c "\.json$"')"
check "bad file in failed/" "1" "$(k exec upstream -- sh -c 'ls /data/inbound/failed | grep -c "\.json$"')"

# 2 × file.started, 20 × section.stored, 1 × file.completed (the bad file stores nothing).
messages="$(svc exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic sdh-sections --from-beginning --timeout-ms 15000 2>/dev/null | grep -c .)"
check "messages on Kafka" "23" "$messages"
check "messages encrypted (no PHI marker on the topic)" "0" "$(svc exec deploy/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sdh-sections \
  --from-beginning --timeout-ms 10000 2>/dev/null | grep -c "$MARKER")"

k logs "$POD" > "$WORK/connector.log"
check "pod log holds no PHI or filenames" "0" "$(grep -cE "$MARKER|good\.csv|bad\.csv" "$WORK/connector.log")"

echo ">> Operator commands through kubectl exec (README)"
ADMIN=(java @/app/jvm.options @/app/container.options @/app/admin.args)
status="$(k exec "$POD" -- "${ADMIN[@]}" status --operator k8s-operator 2>&1)"
if grep -qE "lease: +file-connector held by .+, epoch 1," <<<"$status"; then
  pass "admin status (lease held, epoch 1)"
else
  fail "admin status"; echo "$status" | tail -20
fi
bad_id="$(k exec upstream -- ls /data/inbound/failed | sed -n 's/\.json$//p' | head -n 1)"
[[ -n "$bad_id" ]] || bad_id="none"
restore="$(k exec "$POD" -- "${ADMIN[@]}" restore --operator k8s-operator --file-id "$bad_id" \
  --to /data/inbound/failed 2>&1)"
if grep -q "digest verified" <<<"$restore"; then pass "admin restore"; else fail "admin restore"; echo "$restore" | tail -20; fi
check "restored file matches what was delivered" "$(cat "$WORK/bad.csv")" \
  "$(k exec upstream -- cat "/data/inbound/failed/$bad_id" 2>&1)"
k exec upstream -- rm -f "/data/inbound/failed/$bad_id"
k logs "$POD" | grep "admin.status operator=k8s-operator" >/dev/null \
  && fail "admin audit event in the connector's log (expected only in the exec's output)" || true
grep -q "admin.status operator=k8s-operator" <<<"$status" && pass "audit event names the operator" \
  || fail "audit event names the operator"

echo ">> kubectl debug (README troubleshooting)"
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

echo ">> Pod deleted: the replacement takes the lease over"
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
printf 'id,name,note\n1,after-restart,x\n' | k exec -i upstream -- sh -c 'cat > /data/inbound/again.csv'
gone() { ! k exec upstream -- ls /data/inbound/again.csv; }
if wait_for 60 gone; then pass "replacement ingests"; else fail "replacement ingests"; fi

echo ">> JVM killed (no clean shutdown, as an OOM kill or crash): the restart waits for the lease to lapse"
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
  check "connector running again after the kill ($((SECONDS - t0)) s), without exiting on LEASE_HELD" "1" "$restarts"
else
  fail "connector running again after the kill"; k get pod "$POD2"
fi
status3="$(k exec "$POD2" -- "${ADMIN[@]}" status --operator k8s-operator 2>&1)"
if grep -qE "held by .+, epoch 3," <<<"$status3"; then pass "lease taken over (epoch 3)"; else
  fail "lease taken over (epoch 3)"; grep lease <<<"$status3"; fi

echo ">> A second instance is refused"
# The same deployment under another name: same lease, so LEASE_HELD.
sed -e 's/metadata: { name: file-connector }/metadata: { name: file-connector-copy }/' -e 's/app: file-connector }/app: file-connector-copy }/g' \
  "$WORK/deployment.yaml" > "$WORK/copy.yaml"
# Another connector (its own ID and lease) pointed at the same directory: INBOUND_DIR_CLAIMED.
sed -e 's/metadata: { name: file-connector }/metadata: { name: file-connector-other }/' -e 's/app: file-connector }/app: file-connector-other }/g' \
  -e 's#^            - { name: SPRING_CONFIG_ADDITIONAL_LOCATION, value: "file:/config/" }#&\n            - { name: SDH_CONNECTOR_ID, value: other-connector }\n            - { name: SDH_LEASE_NAME, value: other-connector }#' \
  "$WORK/deployment.yaml" > "$WORK/other.yaml"
k apply -f "$WORK/copy.yaml" -f "$WORK/other.yaml" >/dev/null
refused() {  # refused <app label> <error code>
  local p; p="$(k get pod -l "app=$1" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [[ -n "$p" ]] || return 1
  # Read whole, then searched: with pipefail, grep -q closing the pipe early would fail kubectl logs.
  local logs; logs="$(k logs "$p" 2>/dev/null; k logs "$p" --previous 2>/dev/null)"
  grep -qE "$2" <<<"$logs"
}
if wait_for 180 refused file-connector-copy LEASE_HELD; then pass "copy of the deployment refused (LEASE_HELD)"; else
  fail "copy of the deployment refused (LEASE_HELD)"; k logs deploy/file-connector-copy --tail=20; fi
if wait_for 180 refused file-connector-other 'connector.directory_claimed'; then
  pass "another connector on the same directory refused (INBOUND_DIR_CLAIMED)"; else
  fail "another connector on the same directory refused (INBOUND_DIR_CLAIMED)"; k logs deploy/file-connector-other --tail=20; fi
check "the running connector kept the lease" "1" \
  "$(k exec "$POD2" -- "${ADMIN[@]}" status --operator k8s-operator 2>&1 | grep -cE "held by .+, epoch 3,")"
k delete -f "$WORK/copy.yaml" -f "$WORK/other.yaml" --wait=false >/dev/null

mkdir -p "$ROOT/file-connector/target"
k logs "$POD2" -c file-connector > "$ROOT/file-connector/target/k8s-minikube.log" 2>&1
echo
if ((failures == 0)); then
  echo ">> All checks passed ($IMAGE on Kubernetes $(minikube -p "$PROFILE" kubectl -- version 2>/dev/null \
    | sed -n 's/^Server Version: //p'))"
else
  echo ">> $failures check(s) failed"
fi
stop_vm
exit $((failures > 0))
