#!/usr/bin/env bash
# Runs the integration tests (-Pit) against a Docker daemon inside a minikube VM (KVM2 driver), for machines
# without a local Docker daemon. Testcontainers reaches the VM through DOCKER_HOST/TLS from `minikube docker-env`.
#
#   scripts/it-minikube.sh                      # create/start the VM if needed, run all ITs
#   scripts/it-minikube.sh -Dit.test=KafkaIT    # extra Maven arguments are passed through
#   SDH_IT_STOP=1 scripts/it-minikube.sh        # stop the VM afterwards
#
# Environment (defaults): SDH_IT_PROFILE=sdh-it SDH_IT_CPUS=6 SDH_IT_MEMORY=12g SDH_IT_DISK=60g
#
# The profile is single-node with no Kubernetes: only its Docker daemon is used. `minikube docker-env` doesn't
# work on multi-node clusters, and the tests shouldn't share a cluster you use for other work.
# Remove it with: minikube delete -p sdh-it
set -euo pipefail

PROFILE="${SDH_IT_PROFILE:-sdh-it}"
CPUS="${SDH_IT_CPUS:-6}"
MEMORY="${SDH_IT_MEMORY:-12g}"
DISK="${SDH_IT_DISK:-60g}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

command -v minikube >/dev/null || { echo "minikube is not installed" >&2; exit 1; }

host_state() {
  # First line only: minikube appends kubectl warnings to stdout, and exits non-zero without Kubernetes.
  minikube status -p "$PROFILE" --format '{{.Host}}' 2>/dev/null | head -n 1 || true
}

state="$(host_state)"
if [[ "$state" != "Running" ]]; then
  echo ">> Starting minikube profile '$PROFILE' (kvm2, docker runtime, no Kubernetes, $CPUS CPUs, $MEMORY)"
  # Settings only apply when the VM is first created; an existing profile keeps its size.
  minikube start -p "$PROFILE" --driver=kvm2 --container-runtime=docker --no-kubernetes \
    --cpus="$CPUS" --memory="$MEMORY" --disk-size="$DISK"
fi

eval "$(minikube -p "$PROFILE" docker-env --shell bash)"
echo ">> Docker API: $DOCKER_HOST"

# Fail early with a clear message if the daemon isn't answering (Testcontainers' own error is less direct).
if command -v docker >/dev/null; then
  docker version --format '>> Docker server {{.Server.Version}} (API {{.Server.APIVersion}})' \
    || { echo "Docker daemon in '$PROFILE' is not reachable" >&2; exit 1; }
fi

# Ryuk (Testcontainers' cleanup container) mounts the VM's own /var/run/docker.sock, which is correct here.
# Never bind-mount host paths in tests: they don't exist inside the VM. Copy files in instead.
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock

status=0
(cd "$ROOT" && ./mvnw -Pit verify "$@") || status=$?

if [[ "${SDH_IT_STOP:-0}" == "1" ]]; then
  echo ">> Stopping minikube profile '$PROFILE'"
  minikube stop -p "$PROFILE"
fi
exit "$status"
