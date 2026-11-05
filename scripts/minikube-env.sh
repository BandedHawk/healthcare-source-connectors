# Sourced by it-minikube.sh and build-image.sh: starts the minikube profile if needed (KVM2 driver, Docker runtime,
# no Kubernetes) and points the Docker CLI and Testcontainers at the Docker daemon inside the VM.
#
# Environment (defaults): SDH_IT_PROFILE=sdh-it SDH_IT_CPUS=8 SDH_IT_MEMORY=8g SDH_IT_DISK=100g
# Where the VM is kept: see minikube-home.sh (SDH_SANDBOX). 8 GB leaves room for the demo cluster alongside
# (scripts/sandbox.sh); the integration tests need about 6.
#
# The profile is single-node with no Kubernetes: only its Docker daemon is used. `minikube docker-env` doesn't
# work on multi-node clusters, and the tests shouldn't share a cluster you use for other work.
# Remove it with: minikube delete -p sdh-it

PROFILE="${SDH_IT_PROFILE:-sdh-it}"
CPUS="${SDH_IT_CPUS:-8}"
MEMORY="${SDH_IT_MEMORY:-8g}"
DISK="${SDH_IT_DISK:-100g}"

source "$(dirname "${BASH_SOURCE[0]}")/minikube-home.sh" || exit 1

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

# Fail early with a clear message if the daemon isn't answering.
if command -v docker >/dev/null; then
  docker version --format '>> Docker server {{.Server.Version}} (API {{.Server.APIVersion}})' \
    || { echo "Docker daemon in '$PROFILE' is not reachable" >&2; exit 1; }
fi

stop_vm_if_asked() {
  if [[ "${SDH_IT_STOP:-0}" == "1" || "${SDH_IT_STOP:-}" == "true" ]]; then
    echo ">> Stopping minikube profile '$PROFILE'"
    minikube stop -p "$PROFILE"
  fi
}
