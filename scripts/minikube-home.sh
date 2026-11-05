# Sourced by the scripts that run minikube: where minikube keeps its VMs, disks and caches for this project's
# profiles (sdh-it, sdh-k8s, sdh-demo).
#
# Set SDH_SANDBOX to a directory on a disk with room for the VMs, either in the environment or, so that NetBeans
# and Maven see it too, in ~/.config/sdh/sandbox.env:
#
#   SDH_SANDBOX=/mnt/sandbox
#
# The profiles then live in $SDH_SANDBOX/minikube/.minikube (MINIKUBE_HOME is $SDH_SANDBOX/minikube; minikube adds
# .minikube), their kubeconfigs in $SDH_SANDBOX/minikube/kube (SDH_KUBE_DIR), and the demo cluster's own state
# (keys, Vault's unseal key) in $SDH_SANDBOX/sdh-demo. Without SDH_SANDBOX, minikube's default (~/.minikube) and
# ~/.kube are used. A MINIKUBE_HOME already set in the environment wins over both.
#
# To use minikube by hand with the same home, set MINIKUBE_HOME=$SDH_SANDBOX/minikube in ~/.profile as well.

SDH_SANDBOX_ENV="${XDG_CONFIG_HOME:-$HOME/.config}/sdh/sandbox.env"
if [[ -z "${SDH_SANDBOX:-}" && -r "$SDH_SANDBOX_ENV" ]]; then
  SDH_SANDBOX="$(sed -n 's/^SDH_SANDBOX=//p' "$SDH_SANDBOX_ENV" | tail -n 1)"
fi
if [[ -n "${SDH_SANDBOX:-}" ]]; then
  # A disk that isn't mounted would leave an empty directory on the root filesystem: refuse rather than fill it.
  if ! mountpoint -q "$SDH_SANDBOX" && [[ ! -d "$SDH_SANDBOX/minikube" ]]; then
    echo "SDH_SANDBOX=$SDH_SANDBOX is neither a mounted disk nor holds minikube/; is the disk mounted?" >&2
    return 1 2>/dev/null || exit 1
  fi
  export SDH_SANDBOX
  export MINIKUBE_HOME="${MINIKUBE_HOME:-$SDH_SANDBOX/minikube}"
  mkdir -p "$MINIKUBE_HOME"
fi
# Where the profiles' kubeconfigs go (one file per profile, so the current context elsewhere doesn't change).
SDH_KUBE_DIR="${MINIKUBE_HOME:+$MINIKUBE_HOME/kube}"
export SDH_KUBE_DIR="${SDH_KUBE_DIR:-$HOME/.kube}"
mkdir -p "$SDH_KUBE_DIR"
chmod 700 "$SDH_KUBE_DIR"
# kubectl's discovery cache, otherwise in ~/.kube/cache whatever the kubeconfig.
export KUBECACHEDIR="${KUBECACHEDIR:-$SDH_KUBE_DIR/cache}"
