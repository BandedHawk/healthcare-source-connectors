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

# --- Host memory ------------------------------------------------------------------------------------------------
# A KVM guest takes memory from the host as it uses it and doesn't give it back, so a guest ends up holding all it
# was given. The VMs together are kept to what the host can spare: its memory less SDH_HOST_RESERVE (MiB, default
# 9216: the desktop with NetBeans and a browser open used 8.6 GiB, measured 2026-11-07), less 192 MiB of QEMU's
# own per VM. docs/SANDBOX.md "Memory" has the sums.
SDH_HOST_RESERVE="${SDH_HOST_RESERVE:-9216}"
SDH_VM_OVERHEAD=192

# sdh_vm_mib <domain>: the memory a libvirt domain is given, in MiB; 0 if there is no such domain.
sdh_vm_mib() {
  virsh -c qemu:///system dominfo "$1" 2>/dev/null \
    | awk '/^Max memory:/ {print int($3 / 1024); f=1} END {if (!f) print 0}'
}

# sdh_size_mib <domain> <size>: the domain's memory in MiB if it exists, else the size minikube would give a new
# one (8g, 6656m or MiB).
sdh_size_mib() {
  local mib
  mib=$(sdh_vm_mib "$1")
  if ((mib > 0)); then echo "$mib"; return; fi
  case "$2" in
    *g|*G) echo $((${2%[gG]} * 1024)) ;;
    *m|*M) echo "${2%[mM]}" ;;
    *) echo "$2" ;;
  esac
}

# sdh_host_guard <MiB>:<domain>...: refuses (returns 1) to start the domains given, at the sizes given, unless every
# VM already running plus these stays within what the host can spare, and the host has that much available now.
sdh_host_guard() {
  command -v virsh >/dev/null || { echo ">> virsh not found: the host's memory isn't checked" >&2; return 0; }
  local total available spare starting=0 running=0 names=() vm d
  total=$(awk '/^MemTotal:/ {print int($2 / 1024)}' /proc/meminfo)
  available=$(awk '/^MemAvailable:/ {print int($2 / 1024)}' /proc/meminfo)
  spare=$((total - SDH_HOST_RESERVE))
  for vm in "$@"; do
    starting=$((starting + ${vm%%:*} + SDH_VM_OVERHEAD)); names+=("${vm#*:}")
  done
  for d in $(virsh -c qemu:///system list --state-running --name 2>/dev/null); do
    [[ " ${names[*]} " == *" $d "* ]] && continue
    running=$((running + $(sdh_vm_mib "$d") + SDH_VM_OVERHEAD))
  done
  if ((running + starting > spare)); then
    echo ">> Not starting ${names[*]}: with the VMs running (${running} MiB) they would hold" \
      "$((running + starting)) MiB of the host's ${total}, more than the ${spare} it can spare" \
      "(SDH_HOST_RESERVE=${SDH_HOST_RESERVE} kept for the desktop). Stop a VM first:" \
      "$(virsh -c qemu:///system list --state-running --name 2>/dev/null | xargs)" >&2
    return 1
  fi
  if ((starting > available - 512)); then
    echo ">> Not starting ${names[*]}: they need ${starting} MiB, and the host has ${available} MiB available" \
      "now. Close something on the desktop first." >&2
    return 1
  fi
}
