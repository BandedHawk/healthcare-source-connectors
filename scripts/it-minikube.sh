#!/usr/bin/env bash
# Runs the integration tests (-Pit) against a Docker daemon inside a minikube VM (KVM2 driver), for machines
# without a local Docker daemon. Testcontainers reaches the VM through DOCKER_HOST/TLS from `minikube docker-env`.
#
#   scripts/it-minikube.sh                      # create/start the VM if needed, run all ITs
#   scripts/it-minikube.sh -Dit.test=KafkaIT    # extra Maven arguments are passed through
#   SDH_IT_STOP=1 scripts/it-minikube.sh        # stop the VM afterwards
#
# VM settings: see minikube-env.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/minikube-env.sh"

# Ryuk (Testcontainers' cleanup container) mounts the VM's own /var/run/docker.sock, which is correct here.
# Never bind-mount host paths in tests: they don't exist inside the VM. Copy files in instead.
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock

status=0
(cd "$ROOT" && ./mvnw -Pit verify "$@") || status=$?
stop_vm_if_asked
exit "$status"
