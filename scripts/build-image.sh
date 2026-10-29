#!/usr/bin/env bash
# Builds the file-connector jar and its container image into the minikube VM's Docker daemon (KVM2 driver; see
# minikube-env.sh). The image is then available to containers and Testcontainers in that VM.
#
#   scripts/build-image.sh                       # tag sdh/file-connector:<project version>
#   SDH_IMAGE=registry.example/sdh/file-connector:1.0 scripts/build-image.sh
#   SDH_SKIP_MAVEN=1 scripts/build-image.sh      # reuse file-connector/target/*.jar
#   SDH_IT_STOP=1 scripts/build-image.sh         # stop the VM afterwards
#
# Extra arguments go to `docker build` (e.g. --build-arg JRE_IMAGE=eclipse-temurin@sha256:…).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${SDH_SKIP_MAVEN:-0}" != "1" ]]; then
  (cd "$ROOT" && ./mvnw -q -pl file-connector -am package -DskipTests)
fi
VERSION="$(cd "$ROOT" && ./mvnw -q -N help:evaluate -Dexpression=project.version -DforceStdout)"
IMAGE="${SDH_IMAGE:-sdh/file-connector:$VERSION}"

jars=("$ROOT"/file-connector/target/file-connector-*.jar)
if [[ ${#jars[@]} -ne 1 || ! -f "${jars[0]}" ]]; then
  echo "Expected exactly one file-connector/target/file-connector-*.jar, found: ${jars[*]}" >&2
  exit 1
fi

source "$ROOT/scripts/minikube-env.sh"

status=0
docker build -t "$IMAGE" "$@" "$ROOT/file-connector" || status=$?
if [[ $status -eq 0 ]]; then
  docker image inspect "$IMAGE" --format '>> Built {{index .RepoTags 0}} ({{.Size}} bytes, user {{.Config.User}})'
fi
stop_vm_if_asked
exit "$status"
