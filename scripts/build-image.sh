#!/usr/bin/env bash
# Builds a file-connector container image into the minikube VM's Docker daemon (KVM2 driver; see minikube-env.sh).
# The image is then available to containers and Testcontainers in that VM. Design §14.1 and §14.3.
#
#   scripts/build-image.sh                  # service image (internal, not for distribution): sdh/file-connector-service:<version>
#   scripts/build-image.sh customer         # customer image (distributed): sdh/file-connector:<version>
#   SDH_IMAGE=registry.example/sdh/file-connector:1.0 scripts/build-image.sh customer
#   SDH_JAVA_IMAGE=eclipse-temurin:25-jre scripts/build-image.sh customer   # another Java 25 base
#   SDH_SKIP_MAVEN=1 scripts/build-image.sh  # reuse what file-connector/target already holds
#   SDH_IT_STOP=1 scripts/build-image.sh     # stop the VM afterwards
#
# service:  file-connector/Dockerfile, from the packaged jar (full Ubuntu/Temurin image with a shell and sdh-admin).
# customer: the distribution kit's container/Dockerfile on a shell-less Java 25 base (distroless by default), then
#           file-connector/docker/customer-inventory.Dockerfile adds the OS package inventory.
# Both bases are pinned by digest in file-connector/docker/base-images.env (design §14.5).
# Extra arguments go to `docker build` (for the customer image, to the first build).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VARIANT="service"
if [[ "${1:-}" == "service" || "${1:-}" == "customer" ]]; then
  VARIANT="$1"
  shift
fi
# The pinned bases; scripts/base-images.sh checks and updates them.
source "$ROOT/file-connector/docker/base-images.env"

if [[ "${SDH_SKIP_MAVEN:-0}" != "1" ]]; then
  (cd "$ROOT" && ./mvnw -q -pl file-connector -am package -DskipTests)
fi
VERSION="$(cd "$ROOT" && ./mvnw -q -N help:evaluate -Dexpression=project.version -DforceStdout)"

source "$ROOT/scripts/minikube-env.sh"

status=0
if [[ "$VARIANT" == "service" ]]; then
  IMAGE="${SDH_IMAGE:-sdh/file-connector-service:$VERSION}"
  jars=("$ROOT"/file-connector/target/file-connector-*.jar)
  if [[ ${#jars[@]} -ne 1 || ! -f "${jars[0]}" ]]; then
    echo "Expected exactly one file-connector/target/file-connector-*.jar, found: ${jars[*]}" >&2
    exit 1
  fi
  docker build --build-arg JRE_IMAGE="$TEMURIN_JRE_IMAGE" --build-arg JDK_IMAGE="$TEMURIN_JDK_IMAGE" \
    -t "$IMAGE" "$@" "$ROOT/file-connector" || status=$?
else
  IMAGE="${SDH_IMAGE:-sdh/file-connector:$VERSION}"
  KIT="$ROOT/file-connector/target/file-connector-$VERSION-dist-dir/file-connector-$VERSION"
  if [[ ! -f "$KIT/container/Dockerfile" ]]; then
    echo "No distribution kit at $KIT; package file-connector first" >&2
    exit 1
  fi
  docker build -f "$KIT/container/Dockerfile" --build-arg JAVA_IMAGE="${SDH_JAVA_IMAGE:-$DISTROLESS_IMAGE}" \
    -t "$IMAGE-kit" "$@" "$KIT" || status=$?
  if [[ $status -eq 0 ]]; then
    docker build -f "$ROOT/file-connector/docker/customer-inventory.Dockerfile" --build-arg KIT_IMAGE="$IMAGE-kit" \
      -t "$IMAGE" "$ROOT/file-connector/docker" || status=$?
    docker image rm "$IMAGE-kit" >/dev/null 2>&1 || true
  fi
fi
if [[ $status -eq 0 ]]; then
  docker image inspect "$IMAGE" --format '>> Built {{index .RepoTags 0}} ({{.Size}} bytes, user {{.Config.User}})'
fi
stop_vm_if_asked
exit "$status"
