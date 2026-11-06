#!/usr/bin/env bash
# Builds an application's container image into the minikube VM's Docker daemon (KVM2 driver; see minikube-env.sh).
# The image is then available to containers and Testcontainers in that VM. Design §14.1 and §14.3.
#
#   scripts/build-image.sh                         # split stage service image (internal): sdh/split-stage-service:<version>
#   scripts/build-image.sh customer                # split stage customer image (distributed): sdh/split-stage:<version>
#   scripts/build-image.sh landing                 # landing service image: sdh/landing-connector-service:<version>
#   scripts/build-image.sh landing customer        # landing customer image: sdh/landing-connector:<version>
#   scripts/build-image.sh convert                 # convert stage service image: sdh/convert-stage-service:<version>
#   scripts/build-image.sh convert customer        # convert stage customer image: sdh/convert-stage:<version>
#   SDH_IMAGE=registry.example/sdh/split-stage:1.0 scripts/build-image.sh customer
#   SDH_JAVA_IMAGE=eclipse-temurin:25-jre scripts/build-image.sh customer   # another Java 25 base
#   SDH_SKIP_MAVEN=1 scripts/build-image.sh        # reuse what <module>/target already holds
#   SDH_IT_STOP=1 scripts/build-image.sh           # stop the VM afterwards
#
# The application is file-connector (also: split, splitter; the default; its images are named split-stage, as
# decided in STAGES.md §11), landing-connector (also: landing) or convert-stage (also: convert).
# service:  <module>/Dockerfile, from the packaged jar (full Ubuntu/Temurin image with a shell).
# customer: the distribution kit's container/Dockerfile on a shell-less Java 25 base (distroless by default), then
#           docker/customer-inventory.Dockerfile adds the OS package inventory.
# Both bases are pinned by digest in docker/base-images.env (design §14.5).
# Extra arguments go to `docker build` (for the customer image, to the first build).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="file-connector"
case "${1:-}" in
  file-connector|split|splitter) APP="file-connector"; shift ;;
  landing-connector|landing) APP="landing-connector"; shift ;;
  convert-stage|convert) APP="convert-stage"; shift ;;
esac
# The image's name: the module's, but for the split stage, which kept its module name.
NAME="$APP"
[[ "$APP" == "file-connector" ]] && NAME="split-stage"
VARIANT="service"
if [[ "${1:-}" == "service" || "${1:-}" == "customer" ]]; then
  VARIANT="$1"
  shift
fi
# The pinned bases; scripts/base-images.sh checks and updates them.
source "$ROOT/docker/base-images.env"

if [[ "${SDH_SKIP_MAVEN:-0}" != "1" ]]; then
  (cd "$ROOT" && ./mvnw -q -pl "$APP" -am package -DskipTests)
fi
VERSION="$(cd "$ROOT" && ./mvnw -q -N help:evaluate -Dexpression=project.version -DforceStdout)"

source "$ROOT/scripts/minikube-env.sh"

status=0
if [[ "$VARIANT" == "service" ]]; then
  IMAGE="${SDH_IMAGE:-sdh/$NAME-service:$VERSION}"
  jars=("$ROOT/$APP/target/$APP"-*.jar)
  if [[ ${#jars[@]} -ne 1 || ! -f "${jars[0]}" ]]; then
    echo "Expected exactly one $APP/target/$APP-*.jar, found: ${jars[*]}" >&2
    exit 1
  fi
  docker build --build-arg JRE_IMAGE="$TEMURIN_JRE_IMAGE" --build-arg JDK_IMAGE="$TEMURIN_JDK_IMAGE" \
    -t "$IMAGE" "$@" "$ROOT/$APP" || status=$?
else
  IMAGE="${SDH_IMAGE:-sdh/$NAME:$VERSION}"
  KIT="$ROOT/$APP/target/$APP-$VERSION-dist-dir/$APP-$VERSION"
  if [[ ! -f "$KIT/container/Dockerfile" ]]; then
    echo "No distribution kit at $KIT; package $APP first" >&2
    exit 1
  fi
  docker build -f "$KIT/container/Dockerfile" --build-arg JAVA_IMAGE="${SDH_JAVA_IMAGE:-$DISTROLESS_IMAGE}" \
    -t "$IMAGE-kit" "$@" "$KIT" || status=$?
  if [[ $status -eq 0 ]]; then
    docker build -f "$ROOT/docker/customer-inventory.Dockerfile" --build-arg KIT_IMAGE="$IMAGE-kit" \
      -t "$IMAGE" "$ROOT/docker" || status=$?
    docker image rm "$IMAGE-kit" >/dev/null 2>&1 || true
  fi
fi
if [[ $status -eq 0 ]]; then
  docker image inspect "$IMAGE" --format '>> Built {{index .RepoTags 0}} ({{.Size}} bytes, user {{.Config.User}})'
fi
stop_vm_if_asked
exit "$status"
