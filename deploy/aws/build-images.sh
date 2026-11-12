#!/usr/bin/env bash
# Builds the four customer images for arm64 (the PoC's Graviton nodes) and pushes them to the PoC's ECR
# repositories (docs/AWS-POC.md §10, step 3).
#
#   deploy/aws/build-images.sh                 # Maven (offline), then the images, then push
#   SDH_SKIP_MAVEN=1 deploy/aws/build-images.sh   # reuse what target/ already holds
#
# What it does:
#   1. Maven, offline (./mvnw -o): installs this repository, so the transformer builds on it as it is here, then
#      packages the transformer. As scripts/sandbox.sh does.
#   2. Builds each image in the sdh-it VM's Docker with scripts/build-image.sh (SDH_PLATFORM=linux/arm64). No
#      emulation is needed: nothing in the builds runs on arm64.
#   3. Pushes them to ECR.
#   4. Stops sdh-it again, and writes the digests to $SDH_POC_STATE/images.env for the pipeline's deployment.
#
# ECR tags are immutable, so each build gets a tag of its own: <version>-<UTC time>. Deployments use the digest.
#
# sdh-it must not run alongside sdh-demo (docs/SANDBOX.md "Memory"): the script refuses if sdh-demo is running.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TRANSFORMER="${SDH_TRANSFORMER:-$(cd "$ROOT/.." && pwd)/healthcare-data-transformer}"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export AWS_PROFILE="${AWS_PROFILE:-claude-poc}" AWS_REGION=us-east-2 AWS_PAGER=""
REGISTRY="493469777520.dkr.ecr.us-east-2.amazonaws.com"
PLATFORM=linux/arm64
STAMP="$(date -u +%Y%m%dT%H%MZ)"

say() { printf '>> %s\n' "$*"; }
die() { printf 'build-images.sh: %s\n' "$*" >&2; exit 1; }

source "$ROOT/scripts/minikube-home.sh"
[[ "$(minikube status -p sdh-demo --format '{{.Host}}' 2>/dev/null | head -n 1 || true)" != "Running" ]] \
  || die "sdh-demo is running; stop it first (scripts/sandbox.sh stop): sdh-it and sdh-demo don't fit together"
[[ -f "$TRANSFORMER/transform-stage/pom.xml" ]] || die "no transform stage at $TRANSFORMER (set SDH_TRANSFORMER)"

version="$(cd "$ROOT" && ./mvnw -o -q -N help:evaluate -Dexpression=project.version -DforceStdout)"
transform_version="$(cd "$TRANSFORMER" && ./mvnw -o -q -N help:evaluate -Dexpression=project.version -DforceStdout)"

if [[ "${SDH_SKIP_MAVEN:-0}" != "1" ]]; then
  say "Maven (offline): installing healthcare-source-connectors $version"
  (cd "$ROOT" && ./mvnw -o -q install -DskipTests)
  say "Maven (offline): packaging the transform stage $transform_version"
  (cd "$TRANSFORMER" && ./mvnw -o -q package -DskipTests)
fi

stop_vm() { say "Stopping the sdh-it VM"; minikube stop -p "${SDH_IT_PROFILE:-sdh-it}" >/dev/null 2>&1 || true; }
trap stop_vm EXIT

# module=image: the split stage kept its module's name, file-connector.
images=("landing-connector=landing-connector:$version" "file-connector=split-stage:$version"
        "convert-stage=convert-stage:$version" "$TRANSFORMER/transform-stage=transform-stage:$transform_version")
for i in "${images[@]}"; do
  module="${i%%=*}"; ref="${i#*=}"
  say "Building $ref for $PLATFORM"
  SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 SDH_PLATFORM="$PLATFORM" SDH_IMAGE="$REGISTRY/sdh-poc/$ref-$STAMP" \
    "$ROOT/scripts/build-image.sh" "$module" customer
done

# The Docker daemon is the VM's; the client, and so the ECR login, is this machine's.
source "$ROOT/scripts/minikube-env.sh" >/dev/null
aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null

mkdir -p "$STATE"; chmod 700 "$STATE"
: > "$STATE/images.env.tmp"
for i in "${images[@]}"; do
  ref="${i#*=}"; name="${ref%%:*}"; image="$REGISTRY/sdh-poc/$ref-$STAMP"
  arch="$(docker image inspect "$image" --format '{{.Architecture}}')"
  [[ "$arch" == "arm64" ]] || die "$image is $arch, not arm64"
  say "Pushing $image"
  docker push -q "$image" >/dev/null
  digest="$(docker image inspect "$image" --format '{{range .RepoDigests}}{{println .}}{{end}}' | grep "^$REGISTRY/sdh-poc/$name@" | head -n 1)"
  [[ -n "$digest" ]] || die "no digest for $image after the push"
  echo "${name//-/_}=$digest" >> "$STATE/images.env.tmp"
  say "  $digest"
done
mv "$STATE/images.env.tmp" "$STATE/images.env"
docker logout "$REGISTRY" >/dev/null 2>&1 || true
say "Done: digests in $STATE/images.env"
