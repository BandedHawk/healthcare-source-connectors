#!/usr/bin/env bash
# Checks or updates the pinned base images (design §14.5) against their registries.
#
#   scripts/base-images.sh            # check: exit 1 if any pinned tag has moved to a new digest
#   scripts/base-images.sh update     # rewrite file-connector/docker/base-images.env with the current digests
#
# Each pin is <image>:<tag>@sha256:<digest>. The tag is what we follow; the digest is what we build. When a tag
# moves, its maintainers have published a new build of it, usually with security fixes. After an update, rebuild
# and re-run the image tests before releasing:
#   scripts/it-minikube.sh -Dit.test='*ImageIT' -Dfailsafe.failIfNoSpecifiedTests=false
#
# Renovate (renovate.json) does the same check on GitHub and opens a pull request; this script is for doing it by
# hand, and sign-release.sh runs the check before signing. It talks to the registries directly (curl), so it needs
# neither Docker nor the VM.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PINS="$ROOT/file-connector/docker/base-images.env"
MODE="${1:-check}"
[[ "$MODE" == "check" || "$MODE" == "update" ]] || { echo "usage: $0 [check|update]" >&2; exit 2; }

ACCEPT="application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json"
ACCEPT+=",application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"

# The digest a tag points to now: the index (multi-platform) digest, the same one `docker pull <tag>` records.
current_digest() {
  local image="$1" tag="$2" registry repo url headers token=""
  if [[ "$image" == */* && "${image%%/*}" == *.* ]]; then
    registry="${image%%/*}"; repo="${image#*/}"
  else
    registry="registry-1.docker.io"; repo="$image"; [[ "$repo" == */* ]] || repo="library/$repo"
  fi
  url="https://$registry/v2/$repo/manifests/$tag"
  headers="$(curl -sSI -H "Accept: $ACCEPT" "$url")"
  if grep -qi '^HTTP/[0-9.]* 401' <<<"$headers"; then
    # Anonymous pull token, from the realm the registry names (Docker Hub, GHCR and others work this way).
    local challenge realm service
    challenge="$(grep -i '^www-authenticate:' <<<"$headers" | tr -d '\r')"
    realm="$(sed -n 's/.*realm="\([^"]*\)".*/\1/p' <<<"$challenge")"
    service="$(sed -n 's/.*service="\([^"]*\)".*/\1/p' <<<"$challenge")"
    token="$(curl -sSf -G "$realm" --data-urlencode "service=$service" \
      --data-urlencode "scope=repository:$repo:pull" | sed -n 's/.*"token" *: *"\([^"]*\)".*/\1/p')"
    headers="$(curl -sSI -H "Accept: $ACCEPT" -H "Authorization: Bearer $token" "$url")"
  fi
  grep -qi '^HTTP/[0-9.]* 200' <<<"$headers" || { echo "cannot read $image:$tag from $registry" >&2; return 1; }
  grep -i '^docker-content-digest:' <<<"$headers" | awk '{print $2}' | tr -d '\r'
}

stale=0
updated="$(mktemp)"
trap 'rm -f "$updated"' EXIT
while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ "$line" =~ ^([A-Z_]+)=([^:@[:space:]]+(/[^:@[:space:]]+)*):([^@[:space:]]+)@(sha256:[0-9a-f]{64})$ ]]; then
    name="${BASH_REMATCH[1]}"; image="${BASH_REMATCH[2]}"; tag="${BASH_REMATCH[4]}"; pinned="${BASH_REMATCH[5]}"
    now="$(current_digest "$image" "$tag")"
    if [[ "$now" == "$pinned" ]]; then
      echo "   current  $name  $image:$tag"
    else
      echo "   STALE    $name  $image:$tag  pinned ${pinned:7:12}, now ${now:7:12}"
      stale=$((stale + 1))
      line="$name=$image:$tag@$now"
    fi
  elif [[ "$line" == *_IMAGE=* ]]; then
    echo "Not a <image>:<tag>@sha256:<digest> pin: $line" >&2; exit 2
  fi
  printf '%s\n' "$line" >> "$updated"
done < "$PINS"

if ((stale == 0)); then
  echo ">> Base images are current"
elif [[ "$MODE" == "update" ]]; then
  cat "$updated" > "$PINS"
  echo ">> Updated $stale pin(s) in ${PINS#$ROOT/}; rebuild and re-run the image tests"
else
  echo ">> $stale base image(s) have newer builds: run scripts/base-images.sh update" >&2
  exit 1
fi
