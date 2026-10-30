#!/usr/bin/env bash
# Builds the release artifacts, generates their SBOMs, scans them for vulnerabilities and signs them, then verifies
# every signature (design §14.4, §14.5). Nothing is signed if a base image is stale or the scan fails its policy.
#
#   scripts/sign-release.sh                         # test run: throwaway key, throwaway registry in the VM
#   SDH_SIGNING_KEY=azurekms://<vault>.vault.azure.net/<key> SDH_REGISTRY=<registry>/sdh scripts/sign-release.sh
#   SDH_IT_STOP=1 scripts/sign-release.sh           # stop the VM afterwards
#   SDH_SKIP_BUILD=1 ...                            # reuse the tarball and images already built
#   SDH_ALLOW_STALE_BASE=1 ...                      # sign although a pinned base image has a newer build (warns)
#
# Signs:
#   - the distribution tarball: a detached signature (file-connector-<v>-dist.tar.gz.sig), verifiable with cosign or
#     with openssl alone;
#   - the customer image and the service image: cosign signatures in the registry, next to each image, plus a signed
#     CycloneDX SBOM of each image as an attestation.
# Writes to file-connector/target/release/: the tarball and its signature, the public key (cosign.pub), the SBOMs
# (jar, customer image, service image), their vulnerability reports (*.vulns.json, not published) and
# image-digests.txt.
#
# Environment:
#   SDH_SIGNING_KEY   cosign key reference. Unset: a throwaway key pair is generated for this run (testing only).
#                     Release: azurekms://<vault>.vault.azure.net/<key>; the private key never leaves Key Vault.
#                     Azure credentials come from AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET (a service
#                     principal with the Key Vault Crypto User role on the key). A key file path also works, with
#                     COSIGN_PASSWORD.
#   SDH_REGISTRY      where images are pushed and signed, e.g. myregistry.azurecr.io/sdh. Unset: a throwaway
#                     registry (registry:2) on localhost:5000 inside the VM. Log in with `docker login` first; the
#                     credentials in ~/.docker/config.json are passed to cosign.
#
# Signatures are not uploaded to Sigstore's public transparency log (Rekor), which would publish the artifact digests
# and key fingerprint; verification therefore passes --insecure-ignore-tlog=true (the key is the trust anchor).
# cosign, syft and the registry run as pinned containers in the minikube VM (see minikube-env.sh); nothing is
# installed on the host.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COSIGN_IMAGE="gcr.io/projectsigstore/cosign:v2.4.1"
SYFT_IMAGE="anchore/syft:v1.54.1"
REGISTRY_IMAGE="registry:2.8.3"
OUT="$ROOT/file-connector/target/release"

echo ">> Base images (file-connector/docker/base-images.env)"
if ! "$ROOT/scripts/base-images.sh"; then
  if [[ "${SDH_ALLOW_STALE_BASE:-0}" == "1" ]]; then
    echo "   WARNING: signing with a stale base image (SDH_ALLOW_STALE_BASE=1)"
  else
    echo "   Update the pins, rebuild and re-run the image tests first (or set SDH_ALLOW_STALE_BASE=1)" >&2
    exit 1
  fi
fi

if [[ "${SDH_SKIP_BUILD:-0}" != "1" ]]; then
  (cd "$ROOT" && ./mvnw -q -pl file-connector -am -Pdist package -DskipTests)
fi
VERSION="$(cd "$ROOT" && ./mvnw -q -N help:evaluate -Dexpression=project.version -DforceStdout)"
TARBALL="$ROOT/file-connector/target/file-connector-$VERSION-dist.tar.gz"
[[ -f "$TARBALL" ]] || { echo "No tarball at $TARBALL; build with -Pdist" >&2; exit 1; }

source "$ROOT/scripts/minikube-env.sh"
if [[ "${SDH_SKIP_BUILD:-0}" != "1" ]]; then
  SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 "$ROOT/scripts/build-image.sh" customer
  SDH_IT_STOP=0 SDH_SKIP_MAVEN=1 "$ROOT/scripts/build-image.sh" service
fi

# Files reach the tool containers through a volume in the VM: the host's directories aren't mounted there.
WORK_VOL="sdh-sign-$$"
docker volume create "$WORK_VOL" >/dev/null
HELPER="$(docker create -v "$WORK_VOL:/work" busybox:1.37.0 true)"
LOCAL_REGISTRY=""
cleanup() {
  docker rm -f "$HELPER" >/dev/null 2>&1 || true
  docker volume rm "$WORK_VOL" >/dev/null 2>&1 || true
  [[ -n "$LOCAL_REGISTRY" ]] && docker rm -f "$LOCAL_REGISTRY" >/dev/null 2>&1 || true
  stop_vm_if_asked
}
trap cleanup EXIT
put() { docker cp "$1" "$HELPER:/work/$2" >/dev/null; }
get() { docker cp "$HELPER:/work/$1" "$2" >/dev/null; }
AZURE_ENV=()
for v in AZURE_TENANT_ID AZURE_CLIENT_ID AZURE_CLIENT_SECRET; do
  [[ -n "${!v:-}" ]] && AZURE_ENV+=(-e "$v")
done
cosign() {
  docker run --rm --network host --user 0 -v "$WORK_VOL:/work" -w /work -e COSIGN_PASSWORD \
    -e DOCKER_CONFIG=/work/.docker "${AZURE_ENV[@]}" "$COSIGN_IMAGE" "$@"
}
SYFT_ENV=()
syft() {
  docker run --rm --network host --user 0 -v "$WORK_VOL:/work" -w /work -e DOCKER_CONFIG=/work/.docker \
    "${SYFT_ENV[@]}" "$SYFT_IMAGE" "$@"
}

mkdir -p "$OUT"
rm -f "$OUT"/*
docker run --rm -v "$WORK_VOL:/work" busybox:1.37.0 mkdir -p /work/.docker >/dev/null
[[ -f "$HOME/.docker/config.json" ]] && put "$HOME/.docker/config.json" .docker/config.json

echo ">> Signing key"
if [[ -z "${SDH_SIGNING_KEY:-}" ]]; then
  echo "   SDH_SIGNING_KEY is unset: generating a throwaway key pair (testing only)"
  export COSIGN_PASSWORD="test-only-$$"
  cosign generate-key-pair >/dev/null 2>&1
  KEY="/work/cosign.key"
elif [[ "$SDH_SIGNING_KEY" == *"://"* ]]; then
  KEY="$SDH_SIGNING_KEY"   # a KMS reference: the private key stays in the KMS
else
  put "$SDH_SIGNING_KEY" cosign.key
  KEY="/work/cosign.key"
fi
cosign public-key --key "$KEY" > "$OUT/cosign.pub"
put "$OUT/cosign.pub" cosign.pub
echo "   public key: $OUT/cosign.pub"

echo ">> Registry"
if [[ -z "${SDH_REGISTRY:-}" ]]; then
  LOCAL_REGISTRY="sdh-test-registry-$$"
  docker run -d --name "$LOCAL_REGISTRY" -p 127.0.0.1:5000:5000 "$REGISTRY_IMAGE" >/dev/null
  REGISTRY="localhost:5000/sdh"
  SYFT_ENV=(-e SYFT_REGISTRY_INSECURE_USE_HTTP=true)   # cosign treats localhost as plain HTTP by itself
  echo "   SDH_REGISTRY is unset: throwaway registry at $REGISTRY (inside the VM)"
else
  REGISTRY="$SDH_REGISTRY"
fi

echo ">> Distribution tarball"
TAR_NAME="$(basename "$TARBALL")"
cp "$TARBALL" "$OUT/"
cp "$ROOT/file-connector/target/sbom.cdx.json" "$OUT/file-connector-$VERSION-jar.cdx.json"
put "$TARBALL" "$TAR_NAME"

: > "$OUT/image-digests.txt"
for variant in customer service; do
  if [[ "$variant" == "customer" ]]; then name="file-connector"; else name="file-connector-service"; fi
  local_tag="sdh/$name:$VERSION"
  remote_tag="$REGISTRY/$name:$VERSION"
  echo ">> Image $local_tag"
  docker tag "$local_tag" "$remote_tag"
  docker push -q "$remote_tag" >/dev/null
  ref="$(docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$remote_tag" | grep "^$REGISTRY/$name@" | head -n 1)"
  [[ -n "$ref" ]] || { echo "No digest for $remote_tag" >&2; exit 1; }
  echo "$ref" >> "$OUT/image-digests.txt"
  echo "   pushed: $ref"
  # The image's own SBOM: OS packages and the Java components in app.jar.
  syft "registry:$ref" -q -o "cyclonedx-json=/work/$name.cdx.json"
  get "$name.cdx.json" "$OUT/$name-$VERSION-image.cdx.json"
  echo "   SBOM: $name-$VERSION-image.cdx.json"
done

# Pushed but not yet signed: an unsigned image doesn't verify, so nothing trusts it if the scan stops the release.
SDH_VULN_SCAN_IN_VM=1 "$ROOT/scripts/vuln-scan.sh" "$OUT"/*.cdx.json

echo ">> Signing"
cosign sign-blob --yes --key "$KEY" --tlog-upload=false --output-signature "/work/$TAR_NAME.sig" \
  "/work/$TAR_NAME" >/dev/null 2>&1
get "$TAR_NAME.sig" "$OUT/$TAR_NAME.sig"
chmod 0644 "$OUT/$TAR_NAME.sig"   # published alongside the tarball
echo "   $TAR_NAME.sig"
while read -r ref; do
  name="${ref##*/}"; name="${name%@*}"
  # Signed by digest, never by tag, so the signature covers exactly these bytes.
  cosign sign --yes --key "$KEY" --tlog-upload=false "$ref" >/dev/null 2>&1
  cosign attest --yes --key "$KEY" --tlog-upload=false --type cyclonedx --predicate "/work/$name.cdx.json" \
    "$ref" >/dev/null 2>&1
  echo "   $name, with its SBOM attested"
done < "$OUT/image-digests.txt"

echo ">> Verifying, as a customer would (public key only)"
failures=0
ok() { echo "   ok    $1"; }
bad() { echo "   FAIL  $1"; failures=$((failures + 1)); }
if cosign verify-blob --key /work/cosign.pub --insecure-ignore-tlog=true --signature "/work/$TAR_NAME.sig" \
  "/work/$TAR_NAME" >/dev/null 2>&1; then ok "tarball signature (cosign)"; else bad "tarball signature (cosign)"; fi
# Without cosign: the signature is base64 ECDSA over the file's SHA-256.
if base64 -d "$OUT/$TAR_NAME.sig" > "$OUT/.sig.der" \
  && openssl dgst -sha256 -verify "$OUT/cosign.pub" -signature "$OUT/.sig.der" "$OUT/$TAR_NAME" >/dev/null; then
  ok "tarball signature (openssl)"; else bad "tarball signature (openssl)"; fi
rm -f "$OUT/.sig.der"
# A changed byte must fail.
cp "$OUT/$TAR_NAME" "$OUT/.tampered"
printf 'x' | dd of="$OUT/.tampered" bs=1 seek=1000 conv=notrunc status=none
if base64 -d "$OUT/$TAR_NAME.sig" | openssl dgst -sha256 -verify "$OUT/cosign.pub" -signature /dev/stdin \
  "$OUT/.tampered" >/dev/null 2>&1; then bad "a tampered tarball is rejected"; else ok "a tampered tarball is rejected"; fi
rm -f "$OUT/.tampered"
while read -r ref; do
  name="${ref##*/}"; name="${name%@*}"
  if cosign verify --key /work/cosign.pub --insecure-ignore-tlog=true "$ref" >/dev/null 2>&1; then
    ok "$name signature"; else bad "$name signature"; fi
  if cosign verify-attestation --key /work/cosign.pub --insecure-ignore-tlog=true --type cyclonedx "$ref" \
    >/dev/null 2>&1; then ok "$name SBOM attestation"; else bad "$name SBOM attestation"; fi
done < "$OUT/image-digests.txt"
# Another key must not verify.
docker run --rm -v "$WORK_VOL:/work" busybox:1.37.0 mkdir -p /work/other >/dev/null
docker run --rm --user 0 -v "$WORK_VOL:/work" -w /work/other -e COSIGN_PASSWORD=other "$COSIGN_IMAGE" \
  generate-key-pair >/dev/null 2>&1
first="$(head -n 1 "$OUT/image-digests.txt")"
if cosign verify --key /work/other/cosign.pub --insecure-ignore-tlog=true "$first" >/dev/null 2>&1; then
  bad "another key is rejected"; else ok "another key is rejected"; fi

echo
ls -1 "$OUT"
if ((failures == 0)); then echo ">> Signed and verified"; else echo ">> $failures check(s) failed"; exit 1; fi
