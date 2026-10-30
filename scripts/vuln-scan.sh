#!/usr/bin/env bash
# Scans CycloneDX SBOMs for known vulnerabilities with grype and applies the release policy (design §14.5).
#
#   scripts/vuln-scan.sh file-connector/target/release/*.cdx.json
#   scripts/vuln-scan.sh file-connector/target/sbom.cdx.json        # just the jar's dependencies
#   SDH_IT_STOP=1 scripts/vuln-scan.sh ...                          # stop the VM afterwards
#
# Policy: fails when any SBOM has a High or Critical vulnerability with a fix available (a newer package, library or
# base image exists), unless it is accepted in file-connector/docker/vuln-accepted.yaml with a reason. Unfixed and
# lower-severity findings are reported, not failed: there is nothing to upgrade to yet.
#
# Writes <sbom>.vulns.json (grype's full report) next to each SBOM, and prints a summary per SBOM and every finding
# that fails the policy. sign-release.sh runs this before signing; it can also be run on the SBOMs of an earlier
# release to see whether new vulnerabilities affect it. grype runs as a pinned container in the minikube VM; its
# vulnerability database is downloaded on each run (into a Docker volume, so only changes are fetched).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GRYPE_IMAGE="anchore/grype:v0.120.1"
ACCEPTED="$ROOT/file-connector/docker/vuln-accepted.yaml"
DB_VOL="sdh-grype-db"
(($# > 0)) || { echo "usage: $0 <sbom.cdx.json>..." >&2; exit 2; }
for f in "$@"; do [[ -f "$f" ]] || { echo "No such SBOM: $f" >&2; exit 2; }; done

if [[ -z "${SDH_VULN_SCAN_IN_VM:-}" ]]; then
  source "$ROOT/scripts/minikube-env.sh"
else
  stop_vm_if_asked() { :; }   # the caller (sign-release.sh) owns the VM
fi

WORK_VOL="sdh-scan-$$"
docker volume create "$WORK_VOL" >/dev/null
docker volume create "$DB_VOL" >/dev/null
HELPER="$(docker create -v "$WORK_VOL:/work" busybox:1.37.0 true)"
cleanup() {
  docker rm -f "$HELPER" >/dev/null 2>&1 || true
  docker volume rm "$WORK_VOL" >/dev/null 2>&1 || true
  stop_vm_if_asked
}
trap cleanup EXIT
docker cp "$ACCEPTED" "$HELPER:/work/vuln-accepted.yaml" >/dev/null

echo ">> Vulnerability database"
docker run --rm -v "$DB_VOL:/db" -e GRYPE_DB_CACHE_DIR=/db "$GRYPE_IMAGE" db update 2>&1 | sed 's/^/   /' | tail -n 2
docker run --rm -v "$DB_VOL:/db" -e GRYPE_DB_CACHE_DIR=/db "$GRYPE_IMAGE" db status 2>/dev/null \
  | grep -iE 'built|schema' | sed 's/^/   /' || true

failed=0
for sbom in "$@"; do
  name="$(basename "$sbom")"
  docker cp "$sbom" "$HELPER:/work/$name" >/dev/null
  # Report everything; the policy is applied below, so unfixed findings still show up in the report.
  docker run --rm -v "$WORK_VOL:/work" -v "$DB_VOL:/db" -e GRYPE_DB_CACHE_DIR=/db -e GRYPE_DB_AUTO_UPDATE=false \
    -e GRYPE_CHECK_FOR_APP_UPDATE=false "$GRYPE_IMAGE" "sbom:/work/$name" -c /work/vuln-accepted.yaml -q -o json \
    > "${sbom%.json}.vulns.json"
  echo ">> $name"
  python3 -I - "${sbom%.json}.vulns.json" <<'EOF' || failed=$((failed + 1))
import json, sys
from collections import Counter
report = json.load(open(sys.argv[1]))
matches, accepted = report.get("matches", []), report.get("ignoredMatches", [])
order = ["Critical", "High", "Medium", "Low", "Negligible", "Unknown"]
def fixed(m):
    return m["vulnerability"].get("fix", {}).get("state") == "fixed"
by = Counter((m["vulnerability"]["severity"], fixed(m)) for m in matches)
print("   " + ", ".join(f"{s} {by[(s, True)] + by[(s, False)]} ({by[(s, True)]} fixable)" for s in order
                      if by[(s, True)] + by[(s, False)]) if matches else "   no known vulnerabilities")
if accepted:
    print(f"   {len(accepted)} accepted in vuln-accepted.yaml")
failing = [m for m in matches if m["vulnerability"]["severity"] in ("Critical", "High") and fixed(m)]
seen = set()
for m in sorted(failing, key=lambda m: (order.index(m["vulnerability"]["severity"]), m["artifact"]["name"])):
    v, a = m["vulnerability"], m["artifact"]
    key = (v["id"], a["name"], a["version"])
    if key in seen:
        continue
    seen.add(key)
    print(f"   FAIL  {v['severity']:<8} {v['id']:<20} {a['name']} {a['version']} -> {', '.join(v['fix'].get('versions', []))}")
sys.exit(1 if failing else 0)
EOF
done

if ((failed == 0)); then
  echo ">> No fixable High or Critical vulnerabilities"
else
  echo ">> $failed SBOM(s) fail the policy: upgrade the package or base image (scripts/base-images.sh update), or" \
    "accept the finding in file-connector/docker/vuln-accepted.yaml with a reason" >&2
  exit 1
fi
