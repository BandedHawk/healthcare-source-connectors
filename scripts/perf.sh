#!/usr/bin/env bash
# Performance tests (design §13.1): throughput and memory use. Generates synthetic C-CDA and X12 files (CSV on
# request) and runs each through the full ingest path. Not part of any normal build or of the integration tests.
# Results: connector-core/target/perf/throughput-<storage>-<recipient>.md (this run) and history.csv (every run
# until `mvn clean`, one row per format).
#
#   scripts/perf.sh                          # 1 GiB each of C-CDA and X12, Iceberg on local disk
#   scripts/perf.sh -Dperf.size=4GiB         # bigger files
#   scripts/perf.sh -Dperf.formats=ccda,x12,csv   # CSV as well
#   scripts/perf.sh -Dperf.storage=s3        # SeaweedFS through S3FileIO, in the minikube VM (KVM2)
#   scripts/perf.sh -Dperf.recipient=rsa     # messages sealed to an RSA-3072 recipient instead of P-256
#   scripts/perf.sh -Dperf.heap=512m         # a smaller heap than the default 1024m
#   SDH_IT_STOP=1 scripts/perf.sh -Dperf.storage=s3   # stop the VM afterwards
#
# The same run as a plain Maven task: ./mvnw -Pperf -pl connector-core -am verify [-Dperf.size=...]
# (NetBeans: right-click the root project > Run Maven > Performance test, and the variants beside it.)
#
# Needs a few GB free under connector-core/target for the input file and the stored tables.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

uses_s3=0
for a in "$@"; do
  [[ "$a" == "-Dperf.storage=s3" ]] && uses_s3=1
done
if [[ $uses_s3 == 1 ]]; then
  source "$ROOT/scripts/minikube-env.sh"
  export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
fi

rm -f "$ROOT"/connector-core/target/perf/throughput-*.md
status=0
(cd "$ROOT" && ./mvnw -q -Pperf -pl connector-core -am verify "$@") || status=$?
for f in "$ROOT"/connector-core/target/perf/throughput-*.md; do
  [[ -f "$f" ]] && { echo; cat "$f"; }
done
if [[ $uses_s3 == 1 ]]; then
  stop_vm_if_asked
fi
exit "$status"
