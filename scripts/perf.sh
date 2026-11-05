#!/usr/bin/env bash
# Performance tests (design §13.1): throughput, per-item latency and memory use of each part of the pipeline, in
# shared columns. Not part of any normal build or of the integration tests. The stages, in order (-Dperf.stages):
#   core      split, encrypt and store without the apps (connector-core ThroughputPerf)
#   landing   the real landing node (landing-connector LandingPerf): bulk files through a directory, many small
#             C-CDA documents at a steady rate through a directory and through HTTP push with mTLS
#   split     the real splitter (file-connector SplitterPerf), fed the same items at the landing run's
#             announcements; then end to end, each item's landing plus splitting latency
# Results, in target/perf/ at the root: pipeline-summary.md (each workload's stages side by side), <stage>.md (each
# stage's latest run) and history.csv (every run until `mvn clean`, one row per stage, workload and intake).
#
#   scripts/perf.sh                          # every stage: 1 GiB bulk files, 1000 small documents at 10/s
#   scripts/perf.sh -Dperf.stages=landing    # one stage; landing,split for both apps and end to end
#   scripts/perf.sh -Dperf.size=64MiB -Dperf.items=100 -Dperf.rate=20   # quick
#   scripts/perf.sh -Dperf.size=4GiB         # bigger bulk files
#   scripts/perf.sh -Dperf.rate=0            # small documents all at once: the most each app can take
#   scripts/perf.sh -Dperf.workloads=small-ccda -Dperf.intakes=http   # one workload, one intake
#   scripts/perf.sh -Dperf.formats=ccda,x12,csv   # CSV as well, in the core stage
#   scripts/perf.sh -Dperf.storage=s3        # SeaweedFS through S3FileIO, in the minikube VM (KVM2)
#   scripts/perf.sh -Dperf.recipient=rsa     # core: messages sealed to an RSA-3072 recipient instead of P-256
#   scripts/perf.sh -Dperf.heap=512m         # a smaller heap than the default 1024m
#   SDH_IT_STOP=1 scripts/perf.sh -Dperf.storage=s3   # stop the VM afterwards
#
# The same run as a plain Maven task: ./mvnw -Pperf -pl landing-connector,file-connector -am verify [-D...]
# (NetBeans: right-click the root project > Run Maven > Performance test, and the variants beside it.)
#
# Needs a few GB free under each module's target for the input files and the stored tables.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/target/perf"

uses_s3=0
for a in "$@"; do
  [[ "$a" == "-Dperf.storage=s3" ]] && uses_s3=1
done
if [[ $uses_s3 == 1 ]]; then
  source "$ROOT/scripts/minikube-env.sh"
  export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
fi

status=0
(cd "$ROOT" && ./mvnw -q -Pperf -pl landing-connector,file-connector -am verify "$@") || status=$?
if [[ -f "$OUT/pipeline-summary.md" ]]; then
  echo
  cat "$OUT/pipeline-summary.md"
fi
if [[ $uses_s3 == 1 ]]; then
  stop_vm_if_asked
fi
exit "$status"
