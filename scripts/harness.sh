#!/usr/bin/env bash
# The control API's test harness (docs/CONTROL-API-DESIGN.md §14): one set of scenarios, run against a target.
#
#   scripts/harness.sh testcontainers [scenario,...]   # the pipeline on Testcontainers in the sdh-it VM (H1)
#   scripts/harness.sh sandbox [scenario,...]          # the minikube sandbox (H2, not yet built)
#   scripts/harness.sh aws [scenario,...]              # the AWS PoC stack (H3, not yet built)
#
# With no scenarios named, all run, in order: support, access, load, ... (control-harness's Scenarios). The
# Testcontainers level builds the stages' jars, runs HarnessIT through scripts/it-minikube.sh, and stops the sdh-it
# VM afterwards unless SDH_IT_STOP=0. Each run's stage logs and test keys are under
# control-harness/target/harness/<time>/. Synthetic data only.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="${1:-}"
scenarios="${2:-}"

case "$target" in
  testcontainers)
    echo ">> Building the stages' jars"
    (cd "$ROOT" && ./mvnw -o -q -pl landing-connector,file-connector,convert-stage,control-harness -am package \
      -DskipTests)
    echo ">> Running the scenarios${scenarios:+: $scenarios}"
    SDH_IT_STOP="${SDH_IT_STOP:-1}" "$ROOT/scripts/it-minikube.sh" -o -pl control-harness \
      -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
      -Dit.test=HarnessIT -Dfailsafe.failIfNoSpecifiedTests=false \
      "-Dsdh.harness.scenarios=$scenarios"
    ;;
  sandbox|aws)
    echo "The $target target isn't built yet (CONTROL-API-DESIGN.md §15, H2 and H3)." >&2
    exit 2
    ;;
  *)
    echo "usage: scripts/harness.sh <testcontainers|sandbox|aws> [scenario,...]" >&2
    exit 2
    ;;
esac
