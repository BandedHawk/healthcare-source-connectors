#!/usr/bin/env bash
# The control API's test harness (docs/CONTROL-API-DESIGN.md §14): one set of scenarios, run against a target.
#
#   scripts/harness.sh testcontainers [scenario,...]   # the pipeline on Testcontainers in the sdh-it VM (H1)
#   SDH_HARNESS_KAFKA_LOGIN=certificate scripts/harness.sh testcontainers   # the stages log in to Kafka with
#                                                      # client certificates (mutual TLS) instead of SCRAM
#   scripts/harness.sh sandbox [scenario,...]          # the minikube sandbox's harness pipeline (H2)
#   scripts/harness.sh aws [scenario,...]              # the AWS PoC stack's harness pipeline (H3)
#
# With no scenarios named, all run, in order: support, access, load, ... (control-harness's Scenarios). The
# Testcontainers level builds the stages' jars, runs HarnessIT through scripts/it-minikube.sh, and stops the sdh-it
# VM afterwards unless SDH_IT_STOP=0. Each run's stage logs and test keys are under
# control-harness/target/harness/<time>/.
#
# The sandbox level runs the stages' customer images as `scripts/sandbox.sh images` last built them, so rebuild
# them first after changing a stage. It starts the sandbox if it is stopped (sdh-it must be stopped: the host
# holds one or the other), stops the demo pipelines and deploys the harness pipeline (`sandbox.sh harness-up`),
# runs HarnessIT from this machine against it, then removes the harness pipeline and starts the demo pipelines
# again (`harness-down`), and stops the sandbox if it wasn't running before, unless SDH_SANDBOX_STOP=0.
#
# The AWS level needs a PoC session up (deploy/aws/poc.sh up; it doesn't start one, since a session costs money)
# and the images as deploy/aws/build-images.sh last pushed them. It holds the PoC's pipelines and deploys the
# harness pipeline (`poc.sh harness-up`), runs HarnessIT from this machine against it, through port-forwards,
# then removes the harness pipeline and starts the pipelines again (`harness-down`). The session is left up:
# bring it down with `poc.sh down`.
#
# Synthetic data only.
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
      "-Dsdh.harness.scenarios=$scenarios" "-Dsdh.harness.kafka-login=${SDH_HARNESS_KAFKA_LOGIN:-scram}"
    ;;
  sandbox)
    # shellcheck source=minikube-home.sh
    source "$ROOT/scripts/minikube-home.sh" || exit 1
    profile="${SDH_DEMO_PROFILE:-sdh-demo}"
    export KUBECONFIG="$SDH_KUBE_DIR/$profile"
    was="$(minikube status -p "$profile" --format '{{.Host}}' 2>/dev/null | head -n 1 || true)"
    finish() {
      local status=$?
      "$ROOT/scripts/sandbox.sh" harness-down || status=1
      if [[ "$was" != "Running" && "${SDH_SANDBOX_STOP:-1}" == "1" ]]; then
        echo ">> Stopping the sandbox, as it was before"
        "$ROOT/scripts/sandbox.sh" stop || status=1
      fi
      exit "$status"
    }
    trap finish EXIT
    "$ROOT/scripts/sandbox.sh" harness-up
    echo ">> Running the scenarios${scenarios:+: $scenarios}"
    (cd "$ROOT" && ./mvnw -o -Pit -pl control-harness verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
      -Dit.test=HarnessIT -Dfailsafe.failIfNoSpecifiedTests=false -Dsdh.harness.target=sandbox \
      "-Dsdh.harness.state=$SDH_SANDBOX/sdh-demo/state" "-Dsdh.harness.profile=$profile" \
      "-Dsdh.harness.scenarios=$scenarios")
    ;;
  aws)
    poc="$ROOT/deploy/aws/poc.sh"
    state="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
    finish() {
      local status=$?
      "$poc" harness-down || status=1
      exit "$status"
    }
    trap finish EXIT
    "$poc" harness-up
    echo ">> Running the scenarios${scenarios:+: $scenarios}"
    (cd "$ROOT" && ./mvnw -o -Pit -pl control-harness verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
      -Dit.test=HarnessIT -Dfailsafe.failIfNoSpecifiedTests=false -Dsdh.harness.target=aws \
      "-Dsdh.harness.state=$state" "-Dsdh.harness.profile=${AWS_PROFILE:-claude-poc}" \
      "-Dsdh.harness.scenarios=$scenarios")
    ;;
  *)
    echo "usage: scripts/harness.sh <testcontainers|sandbox|aws> [scenario,...]" >&2
    exit 2
    ;;
esac
