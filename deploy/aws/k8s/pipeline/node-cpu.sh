#!/usr/bin/env bash
# Prints each node's CPU since it started, for the cost of a load run (docs/PERFORMANCE.md §10): one line a node,
# "<node> <instance type> <CPU seconds used> <seconds since it started>", from the kubelet's stats summary. A T
# instance (t4g) runs in unlimited mode, EC2's default for T instances: CPU above its baseline spends surplus credits,
# charged on top of the hour's price when the node ends unless repaid; measure.sh works them out from these lines.
# load-test.sh saves them at t0 ($STATE/load/<run>/nodecpu0); measure.sh reads them again at the end. Prints node
# names, types and numbers only.
#
#   deploy/aws/k8s/pipeline/node-cpu.sh
set -euo pipefail

STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's

now="$(date -u +%s)"
kubectl get nodes -o go-template='{{range .items}}{{.metadata.name}} {{index .metadata.labels "node.kubernetes.io/instance-type"}} {{.metadata.creationTimestamp}}{{"\n"}}{{end}}' \
  | while read -r node type created; do
      used="$(kubectl get --raw "/api/v1/nodes/$node/proxy/stats/summary" 2>/dev/null \
        | python3 -I -c 'import json, sys
try:
    print("%.0f" % (json.load(sys.stdin)["node"]["cpu"]["usageCoreNanoSeconds"] / 1e9))
except Exception:
    print("-")')"
      [[ "$used" == - ]] && continue
      echo "$node $type $used $((now - $(date -u -d "$created" +%s)))"
    done
