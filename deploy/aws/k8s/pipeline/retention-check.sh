#!/usr/bin/env bash
# Registry retention on the PoC (docs/DEPLOYMENT.md "Registry retention", docs/PERFORMANCE.md §13.2): checks that
# the convert and transform stages' duties pods delete the registry rows of items finished longer ago than
# sdh.stage.retention.after, and only those. Retention can't delete a row less than a day old (the stage refuses
# an after under its queue's retention or its reconciler's lookback, each plus a day), so the check spans two
# sessions:
#
#   1. After a load test run with retention on (apply.sh SDH_STAGE_RETENTION_AFTER=25h
#      SDH_STAGE_QUEUE_RETENTION=1h SDH_RECONCILE_LOOKBACK=1h SDH_STAGE_RETENTION_INTERVAL=10m), and before
#      `poc.sh down` (never reset: that empties the registries): `retention-check.sh note`. It keeps each
#      stage's counts in $STATE/retention/noted.
#   2. At least `after` later: `poc.sh up`, apply.sh with the same settings (the duties pods prune at startup,
#      then every interval), then `retention-check.sh verify`. It waits for each stage's first
#      stage.registry_pruned (up to SDH_RETENTION_WAIT_MIN, 20), then compares: every item finished with no open
#      failure is gone, with its records; every item with an open failure is kept, with its records; no failure
#      is deleted.
#
# Counts only: no item ID, key or value is printed.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STATE="${SDH_POC_STATE:-$HOME/.local/state/sdh-poc}"
export KUBECONFIG="$STATE/kubeconfig"   # always the PoC's, never a sandbox's
P="${SDH_PIPELINES:-ccda-push}"; P="${P%% *}"; PREFIX="${P//-/_}"
WAIT_MIN="${SDH_RETENTION_WAIT_MIN:-20}"
NOTED="$STATE/retention/noted"

say() { printf '>> %s\n' "$*"; }
die() { printf 'retention-check.sh: %s\n' "$*" >&2; exit 1; }
db() { kubectl -n sdh-infra exec -i postgres-0 -- psql -U postgres -d sdh -At -F '|' -c "$1"; }

# counts <schema>: items, items finished with no open failure, items with an open failure, records of each kind,
# failures; one line, |-separated.
counts() {
  db "WITH f AS (SELECT DISTINCT item_id FROM $1.stage_failure WHERE state IN ('FAILED', 'REVISION_PENDING')),
    i AS (SELECT item_id, completed_at IS NOT NULL AND item_id NOT IN (SELECT item_id FROM f) AS done,
      item_id IN (SELECT item_id FROM f) AS open FROM $1.stage_item)
    SELECT (SELECT count(*) FROM i), (SELECT count(*) FROM i WHERE done), (SELECT count(*) FROM i WHERE open),
      (SELECT count(*) FROM $1.stage_record r JOIN i USING (item_id) WHERE i.done),
      (SELECT count(*) FROM $1.stage_record r JOIN i USING (item_id) WHERE i.open),
      (SELECT count(*) FROM $1.stage_failure)"
}
show() {  # <stage> <counts>
  local items done open rdone ropen failures
  IFS='|' read -r items done open rdone ropen failures <<< "$2"
  say "  $1: $items items ($done finished with no open failure, $rdone records; $open with an open failure," \
    "$ropen records); $failures failures"
}
# pruned <namespace> <deployment>: the duties pod's stage.registry_pruned lines, as "items records" each.
pruned() {
  kubectl -n "$1" logs "deploy/$2" 2>/dev/null \
    | sed -n 's/.*stage\.registry_pruned stage=[^ ]* items=\([0-9]*\) records=\([0-9]*\).*/\1 \2/p'
}

case "${1:-}" in
  note)
    mkdir -p "$(dirname "$NOTED")"; chmod 700 "$(dirname "$NOTED")"
    c="$(counts "convert_$PREFIX")"; t="$(counts "transform_$PREFIX")"
    { echo "noted_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      echo "last_completed=$(db "SELECT max(completed_at) FROM transform_$PREFIX.stage_item")"
      echo "convert=$c"; echo "transform=$t"; } > "$NOTED"
    say "Noted in $NOTED; the last item finished at $(sed -n 's/^last_completed=//p' "$NOTED")"
    show convert "$c"; show transform "$t"
    say "Next: poc.sh down (no reset), then at least the retention's after later: up, apply.sh with the same" \
      "settings, retention-check.sh verify"
    ;;
  verify)
    [[ -s "$NOTED" ]] || die "nothing noted ($NOTED): run note after the load test, before down"
    say "Noted: $(sed -n 's/^noted_at=//p' "$NOTED");" \
      "the last item finished at $(sed -n 's/^last_completed=//p' "$NOTED")"
    nc="$(sed -n 's/^convert=//p' "$NOTED")"; nt="$(sed -n 's/^transform=//p' "$NOTED")"
    show "convert, noted" "$nc"; show "transform, noted" "$nt"
    say "Waiting up to $WAIT_MIN minutes for each duties pod's first stage.registry_pruned"
    for ((i = 0; i < WAIT_MIN * 4; i++)); do
      [[ -n "$(pruned "sdh-$P" convert-duties)" && -n "$(pruned sdh-fhir "transform-$P-duties")" ]] && break
      sleep 15
    done
    fail=0
    for s in convert transform; do
      ns="sdh-$P" dep=convert-duties noted="$nc"
      [[ "$s" == convert ]] || { ns=sdh-fhir dep="transform-$P-duties" noted="$nt"; }
      now="$(counts "${s}_$PREFIX")"
      show "$s, now" "$now"
      read -r items rows < <(pruned "$ns" "$dep" | awk '{i += $1; r += $2} END {print i + 0, r + 0}')
      say "  $s: stage.registry_pruned deleted $items items and $rows records"
      IFS='|' read -r n_items n_done n_open n_rdone n_ropen n_failures <<< "$noted"
      IFS='|' read -r items_now done_now open_now rdone_now ropen_now failures_now <<< "$now"
      # Every finished item (noted) gone with its records, every item with an open failure kept with its records,
      # and no failure deleted. Items a new run added since would show in the counts now: there should be none.
      check() {
        [[ "$2" == "$3" ]] && say "    ok: $1 ($2)" || { say "    NOT AS EXPECTED: $1: $2, expected $3"; fail=1; }
      }
      check "$s items deleted" "$items" "$n_done"
      check "$s records deleted" "$rows" "$n_rdone"
      check "$s items left" "$items_now" "$((n_items - n_done))"
      check "$s items with an open failure kept" "$open_now" "$n_open"
      check "$s records of those kept" "$ropen_now" "$n_ropen"
      check "$s failures kept" "$failures_now" "$n_failures"
    done
    ((fail == 0)) && say "Retention as expected" || { say "Retention NOT as expected"; exit 1; }
    ;;
  *)
    echo "usage: retention-check.sh note|verify (see its header)" >&2; exit 2 ;;
esac
