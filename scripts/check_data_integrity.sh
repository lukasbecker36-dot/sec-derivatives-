#!/usr/bin/env bash
#
# Data-integrity gate. Run this BEFORE generating or sending a digest.
#
# A weekly briefing once reported Microsoft's non-designated FX notional as
# "+352%". The $63.5B figure was real, but the percentage was computed against
# a corrupted prior value. A percentage is only as trustworthy as both of its
# endpoints, so the corpus has to be checked before anything is written about
# it -- not after a reader queries the number.
#
# The corpus carries known defects that need re-extraction to clear, so this
# gate fails on REGRESSION rather than demanding zero defects: the totals must
# not grow, and misaligned_row / stale_null must stay at zero. Those two are
# zero-tolerance because both were driven to zero and both silently corrupt
# figures that otherwise look sound -- misaligned rows put values in the wrong
# fields, stale_null means correct data exists on another branch and is being
# ignored.
#
# Usage:
#   scripts/check_data_integrity.sh              # gate against the baseline
#   scripts/check_data_integrity.sh --report     # print findings, never fail
#
# After legitimately reducing defects, re-record the baseline so the gate
# ratchets down and cannot drift back up:
#   python -m src.audit --write-baseline audit_baseline.json
#
# Exit codes: 0 = safe to proceed, 1 = regression, do not publish.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BASELINE="${AUDIT_BASELINE:-audit_baseline.json}"
REPORT_JSON="${AUDIT_REPORT:-audit_report.json}"

if [ "${1:-}" = "--report" ]; then
  python -m src.audit --json "$REPORT_JSON" --exit-zero
  exit 0
fi

if [ ! -f "$BASELINE" ]; then
  echo "ERROR: baseline '$BASELINE' not found."
  echo "Record one with: python -m src.audit --write-baseline $BASELINE"
  exit 1
fi

python -m src.audit --baseline "$BASELINE" --json "$REPORT_JSON" --limit 25
STATUS=$?

if [ $STATUS -ne 0 ]; then
  cat <<'EOF'

------------------------------------------------------------------
DO NOT PUBLISH A DIGEST FROM THIS DATA.

Data quality regressed against the recorded baseline. Any figure
derived from it -- especially a percentage change -- may be wrong
even where the underlying number looks plausible.

Investigate with:
  python -m src.audit --limit 60
  python -m src.audit --compare-ref origin/master   # find data that exists elsewhere

If the regression is understood and acceptable, re-record the
baseline explicitly. Do not pass --exit-zero to silence this.
------------------------------------------------------------------
EOF
fi

exit $STATUS
