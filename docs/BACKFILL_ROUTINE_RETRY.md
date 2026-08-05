# Wave 1 Retry — Annual-Only Disclosers

Retry pass for 38 tickers that were stuck `staged` (gate-failed) under the
old all-or-nothing gate. `_rebuild_issuer` now passes the gate if *either*
the overall median fill rate *or* the 10-K-only median clears
`GATE_MEDIAN_FILL` (0.5) — see the "gate on annual-only fill rate" commit.
Simulating the new gate against the live ledger showed these 38 tickers
would now pass on their 10-K alone, even though interim 10-Qs correctly
show near-zero fill (they cross-reference the 10-K rather than restating
notionals — e.g. ITW's 10-Q says "See Note 8. Debt for additional
information" and nothing else).

The full list lives in `backfill/retry_list_annual_gate.txt` (one ticker
per line, `#` comments ignored). The other 31 originally-stuck tickers
(ROP, UNP, COR, MAS, PWR, TPL, NSC, etc.) are **not** on this list — their
10-Ks genuinely have nothing extractable either, and re-running them would
just burn quota for no gain.

## Why this needs a retry list, not `--next`

`prepare --next N` only seeds issuers that have **never** been seeded.
All 391 universe issuers have been seeded at least once, so `--next`
always returns 0 now. These 38 need to be **re-seeded explicitly**,
because their original extraction artifacts (`backfill/requests/`,
`backfill/results/`, `backfill/units/`) are gitignored and don't survive
between separate cloud sessions — only `backfill/state.csv` and
`manifest.json` persist. `prepare()` already detects this
(`_unit_artifacts_present`) and re-seeds automatically when artifacts are
missing.

## One static command, works across repeated fires

`prepare` now supports `--retry-file`, which reads a fixed ticker list and
seeds the next `--batch-size` tickers **that haven't committed yet**:

```
python -m src.backfill prepare --since 2025-01-01 \
    --retry-file backfill/retry_list_annual_gate.txt --batch-size 5
```

Each fire picks up wherever the last one left off (it checks
`backfill/state.csv` for tickers already `committed` and skips them), and
once nothing is left to seed it logs "nothing to seed" and exits cleanly
rather than looping. That means the **same routine prompt can be
scheduled to fire repeatedly** — no manual ticker-list editing between
runs — and it winds itself down after roughly 8 fires (38 tickers ÷ 5
per batch).

**Termination guarantee.** A ticker is dropped from the rotation once it
either commits *or* burns `--max-attempts` seed attempts (default 2)
without committing — the attempt counter is written to `state.csv`, so it
survives across fires. Without this cap, any ticker that kept failing the
gate would be re-seeded and re-processed on every fire forever (a full
session's quota each time, committing nothing), and a recurring routine
would never stop. With it, the routine provably reaches the "nothing to
seed" no-op even if some tickers never pass. Tickers that exhaust their
attempts are left in `review_queue.csv` for manual attention rather than
silently retried forever.

## Routine prompt

Copy this verbatim into the routine prompt field. Schedule it (e.g. every
4-6 hours) until the retry list is exhausted, then pause the routine.

IMPORTANT — set the routine's branch to `claude/determined-franklin-1yztdn`
in the routine config, AND keep Step 0 below. All the backfill code
(`src/backfill.py`, the retry list) lives ONLY on that branch — it has
never been merged to `master`. If a fire starts on `master`, `src.backfill`
and everything else will look "missing"; that is a wrong-branch symptom,
NOT a missing module. Step 0 defends against it.

```
Run one Wave 1 RETRY batch for the sec-derivatives- repo.

Branch: claude/determined-franklin-1yztdn
Working dir: repo root

Context: the tickers in backfill/retry_list_annual_gate.txt previously
failed the backfill gate because their 10-Qs cross-reference the 10-K
instead of restating derivatives notionals (a genuine disclosure pattern,
not an extraction bug). The gate now passes on a strong 10-K alone. Their
original request/result artifacts are gone (gitignored, ephemeral), so
this re-seeds them from scratch.

BUDGET RULE: This routine MUST complete in a single session. Do NOT
continue into a second quota window. If you hit a spend-cap or context
warning, skip to Step 6 immediately and commit whatever is done.

Step 0 — Get onto the right branch FIRST (do not skip):
    git fetch origin claude/determined-franklin-1yztdn
    git checkout claude/determined-franklin-1yztdn
    git pull --ff-only origin claude/determined-franklin-1yztdn
    test -f src/backfill.py || { echo "ABORT: src/backfill.py absent after checkout"; exit 1; }

All the backfill code lives ONLY on this branch, never on master. If
src/backfill.py is absent after checkout, the branch checkout failed —
STOP and report "wrong branch / checkout failed". Do NOT try to
reimplement src.backfill, do NOT fall back to src.cc_bridge, do NOT
switch directory layouts. The module exists; a fresh clone just needs
the branch checked out.

Step 1 — Seed the next batch from the retry list:
    python -m src.backfill prepare --since 2025-01-01 \
        --retry-file backfill/retry_list_annual_gate.txt --batch-size 5

This seeds up to 5 tickers that are on the retry list, not yet committed,
and not yet out of attempts. If the log contains "nothing to seed", there
is no more work for this routine — commit nothing, skip straight to git
status and stop; do not fall back to --next or process anything else.

Step 2 — Process request files using SUBAGENTS.

CRITICAL RULES:
  - You MUST use the Agent tool for each ticker — do NOT read request
    files or write result files in the main conversation. Every request
    file read in the main context wastes budget.
  - Launch subagents SERIALLY — one at a time, wait for completion
    before launching the next. Parallel subagents trip the spend cap.
  - Each subagent gets ONE ticker and a fresh context.

For each ticker that has pending requests in backfill/requests/,
launch one subagent with this prompt (fill in {TICKER}):

    Process all backfill/requests/{TICKER}_*.json files that do NOT
    yet have a matching result in backfill/results/. For each file:

    If the filename contains "_locate":
      Read the 'prompt' field. Decide whether the target section
      exists in the supplied filing text windows. Write a result
      JSON to backfill/results/ with the SAME filename:
        {"found": true, "heading_text": "...", "start_anchor": "...",
         "end_anchor": "..." or null}
      OR
        {"found": false, "reason": "not_disclosed" | "insufficient_context",
         "note": "..."}

    If the filename contains "_extract":
      Read 'section_text' + 'schema'. Extract numeric values for each
      schema field. Write result JSON to backfill/results/:
        {"fields": {"field_name": {"value": N, "evidence": "..."}, ...},
         "flags": ["any concerns"]}
      Use null for fields not in the text. Numbers in millions unless
      schema says billions. Don't fabricate values — null is fine.

Step 3 — Apply locate results:
    python -m src.backfill resolve

Step 4 — If resolve produced new pending extraction requests (check
backfill/requests/manifest.json for pending items), launch one more
subagent per affected ticker to process them, same SERIAL pattern.

Step 5 — Stage, gate, and cut over:
    python -m src.backfill finalize --commit

Step 6 — Commit and push:
    git add -A
    git commit -m "backfill retry: annual-only gate — <N> issuers committed

    Issuers: <comma-separated tickers that passed gate>
    Gate failures: <tickers that still didn't pass, or 'none'>
    "
    git push -u origin claude/determined-franklin-1yztdn

STOP HERE. Do not loop back. Do not process more tickers than this batch
seeded. This routine is done after Step 6.
```

## Monitoring

- Each fire commits a `backfill retry: annual-only gate` commit naming
  which tickers passed.
- `python -m src.backfill status` shows the ledger state; all 38 should
  end up `committed`.
- Any that still fail (unexpected — the simulation said all 38 should
  pass) are worth a manual look; they may have a different failure mode
  than the annual-only pattern this fix targets. Each gets `--max-attempts`
  (default 2) re-tries, then is abandoned and logged to `review_queue.csv`
  — it will NOT be re-seeded on every fire after that.
- Once `prepare --retry-file ...` reports "nothing to seed" for the first
  time, the retry is done — stop the routine. From then on it's a harmless
  no-op, but a stopped routine doesn't burn scheduled quota, so disable it.
- To force another pass at abandoned tickers (e.g. after fixing a config),
  bump `--max-attempts` or clear their `attempts` in `state.csv`.
