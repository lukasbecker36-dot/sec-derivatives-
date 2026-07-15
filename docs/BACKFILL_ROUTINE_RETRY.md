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
`backfill/state.csv` for tickers already marked `committed` and skips
them), and once all 38 have committed, it logs "all tickers already
committed — nothing to seed" and exits cleanly rather than looping.
That means the **same routine prompt can be scheduled to fire repeatedly**
— no manual ticker-list editing between runs — and it will naturally wind
itself down after roughly 8 fires (38 tickers ÷ 5 per batch).

## Routine prompt

Copy this verbatim into the routine prompt field. Point the routine at
this branch and schedule it (e.g. every 4-6 hours) until the retry list
is exhausted, then switch back to the standard `BACKFILL_ROUTINE.md`
prompt or pause the routine.

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

Step 1 — Seed the next batch from the retry list:
    python -m src.backfill prepare --since 2025-01-01 \
        --retry-file backfill/retry_list_annual_gate.txt --batch-size 5

This seeds up to 5 tickers that are on the retry list and not yet
committed. If it logs "all tickers already committed — nothing to seed",
there is no more work for this routine — commit nothing, skip straight to
git status and stop; do not fall back to --next or process anything else.

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
  than the annual-only pattern this fix targets. They'll keep showing up
  in each subsequent fire's batch until you either fix them or remove
  them from the retry list.
- Once `prepare --retry-file ...` reports "nothing to seed" for the first
  time, the retry is done — stop the routine (or let it keep firing; it's
  a harmless no-op from then on, just wasted quota).
