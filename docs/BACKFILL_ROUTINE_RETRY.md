# Wave 1 Retry — Annual-Only Disclosers

One-off retry pass for 38 tickers that were stuck `staged` (gate-failed)
under the old all-or-nothing gate. `_rebuild_issuer` now passes the gate
if *either* the overall median fill rate *or* the 10-K-only median clears
`GATE_MEDIAN_FILL` (0.5) — see the "gate on annual-only fill rate" commit.
Simulating the new gate against the live ledger showed these 38 tickers
would now pass on their 10-K alone, even though interim 10-Qs correctly
show near-zero fill (they cross-reference the 10-K rather than restating
notionals — e.g. ITW's 10-Q says "See Note 8. Debt for additional
information" and nothing else).

The other 31 originally-stuck tickers (ROP, UNP, COR, MAS, PWR, TPL, NSC,
etc.) are **not** in this list — their 10-Ks genuinely have nothing
extractable either, and re-running them would just burn quota for no
gain.

## Why this needs `--tickers`, not `--next`

`prepare --next N` only seeds issuers that have **never** been seeded.
All 391 universe issuers have been seeded at least once, so `--next` now
always returns 0. These 38 need to be **re-seeded explicitly by name**,
because their original extraction artifacts (`backfill/requests/`,
`backfill/results/`, `backfill/units/`) are gitignored and don't survive
between separate cloud sessions — only `backfill/state.csv` and
`manifest.json` persist. `prepare()` already detects this
(`_unit_artifacts_present`) and re-seeds automatically when artifacts are
missing, so this is a normal chunk — just targeted by ticker instead of
"next unseeded".

## Batches

38 tickers is too many for one session (Wave 1 chunks were capped at
3–4 to avoid the context blowup documented in `BACKFILL_ROUTINE.md`).
Run these as **8 separate one-off sessions** (not a recurring cron),
back to back or spread across a day:

1. `ITW,GE,GM,XOM,FDX`
2. `CI,CHTR,DAL,DDOG,DG`
3. `ETN,FOXA,HLT,IEX,ISRG`
4. `JBL,LDOS,LII,LOW,NSC`
5. `NXPI,ON,PLTR,PNR,SRE`
6. `SW,SWK,SWKS,TDY,TECH`
7. `TEL,VRSN,WAT,XYL,YUM`
8. `HSIC,LHX,UAL`

## Retry prompt (fill in {BATCH} with one line from the list above)

Copy this verbatim, substituting the ticker list for `{BATCH}`:

```
Run one Wave 1 RETRY batch for the sec-derivatives- repo.

Branch: claude/determined-franklin-1yztdn
Working dir: repo root

Context: these tickers previously failed the backfill gate because their
10-Qs cross-reference the 10-K instead of restating derivatives notionals
(a genuine disclosure pattern, not an extraction bug). The gate now passes
on a strong 10-K alone. Their original request/result artifacts are gone
(gitignored, ephemeral), so this re-seeds them from scratch.

BUDGET RULE: This routine MUST complete in a single session. Do NOT
continue into a second quota window. If you hit a spend-cap or context
warning, skip to Step 6 immediately and commit whatever is done.

Step 1 — Re-seed this batch explicitly by ticker:
    python -m src.backfill prepare --since 2025-01-01 --tickers {BATCH}

This re-fetches filings and writes fresh request files to
backfill/requests/ for these 5 tickers (artifacts from any prior session
are gone, so prepare will detect that and regenerate them).

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

STOP HERE. Do not loop back. Do not process more tickers. Do not
start the next batch. This routine is done after Step 6.
```

## After all 8 batches

- Check `python -m src.backfill status` (or `state.csv`) — all 38 should
  show `committed`.
- Any that still fail (unexpected — the simulation said all 38 should
  pass) are worth a manual look; they may have a different failure mode
  than the annual-only pattern this fix targets.
- Once done, this doc's job is finished — go back to the steady-state
  Wave 1 routine (`BACKFILL_ROUTINE.md`) only if new issuers get added to
  the universe; otherwise the routine can be paused, since `--next` will
  keep returning 0.
