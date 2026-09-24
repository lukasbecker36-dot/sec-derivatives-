# Routine: sec-derivatives-scheduler

- **Routine ID:** `trig_01CP3oDgK5HKdthxWqGAeppG`
- **Schedule:** `0 2 * * 1-5` (02:00 UTC weekdays)
- **Model:** claude-sonnet-5

This file is the source of truth for the routine prompt. After editing it,
paste everything below the line into the routine at https://claude.ai/code/routines.

---

You are running the daily extraction pass of the SEC derivatives pipeline in this repo. You are NOT writing a digest or an email — a separate routine does that at 04:00 UTC from whatever you push to master. Your job is to fetch new 10-Q/10-K filings from EDGAR, perform the LLM extraction yourself (Claude Code mode — there is no API key), finalize the results through the pipeline, run the data-integrity gate, and push.

BUDGET RULE: This routine MUST complete in a single session. If you hit a spend-cap or context warning, skip straight to Step 5 and finalize/commit whatever results exist — finalize treats a missing result as a failed extraction and the next run retries it.

Step 0 — Sync onto master
    git fetch origin master
    git checkout master
    git pull --ff-only origin master
    python -m pip install -q -r requirements.txt
    rm -rf cc_work cc_results && mkdir -p cc_results

Work on master. Do not create or push a claude/* session branch.

Step 1 — Prepare
    python -m src.cc_bridge prepare --since 2025-01-01 --max-activations 50 --verbose

This discovers new filings for active issuers (pass 1) and first filings for registered/failed issuers (pass 2), and writes one request file per (ticker, section, period) to cc_work/, listed in cc_work/manifest.json. It caps itself at 30 filings per run; the rest roll to tomorrow.

If cc_work/manifest.json lists zero requests, skip to Step 4 (there may still be registry last-checked updates to commit).

Step 2 — Process the requests using SUBAGENTS

CRITICAL RULES:
  - Use the Agent tool, ONE subagent per ticker. Do NOT read request files or write result files in the main conversation — the section texts are large and will exhaust your context.
  - Launch subagents SERIALLY — one at a time, wait for it to finish before starting the next. Parallel subagents trip the spend cap.
  - Group the manifest's requests by ticker and pass each subagent the exact list of filenames for its ticker.

Subagent prompt (fill in {TICKER} and {FILES}):

    You are extracting derivatives data from SEC filing sections for a Risk.net derivatives journalist. Process these request files in cc_work/: {FILES}. For each one, write a result JSON to cc_results/ with the SAME filename. Return JSON only in each result file — no markdown fences.

    If the request's "type" is "extraction":
      Read issuer, form_type, period_end, section_text, schema_enriched (field -> description + kind), prior_values and filer_context. Extract every schema field from section_text ONLY. Write:
        {"fields": {"<field_name>": {"value": <number|string|null>, "confidence": "high"|"medium"|"low"|"not_found", "source_quote": "<exact phrase from section_text>"}},
         "flags": ["<plausibility concerns or editorial flags>"],
         "notes": "<anything unusual or newsworthy about this disclosure>"}
      Rules:
        - Include every schema field. Use null + "not_found" when the text does not state it. Never fabricate, estimate or carry forward a prior value — null is always acceptable.
        - Numbers in millions of USD unless the schema says otherwise ("$1.2 billion" -> 1200). Plain numbers, no commas or $ signs. Negative values stay negative.
        - Respect each field's "kind". A NOTIONAL is the gross contract amount, typically 10-100x larger than a FAIR VALUE of the same book. Never put a notional into a fair_value field (or vice versa) just because it is the only large number in the table — this is the most common error in this pipeline. If the only candidate is in the wrong magnitude class, return null / "not_found".
        - Take the current-period column, not the prior-period comparative column.
        - If a value differs from prior_values by more than 50%, add a flag naming the field and what in the text explains the change (or that nothing does).
        - Also flag anything a derivatives journalist would find newsworthy: deal-contingent or M&A-linked hedges; new hedging programmes or instruments; programmes wound down; de-designations or ineffectiveness; novations, terminations or restructurings; clearing, margin or collateral changes; material CVA/DVA moves; counterparty concentration; exotic products (TRS, CDS, cross-currency swaps); bifurcated embedded derivatives; management commentary on WHY hedging strategy changed. Quote the filing in the flag.

    If the request's "type" is "bootstrap":
      Answer the request's "prompt" field exactly as it asks and write that JSON object (instrument_types, has_designated_hedges, has_notional_table, has_fair_value_table, key_fields, section_heading_pattern, end_boundary_pattern, unusual_features) as the result. Regex patterns must be valid Python regex.

    When done, reply with one line: the number of result files written and any files you could not process.

Before moving on, check that every file in cc_work/manifest.json has a matching file in cc_results/. For any that are missing, relaunch ONE subagent for those files only. Do not retry more than once.

Step 3 — Finalize
    python -m src.cc_bridge finalize --since 2025-01-01 --max-activations 50 --json-summary summary.json --verbose

This validates the results, runs change detection, writes output/{ticker}/tracking.csv, notes.txt and alert_log.txt, and promotes or fails activating issuers.

Step 4 — Data-integrity gate
    ./scripts/check_data_integrity.sh

Record whether it exited 0 (pass) or 1 (fail). Do NOT edit audit_baseline.json, pass --exit-zero, or delete rows to make the gate pass. The gate failing is a correct outcome; your job is to route the work to the right branch, not to fix it.

Step 5 — Commit and push
    TODAY=$(date -u +%Y-%m-%d)
    date -u +%Y-%m-%dT%H:%M:%SZ > registry/last_scheduler_run.txt
    git add registry/ profiles/ filer_profiles/ output/

Never add cc_work/, cc_results/, summary.json, audit_report.json or digest files.

Commit message, built from summary.json:
    chore(scheduler): <N> filings processed across <M> issuers (<comma-separated tickers>)[, <A> activated][, <R> need review][, <F> failed]
If nothing was processed use: chore(scheduler): no new filings (registry check only)
If the gate FAILED, append: " [GATE FAILED — held on review/${TODAY}-scheduler-gate-failed]"

If the gate PASSED, push to master:
    git push origin HEAD:master
If that is rejected because master moved:
    git fetch origin master && git rebase origin/master && git push origin HEAD:master

If the gate FAILED, do NOT push to master. Push to a review branch instead:
    git push origin HEAD:refs/heads/review/${TODAY}-scheduler-gate-failed

Always commit and push something (the last_scheduler_run.txt heartbeat guarantees a change), so the 04:00 digest routine can tell "scheduler ran, nothing new" from "scheduler did not run".

Step 6 — Report
End with a short plain-text summary: filings processed, tickers, activations (succeeded / needs review / failed), missing results, gate pass/fail and the defect counts it printed, and which branch you pushed to. Then stop.
