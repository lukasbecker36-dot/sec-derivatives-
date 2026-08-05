# Backfill Re-Extraction — Design

**Status:** proposed (2026-06-10)
**Problem:** ~300 of 381 issuers have near-empty tracking.csv rows. The LLM was
invoked for every 2025 filing, but regex section slicing handed it empty or
wrong text (e.g. AAPL: 0/38 fields across all 5 quarters — its config points
every field at a `market_risk` section that only exists in 10-Ks). A naive
historical backfill on the current pipeline would reproduce the same empty rows
for 2023–24. This design fixes section location first, then re-extracts 2025–26,
then extends history backward — all through one re-runnable structure.

---

## 1. Goals

1. **Locate sections reliably** — LLM-assisted location when regex fails, with
   the discovered pattern persisted so the daily ingester is permanently fixed.
2. **Distinguish "not disclosed" from "extraction failed"** — blanks today are
   ambiguous; after backfill every empty cell has a recorded reason.
3. **Chronological integrity** — rows ordered by period, prior-value context and
   change detection computed in order, alerts regenerated (not appended) so the
   weekly digest isn't flooded with 2023 "changes".
4. **Idempotent and resumable** — a state file tracks every (issuer, filing)
   work unit; re-running skips completed units; batches fit in one Claude Code
   session.
5. **Quality-gated cutover** — an issuer's live tracking.csv is only replaced
   when its backfilled series passes a fill-rate gate; failures go to the
   review queue instead of silently degrading output.

## 2. New on-disk structure

```
backfill/
  state.csv                  # one row per (ticker, accession): the work ledger
  requests/                  # extraction request JSONs (same shape as cc_work/)
  results/                   # Claude Code extraction results (same as cc_results/)
  staging/
    {ticker}/
      rows.jsonl             # one JSON object per filing row, with provenance
      sections_report.json   # per-filing section location outcomes
      alerts.txt             # regenerated chronological alert log
      notes.txt              # regenerated notes
```

`backfill/` is gitignored except `state.csv` and `staging/*/sections_report.json`
(small, and useful history). Output cutover copies staged artifacts into
`output/{ticker}/` atomically.

### state.csv columns

```
ticker, cik, accession_number, period_end, form_type, status,
sections_located, sections_total, fill_rate, attempts, last_error, updated_at
```

### Work-unit status machine

```
pending → located → extracted → validated → staged
                ↘ locate_failed (→ review_queue)
                       ↘ extract_failed (retryable, attempts capped at 3)
issuer-level:  staged-all → gated → committed   |   gate_failed (→ review_queue)
```

## 3. Section location: regex → score → LLM-locate → persist

New module `src/section_locate.py`, used by backfill and (after rollout) by the
daily ingester as a fallback.

**Step 1 — regex (existing).** Run `extract_all_sections()` as today, including
the content-anchored notional fallback.

**Step 2 — score the slice.** A section is *acceptable* if: length ≥ 300 chars,
not `is_likely_cross_reference()`, and ≥1 validation keyword present. Track
per-section verdict: `ok | empty | stub | keyword_miss`.

**Step 3 — LLM-locate (new).** For each unacceptable *required* section, emit a
`locate` request containing:
- the filing's note-heading inventory (`_find_note_headings`, already exists in
  bootstrap.py),
- ~3k-char windows around candidate anchors (notional/derivative/market-risk
  keyword hits),
- the section's purpose and field list from the config.

The LLM returns one of:
- `{"found": true, "heading_text": ..., "start_anchor": ..., "end_anchor": ...}`
  → slice text between anchors (verified by literal string search), re-score;
- `{"found": false, "reason": "not_disclosed", "note": "..."}` — e.g. "10-Q
  cross-references the 2024 10-K Item 7A"; recorded as a legitimate absence,
  **not** a failure.

**Step 4 — persist the fix.** When LLM-locate succeeds, write the discovered
heading back as a regex into `profiles/{ticker}.yaml` (escaped literal with
`Note\s+\d+` generalisation) and record it in `filer_profiles/{cik}.json` under
`learned_headings`. This is the lasting payoff: the daily ingester stops
failing for this issuer without further LLM cost.

**Step 5 — schema audit (once per issuer, first filing only).** Many configs
have fields pointed at sections that never carry them (AAPL's 38 market_risk
fields). Alongside the first filing's locate pass, ask the LLM: *given these
located sections, which configured fields are actually disclosed, and in which
section?* Output drives:
- field → section remapping in the YAML,
- `disclosure_scope: 10-K` tags for annual-only fields (sensitivity analyses),
  so 10-Q rows don't count them against fill rate,
- removal/flagging of ghost fields the company never discloses.

## 4. Extraction & row assembly

Unchanged prompt/schema machinery (`build_extraction_prompt`,
`_build_schema_for_section`), with three changes:

1. **Ordering.** Filings processed oldest-first per issuer; `prior_values` come
   from the staged prior row, not `output/.../tracking.csv` `rows[-1]`.
2. **Explicit absence.** The schema instructions require `"not_disclosed"`
   rather than null when a field is genuinely absent from the section. Row
   assembly writes a sidecar provenance map per field:
   `extracted | not_disclosed | section_missing | extract_failed`.
3. **Completeness rule.** A row is only `validated` when every required section
   is `ok` or `not_disclosed`. Partial results (missing result files) keep the
   unit at `extracted_failed` and it is retried — never written half-filled.

### tracking.csv schema additions

```
accession_number, filing_date, processed_at, extraction_version
```

`extraction_version` starts at 2 for backfilled rows (1 = legacy). The daily
ingester's dedupe switches from `period_end_date` to `accession_number`,
which also fixes the 10-Q/A amendment blind spot.

## 5. Chronological rebuild & alert regeneration

Per issuer, once all units are `staged`:

1. Sort `rows.jsonl` by `period_end` (then form_type, 10-K after 10-Q for the
   same date).
2. Run `validate_row` + `detect_changes` over the ordered series in one pass,
   writing a fresh `staging/{ticker}/alerts.txt`. Alerts for periods older than
   the previous live coverage are tagged `[HISTORICAL]`; the weekly digest
   builder skips `[HISTORICAL]` blocks.
3. Regenerate `notes.txt` from the qualitative extraction of each filing in
   order (so `[NEW]` tags are correct).

## 6. Quality gate & cutover

Issuer-level gate before replacing live output:

- **fill_rate** = extracted fields / applicable fields (excludes
  `not_disclosed` and out-of-scope-for-form-type fields), median across rows.
- Gate: median fill_rate ≥ 0.5 **and** no row with all required sections
  missing → `committed`: atomically replace `output/{ticker}/tracking.csv`,
  `notes.txt`, `alert_log.txt`; promote `active_needs_review → active` in the
  universe if previously weak.
- Below gate → `gate_failed`: live output untouched, review_queue entry with
  the sections_report attached, issuer stays/becomes `active_needs_review`.

Because `not_disclosed` is excluded from the denominator, genuinely minimal
filers (no derivatives note at all) pass the gate with sparse-but-honest rows
instead of failing forever.

## 7. Orchestration

New CLI `src/backfill.py`, mirroring the cc_bridge three-phase pattern:

```
python -m src.backfill prepare  --since 2023-01-01 --tickers AAPL,MSFT --batch 25
python -m src.backfill prepare  --since 2023-01-01 --next 25         # next 25 incomplete issuers
# (Claude Code processes backfill/requests/ → backfill/results/, including locate requests)
python -m src.backfill finalize --commit                              # stage, gate, cutover
python -m src.backfill status                                         # ledger summary
```

- `prepare` seeds/updates `state.csv` from EDGAR discovery (`since` →
  `get_unprocessed_filings` against the *staging* ledger, not live CSVs),
  fetches filings, runs regex+score, writes `locate` requests for failed
  sections and `extraction` requests for located ones.
- Locate and extract are two Claude Code passes for a batch: locate results
  feed a second `prepare --resolve` that slices text and writes extraction
  requests. In practice one session per batch handles both.
- `finalize` assembles rows, rebuilds chronology, applies the gate, commits.

**Batching:** ~25 issuers/session ≈ 25 × 10 filings × 2–3 sections ≈ 600–750
requests, comfortably one scheduled session. Full universe (381 issuers,
2023→present ≈ 13–14 filings each) ≈ 16 sessions.

## 8. Rollout plan

| Wave | Scope | Purpose |
|---|---|---|
| 0 | 10 pilot issuers spanning archetypes incl. AAPL, NUE, ADM, A, AES + a healthy control (BA) | Validate locate accuracy, gate thresholds; hand-check fills |
| 1 | Re-extract 2025–26 for all issuers below 50% fill rate (~300) | Fix the live dataset the weekly digest reads |
| 2 | Extend `--since 2023-01-01` for all gated-passing issuers | Enable YoY / trend analysis in alerts.py |
| 3 | Enable LLM-locate fallback in the daily ingester (cc_bridge prepare) using the same `section_locate` module | Stop new gaps forming |

Wave 1 before Wave 2: re-extracting the recent period validates each issuer's
repaired config cheaply before spending ~8 more filings per issuer on history.

## 9. What this fixes in the daily ingester (carried over from review)

- Empty-section rows no longer written and forgotten (completeness rule + retry).
- Partial Claude Code results no longer poison a filing.
- Amendments (10-Q/A) detected via accession-based dedupe.
- `processed_at` column lets the weekly digest list "new filings this week"
  (currently dead code in `build_weekly_digest.py`).
- Learned headings + schema audits permanently repair the ~187
  `active_needs_review` configs.
