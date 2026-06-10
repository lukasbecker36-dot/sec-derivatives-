# Handover Note — FX Extraction Improvements & Re-Extraction

**Date:** 2026-06-10
**Branch:** `claude/check-filing-updates-im5uK`
**Previous handover:** See git history for the March 2026 version of this file.

---

## Summary of This Session's Work

This session improved the section extraction pipeline to handle diverse derivatives note headings across the S&P 500 universe, ran a full re-extraction, and compiled an FX derivatives notional leaderboard. The pipeline went from ~30 tickers with good data to 83 tickers with confirmed `has_derivatives=Yes` and FX notional values.

---

## Changes Made

### 1. Content-Anchored Fallback Extractor (key change)

**Problem:** The regex heading-based section extractor (`section_extract.py`) missed derivatives data for hundreds of filers because note headings vary wildly: "Note 5", "Note 8", "Derivative Instruments", "Financial Instruments", all-caps, etc.

**Fix:** Added `extract_derivatives_by_content()` in `src/section_extract.py` — a content-anchored fallback that finds the derivatives notional table by searching for "notional amount" near derivative-context words (swap, forward, hedge, etc.), regardless of heading format. Integrated into `extract_all_sections()` so it fires automatically when heading-based extraction misses or returns a cross-reference stub.

**Key patterns:**
- `_NOTIONAL_ANCHOR` — matches "notional amounts of", "aggregate/total/outstanding notional", bare "notional"
- `_DERIV_CONTEXT` — derivative, forward contract, interest rate swap, cross-currency, hedg*, CDS, strike price, collar, etc.

Commits: `bbec159` through `0d9b9a5`

### 2. Reset Script `--last N` Flag

**Problem:** `scripts/reset_for_reextraction.py --periods` only matched exact dates (2026-03-31, 2025-12-31) and missed 64 filers with non-standard fiscal calendars (AAPL 2026-03-28, CSCO 2026-04-25, NVDA 2026-04-26, etc.).

**Fix:** Added `--last N` to remove the last N rows regardless of period date.

Commit: `f40b2bf`

### 3. Full Re-Extraction (commit `617e29a` on master, now merged)

A re-extraction of ~235 tickers was run via OpenAI backend after resetting tracking CSVs. This data is now on this branch.

### 4. Diagnostic Scripts Added

- `scripts/audit_sections.py` — Batch audit classifying all issuers into captured/uncaptured/no_notional
- `scripts/diagnose_ge_gd.py` — GE/GD targeted diagnostic
- `scripts/diagnose_borderline.py` — CDW/EME/STE diagnostic

---

## Current Data Status

**379 tracking.csv files** across `output/`:

| Category | Count | Description |
|----------|-------|-------------|
| **Good extraction** | **83** | `has_derivatives=Yes`, FX notional populated correctly |
| Partial / misaligned | 232 | Some fields filled but values in wrong columns (schema mismatch from OpenAI extraction) |
| Empty rows | 64 | Only `period_end_date` and `form_type`, all other fields blank |

### FX Derivatives Notional Leaderboard (Top 25)

From the 83 tickers with good extraction:

| Rank | Ticker | FX Notional ($M) | Period | Notes |
|------|--------|-----------------|--------|-------|
| 1 | MSFT | 54,543 | 2026-03-31 | |
| 2 | PM | 48,871 | 2026-03-31 | |
| 3 | PFE | 45,761 | 2025-12-31 | 10-K; Q1 2026 not yet extracted |
| 4 | GEV | 43,975 | 2026-03-31 | |
| 5 | MRK | 42,106 | 2026-03-31 | |
| 6 | F | 41,232 | 2026-03-31 | Includes Ford Credit |
| 7 | RTX | 26,000 | 2026-03-31 | ⚠ may include IR+FX combined |
| 8 | KO | 21,128 | 2025-12-31 | 10-K |
| 9 | INTC | 19,177 | 2026-03-28 | |
| 10 | DOW | 18,874 | 2026-03-31 | |
| 11 | MDLZ | 18,220 | 2026-03-31 | |
| 12 | PG | 16,203 | 2026-03-31 | |
| 13 | UPS | 15,286 | 2026-03-31 | |
| 14 | BSX | 14,630 | 2026-03-31 | |
| 15 | LIN | 13,827 | 2026-03-31 | |
| 16 | LLY | 13,594 | 2026-03-31 | |
| 17 | HON | 13,228 | 2026-03-31 | |
| 18 | ABT | 12,600 | 2026-03-31 | |
| 19 | ABBV | 9,300 | 2026-03-31 | |
| 20 | IBM | 9,000 | 2026-03-31 | |
| 21 | GD | 8,500 | 2025-12-31 | 10-K |
| 22 | GM | 8,190 | 2026-03-31 | |
| 23 | DHR | 7,795 | 2025-12-31 | 10-K |
| 24 | SYK | 7,569 | 2026-03-31 | |
| 25 | LMT | 7,200 | 2025-12-31 | 10-K |

Full list: 83 tickers with data, extending down to ~$50M (OXY, GILD, GOOGL at the bottom).

---

## Known Issues — Priority Order

### P0: AAPL extraction empty

AAPL's Q2 FY2026 (period 2026-03-28) tracking row has ALL empty fields despite `notes.txt` containing the full notional table showing ~$62,647M FX designated. The section text IS being captured (content fallback works), but the LLM extraction step returned nothing. AAPL would likely be **#1 on this leaderboard** (~$100B+ total FX). Needs targeted re-extraction or investigation of why the extraction prompt fails on AAPL's format.

Other key tickers with empty extraction: **JNJ, AMZN, ACN, CSCO, CRM, NKE, ADSK** — all have `notes.txt` with data but empty tracking rows.

### P1: 232 tickers with misaligned field data

The OpenAI re-extraction produced schema mismatches for most tickers — values landed in wrong columns (e.g., `equity_derivatives_notional` = "Yes", `has_derivatives` = "343.4"). This is a prompt/field-schema alignment issue in `src/llm_extract.py` when using the OpenAI backend vs Anthropic. These tickers have data in their notes.txt but the structured extraction didn't map correctly.

**Fix approach:** Either re-extract using Anthropic backend (which has better schema alignment), or fix the OpenAI extraction prompt to match field names exactly.

### P2: RTX and similar overcounting

RTX shows `fx_derivatives_notional` of $26B but `fx_designated_notional` is only $293M. The $26B likely captures total derivative notional (IR + FX + commodity combined). Similar issue may affect other tickers. Need field-level validation.

### P3: EDGAR access blocked in sandbox

SEC EDGAR returns 403 from this sandboxed environment (all outbound HTTP blocked). Re-extraction requires either:
- Running from an environment with internet access
- Using the scheduled routine (Routine ID: `trig_01CP3oDgK5HKdthxWqGAeppG`)
- Running locally: `python -m src.scheduler --provider openai --since 2025-01-01 --max-activations 0 --verbose`

---

## How to Continue

### Quick wins
```bash
# Reset AAPL + other failed tickers and re-extract (needs internet)
python scripts/reset_for_reextraction.py --tickers AAPL JNJ AMZN ACN CSCO CRM NKE --last 2
python -m src.scheduler --provider openai --since 2025-01-01 --max-activations 0 --verbose
```

### Larger effort: fix the 232 misaligned tickers
The field schema in `src/llm_extract.py` needs alignment for the OpenAI backend. Compare the prompt sent to OpenAI vs Anthropic and ensure field names in the extraction prompt match the tracking CSV headers exactly.

### Or: use the Claude Code scheduled routine
The pipeline supports a three-phase Claude Code extraction mode (see CLAUDE.md) that uses subscription tokens instead of API credits. The routine runs weekdays at 09:00 UTC.

---

## Key Files Modified (this branch)

| File | Change |
|------|--------|
| `src/section_extract.py` | Content-anchored `extract_derivatives_by_content()` fallback |
| `scripts/reset_for_reextraction.py` | `--last N` flag for non-standard fiscal calendars |
| `scripts/audit_sections.py` | Batch section audit tool |
| `scripts/diagnose_ge_gd.py` | GE/GD diagnostic |
| `scripts/diagnose_borderline.py` | CDW/EME/STE diagnostic |
| `output/*/tracking.csv` | ~235 tickers re-extracted |
| `output/*/notes.txt` | Updated extraction notes |
| `output/*/alert_log.txt` | Updated alert logs |
