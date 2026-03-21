# SEC Derivatives & Market Risk Extractor — Handover Note

**Date:** 2026-03-21
**Project:** `C:\Users\lukas\sec-derivatives`

---

## What this is

A config-driven pipeline that extracts derivatives and market risk data from SEC 10-Q/10-K filings. It fetches filings from EDGAR, slices out the relevant note sections using regex, sends them to Claude Haiku for structured field extraction, then tracks changes period-over-period and flags anomalies.

Built for a Risk.net journalist who needs a weekly digest of what changed in corporate derivatives disclosures.

---

## Architecture

```
EDGAR API  →  section_extract.py (regex slicing)  →  llm_extract.py (Haiku)  →  engine.py (CSV/notes/alerts)
                                                                                      ↓
                                                                              output/{ticker}/
                                                                                tracking.csv
                                                                                notes.txt
                                                                                alert_log.txt
```

**Two-stage extraction:**
1. **Deterministic regex** — find "Note X — Derivative Financial Instruments" heading, slice to next note heading. Configurable per issuer via YAML.
2. **LLM (Claude Haiku)** — send section text + field schema → get structured JSON with values, confidence scores, source quotes.

---

## What's built (all 10 plan phases implemented)

| Module | Purpose |
|--------|---------|
| `src/utils.py` | Dollar parsing, text cleaning, rate limiter, sentence extraction |
| `src/filing_fetcher.py` | EDGAR API: discover filings, fetch text, diff against CSV |
| `src/config.py` | YAML config loading with archetype inheritance (deep merge) |
| `src/section_extract.py` | Regex-based section slicing with cross-reference filtering |
| `src/llm_extract.py` | Haiku extraction with retry on JSON parse failure |
| `src/engine.py` | Pipeline orchestration, CSV/notes/alerts output |
| `src/qualitative.py` | Keyword sentence matching, [NEW] tagging |
| `src/change_detect.py` | Period-over-period alerts (numeric thresholds, appeared/disappeared) |
| `src/validate.py` | Sanity checks: completeness, positivity, units, plausibility |
| `src/bootstrap.py` | Auto-generate YAML config from CIK (sends filing to Sonnet) |
| `src/monitor.py` | CLI entry point with --issuer, --since, --watch |
| `src/alerts.py` | Cross-issuer dashboard, trend breaks, story leads (Sonnet) |

**Tests:** 66 unit tests, all passing. Run: `python -m pytest tests/ -q`

---

## Issuers configured and live-tested

All 7 issuers have 8 filings extracted (last ~2 years, `--since 2024-01-01`):

| Ticker | Issuer | CIK | Archetype | Data quality |
|--------|--------|-----|-----------|-------------|
| META | Meta Platforms | 0001326801 | minimal_hedger | Good. Dual heading pattern for pre/post-2023 format change. |
| BA | Boeing | 0000012927 | active_fx_commodity_hedger | Good. 31 fields. Needed max_tokens bump to 4096. |
| F | Ford Motor | 0000037996 | active_fx_commodity_hedger | Good. IR + FX + commodity, Ford Credit sensitivity. |
| PM | Philip Morris | 0001413329 | active_fx_commodity_hedger | Good for 10-Qs. FX notionals $40-53B. 2025 10-K empty (see known issues). |
| INTC | Intel | 0000050863 | active_ir_fx_hedger | Excellent. IR swaps, FX forwards, equity hedges all tracked. |
| GEV | GE Vernova | 0001996810 | active_fx_commodity_hedger | Excellent. Clean progression, net investment hedges, AOCI. |
| MRK | Merck | 0000310158 | active_ir_fx_hedger | Good. FX forwards $32-44B, IR swaps. Market risk sensitivity only in 10-Ks. |

**Total LLM cost for all runs:** ~$2.76 across 559 Haiku calls (~$0.005 each).

---

## How to run

```bash
# Set API key (already set via setx, but bash sessions need export)
export ANTHROPIC_API_KEY="<your-key-here>"

# Single issuer
python -m src.monitor --issuer BA --since 2024-01-01

# All issuers
python -m src.monitor --since 2024-01-01

# Poll mode (hourly)
python -m src.monitor --watch --interval 3600

# Bootstrap a new issuer
python -m src.bootstrap --cik 0000789019 --ticker MSFT

# Cross-issuer dashboard + story leads
python -m src.alerts
```

---

## Config system

Each issuer has a YAML file in `profiles/`. Archetypes in `profiles/_archetypes/` provide default field sets. Issuer YAML inherits from archetype via deep merge and can override/extend.

**4 archetypes:**
- `active_fx_commodity_hedger` — Boeing/Ford/PM/GEV pattern (FX + commodity designated hedges)
- `active_ir_fx_hedger` — Intel/Merck pattern (IR swaps + FX forwards)
- `minimal_hedger` — Meta pattern (mostly fair value disclosures, minimal derivatives)
- `no_derivatives` — Market risk sensitivities only

Key YAML fields: `sections` (heading regex, match_strategy, end_boundary, max_length, validation_keywords), `fields` (name, description, section), `qualitative` (keyword categories), `alert_thresholds`.

---

## Known issues and gaps

### Must fix
1. **PM 2025 10-K empty** — the annual report uses a different note heading format or numbering than the 10-Qs. The `Note\s+\d+\.\s+Financial Instruments` pattern doesn't match. Need to check the actual 10-K heading.

2. **MRK market_risk empty in 10-Qs** — Merck's 10-Qs cross-reference market risk to the prior 10-K ("see Item 7 in our 2024 Form 10-K") instead of repeating the data. The `ir_sensitivity_100bp` and `fx_sensitivity_10pct` fields are only available in 10-Ks. This is by design on Merck's side, but means quarterly sensitivity data is missing.

### Should fix
3. **Cross-reference filtering** — improved but still fragile. The XREF_PATTERN in `section_extract.py` checks the 100 chars after a heading match for phrases like "to our condensed consolidated" or "for disclosures related to". New issuers may use different cross-reference wording. Consider a more robust approach (e.g., check section length — real notes are >500 chars, xrefs are <200).

4. **Bootstrap quality** — `src/bootstrap.py` auto-generates YAML configs but they're stubs that need manual review. The auto-classifier tends to default to `minimal_hedger`. All 4 new issuers (PM, INTC, GEV, MRK) needed hand-written configs after bootstrapping.

5. **No git repo** — the project directory is not a git repository. Should `git init` and make an initial commit.

### Nice to have
6. **Backfill validation** — spot-check LLM-extracted values against the original bespoke scripts for Meta and Boeing to measure accuracy.

7. **10-K vs 10-Q heading divergence** — some issuers use different note numbering in annual vs quarterly filings. Could add a `heading_10k` override in YAML.

8. **Dashboard/alerts module untested live** — `src/alerts.py` (cross-issuer dashboard, story leads) is written but hasn't been run against real data yet.

9. **Scale to ~100 issuers** — current bootstrap workflow: run `python -m src.bootstrap --cik X`, review the stub YAML, rewrite it by hand. For 100 issuers this needs a better semi-automated flow.

---

## Output structure

```
output/
├── llm_usage.log              # Every Haiku call: timestamp, issuer, section, model, tokens, cost
├── meta/
│   ├── tracking.csv           # One row per filing period, all extracted fields
│   ├── notes.txt              # Qualitative keyword matches (newest first)
│   └── alert_log.txt          # Period-over-period alerts (newest first)
├── ba/
│   ├── tracking.csv
│   ├── notes.txt
│   └── alert_log.txt
├── f/
├── pm/
├── intc/
├── gev/
└── mrk/
```

**tracking.csv columns:** `period_end_date, form_type, {all fields from YAML config}`

**alert types:** NUMERIC (threshold breach), DROPPED_TO_ZERO, NEW_FIELD, DISAPPEARED_FIELD, LLM_FLAG (Haiku's own plausibility flags), VALIDATION (sanity check failures)

---

## Dependencies

```
anthropic          # Claude API
beautifulsoup4     # HTML parsing (used in clean_filing_text)
pyyaml             # Config files
requests           # EDGAR HTTP
duckdb             # Cross-issuer queries (alerts.py)
pytest             # Tests
```

Install: `pip install -r requirements.txt`

---

## What to do next

1. **Fix PM 2025 10-K** — check heading, add fallback pattern
2. **Run `src/alerts.py`** — generate cross-issuer dashboard and story leads
3. **Add more issuers** — priority: large derivative users (JPM, GS, Apple, Microsoft, 3M, Caterpillar)
4. **Set up weekly cron** — `python -m src.monitor --since 2025-01-01` on a schedule
5. **Validate extraction accuracy** — compare a few quarters against hand-checked values
