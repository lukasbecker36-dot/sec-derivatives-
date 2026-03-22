# SEC Derivatives & Market Risk Extractor — Handover Note

**Date:** 2026-03-22
**Project:** `C:\Users\lukas\sec-derivatives`
**Repo:** `github.com/lukasbecker36-dot/sec-derivatives-` (private)

---

## What this is

A config-driven pipeline that extracts derivatives and market risk data from SEC 10-Q/10-K filings. It fetches filings from EDGAR, slices out the relevant note sections using regex, sends them to Claude Haiku for structured field extraction, then tracks changes period-over-period and flags anomalies.

Built for a Risk.net journalist who needs a weekly digest of what changed in corporate derivatives disclosures.

The system now monitors 391 non-financial S&P 500 companies via a universe registry with lazy activation — issuers are automatically onboarded when they next file a 10-Q/10-K.

---

## Architecture

```
EDGAR API  →  section_extract.py (regex slicing)  →  llm_extract.py (Haiku)  →  engine.py (CSV/notes/alerts)
                                                           ↑                           ↓
                                                   filer_profile.py              output/{ticker}/
                                                   (per-CIK memory)               tracking.csv
                                                                                   notes.txt
                                                                                   alert_log.txt

scheduler.py (daily GitHub Actions)
  ├── Pass 1: run active issuers through monitor pipeline
  └── Pass 2: check registered issuers for new filings → activate
                  ↓
          activation.py → bootstrap.py → engine.py → promote or fail
```

**Two-stage extraction:**
1. **Deterministic regex** — find "Note X — Derivative Financial Instruments" heading, slice to next note heading. Configurable per issuer via YAML.
2. **LLM (Claude Haiku)** — send section text + field schema + filer profile context → get structured JSON with values, confidence scores, source quotes.

---

## Modules

| Module | Purpose |
|--------|---------|
| `src/utils.py` | Dollar parsing, text cleaning, rate limiter, sentence extraction |
| `src/filing_fetcher.py` | EDGAR API: discover filings, fetch text, diff against CSV |
| `src/config.py` | YAML config loading with archetype inheritance (deep merge) |
| `src/section_extract.py` | Regex-based section slicing with cross-reference filtering |
| `src/llm_extract.py` | Haiku extraction with retry on JSON parse failure |
| `src/engine.py` | Pipeline orchestration, CSV/notes/alerts output, filer profile integration |
| `src/qualitative.py` | Keyword sentence matching, [NEW] tagging |
| `src/change_detect.py` | Period-over-period alerts (numeric thresholds, appeared/disappeared) |
| `src/validate.py` | Sanity checks: completeness, positivity, units, plausibility |
| `src/bootstrap.py` | Auto-generate YAML config from CIK (Sonnet), activation-mode bootstrap with confidence scoring |
| `src/monitor.py` | CLI entry point with --issuer, --since, --watch; run_from_configs() for scheduler |
| `src/alerts.py` | Cross-issuer dashboard, trend breaks, story leads (Sonnet) |
| `src/registry.py` | Universe CSV management, issuer lifecycle states, seeding |
| `src/activation.py` | Lazy activation pipeline: detect filing → bootstrap → extract → score → promote/fail |
| `src/scheduler.py` | Two-pass daily orchestrator (active + registered issuers), CLI + GitHub Actions |
| `src/filer_profile.py` | Per-CIK JSON profiles: structural features, language patterns, prompt injection |

**Tests:** 133 unit tests, all passing. Run: `python -m pytest tests/ -q`

---

## Universe and lifecycle

**Registry:** `registry/universe.csv` — 391 non-financial S&P 500 issuers seeded from `CompanyCIKs.csv`.

**Status lifecycle:**
- `registered` → issuer is known but not yet configured
- `activating` → new filing detected, bootstrap + extraction in progress
- `active` → working config, participates in daily monitoring
- `active_needs_review` → activated but with weak confidence
- `failed_activation` → activation failed, logged for review

**Activation scoring:** Combined bootstrap score (archetype confidence, sections found, LLM analysis) and extraction score (field fill rate, validation errors, cross-reference detection). Score >= 0.60 → active, >= 0.35 → active_needs_review, < 0.35 → failed_activation.

**Registry files:**
- `registry/universe.csv` — canonical universe with lifecycle state
- `registry/activation_log.csv` — lifecycle transition events
- `registry/review_queue.csv` — items needing human review

---

## Filer profiles

Per-CIK JSON files in `filer_profiles/` capturing company-specific reporting patterns:
- **Document structure:** heading patterns, section locations
- **Filing patterns:** heading variations seen, cross-reference usage
- **Idiosyncrasies:** recurring phrases, non-GAAP metrics, unusual patterns
- **History:** list of processed filings with field counts

Profiles are injected into LLM extraction prompts as "known company-specific patterns" to improve accuracy over time. Created lazily on first filing processed.

7 profiles seeded from existing active issuers with historical tracking data.

---

## Issuers configured and live-tested

All 7 active issuers have 12 filings extracted (8 historical + 4 from first automated run):

| Ticker | Issuer | CIK | Archetype | Data quality |
|--------|--------|-----|-----------|-------------|
| META | Meta Platforms | 0001326801 | minimal_hedger | Good. Dual heading pattern for pre/post-2023 format change. |
| BA | Boeing | 0000012927 | active_fx_commodity_hedger | Good. 31 fields. Needed max_tokens bump to 4096. |
| F | Ford Motor | 0000037996 | active_fx_commodity_hedger | Good. IR + FX + commodity, Ford Credit sensitivity. |
| PM | Philip Morris | 0001413329 | active_fx_commodity_hedger | Good for 10-Qs. FX notionals $40-53B. 2025 10-K empty (see known issues). |
| INTC | Intel | 0000050863 | active_ir_fx_hedger | Excellent. IR swaps, FX forwards, equity hedges all tracked. |
| GEV | GE Vernova | 0001996810 | active_fx_commodity_hedger | Excellent. Clean progression, net investment hedges, AOCI. |
| MRK | Merck | 0000310158 | active_ir_fx_hedger | Good. FX forwards $32-44B, IR swaps. Market risk sensitivity only in 10-Ks. |

---

## How to run

```bash
# Local: set API key
export ANTHROPIC_API_KEY="<your-key>"

# Single issuer (existing monitor)
python -m src.monitor --issuer BA --since 2024-01-01

# All configured issuers
python -m src.monitor --since 2024-01-01

# Scheduler dry run (check EDGAR, no activations)
python -m src.scheduler --dry-run --verbose

# Scheduler full run (process active + activate registered)
python -m src.scheduler --max-activations 10 --since 2025-01-01

# Seed universe (one-time, already done)
python -m src.registry seed --cik-csv CompanyCIKs.csv --seed-profiles

# Bootstrap a single issuer manually
python -m src.bootstrap --cik 0000789019 --ticker MSFT

# Cross-issuer dashboard + story leads
python -m src.alerts
```

---

## Automated deployment

**GitHub Actions** workflow at `.github/workflows/scheduler.yml`:
- Runs daily at **06:00 UTC**
- Can be manually triggered from Actions tab (with dry_run and max_activations options)
- Pass 1: processes new filings for active issuers
- Pass 2: checks registered issuers for new filings, activates up to `max_activations` per run
- Commits registry/profile/config changes back to repo
- Uploads JSON run summary as artifact
- API key stored as GitHub secret `ANTHROPIC_API_KEY`

**Skip-if-recently-checked:** Registered issuers checked within the last 3 days are skipped to reduce EDGAR load. On daily runs, the full universe cycles through in ~3 days.

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
3. **First activation batch failed** — 5 issuers (ACN, FDX, GIS, MU, ORCL) failed activation on the first automated run due to a bug (now fixed). They are in `failed_activation` status and will need to be retried or manually re-triggered.

4. **Bootstrap quality** — `src/bootstrap.py` auto-generates YAML configs but they're stubs that need manual review. The auto-classifier tends to default to `minimal_hedger`. The activation scoring system mitigates this by flagging weak configs as `active_needs_review`.

### Nice to have
5. **Backfill validation** — spot-check LLM-extracted values against the original bespoke scripts for Meta and Boeing to measure accuracy.

6. **10-K vs 10-Q heading divergence** — some issuers use different note numbering in annual vs quarterly filings. Filer profiles will capture heading variations over time, which could be used to add `heading_10k` overrides automatically.

7. **Dashboard/alerts module untested live** — `src/alerts.py` (cross-issuer dashboard, story leads) is written but hasn't been run against real data yet.

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
├── f/
├── pm/
├── intc/
├── gev/
└── mrk/

registry/
├── universe.csv               # 391 issuers with lifecycle state
├── activation_log.csv         # Lifecycle transition events
└── review_queue.csv           # Items needing human review

filer_profiles/
├── 0000012927.json            # Boeing
├── 0000037996.json            # Ford
├── ...                        # One per active CIK
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

1. **Retry failed activations** — ACN, FDX, GIS, MU, ORCL failed due to a now-fixed bug. Reset their status to `registered` in universe.csv or wait for the automatic retry after 30 days.
2. **Monitor activation quality** — check `registry/review_queue.csv` and `registry/activation_log.csv` after a few daily runs to see how the scoring is working.
3. **Fix PM 2025 10-K** — check heading, add fallback pattern.
4. **Run `src/alerts.py`** — generate cross-issuer dashboard and story leads.
5. **Validate extraction accuracy** — compare a few quarters against hand-checked values.
