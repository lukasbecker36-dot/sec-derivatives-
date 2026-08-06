# SEC Derivatives & Market Risk Extractor

Config-driven pipeline that monitors SEC EDGAR 10-Q/10-K filings for S&P 500 non-financial companies and extracts derivatives and market risk disclosures. Built for Risk.net editorial coverage of corporate derivatives activity.

## Architecture

### Two-Pass Scheduler (`src/scheduler.py`)

The scheduler runs daily and processes issuers in two passes:

**Pass 1 — Active Issuers:** Loads all issuers with status `active` or `active_needs_review` from `registry/universe.csv`. For each, fetches new filings from EDGAR, runs the full extraction pipeline (section extraction → LLM extraction → validation → change detection), and writes results to `output/{ticker}/`.

**Pass 2 — Registered Issuers:** Iterates through `registered` and `failed_activation` issuers. Checks EDGAR for new 10-Q/10-K filings. When found, triggers lazy activation: auto-generates a YAML config (`src/bootstrap.py`), extracts fields, scores the result, and promotes the issuer to `active` (score ≥ 0.60), `active_needs_review` (0.35–0.59), or `failed_activation` (< 0.35).

### Pipeline Flow

```
EDGAR API → filing_fetcher.py → section_extract.py → [LLM extraction] → engine.py → output/
```

- **filing_fetcher.py** — Discovers and fetches filing HTML from EDGAR
- **section_extract.py** — Regex-based slicing of Note sections from filing HTML
- **llm_extract.py** — Structured field extraction (pluggable: API or Claude Code)
- **engine.py** — Orchestrates pipeline, writes tracking.csv / notes.txt / alert_log.txt
- **activation.py** — Lazy onboarding: bootstrap config + first extraction + scoring
- **bootstrap.py** — Auto-generates issuer YAML config from a filing (pluggable LLM backend)
- **cc_bridge.py** — Claude Code bridge: `prepare` discovers work, `finalize` runs pipeline with cached results

### Claude Code Mode (`src/cc_bridge.py`)

The pipeline supports two extraction backends:

1. **API mode** (original) — `llm_extract.py` calls the Anthropic API directly. Requires `ANTHROPIC_API_KEY`.
2. **Claude Code mode** — A three-phase workflow where Claude Code itself performs the extraction, using subscription tokens instead of API credits:

```
Phase 1 (prepare):  python -m src.cc_bridge prepare --since 2025-01-01 --max-activations 50
                    → fetches filings, extracts sections, writes requests to cc_work/

Phase 2 (Claude Code): reads cc_work/*.json, performs extraction, writes results to cc_results/

Phase 3 (finalize): python -m src.cc_bridge finalize --since 2025-01-01 --max-activations 50
                    → injects cached results into pipeline, runs validation/change detection/output
```

The pluggable hooks are in `llm_extract.set_override()` and `bootstrap.set_analysis_override()`.

### Configuration

Issuers inherit from archetypes in `profiles/_archetypes/` via deep merge:
- `active_fx_commodity_hedger` — FX + commodity designated hedges
- `active_ir_fx_hedger` — IR swaps + FX forwards
- `minimal_hedger` — Fair value, minimal derivatives
- `no_derivatives` — Market risk sensitivities only

Per-issuer configs in `profiles/{ticker}.yaml`. Per-CIK structural profiles in `filer_profiles/`.

### Key Directories

- `registry/` — `universe.csv` (391 issuers), `activation_log.csv`, `review_queue.csv`
- `profiles/` — Issuer YAML configs + archetypes
- `filer_profiles/` — Per-CIK JSON structural profiles (heading patterns, filing history)
- `output/` — Per-issuer tracking.csv, notes.txt, alert_log.txt, plus llm_usage.log
- `cc_work/` — Temporary extraction request files (gitignored)
- `cc_results/` — Temporary extraction result files (gitignored)

## Running Manually

```bash
# Full run via API (requires ANTHROPIC_API_KEY)
python -m src.scheduler --max-activations 50 --since 2025-01-01 --json-summary summary.json --verbose

# Claude Code mode — three steps
python -m src.cc_bridge prepare --since 2025-01-01 --max-activations 50 --verbose
# (process cc_work/ requests manually or via Claude Code, write results to cc_results/)
python -m src.cc_bridge finalize --since 2025-01-01 --max-activations 50 --json-summary summary.json --verbose

# Dry run (check for filings without activating)
python -m src.scheduler --max-activations 50 --since 2025-01-01 --dry-run --verbose

# Single issuer via monitor
python -m src.monitor --issuer META --since 2025-01-01
```

### Scheduler CLI Arguments

| Argument | Default | Description |
|---|---|---|
| `--since` | None | Cutoff date for active issuer processing |
| `--cutoff` | 120 days ago | Activation cutoff for registered issuers |
| `--max-activations` | 10 | Max new activations per run |
| `--check-interval` | 3 | Skip registered issuers checked within N days |
| `--dry-run` | false | Check only, no activation |
| `--output` | `./output` | Output directory |
| `--json-summary` | None | Write JSON run summary to path |
| `--verbose` | false | Debug logging |

## Scheduled Execution

Runs as a **Claude Code scheduled routine** (remote, in Anthropic's cloud), weekdays at 09:00 UTC (10am BST). The routine clones the repo, runs the three-phase Claude Code extraction pipeline, and commits/pushes results. No API key needed — Claude Code performs the LLM extraction using subscription tokens.

Routine ID: `trig_01CP3oDgK5HKdthxWqGAeppG`
Manage at: https://claude.ai/code/routines

A `run_scheduler.ps1` script also exists for local API-mode runs (requires `ANTHROPIC_API_KEY` as a user environment variable).

## CME Interest-Rate Bulletin Tracker (`src/cme_bulletin.py`)

A standalone daily tracker for CME Daily Bulletin **Section 02A — Summary Volume and Open Interest, Interest Rate Futures and Options**. It is independent of the SEC pipeline and uses **no LLM / API** — parsing is deterministic (`pdfplumber`).

```bash
# Download today's bulletin, parse it, append to the store (hands-off)
python -m src.cme_bulletin pull --verbose

# Force the headless-browser download (skip the plain-request attempt)
python -m src.cme_bulletin pull --browser --verbose

# Parse a bulletin PDF you downloaded yourself (bypasses the download)
python -m src.cme_bulletin pull --file ~/Downloads/Section02A.pdf --verbose

# Re-parse a previously archived PDF (no download)
python -m src.cme_bulletin pull --date 2026-07-02 --force

# Search the history with SQL (DuckDB over the CSV; table is named `data`)
python -m src.cme_bulletin query "SELECT trade_date, total_volume, open_interest FROM data WHERE product_code='SR3' AND report_section='FUTURES' ORDER BY trade_date"
```

### Downloading (`pull`)

`pull` fetches the bulletin in two stages: a plain `requests` call first, then a **real, headed browser** (Playwright driving your installed Chrome/Edge) when the plain request is refused (HTTP 403). CME allows a normal browser but blocks scripted/**headless** clients, so the browser runs headed by default (a window opens briefly) with the automation flag disabled and a persistent profile (`.pw-profile/`, gitignored) to stay warm. Force it with `--browser`; opt into headless with `--headless` only if that works from your network. One-time setup:

```bash
python -m pip install -r requirements.txt
# Uses your installed Chrome/Edge — no extra download. Only if neither exists:
python -m playwright install chromium
```

> **Terms of Use.** CME's website Data Terms of Use restrict automated access to the daily bulletin, and CME blocks scripted requests from data-center/shared IPs (a 403 "suspected web scraping" message). Run `pull` only from a machine/network you're entitled to use for this content. The clearly-compliant alternatives — CME's **free Daily Bulletin email subscription** (Subscription Center) and the licensed **CME DataMine** feed (structured volume/OI via REST/SFTP/S3; `dataminesales@cmegroup.com`) — remain available; the parser/storage/query layer works unchanged behind any source (`--file`).

### Local scheduled job (`run_cme_bulletin.ps1`)

For hands-off daily updates, `run_cme_bulletin.ps1` runs as a Windows Scheduled Task on your own machine (setup + registration snippet in the script header). Each run:
1. **Downloads directly** via `pull` (requests → headless-browser fallback). This is the primary, no-manual-upload path.
2. **Drains an inbox** as a safety net — parses any PDF dropped into `inbox\cme\` via `pull --file`, then moves it to `inbox\cme\processed\`.
3. **Commits & pushes** any new `data/cme/` rows.

`inbox/` is gitignored. Logs go to `logs/` (30-day retention).

- **Storage:** `data/cme/ir_volume_oi.csv` — append-only, one row per product line per section per trading day (git-friendly). Raw PDFs are archived under `data/cme/raw/{trade_date}.pdf`. Both are committed so history accumulates.
- **Idempotency:** re-running `pull` for the same day replaces that day's rows rather than duplicating.
- **Columns:** `trade_date, report_status, report_section` (FUTURES/OPTIONS), `product_code, product_name, option_type` (C/P/blank), `is_total`, `globex_volume, open_outcry_volume, pnt_volume, total_volume, open_interest, oi_change, prior_year_volume, prior_year_open_interest, source_pdf, fetched_at`.
- **Querying note:** the OPTIONS section includes per-family `TOTAL` roll-up rows (`is_total = true`); filter `WHERE NOT is_total` (or `option_type IN ('C','P')`) to avoid double-counting when aggregating.
- **Parsing:** numeric columns are right-aligned and empty cells vanish from the text, so columns are located by clustering token right-edges into anchors and bucketing each cell to its anchor. Every row is validated by `globex+open_outcry+pnt == total_volume`; a layout change raises `BulletinParseError` so the run fails loudly instead of storing garbage.

## Environment

- **Python 3.11+** required
- Dependencies: `pip install -r requirements.txt` (anthropic, beautifulsoup4, pyyaml, requests, duckdb, pdfplumber)
- **ANTHROPIC_API_KEY** only needed for API mode (not for Claude Code mode)

## Data integrity gate — run this before writing any digest

**Never write a briefing without running this first:**

```bash
./scripts/check_data_integrity.sh     # exit 0 = safe to publish, 1 = do not
```

A weekly briefing once reported Microsoft's non-designated FX notional as
"+352%". The $63.5B figure was real (FY2026 10-K), but the percentage was
computed against a corrupted prior value. **A percentage is only as
trustworthy as both of its endpoints** — check the corpus before writing about
it, not after a reader queries the number.

`src/audit.py` reports four defect types:

| Type | Meaning |
|---|---|
| `empty_row` | Every extracted field blank — extraction failed. Not the same as a genuine non-discloser, which records `has_derivatives=No`. Nothing to write about. |
| `reconciliation` | A notional total doesn't equal the sum of its components. The values contradict each other, so at least one is wrong. |
| `misaligned_row` | More values than header columns; values sat in the wrong fields. Zero tolerance. |
| `stale_null` | Blank here but populated on another git ref — correct data exists and is being ignored. Zero tolerance. |
| `implausible_swing` | A single numeric field moved ≥10x between adjacent periods with a prior magnitude ≥$100M. Almost always a real number from the filing landing in the wrong column — a notional (10-100x larger than fair values) extracted into a `total_derivative_asset` slot. The reconciliation check catches this when the offending field is a total-of-components; `implausible_swing` catches the standalone fields it misses. |

The corpus carries known defects that need re-extraction, so the gate fails on
**regression** against `audit_baseline.json`, not on any defect at all. After
legitimately reducing defects, ratchet the baseline down:

```bash
python -m src.audit --write-baseline audit_baseline.json
```

Useful investigations:

```bash
python -m src.audit --limit 60                    # what's broken
python -m src.audit --ticker MSFT                 # one issuer
python -m src.audit --compare-ref origin/master   # data that exists elsewhere
```

### Re-extraction queue

The audit finds defective rows but does not repair them — that requires the
LLM. The queue records what needs re-extraction and drops the row from
`output/{ticker}/tracking.csv` so the next daily scheduler run picks the
filing up as unprocessed (dedup is by `period_end_date`).

```bash
# 1. Build the queue from the current audit
python scripts/generate_reextract_queue.py --since 2026-04-01

# 2. Preview what would change, then apply
python scripts/apply_reextract_queue.py            # dry-run
python scripts/apply_reextract_queue.py --apply    # actually remove the rows

# 3. Ratchet the baseline down and commit both
python -m src.audit --write-baseline audit_baseline.json
git add output/ backfill/ audit_baseline.json && git commit -m "queue re-extraction (N filings)"
```

The removed rows are backed up to `backfill/reextract_removed_<timestamp>.csv`,
so the original values are recoverable without git archaeology. If the same
accession keeps re-appearing in the queue across runs, the extraction isn't
converging and the prompt needs work — not another retry.

### Writing the digest

Report only what the data shows. Every figure must be traceable to a cell in
`output/{ticker}/tracking.csv` or a line in `output/{ticker}/alert_log.txt`.
Values there are final — never re-apply a growth rate to one, and never
compute a change without reading both endpoints. A blank row is an extraction
failure, not a story: list it as a gap rather than inferring what it would
contain. Don't explain *why* a number moved unless the filing says so and you
are quoting it. A quiet week is a legitimate outcome.

Coverage is determined by what the pipeline extracted — never include an
issuer because it is well known.

## Tests

```bash
python -m pytest tests/ -v
```

CI (`.github/workflows/ci.yml`) runs the tests and the integrity gate on every
push and pull request, including the scheduler's own commits.
