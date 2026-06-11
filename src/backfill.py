"""Backfill re-extraction pipeline — ledger-driven, resumable, quality-gated.

Re-extracts historical filings through a locate-then-extract flow and stages
results per issuer; live output/ files are only replaced when the issuer's
full series passes a fill-rate gate (see BACKFILL_DESIGN.md).

Phases (each idempotent, designed for Claude Code sessions):

    python -m src.backfill prepare  --since 2023-01-01 --tickers AAPL,NUE
    python -m src.backfill prepare  --since 2023-01-01 --next 25
    # Claude Code processes backfill/requests/ -> backfill/results/
    python -m src.backfill resolve            # apply locate results, emit extraction requests
    # Claude Code processes the new extraction requests
    python -m src.backfill finalize           # stage rows, rebuild chronology, report gate
    python -m src.backfill finalize --commit  # also cut over gated issuers to output/
    python -m src.backfill status
"""

import argparse
import csv
import json
import logging
import re
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

from .change_detect import detect_changes
from .config import IssuerConfig, load_config
from .engine import _build_schema_for_section, OUTPUT_DIR
from .filer_profile import get_or_create_profile, build_prompt_context
from .filing_fetcher import discover_filings, fetch_filing_text
from .qualitative import extract_qualitative
from .registry import (
    load_universe, save_universe, get_active, mark_active, append_review_item,
)
from .section_locate import (
    assess_sections, build_locate_request, apply_locate_result,
    heading_to_regex, persist_learned_heading,
)
from .validate import validate_row

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKFILL_DIR = PROJECT_ROOT / 'backfill'
STATE_CSV = BACKFILL_DIR / 'state.csv'
REQUESTS_DIR = BACKFILL_DIR / 'requests'
RESULTS_DIR = BACKFILL_DIR / 'results'
UNITS_DIR = BACKFILL_DIR / 'units'
CACHE_DIR = BACKFILL_DIR / 'cache'
STAGING_DIR = BACKFILL_DIR / 'staging'

EXTRACTION_VERSION = 2
GATE_MEDIAN_FILL = 0.5

STATE_COLUMNS = [
    'ticker', 'cik', 'accession_number', 'period_end', 'form_type', 'status',
    'sections_located', 'sections_total', 'fill_rate', 'attempts',
    'last_error', 'updated_at',
]

# Unit lifecycle: pending -> locate_pending -> located -> staged -> committed
#                            \-> locate_failed (terminal, reviewable)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _unit_key(ticker: str, accession: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', f'{ticker}_{accession}')


# ---------------------------------------------------------------- ledger ----

def load_state() -> dict[str, dict]:
    """Load the work ledger keyed by unit key."""
    if not STATE_CSV.exists():
        return {}
    units = {}
    with open(STATE_CSV, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            units[_unit_key(row['ticker'], row['accession_number'])] = row
    return units


def save_state(units: dict[str, dict]):
    STATE_CSV.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(units.values(), key=lambda r: (r['ticker'], r['period_end']))
    with open(STATE_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=STATE_COLUMNS, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(ordered)


def _load_unit_detail(key: str) -> dict | None:
    path = UNITS_DIR / f'{key}.json'
    if not path.exists():
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _save_unit_detail(key: str, detail: dict):
    UNITS_DIR.mkdir(parents=True, exist_ok=True)
    with open(UNITS_DIR / f'{key}.json', 'w', encoding='utf-8') as f:
        json.dump(detail, f, indent=2, ensure_ascii=False)


def _update_ledger(units: dict, key: str, detail: dict, status: str,
                   fill_rate: str = '', error: str = ''):
    sections = detail.get('sections', {})
    located = sum(1 for s in sections.values()
                  if s.get('status') in ('ok', 'not_disclosed'))
    row = units.get(key, {})
    row.update({
        'ticker': detail['ticker'],
        'cik': detail['cik'],
        'accession_number': detail['accession_number'],
        'period_end': detail['period_end'],
        'form_type': detail['form_type'],
        'status': status,
        'sections_located': str(located),
        'sections_total': str(len(sections)),
        'fill_rate': fill_rate or row.get('fill_rate', ''),
        'attempts': row.get('attempts', '0'),
        'last_error': error,
        'updated_at': _now_iso(),
    })
    units[key] = row


# ----------------------------------------------------------- filing text ----

def _get_filing_text(cik: str, filing_meta: dict) -> str:
    """Fetch filing text, with a local cache (backfill only, gitignored)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{filing_meta['accession_number'].replace('-', '')}.txt"
    if cache_path.exists():
        return cache_path.read_text(encoding='utf-8')
    text = fetch_filing_text(cik, filing_meta['accession_number'],
                             filing_meta['primary_document'])
    cache_path.write_text(text, encoding='utf-8')
    return text


def _find_note_headings(text: str) -> list[str]:
    from .bootstrap import _find_note_headings as fnh
    return fnh(text)


def _unit_artifacts_present(key: str) -> bool:
    """Check that a unit's request files (and detail sidecar) still exist."""
    detail = _load_unit_detail(key)
    if not detail:
        return False
    for sec in detail.get('sections', {}).values():
        for file_key in ('request_file', 'locate_file'):
            name = sec.get(file_key)
            if name and not (REQUESTS_DIR / name).exists():
                return False
    return True


# -------------------------------------------------------------- manifest ----

def _refresh_manifest():
    """List request files that don't have results yet, for Claude Code."""
    REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
    pending = []
    for req_path in sorted(REQUESTS_DIR.glob('*.json')):
        if req_path.name == 'manifest.json':
            continue
        if not (RESULTS_DIR / req_path.name).exists():
            with open(req_path, 'r', encoding='utf-8') as f:
                req = json.load(f)
            pending.append({
                'file': req_path.name,
                'type': req.get('type', ''),
                'ticker': req.get('ticker', ''),
                'section': req.get('section_name', ''),
                'period_end': req.get('period_end', ''),
            })
    manifest = {'timestamp': _now_iso(), 'count': len(pending), 'requests': pending}
    with open(REQUESTS_DIR / 'manifest.json', 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)
    logger.info(f'Manifest: {len(pending)} requests awaiting results')


# --------------------------------------------------------------- prepare ----

def _select_issuers(universe: list[dict], tickers: list[str],
                    next_n: int, units: dict) -> list[dict]:
    """Pick issuers to (re)backfill: explicit tickers, or next N not yet seeded."""
    rows = get_active(universe)
    if tickers:
        wanted = {t.upper() for t in tickers}
        return [r for r in rows if r.get('ticker', '').upper() in wanted]
    seeded = {u['ticker'].upper() for u in units.values()}
    fresh = [r for r in rows if r.get('ticker', '').upper() not in seeded]
    fresh.sort(key=lambda r: r.get('ticker', ''))
    return fresh[:next_n]


def _resolve_config_path(raw_path: str) -> Path:
    from .cc_bridge import _resolve_config_path as rcp
    return rcp(raw_path)


def _write_extraction_request(config: IssuerConfig, config_path: Path,
                              filing_meta: dict, section_name: str,
                              section_text: str, filer_context: str) -> str:
    """Write one extraction request; returns its filename."""
    schema = _build_schema_for_section(config, section_name)
    key = _unit_key(config.ticker, filing_meta['accession_number'])
    filename = f'{key}_{section_name}_extract.json'
    request = {
        'type': 'extraction',
        'ticker': config.ticker,
        'cik': config.cik,
        'issuer': config.issuer,
        'period_end': filing_meta['period_end'],
        'form_type': filing_meta['form_type'],
        'accession_number': filing_meta['accession_number'],
        'primary_document': filing_meta.get('primary_document', ''),
        'section_name': section_name,
        'section_text': section_text,
        'schema': schema,
        'prior_values': {},
        'filer_context': filer_context,
        'config_path': str(config_path),
    }
    REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REQUESTS_DIR / filename, 'w', encoding='utf-8') as f:
        json.dump(request, f, indent=2, ensure_ascii=False)
    return filename


def prepare(since: str, tickers: list[str], next_n: int):
    """Seed ledger units and write locate/extraction requests."""
    universe = load_universe()
    if not universe:
        logger.error('Universe is empty')
        return

    units = load_state()
    issuers = _select_issuers(universe, tickers, next_n, units)
    logger.info(f'Preparing {len(issuers)} issuers (since {since})')

    for row in issuers:
        ticker = row.get('ticker', '')
        cik = row.get('cik', '')
        config_path = _resolve_config_path(row.get('config_path', ''))
        if not config_path.exists():
            logger.warning(f'{ticker}: config not found, skipping')
            continue
        try:
            config = load_config(config_path)
        except Exception as e:
            logger.error(f'{ticker}: config load failed: {e}')
            continue

        try:
            filings = [f for f in discover_filings(cik) if f['period_end'] >= since]
        except Exception as e:
            logger.error(f'{ticker}: EDGAR discovery failed: {e}')
            continue
        logger.info(f'{ticker}: {len(filings)} filings since {since}')

        profile = get_or_create_profile(cik, ticker, config.issuer)
        filer_context = build_prompt_context(profile)

        for filing_meta in filings:
            key = _unit_key(ticker, filing_meta['accession_number'])
            existing = units.get(key, {})
            if existing.get('status') == 'committed':
                continue
            if existing.get('status') in ('located', 'staged'):
                # Requests/results are gitignored; if a previous session's
                # artifacts are gone, re-prepare instead of stalling forever.
                if _unit_artifacts_present(key):
                    continue
                logger.info(f'{ticker} {filing_meta["period_end"]}: artifacts missing, re-preparing')

            try:
                filing_text = _get_filing_text(cik, filing_meta)
            except Exception as e:
                logger.error(f'{ticker} {filing_meta["period_end"]}: fetch failed: {e}')
                detail = _load_unit_detail(key) or {
                    'ticker': ticker, 'cik': cik,
                    'accession_number': filing_meta['accession_number'],
                    'period_end': filing_meta['period_end'],
                    'form_type': filing_meta['form_type'],
                    'sections': {},
                }
                _update_ledger(units, key, detail, 'pending', error=str(e))
                continue

            assessed = assess_sections(filing_text, config)
            note_headings = None
            detail = {
                'ticker': ticker,
                'cik': cik,
                'accession_number': filing_meta['accession_number'],
                'period_end': filing_meta['period_end'],
                'form_type': filing_meta['form_type'],
                'filing_date': filing_meta.get('filing_date', ''),
                'primary_document': filing_meta.get('primary_document', ''),
                'config_path': str(config_path),
                'sections': {},
            }

            any_locate = False
            for section_name, info in assessed.items():
                if info['status'] == 'ok':
                    req_file = _write_extraction_request(
                        config, config_path, filing_meta, section_name,
                        info['text'], filer_context)
                    detail['sections'][section_name] = {
                        'status': 'ok', 'request_file': req_file,
                        'how': 'regex',
                    }
                else:
                    if note_headings is None:
                        note_headings = _find_note_headings(filing_text)
                    locate_req = build_locate_request(
                        filing_text, config, section_name, filing_meta, note_headings)
                    locate_file = f'{key}_{section_name}_locate.json'
                    with open(REQUESTS_DIR / locate_file, 'w', encoding='utf-8') as f:
                        json.dump(locate_req, f, indent=2, ensure_ascii=False)
                    detail['sections'][section_name] = {
                        'status': info['status'],  # empty / stub / keyword_miss
                        'locate_file': locate_file,
                    }
                    any_locate = True

            _save_unit_detail(key, detail)
            status = 'locate_pending' if any_locate else 'located'
            _update_ledger(units, key, detail, status)

    save_state(units)
    _refresh_manifest()


# --------------------------------------------------------------- resolve ----

def resolve():
    """Apply locate results: slice text, emit extraction requests, learn headings."""
    units = load_state()
    applied = failed = absent = 0

    for key, ledger_row in units.items():
        if ledger_row.get('status') != 'locate_pending':
            continue
        detail = _load_unit_detail(key)
        if not detail:
            continue

        try:
            config_path = Path(detail['config_path'])
            config = load_config(config_path)
        except Exception as e:
            _update_ledger(units, key, detail, 'locate_pending', error=str(e))
            continue

        filing_meta = {
            'period_end': detail['period_end'],
            'form_type': detail['form_type'],
            'accession_number': detail['accession_number'],
            'primary_document': detail.get('primary_document', ''),
        }
        filing_text = None
        profile = get_or_create_profile(detail['cik'], detail['ticker'], config.issuer)
        filer_context = build_prompt_context(profile)

        still_waiting = False
        for section_name, sec in detail['sections'].items():
            locate_file = sec.get('locate_file')
            if not locate_file or sec.get('status') in ('ok', 'not_disclosed', 'locate_failed'):
                continue
            result_path = RESULTS_DIR / locate_file
            if not result_path.exists():
                still_waiting = True
                continue

            with open(result_path, 'r', encoding='utf-8') as f:
                locate_result = json.load(f)

            if filing_text is None:
                try:
                    filing_text = _get_filing_text(detail['cik'], filing_meta)
                except Exception as e:
                    logger.error(f'{key}: refetch failed: {e}')
                    still_waiting = True
                    break

            text, status = apply_locate_result(
                filing_text, locate_result, config.sections[section_name])

            if status == 'ok':
                req_file = _write_extraction_request(
                    config, config_path, filing_meta, section_name, text, filer_context)
                sec.update({'status': 'ok', 'request_file': req_file, 'how': 'llm_locate'})
                heading = (locate_result.get('heading_text') or '').strip()
                if heading:
                    persist_learned_heading(config_path, section_name,
                                            heading_to_regex(heading))
                applied += 1
            elif status == 'not_disclosed':
                sec.update({'status': 'not_disclosed',
                            'note': locate_result.get('note', '')})
                absent += 1
            else:
                sec.update({'status': 'locate_failed',
                            'note': locate_result.get('note', '')})
                failed += 1

        _save_unit_detail(key, detail)
        if not still_waiting:
            terminal = all(s.get('status') in ('ok', 'not_disclosed', 'locate_failed')
                           for s in detail['sections'].values())
            if terminal:
                _update_ledger(units, key, detail, 'located')

    save_state(units)
    _refresh_manifest()
    logger.info(f'Resolve: {applied} located, {absent} not disclosed, {failed} failed')


# -------------------------------------------------------------- finalize ----

def _live_columns(config: IssuerConfig) -> list[str]:
    cols = ['period_end_date', 'form_type']
    cols.extend(config.fields.keys())
    cols.extend(['accession_number', 'filing_date', 'processed_at', 'extraction_version'])
    return cols


def _assemble_row(key: str, detail: dict, config: IssuerConfig) -> dict | None:
    """Build a staged row from extraction results. None if results incomplete."""
    row = {
        'period_end_date': detail['period_end'],
        'form_type': detail['form_type'],
        'accession_number': detail['accession_number'],
        'filing_date': detail.get('filing_date', ''),
        'processed_at': _now_iso(),
        'extraction_version': EXTRACTION_VERSION,
    }
    provenance = {}
    flags = []
    sections_status = {}
    section_texts = {}

    for section_name, sec in detail['sections'].items():
        status = sec.get('status', '')
        sections_status[section_name] = status
        schema = _build_schema_for_section(config, section_name)

        if status == 'ok':
            result_path = RESULTS_DIR / sec['request_file']
            if not result_path.exists():
                return None  # extraction result missing — stay located, retry later
            with open(result_path, 'r', encoding='utf-8') as f:
                llm_result = json.load(f)
            for field_name in schema:
                field_data = llm_result.get('fields', {}).get(field_name, {})
                value = field_data.get('value')
                row[field_name] = value
                provenance[field_name] = 'extracted' if value is not None else 'not_disclosed'
            flags.extend(f for f in llm_result.get('flags', []) if f)
            req_path = REQUESTS_DIR / sec['request_file']
            if req_path.exists():
                with open(req_path, 'r', encoding='utf-8') as f:
                    section_texts[section_name] = json.load(f).get('section_text', '')
        elif status == 'not_disclosed':
            for field_name in schema:
                provenance[field_name] = 'not_disclosed'
        else:  # locate_failed / unresolved
            for field_name in schema:
                provenance[field_name] = 'section_missing'

    return {
        'row': row,
        'provenance': provenance,
        'sections_status': sections_status,
        'section_texts': section_texts,
        'flags': flags,
    }


def _row_fill_rate(staged: dict) -> float | None:
    """Filled / applicable. Excludes not_disclosed fields from the denominator;
    section_missing fields count against the rate."""
    filled = applicable = 0
    for field, prov in staged['provenance'].items():
        if prov == 'extracted':
            filled += 1
            applicable += 1
        elif prov == 'section_missing':
            applicable += 1
        # not_disclosed: excluded entirely
    return filled / applicable if applicable else None


def _rebuild_issuer(ticker: str, staged_rows: list[dict],
                    config: IssuerConfig) -> dict:
    """Chronological rebuild: alerts, notes, fill rates, gate verdict."""
    issuer_dir = STAGING_DIR / ticker.lower()
    issuer_dir.mkdir(parents=True, exist_ok=True)

    staged_rows.sort(key=lambda s: (s['row']['period_end_date'], s['row']['form_type']))

    # Periods already covered by live output keep their alert history meaning;
    # anything at or before the live max period is tagged historical so the
    # weekly digest ignores it.
    live_csv = OUTPUT_DIR / ticker.lower() / 'tracking.csv'
    live_max_period = ''
    if live_csv.exists():
        with open(live_csv, 'r', encoding='utf-8') as f:
            periods = [r.get('period_end_date', '') for r in csv.DictReader(f)]
        live_max_period = max(periods) if periods else ''

    alert_blocks = []
    notes_blocks = []
    fill_rates = []
    prior_row = None
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    for staged in staged_rows:
        row = staged['row']
        alerts = detect_changes(row, prior_row, config)
        validation = validate_row(row, prior_row, config)
        alerts.extend(f'[VALIDATION] {v["message"]}' for v in validation
                      if v['level'] == 'error')
        alerts.extend(f'[LLM_FLAG] {f}' for f in staged.get('flags', []))

        historical = bool(live_max_period) and row['period_end_date'] <= live_max_period
        if historical:
            alerts = [f'[HISTORICAL] {a}' for a in alerts]

        if alerts:
            block = [f'=== {row["form_type"]} | Period ending {row["period_end_date"]} | Processed {now} ===']
            block.extend(alerts)
            alert_blocks.append('\n'.join(block))

        notes = extract_qualitative(staged.get('section_texts', {}), config, prior_row)
        if notes:
            lines = [f'--- {row["form_type"]} | Period ending {row["period_end_date"]} ---']
            for category, items in notes.items():
                lines.append(f'\n  [{category}]')
                for item in items:
                    display = item if len(item) <= 300 else item[:297] + '...'
                    lines.append(f'    - {display}')
            notes_blocks.append('\n'.join(lines))

        fr = _row_fill_rate(staged)
        if fr is not None:
            fill_rates.append(fr)
        prior_row = row

    # Newest first, matching live file conventions
    (issuer_dir / 'alerts.txt').write_text(
        '\n\n'.join(reversed(alert_blocks)) + ('\n' if alert_blocks else ''),
        encoding='utf-8')
    (issuer_dir / 'notes.txt').write_text(
        '\n\n'.join(reversed(notes_blocks)) + ('\n' if notes_blocks else ''),
        encoding='utf-8')

    with open(issuer_dir / 'rows.jsonl', 'w', encoding='utf-8') as f:
        for staged in staged_rows:
            f.write(json.dumps({k: v for k, v in staged.items()
                                if k != 'section_texts'},
                               ensure_ascii=False) + '\n')

    any_missing = any('section_missing' in s['provenance'].values()
                      for s in staged_rows)
    median_fill = statistics.median(fill_rates) if fill_rates else None

    if median_fill is not None:
        gate_passed = median_fill >= GATE_MEDIAN_FILL
    else:
        # No applicable fields anywhere: honest minimal filer passes only if
        # nothing actually failed to locate.
        gate_passed = not any_missing

    verdict = {
        'ticker': ticker,
        'rows': len(staged_rows),
        'median_fill_rate': round(median_fill, 3) if median_fill is not None else None,
        'rows_with_missing_sections': sum(
            1 for s in staged_rows if 'section_missing' in s['provenance'].values()),
        'gate_passed': gate_passed,
        'evaluated_at': _now_iso(),
    }
    with open(issuer_dir / 'gate.json', 'w', encoding='utf-8') as f:
        json.dump(verdict, f, indent=2)
    return verdict


def _commit_issuer(ticker: str, staged_rows: list[dict], config: IssuerConfig,
                   universe: list[dict]) -> list[dict]:
    """Atomically replace live output with the staged series."""
    issuer_dir = OUTPUT_DIR / ticker.lower()
    issuer_dir.mkdir(parents=True, exist_ok=True)
    columns = _live_columns(config)

    tmp_csv = issuer_dir / 'tracking.csv.tmp'
    with open(tmp_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        for staged in staged_rows:
            writer.writerow(staged['row'])
    tmp_csv.replace(issuer_dir / 'tracking.csv')

    # Write all rows to consolidated DB
    try:
        from .db import get_connection, upsert_many
        db_rows = []
        for staged in staged_rows:
            db_row = dict(staged['row'])
            db_row['ticker'] = ticker.upper()
            db_row['issuer_name'] = config.issuer
            db_row['cik'] = config.cik
            db_row['sector'] = config.sector
            db_rows.append(db_row)
        conn = get_connection()
        upsert_many(conn, db_rows)
        conn.close()
    except Exception as e:
        logger.debug(f'DB write skipped for {ticker}: {e}')

    staging_dir = STAGING_DIR / ticker.lower()
    for src_name, dst_name in (('alerts.txt', 'alert_log.txt'), ('notes.txt', 'notes.txt')):
        src = staging_dir / src_name
        if src.exists():
            (issuer_dir / dst_name).write_text(src.read_text(encoding='utf-8'),
                                               encoding='utf-8')

    for row in universe:
        if row.get('ticker', '').upper() == ticker.upper():
            if row.get('status') == 'active_needs_review':
                universe = mark_active(universe, ticker,
                                       row.get('config_path', ''))
            break
    return universe


def finalize(commit: bool):
    """Stage extraction results, rebuild chronology, gate, optionally cut over."""
    units = load_state()
    universe = load_universe()

    # Stage any located units whose extraction results are complete
    by_ticker: dict[str, list[str]] = {}
    for key, ledger_row in units.items():
        by_ticker.setdefault(ledger_row['ticker'], []).append(key)

    summary = {'staged': 0, 'waiting': 0, 'gated': [], 'committed': [],
               'gate_failed': [], 'incomplete': []}

    for ticker, keys in sorted(by_ticker.items()):
        config = None
        staged_rows = []
        all_terminal = True

        for key in keys:
            ledger_row = units[key]
            detail = _load_unit_detail(key)
            if not detail:
                all_terminal = False
                continue
            if config is None:
                try:
                    config = load_config(Path(detail['config_path']))
                except Exception as e:
                    logger.error(f'{ticker}: config load failed: {e}')
                    break

            status = ledger_row.get('status', '')
            if status in ('pending', 'locate_pending'):
                all_terminal = False
                continue

            staged = _assemble_row(key, detail, config)
            if staged is None:
                all_terminal = False
                summary['waiting'] += 1
                continue

            fr = _row_fill_rate(staged)
            if status != 'committed':
                _update_ledger(units, key, detail, 'staged',
                               fill_rate=f'{fr:.2f}' if fr is not None else 'n/a')
                summary['staged'] += 1
            staged_rows.append(staged)

        if config is None or not staged_rows:
            continue
        if not all_terminal:
            summary['incomplete'].append(ticker)
            continue

        verdict = _rebuild_issuer(ticker, staged_rows, config)
        summary['gated'].append(verdict)

        if not verdict['gate_passed']:
            summary['gate_failed'].append(ticker)
            append_review_item(
                ticker, units[keys[0]].get('cik', ''),
                reason=f"backfill gate failed: median fill "
                       f"{verdict['median_fill_rate']}, "
                       f"{verdict['rows_with_missing_sections']} rows with missing sections",
                severity='warning')
            continue

        if commit:
            universe = _commit_issuer(ticker, staged_rows, config, universe)
            for key in keys:
                detail = _load_unit_detail(key)
                if detail and units[key].get('status') == 'staged':
                    _update_ledger(units, key, detail, 'committed',
                                   fill_rate=units[key].get('fill_rate', ''))
            summary['committed'].append(ticker)

    save_state(units)
    if commit and summary['committed']:
        save_universe(universe)

    print(f'\n{"=" * 60}')
    print('Backfill finalize')
    print(f'  Units staged this run: {summary["staged"]}')
    print(f'  Units waiting on extraction results: {summary["waiting"]}')
    print(f'  Issuers with incomplete units: {len(summary["incomplete"])}')
    for v in summary['gated']:
        mark = 'PASS' if v['gate_passed'] else 'FAIL'
        print(f"  {v['ticker']}: gate {mark} "
              f"(median fill {v['median_fill_rate']}, {v['rows']} rows)")
    if commit:
        print(f'  Committed to output/: {", ".join(summary["committed"]) or "none"}')
    elif any(v['gate_passed'] for v in summary['gated']):
        print('  (dry stage — rerun with --commit to cut over gated issuers)')
    print()


# ---------------------------------------------------------------- status ----

def status():
    units = load_state()
    if not units:
        print('Ledger is empty — run prepare first.')
        return
    by_status: dict[str, int] = {}
    by_ticker: dict[str, list[str]] = {}
    for row in units.values():
        by_status[row['status']] = by_status.get(row['status'], 0) + 1
        by_ticker.setdefault(row['ticker'], []).append(row['status'])

    print(f'\nBackfill ledger: {len(units)} units, {len(by_ticker)} issuers')
    for s, n in sorted(by_status.items()):
        print(f'  {s}: {n}')
    done = sum(1 for sts in by_ticker.values()
               if all(s == 'committed' for s in sts))
    print(f'  Issuers fully committed: {done}/{len(by_ticker)}\n')


def main():
    parser = argparse.ArgumentParser(description='Backfill re-extraction pipeline')
    sub = parser.add_subparsers(dest='command')

    prep = sub.add_parser('prepare', help='Seed ledger + write locate/extraction requests')
    prep.add_argument('--since', default='2025-01-01', help='Earliest period_end (YYYY-MM-DD)')
    prep.add_argument('--tickers', default='', help='Comma-separated tickers')
    prep.add_argument('--next', dest='next_n', type=int, default=25,
                      help='Number of not-yet-seeded issuers to take (default 25)')
    prep.add_argument('--verbose', '-v', action='store_true')

    res = sub.add_parser('resolve', help='Apply locate results, emit extraction requests')
    res.add_argument('--verbose', '-v', action='store_true')

    fin = sub.add_parser('finalize', help='Stage rows, rebuild chronology, gate')
    fin.add_argument('--commit', action='store_true',
                     help='Replace live output/ for issuers that pass the gate')
    fin.add_argument('--verbose', '-v', action='store_true')

    sub.add_parser('status', help='Print ledger summary')

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, 'verbose', False) else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.command == 'prepare':
        tickers = [t.strip() for t in args.tickers.split(',') if t.strip()]
        prepare(since=args.since, tickers=tickers, next_n=args.next_n)
    elif args.command == 'resolve':
        resolve()
    elif args.command == 'finalize':
        finalize(commit=args.commit)
    elif args.command == 'status':
        status()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
