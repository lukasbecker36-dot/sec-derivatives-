"""Build a re-extraction queue from the current audit.

The audit surfaces which committed rows are unusable (blank, contradictory,
or the wrong-column pattern that put a $36B notional into AT&T's fair-value
field). Nothing consumed those findings — the flagged rows stayed in
output/*/tracking.csv and were read back by the digest as if they were
sound data. This script writes them to backfill/reextract_queue.csv, and
scripts/apply_reextract_queue.py removes them from tracking.csv so the
next daily run re-extracts.

Usage:

    python scripts/generate_reextract_queue.py                # all defects
    python scripts/generate_reextract_queue.py --since 2026-04-01
    python scripts/generate_reextract_queue.py --defect-type implausible_swing
    python scripts/generate_reextract_queue.py --ticker MSFT --ticker IBM

Regenerated each run, not append-only: once a re-extraction fixes a row it
drops out of the audit and out of the queue on the next generation. If the
same accession keeps re-appearing, the extraction isn't converging and the
prompt needs work, not another retry.
"""

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.audit import run_audit  # noqa: E402


META = {'period_end_date', 'form_type', 'accession_number', 'filing_date',
        'processed_at', 'extraction_version'}
QUEUE_PATH = REPO / 'backfill' / 'reextract_queue.csv'
STATE_PATH = REPO / 'backfill' / 'state.csv'
OUTPUT_DIR = REPO / 'output'
COLUMNS = ['ticker', 'cik', 'accession_number', 'period_end', 'form_type',
           'defect_types', 'queued_at', 'source']


def _row_lookup() -> dict:
    """Map (TICKER, period_end, form_type) -> {cik, accession_number} from either
    state.csv or the tracking.csv itself, so the queue carries EDGAR identifiers."""
    lookup = {}
    if STATE_PATH.exists():
        with open(STATE_PATH, encoding='utf-8') as f:
            for r in csv.DictReader(f):
                key = (r['ticker'].upper(), r['period_end'], r['form_type'])
                lookup[key] = {'cik': r.get('cik', ''),
                               'accession_number': r.get('accession_number', '')}
    for csv_path in OUTPUT_DIR.glob('*/tracking.csv'):
        ticker = csv_path.parent.name.upper()
        with open(csv_path, encoding='utf-8') as f:
            for r in csv.DictReader(f):
                key = (ticker, r.get('period_end_date', ''), r.get('form_type', ''))
                if key not in lookup:
                    lookup[key] = {
                        'cik': '',
                        'accession_number': (r.get('accession_number') or '').strip()
                                            if not isinstance(r.get('accession_number'), list)
                                            else '',
                    }
    return lookup


def build_queue(defect_types: list[str] | None, since: str | None,
                tickers: list[str] | None) -> list[dict]:
    report = run_audit(OUTPUT_DIR)
    lookup = _row_lookup()

    keep_types = set(defect_types) if defect_types else None
    keep_tickers = {t.upper() for t in tickers} if tickers else None

    grouped: dict[tuple, set] = {}
    for d in report['defects']:
        if keep_types and d['type'] not in keep_types:
            continue
        if since and d['period'] < since:
            continue
        if keep_tickers and d['ticker'].upper() not in keep_tickers:
            continue
        # unreadable / misaligned rows aren't queueable by this path
        if d['type'] in ('unreadable', 'misaligned_row'):
            continue
        key = (d['ticker'].upper(), d['period'], d['form_type'])
        grouped.setdefault(key, set()).add(d['type'])

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows = []
    for (ticker, period, form_type), types in sorted(grouped.items()):
        meta = lookup.get((ticker, period, form_type), {})
        rows.append({
            'ticker': ticker,
            'cik': meta.get('cik', ''),
            'accession_number': meta.get('accession_number', ''),
            'period_end': period,
            'form_type': form_type,
            'defect_types': ','.join(sorted(types)),
            'queued_at': now,
            'source': 'audit',
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--defect-type', action='append',
                    help='Defect type to queue (repeatable). Default: all.')
    ap.add_argument('--since', help='Earliest period_end_date to queue (YYYY-MM-DD).')
    ap.add_argument('--ticker', action='append', help='Ticker to queue (repeatable).')
    ap.add_argument('--output', type=Path, default=QUEUE_PATH)
    args = ap.parse_args()

    rows = build_queue(args.defect_type, args.since, args.ticker)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

    print(f'Queued {len(rows)} filings for re-extraction -> {args.output}')
    if rows:
        by_type = {}
        for r in rows:
            for t in r['defect_types'].split(','):
                by_type[t] = by_type.get(t, 0) + 1
        for t, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
            print(f'  {t}: {n}')
        no_accession = sum(1 for r in rows if not r['accession_number'])
        if no_accession:
            print(f'\n  {no_accession} row(s) have no accession_number recorded — the daily '
                  f'scheduler will still refetch them by period_end_date, but any manual '
                  f're-extraction will need to look them up on EDGAR.')


if __name__ == '__main__':
    main()
