"""Batch section audit across the active universe.

For every active issuer (or a supplied subset), fetch the latest 10-Q and
determine, with no LLM cost, which bucket it falls in:

  A. has_notional_captured  -> 'notional' is in the filing AND a configured
                               section captures it -> re-extraction will work
  B. notional_not_captured  -> 'notional' is in the filing but NO section
                               captures it -> heading pattern needs fixing
  C. no_notional            -> 'notional' absent from 10-Q -> "No mention" is
                               legitimately correct (likely annual-only disclosure)

Writes output/section_audit.csv and prints a summary. For bucket B it records
the ~60 chars preceding the first notional hit (often the section heading),
so heading patterns can be fixed without another EDGAR round-trip.

Usage:
    python scripts/audit_sections.py                # all active issuers
    python scripts/audit_sections.py AAPL CSCO PEP  # subset
"""

import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config
from src.filing_fetcher import discover_filings, fetch_filing_text
from src.section_extract import extract_section

ROOT = Path(__file__).resolve().parent.parent
OUT_CSV = ROOT / 'output' / 'section_audit.csv'


def load_active():
    rows = []
    with open(ROOT / 'registry' / 'universe.csv', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['status'] in ('active', 'active_needs_review'):
                rows.append(row)
    return rows


def audit_one(ticker: str, cik: str) -> dict | None:
    cfg_path = ROOT / 'profiles' / f'{ticker.lower()}.yaml'
    if not cfg_path.exists():
        return {'ticker': ticker, 'bucket': 'no_config', 'note': str(cfg_path)}
    config = load_config(cfg_path)

    filings = discover_filings(cik)
    tenqs = [f for f in filings if f['form_type'] == '10-Q']
    if not tenqs:
        return {'ticker': ticker, 'bucket': 'no_10q', 'note': ''}
    meta = tenqs[-1]
    text = fetch_filing_text(cik, meta['accession_number'], meta['primary_document'])

    hits = [m.start() for m in re.finditer(r'notional', text, re.IGNORECASE)]
    if not hits:
        return {'ticker': ticker, 'bucket': 'no_notional',
                'period': meta['period_end'], 'note': ''}

    # Which section captures a notional?
    capturing = []
    for name, sec_cfg in config.sections.items():
        captured = extract_section(text, sec_cfg)
        if captured and 'notional' in captured.lower():
            capturing.append(name)

    if capturing:
        return {'ticker': ticker, 'bucket': 'captured',
                'period': meta['period_end'], 'note': ','.join(capturing)}

    # Bucket B: notional present but uncaptured — grab context before first hit
    pos = hits[0]
    context = text[max(0, pos - 70):pos].replace('\n', ' ').strip()
    return {'ticker': ticker, 'bucket': 'uncaptured',
            'period': meta['period_end'], 'note': context}


def main():
    active = load_active()
    if len(sys.argv) > 1:
        want = {t.lower() for t in sys.argv[1:]}
        active = [r for r in active if r['ticker'].lower() in want]

    results = []
    for i, row in enumerate(active, 1):
        ticker = row['ticker']
        try:
            res = audit_one(ticker, row['cik'])
        except Exception as e:
            res = {'ticker': ticker, 'bucket': 'error', 'note': str(e)[:120]}
        results.append(res)
        print(f"[{i}/{len(active)}] {ticker}: {res['bucket']}  {res.get('note','')[:70]}")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['ticker', 'bucket', 'period', 'note'])
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, '') for k in ['ticker', 'bucket', 'period', 'note']})

    from collections import Counter
    counts = Counter(r['bucket'] for r in results)
    print('\n' + '=' * 50)
    print('SECTION AUDIT SUMMARY')
    print('=' * 50)
    for b, c in counts.most_common():
        print(f'  {b}: {c}')
    print(f'\nWrote {OUT_CSV}')
    captured = [r['ticker'] for r in results if r['bucket'] == 'captured']
    uncaptured = [r['ticker'] for r in results if r['bucket'] == 'uncaptured']
    print(f"\nRe-extract (captured): {len(captured)}")
    print(f"Need heading fix (uncaptured): {len(uncaptured)}")
    if uncaptured:
        print(f"  {uncaptured}")


if __name__ == '__main__':
    main()
