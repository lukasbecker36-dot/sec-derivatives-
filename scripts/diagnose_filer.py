"""One-shot diagnostic: what do a filer's filings actually contain for derivatives?

Run locally (where EDGAR is reachable):
    python scripts/diagnose_filer.py AAPL AMZN CSCO ORCL

For each ticker it loads the issuer config, fetches the latest 10-Q, and prints
ground truth so we stop guessing at regex patterns:
  - Whether 'notional' appears in the filing at all, and where
  - Where the real derivatives note heading sits vs. cross-references
  - What each configured section actually captures (and if it has 'notional')
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


def cik_for(ticker: str) -> str | None:
    with open(ROOT / 'registry' / 'universe.csv', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['ticker'].lower() == ticker.lower():
                return row['cik']
    return None


def diagnose(ticker: str):
    print('#' * 72)
    print(f'# {ticker}')
    print('#' * 72)
    cik = cik_for(ticker)
    if not cik:
        print(f'  no CIK in universe for {ticker}\n')
        return

    cfg_path = ROOT / 'profiles' / f'{ticker.lower()}.yaml'
    if not cfg_path.exists():
        print(f'  no config at {cfg_path}\n')
        return
    config = load_config(cfg_path)

    filings = discover_filings(cik)
    tenqs = [f for f in filings if f['form_type'] == '10-Q']
    if not tenqs:
        print('  no 10-Q found\n')
        return
    meta = tenqs[-1]
    print(f"  archetype config sections: {list(config.sections.keys())}")
    print(f"  latest 10-Q period={meta['period_end']}")
    text = fetch_filing_text(cik, meta['accession_number'], meta['primary_document'])
    print(f'  cleaned text: {len(text):,} chars')

    # Does 'notional' appear at all?
    hits = [m.start() for m in re.finditer(r'notional', text, re.IGNORECASE)]
    print(f"  'notional' occurrences: {len(hits)}")
    for pos in hits[:3]:
        snip = text[max(0, pos - 70):pos + 90].replace('\n', ' ')
        print(f'     ...{snip}...')

    # What does each section capture?
    for name, sec_cfg in config.sections.items():
        captured = extract_section(text, sec_cfg)
        flag = ''
        if captured:
            flag = '  HAS notional' if 'notional' in captured.lower() else '  (no notional)'
        print(f'  section "{name}": {len(captured)} chars{flag}')
    print()


def main():
    tickers = sys.argv[1:] or ['AAPL', 'AMZN', 'CSCO', 'ORCL']
    for t in tickers:
        try:
            diagnose(t)
        except Exception as e:
            print(f'  ERROR diagnosing {t}: {e}\n')


if __name__ == '__main__':
    main()
