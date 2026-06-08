"""One-shot diagnostic: what does MSFT's 10-Q actually contain for derivatives?

Run once locally (where EDGAR is reachable):
    python scripts/diagnose_msft.py

Prints ground truth so we stop guessing at regex patterns:
  - Whether 'notional' appears in the filing at all, and in what context
  - What each configured section actually captures
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config
from src.filing_fetcher import discover_filings, fetch_filing_text
from src.section_extract import extract_section

CIK = '789019'


def main():
    filings = discover_filings(CIK)
    # Most recent 10-Q and the latest 10-K
    tenqs = [f for f in filings if f['form_type'] == '10-Q']
    tenks = [f for f in filings if f['form_type'] == '10-K']

    targets = []
    if tenqs:
        targets.append(tenqs[-1])
    if tenks:
        targets.append(tenks[-1])

    config = load_config(Path('profiles/msft.yaml'))

    for meta in targets:
        print('=' * 70)
        print(f"{meta['form_type']}  period={meta['period_end']}")
        print('=' * 70)
        text = fetch_filing_text(CIK, meta['accession_number'], meta['primary_document'])
        print(f'Total cleaned text length: {len(text):,} chars\n')

        # 1. Does 'notional' appear at all? Show every occurrence in context.
        notional_hits = [m.start() for m in re.finditer(r'notional', text, re.IGNORECASE)]
        print(f"'notional' occurrences: {len(notional_hits)}")
        for pos in notional_hits[:8]:
            snippet = text[max(0, pos - 80):pos + 120].replace('\n', ' ')
            print(f'   ...{snippet}...')
        print()

        # 2. Where does the actual 'Note 5' heading appear?
        note5_hits = [m.start() for m in re.finditer(r'Note\s+\d+\s*[.–—\-:]?\s*Derivatives', text, re.IGNORECASE)]
        print(f"'Note N - Derivatives' heading matches: {len(note5_hits)}")
        for pos in note5_hits:
            snippet = text[pos:pos + 160].replace('\n', ' ')
            print(f'   @{pos}: {snippet}...')
        print()

        # 3. What does each configured section actually capture?
        for name, sec_cfg in config.sections.items():
            captured = extract_section(text, sec_cfg)
            print(f'--- section "{name}": {len(captured)} chars captured ---')
            if captured:
                has_notional = 'notional' in captured.lower()
                print(f'    contains "notional": {has_notional}')
                print(f'    first 200: {captured[:200].strip()}')
            print()


if __name__ == '__main__':
    main()
