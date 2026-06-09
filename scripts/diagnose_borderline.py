"""Quick diagnostic for CDW, EME, STE — check all 'notional' occurrences."""
import re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.filing_fetcher import discover_filings, fetch_filing_text
from src.section_extract import extract_derivatives_by_content, _NOTIONAL_ANCHOR, _DERIV_CONTEXT

TICKERS = {'CDW': '1402057', 'EME': '105634', 'STE': '37660'}

import csv
ROOT = Path(__file__).resolve().parent.parent
with open(ROOT / 'registry' / 'universe.csv') as f:
    for row in csv.DictReader(f):
        if row['ticker'] in TICKERS:
            TICKERS[row['ticker']] = row['cik']

for ticker, cik in TICKERS.items():
    print(f'\n{"="*60}\n{ticker}  CIK={cik}')
    filings = discover_filings(cik)
    tenqs = [f for f in filings if f['form_type'] == '10-Q']
    if not tenqs: print('no 10-Q'); continue
    meta = tenqs[-1]
    text = fetch_filing_text(cik, meta['accession_number'], meta['primary_document'])
    print(f'text length: {len(text):,}  period: {meta["period_end"]}')

    # Show ALL occurrences of 'notional' with wide context
    hits = [(m.start(), m.end()) for m in re.finditer(r'notional', text, re.IGNORECASE)]
    print(f"'notional' occurrences: {len(hits)}")
    for i, (start, end) in enumerate(hits):
        ctx_before = text[max(0, start-120):start].replace('\n', ' ')
        ctx_after = text[end:end+120].replace('\n', ' ')
        deriv_window = text[max(0, start-200):start+300]
        has_deriv = bool(_DERIV_CONTEXT.search(deriv_window))
        print(f'  [{i+1}] @{start} deriv_ctx={has_deriv}')
        print(f'      ...{ctx_before}<<NOTIONAL>>{ctx_after}...')

    result = extract_derivatives_by_content(text)
    print(f'\nfallback result: {len(result)} chars')
    if result:
        print(f'  has "notional": {"notional" in result.lower()}')
        print(f'  first 300: {result[:300]}')
