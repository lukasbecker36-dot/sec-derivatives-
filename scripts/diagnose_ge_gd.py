"""Quick targeted diagnostic for GE and GD."""
import re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import load_config
from src.filing_fetcher import discover_filings, fetch_filing_text
from src.section_extract import extract_derivatives_by_content, _NOTIONAL_ANCHOR, _DERIV_CONTEXT

for ticker, cik in [('ge', '40533'), ('gd', '40533')]:
    # Fix CIKs
    pass

TICKERS = {'ge': '40533', 'gd': '101830'}

for ticker, cik in TICKERS.items():
    print(f'\n{"="*60}\n{ticker.upper()}  CIK={cik}')
    filings = discover_filings(cik)
    tenqs = [f for f in filings if f['form_type'] == '10-Q']
    if not tenqs: print('no 10-Q'); continue
    meta = tenqs[-1]
    text = fetch_filing_text(cik, meta['accession_number'], meta['primary_document'])
    print(f'text length: {len(text):,}')

    hits = list(_NOTIONAL_ANCHOR.finditer(text))
    print(f'anchor matches: {len(hits)}')
    for m in hits[:5]:
        pos = m.start()
        ctx = text[max(0, pos-200):pos+300]
        has_ctx = bool(_DERIV_CONTEXT.search(ctx))
        snip = text[pos:pos+80].replace('\n',' ')
        print(f'  @{pos} deriv_ctx={has_ctx}: {snip}')

    result = extract_derivatives_by_content(text)
    print(f'fallback result: {len(result)} chars, has notional: {"notional" in result.lower()}')
