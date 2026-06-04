"""Diagnose and fix failed activation issuers by finding their actual filing headings.

Fetches each issuer's latest 10-K from EDGAR, searches for derivatives/market risk
sections using broader patterns than the bootstrap defaults, generates YAML configs,
and updates the registry.

Usage:
    python scripts/fix_failed_issuers.py [--dry-run]
"""

import csv
import json
import re
import sys
import time
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.filing_fetcher import discover_filings, fetch_filing_text
from src.bootstrap import _classify_archetype_with_confidence, _find_note_headings
from src.utils import sec_rate_limiter

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

REGISTRY = Path(__file__).resolve().parent.parent / 'registry' / 'universe.csv'
PROFILES_DIR = Path(__file__).resolve().parent.parent / 'profiles'

# Broader heading patterns to try
DERIVATIVES_PATTERNS = [
    # Standard Note heading with various dash chars
    r'Note\s+\d+\s*[.–—―�‒—–:—–\-]\s*Derivative',
    r'Note\s+\d+\s*[.–—―�‒—–:—–\-]\s*Financial Instruments',
    r'Note\s+\d+\s*[.–—―�‒—–:—–\-]\s*Hedging',
    r'Note\s+\d+\s*[.–—―�‒—–:—–\-]\s*Fair Value',
    # All-caps variants
    r'NOTE\s+\d+\s*[.–—―�‒—–:—–\-]\s*DERIVATIVE',
    r'NOTE\s+\d+\s*[.–—―�‒—–:—–\-]\s*FINANCIAL INSTRUMENTS',
    r'NOTE\s+\d+\s*[.–—―�‒—–:—–\-]\s*FAIR VALUE',
    r'NOTE\s+\d+\s*[.–—―�‒—–:—–\-]\s*HEDGING',
    # Without "Note" prefix (some filings use just numbers)
    r'(?:^|\n)\s*\d+\.\s+Derivative',
    r'(?:^|\n)\s*\d+\.\s+Financial Instruments',
    r'(?:^|\n)\s*\d+\.\s+Fair Value',
    # Parenthetical note references
    r'\(\d+\)\s+Derivative',
    r'\(\d+\)\s+Financial Instruments',
    # Item 7A direct (some 10-Ks put it here without the full preamble)
    r'ITEM\s*7A',
]

MARKET_RISK_PATTERNS = [
    r'Quantitative\s+and\s+Qualitat\s*ive\s+Disclosures?\s+About\s+Market\s+Risk',
    r'QUANTITATIVE\s+AND\s+QUALITATIVE\s+DISCLOSURES?\s+ABOUT\s+MARKET\s+RISK',
    r'Item\s*7A[.\s]*Quantitative',
    r'ITEM\s*7A[.\s]*QUANTITATIVE',
    r'Item\s*7A[.\s—–—–\-]*Market\s+Risk',
    r'ITEM\s*7A',
    r'Market\s+Risk\s+Management',
]


def find_section(text, patterns, label='section'):
    """Try each pattern and return (matched_pattern, start_pos, snippet)."""
    for pat in patterns:
        matches = list(re.finditer(pat, text, re.I))
        if matches:
            # Filter ToC matches (heading followed by page number)
            real = []
            for m in matches:
                after = text[m.end():m.end() + 30].strip()
                if re.match(r'^\d{1,3}\s', after):
                    continue
                real.append(m)
            if not real:
                real = matches  # fallback
            m = real[-1]  # use last match (ToC is usually earlier)
            snippet = text[m.start():m.start() + 200].replace('\n', ' ').strip()
            return pat, m.start(), snippet
    return None, None, None


def extract_section_text(text, start, max_len=8000):
    """Extract section text from start position."""
    # Find next Note or Item heading as boundary
    boundary = re.search(
        r'(?:Note|NOTE)\s+\d+\s*[.–—―�‒—–:—–\-]|Item\s*[\s\xa0]*[489]|ITEM\s*[\s\xa0]*[489]',
        text[start + 80:],
        re.I,
    )
    end = start + 80 + boundary.start() if boundary else start + max_len
    return text[start:end][:max_len]


def has_derivative_keywords(text):
    """Check if text contains derivative-related keywords."""
    keywords = [
        r'derivative', r'hedge', r'forward\s+contract', r'swap',
        r'notional', r'fair\s+value.*(?:asset|liabilit)',
    ]
    count = 0
    for kw in keywords:
        if re.search(kw, text[:50000], re.I):
            count += 1
    return count


def determine_archetype(text, deriv_section, mr_section):
    """Determine archetype from filing content."""
    archetype, confidence = _classify_archetype_with_confidence(text)
    # Override if no derivatives found
    deriv_count = has_derivative_keywords(deriv_section or '')
    if deriv_count == 0 and not deriv_section:
        if mr_section:
            archetype = 'no_derivatives'
        else:
            archetype = 'minimal_hedger'
    return archetype, confidence


def build_config(ticker, issuer_name, cik, archetype, deriv_pattern, mr_pattern):
    """Build a YAML config."""
    lines = [
        f'issuer: "{issuer_name}"',
        f'ticker: {ticker}',
        f'cik: "{cik}"',
        f'archetype: {archetype}',
        f'extraction_mode: llm',
        '',
    ]

    if deriv_pattern:
        lines.append(f'# Derivatives section matched by: {deriv_pattern}')
    if mr_pattern:
        lines.append(f'# Market risk section matched by: {mr_pattern}')
    lines.append('')

    return '\n'.join(lines) + '\n'


def process_issuer(row, dry_run=False):
    """Process one failed issuer."""
    ticker = row['ticker']
    cik = row['cik']
    issuer_name = row.get('issuer_name', ticker)

    logger.info(f'{ticker} ({issuer_name}): fetching filings...')

    try:
        filings = discover_filings(cik)
    except Exception as e:
        logger.error(f'{ticker}: EDGAR fetch failed: {e}')
        return None

    # Prefer 10-K
    ten_ks = [f for f in filings if f['form_type'] == '10-K']
    ten_qs = [f for f in filings if f['form_type'] == '10-Q']

    filing = None
    if ten_ks:
        filing = ten_ks[-1]
    elif ten_qs:
        filing = ten_qs[-1]

    if not filing:
        logger.warning(f'{ticker}: no filings found')
        return None

    logger.info(f'{ticker}: using {filing["form_type"]} {filing["period_end"]}')

    try:
        text = fetch_filing_text(cik, filing['accession_number'], filing['primary_document'])
    except Exception as e:
        logger.error(f'{ticker}: fetch text failed: {e}')
        return None

    # Find sections
    deriv_pat, deriv_start, deriv_snippet = find_section(text, DERIVATIVES_PATTERNS, 'derivatives')
    mr_pat, mr_start, mr_snippet = find_section(text, MARKET_RISK_PATTERNS, 'market_risk')

    deriv_text = extract_section_text(text, deriv_start) if deriv_start else None
    mr_text = extract_section_text(text, mr_start) if mr_start else None

    # Determine archetype
    archetype, confidence = determine_archetype(text, deriv_text, mr_text)

    result = {
        'ticker': ticker,
        'issuer_name': issuer_name,
        'cik': cik,
        'filing': f'{filing["form_type"]} {filing["period_end"]}',
        'deriv_pattern': deriv_pat,
        'deriv_snippet': deriv_snippet,
        'mr_pattern': mr_pat,
        'mr_snippet': mr_snippet,
        'archetype': archetype,
        'confidence': confidence,
        'has_deriv_section': deriv_start is not None,
        'has_mr_section': mr_start is not None,
        'deriv_keywords': has_derivative_keywords(text),
    }

    if not dry_run and (deriv_start or mr_start):
        config = build_config(ticker, issuer_name, cik, archetype, deriv_pat, mr_pat)
        config_path = PROFILES_DIR / f'{ticker.lower()}.yaml'
        config_path.write_text(config, encoding='utf-8')
        result['config_path'] = str(config_path)
        logger.info(f'{ticker}: wrote config ({archetype}, conf={confidence:.2f})')

    return result


def main():
    dry_run = '--dry-run' in sys.argv

    # Load failed issuers
    rows = []
    with open(REGISTRY, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    failed = [r for r in rows if r.get('status') == 'failed_activation']
    logger.info(f'Processing {len(failed)} failed issuers...')

    results = []
    for row in failed:
        result = process_issuer(row, dry_run=dry_run)
        if result:
            results.append(result)

    # Summary
    print(f'\n{"=" * 60}')
    print(f'Results: {len(results)}/{len(failed)} processed')
    print(f'{"=" * 60}')

    found_both = [r for r in results if r['has_deriv_section'] and r['has_mr_section']]
    found_mr_only = [r for r in results if not r['has_deriv_section'] and r['has_mr_section']]
    found_neither = [r for r in results if not r['has_deriv_section'] and not r['has_mr_section']]

    print(f'\nFound both sections: {len(found_both)}')
    for r in found_both:
        print(f'  {r["ticker"]}: {r["archetype"]} (conf={r["confidence"]:.2f}) — {r["filing"]}')
        print(f'    Deriv: {r["deriv_snippet"][:80]}...')

    print(f'\nFound market risk only: {len(found_mr_only)}')
    for r in found_mr_only:
        print(f'  {r["ticker"]}: {r["archetype"]} (keywords={r["deriv_keywords"]}) — {r["filing"]}')

    print(f'\nFound neither section: {len(found_neither)}')
    for r in found_neither:
        print(f'  {r["ticker"]}: keywords={r["deriv_keywords"]} — {r["filing"]}')

    # Update registry
    if not dry_run:
        configs_written = [r for r in results if r.get('config_path')]
        if configs_written:
            for row in rows:
                for r in configs_written:
                    if row.get('ticker') == r['ticker']:
                        row['status'] = 'active_needs_review'
                        row['config_path'] = f'profiles/{r["ticker"].lower()}.yaml'
                        row['activation_fail_count'] = '0'
                        break

            with open(REGISTRY, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            logger.info(f'Updated {len(configs_written)} issuers to active_needs_review')


if __name__ == '__main__':
    main()
