"""Fetch filings from SEC EDGAR by CIK — no local filing storage."""

import csv
import re
import requests
from pathlib import Path

from .utils import clean_filing_text, sec_rate_limiter

HEADERS = {'User-Agent': 'sec-derivatives/1.0 (lukas@risknet.com)'}
BASE_SEC = 'https://data.sec.gov'
ARCHIVES = 'https://www.sec.gov/Archives/edgar/data'


def _pad_cik(cik: str) -> str:
    return cik.lstrip('0').zfill(10)


def discover_filings(cik: str) -> list[dict]:
    """Query EDGAR for all 10-Q and 10-K filings for a CIK.

    Returns list of {period_end, form_type, accession_number, primary_document}
    sorted by period_end ascending.
    """
    padded = _pad_cik(cik)
    url = f'{BASE_SEC}/submissions/CIK{padded}.json'
    sec_rate_limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    recent = data.get('filings', {}).get('recent', {})
    forms = recent.get('form', [])
    accessions = recent.get('accessionNumber', [])
    dates = recent.get('reportDate', [])
    primary_docs = recent.get('primaryDocument', [])
    filing_dates = recent.get('filingDate', [])

    filings = []
    for i, form in enumerate(forms):
        if form in ('10-Q', '10-K', '10-Q/A', '10-K/A'):
            filings.append({
                'period_end': dates[i],
                'form_type': form,
                'accession_number': accessions[i],
                'primary_document': primary_docs[i],
                'filing_date': filing_dates[i] if i < len(filing_dates) else '',
            })

    # Handle older filings in additional files
    for file_entry in data.get('filings', {}).get('files', []):
        file_url = f'{BASE_SEC}/submissions/{file_entry["name"]}'
        sec_rate_limiter.wait()
        resp2 = requests.get(file_url, headers=HEADERS, timeout=30)
        resp2.raise_for_status()
        older = resp2.json()
        older_filing_dates = older.get('filingDate', [])
        for i, form in enumerate(older.get('form', [])):
            if form in ('10-Q', '10-K', '10-Q/A', '10-K/A'):
                filings.append({
                    'period_end': older['reportDate'][i],
                    'form_type': form,
                    'accession_number': older['accessionNumber'][i],
                    'primary_document': older['primaryDocument'][i],
                    'filing_date': older_filing_dates[i] if i < len(older_filing_dates) else '',
                })

    filings.sort(key=lambda x: x['period_end'])
    return filings


def fetch_filing_text(cik: str, accession: str, document: str) -> str:
    """Download a single filing HTML from EDGAR, return cleaned text."""
    cik_num = cik.lstrip('0')
    acc_nodash = accession.replace('-', '')
    url = f'{ARCHIVES}/{cik_num}/{acc_nodash}/{document}'
    sec_rate_limiter.wait()
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return clean_filing_text(resp.text)


MAX_BLANK_RETRIES = 3

# Metadata columns that never count as "extracted data" — presence of these
# alone means the row is a bookkeeping stub, not a real extraction.
_METADATA_COLUMNS = frozenset({
    'period_end_date', 'form_type', 'accession_number', 'filing_date',
    'processed_at', 'extraction_version', 'extraction_attempts',
})


def _row_has_extracted_data(row: dict) -> bool:
    """True if any non-metadata field is populated with a value.

    A row that carries only bookkeeping columns (accession, filing date,
    timestamps, attempt counter) does NOT count as extracted data — that
    was the historical failure mode where a blank row silently marked the
    period 'processed' and blocked all future retries.
    """
    for k, v in row.items():
        if k is None or k in _METADATA_COLUMNS:
            continue
        if isinstance(v, list):
            if any(str(x).strip() for x in v if x):
                return True
        elif str(v or '').strip():
            return True
    return False


def _row_attempts(row: dict) -> int:
    """Read extraction_attempts (default 0, tolerant of missing/bad values)."""
    raw = row.get('extraction_attempts', '') or '0'
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return 0


def get_unprocessed_filings(cik: str, csv_path: Path, since: str = '',
                            max_blank_retries: int = MAX_BLANK_RETRIES) -> list[dict]:
    """Return filings not yet in the tracking CSV.

    A period is considered 'processed' only if the corresponding row has
    at least one populated extraction field OR has already been retried
    the configured number of times. Rows that are still blank after
    max_blank_retries stop being retried — this is the guard against
    infinite loops on legitimate non-discloser filings (incorporation-
    by-reference 10-Qs), while still recovering from silent extraction
    failures on issuers that DO have derivative activity.

    Args:
        since: Optional cutoff date (YYYY-MM-DD). Only return filings on or after this date.
        max_blank_retries: How many times to retry a period that keeps
            producing a blank row before treating it as legitimately blank.
    """
    all_filings = discover_filings(cik)

    if since:
        all_filings = [f for f in all_filings if f['period_end'] >= since]

    processed_periods = set()
    if csv_path.exists():
        with open(csv_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                pe = row.get('period_end_date')
                if not pe:
                    continue
                if _row_has_extracted_data(row):
                    processed_periods.add(pe)
                elif _row_attempts(row) >= max_blank_retries:
                    processed_periods.add(pe)

    return [f for f in all_filings if f['period_end'] not in processed_periods]
