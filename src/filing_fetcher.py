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

    filings = []
    for i, form in enumerate(forms):
        if form in ('10-Q', '10-K', '10-Q/A', '10-K/A'):
            filings.append({
                'period_end': dates[i],
                'form_type': form,
                'accession_number': accessions[i],
                'primary_document': primary_docs[i],
            })

    # Handle older filings in additional files
    for file_entry in data.get('filings', {}).get('files', []):
        file_url = f'{BASE_SEC}/submissions/{file_entry["name"]}'
        sec_rate_limiter.wait()
        resp2 = requests.get(file_url, headers=HEADERS, timeout=30)
        resp2.raise_for_status()
        older = resp2.json()
        for i, form in enumerate(older.get('form', [])):
            if form in ('10-Q', '10-K', '10-Q/A', '10-K/A'):
                filings.append({
                    'period_end': older['reportDate'][i],
                    'form_type': form,
                    'accession_number': older['accessionNumber'][i],
                    'primary_document': older['primaryDocument'][i],
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


def get_unprocessed_filings(cik: str, csv_path: Path, since: str = '') -> list[dict]:
    """Return filings not yet in the tracking CSV.

    Args:
        since: Optional cutoff date (YYYY-MM-DD). Only return filings on or after this date.
    """
    all_filings = discover_filings(cik)

    if since:
        all_filings = [f for f in all_filings if f['period_end'] >= since]

    processed_periods = set()
    if csv_path.exists():
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('period_end_date'):
                    processed_periods.add(row['period_end_date'])

    return [f for f in all_filings if f['period_end'] not in processed_periods]
