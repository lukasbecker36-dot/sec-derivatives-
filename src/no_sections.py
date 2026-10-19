"""Registry of filings where no configured section could be sliced.

In Claude Code mode a filing with no sections produces no request, so
finalize never writes a row for it and get_unprocessed_filings returns it
again the next night. Each such filing still took one of the run's 30
filing slots: on 19 October 2026 a dry run found 14 of 30 slots spent on
the same no-section filings (AME, CHRW, CCL, CMG, FFIV, MAS x5, PKG, PSKY x2,
TXN), so issuers later in the queue were never reached.

Recording them here, rather than writing a blank row to tracking.csv, keeps
them out of the audit's empty_row count, which the integrity gate guards.

Entries are keyed to a hash of the issuer's section config. Fixing a
profile's headings (as for NKE and DAL) changes the hash, so the filing
becomes eligible again with no manual re-queue.
"""

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

REGISTRY_PATH = Path(__file__).resolve().parent.parent / 'registry' / 'no_sections.csv'
COLUMNS = ['ticker', 'cik', 'accession_number', 'period_end', 'form_type',
           'filing_date', 'config_hash', 'recorded_at']


def config_hash(config) -> str:
    """Stable hash of the parts of a config that decide what gets sliced."""
    sections = {
        name: {'heading': s.heading, 'match_strategy': s.match_strategy,
               'end_boundary': s.end_boundary, 'max_length': s.max_length,
               'validation_keywords': list(s.validation_keywords or [])}
        for name, s in sorted(config.sections.items())
    }
    blob = json.dumps(sections, sort_keys=True)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()[:16]


def load(path: Path | None = None) -> list[dict]:
    path = path or REGISTRY_PATH
    if not path.exists():
        return []
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def skip_keys(ticker: str, chash: str, rows: list[dict]) -> set[str]:
    """Accession numbers to skip for this issuer under its current config."""
    return {r['accession_number'] for r in rows
            if r.get('ticker', '').upper() == ticker.upper()
            and r.get('config_hash') == chash}


def record(config, filing_meta: dict, chash: str,
           path: Path | None = None) -> None:
    """Add or refresh the entry for one filing (one row per accession)."""
    path = path or REGISTRY_PATH
    rows = [r for r in load(path)
            if r.get('accession_number') != filing_meta.get('accession_number')]
    rows.append({
        'ticker': config.ticker,
        'cik': config.cik,
        'accession_number': filing_meta.get('accession_number', ''),
        'period_end': filing_meta.get('period_end', ''),
        'form_type': filing_meta.get('form_type', ''),
        'filing_date': filing_meta.get('filing_date', ''),
        'config_hash': chash,
        'recorded_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    })
    rows.sort(key=lambda r: (r['ticker'], r['period_end'], r['form_type']))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.csv.tmp')
    with open(tmp, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)
