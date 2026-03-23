"""Universe registry — CSV-based issuer lifecycle management."""

import argparse
import csv
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

REGISTRY_DIR = Path(__file__).resolve().parent.parent / 'registry'
UNIVERSE_CSV = REGISTRY_DIR / 'universe.csv'
ACTIVATION_LOG_CSV = REGISTRY_DIR / 'activation_log.csv'
REVIEW_QUEUE_CSV = REGISTRY_DIR / 'review_queue.csv'

UNIVERSE_COLUMNS = [
    'ticker', 'issuer_name', 'cik', 'sector', 'status', 'archetype_guess',
    'config_path', 'last_checked_at', 'last_filing_date_seen',
    'last_processed_period', 'activation_fail_count', 'notes',
]

ACTIVATION_LOG_COLUMNS = [
    'timestamp', 'ticker', 'cik', 'old_status', 'new_status',
    'filing_date', 'form_type', 'reason',
]

REVIEW_QUEUE_COLUMNS = [
    'timestamp', 'ticker', 'cik', 'reason', 'severity',
    'filing_date', 'form_type', 'config_path', 'status',
]

VALID_STATUSES = {
    'registered', 'activating', 'active',
    'active_needs_review', 'failed_activation',
}

# Legal transitions: old_status -> set of allowed new_statuses
LEGAL_TRANSITIONS = {
    'registered': {'activating'},
    'activating': {'active', 'active_needs_review', 'failed_activation'},
    'failed_activation': {'activating'},
    'active_needs_review': {'active'},
    'active': set(),  # terminal for now
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, 'r', encoding='utf-8', newline='') as f:
        return list(csv.DictReader(f))


def _save_csv(path: Path, rows: list[dict], columns: list[str]) -> None:
    """Atomic write via temp file + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix='.csv')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp, str(path))
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _append_csv(path: Path, row: dict, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with open(path, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ---- Universe CRUD ----

def load_universe() -> list[dict]:
    return _load_csv(UNIVERSE_CSV)


def save_universe(rows: list[dict]) -> None:
    _save_csv(UNIVERSE_CSV, rows, UNIVERSE_COLUMNS)


def get_by_status(rows: list[dict], status: str) -> list[dict]:
    return [r for r in rows if r.get('status') == status]


def get_active(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r.get('status') in ('active', 'active_needs_review')]


def get_registered(rows: list[dict]) -> list[dict]:
    return get_by_status(rows, 'registered')


def get_failed(rows: list[dict]) -> list[dict]:
    return get_by_status(rows, 'failed_activation')


def find_issuer(rows: list[dict], ticker: str) -> dict | None:
    ticker_lower = ticker.lower()
    for r in rows:
        if r.get('ticker', '').lower() == ticker_lower:
            return r
    return None


def _validate_transition(old_status: str, new_status: str) -> None:
    allowed = LEGAL_TRANSITIONS.get(old_status, set())
    if new_status not in allowed:
        raise ValueError(
            f'Invalid status transition: {old_status} -> {new_status}. '
            f'Allowed from {old_status}: {allowed}'
        )


def update_issuer(rows: list[dict], ticker: str, **updates) -> list[dict]:
    """Return new list with the matching ticker's fields updated."""
    ticker_lower = ticker.lower()
    found = False
    result = []
    for r in rows:
        if r.get('ticker', '').lower() == ticker_lower:
            found = True
            new_row = dict(r)
            if 'status' in updates and updates['status'] != r.get('status'):
                _validate_transition(r.get('status', ''), updates['status'])
            new_row.update(updates)
            result.append(new_row)
        else:
            result.append(r)
    if not found:
        raise KeyError(f'Ticker not found in universe: {ticker}')
    return result


def update_last_checked(rows: list[dict], ticker: str, checked_at: str,
                        filing_date_seen: str = '') -> list[dict]:
    updates = {'last_checked_at': checked_at}
    if filing_date_seen:
        updates['last_filing_date_seen'] = filing_date_seen
    return update_issuer(rows, ticker, **updates)


def mark_activating(rows: list[dict], ticker: str) -> list[dict]:
    return update_issuer(rows, ticker, status='activating')


def mark_active(rows: list[dict], ticker: str, config_path: str) -> list[dict]:
    return update_issuer(rows, ticker, status='active', config_path=config_path)


def mark_active_needs_review(rows: list[dict], ticker: str, config_path: str) -> list[dict]:
    return update_issuer(rows, ticker, status='active_needs_review', config_path=config_path)


def mark_failed(rows: list[dict], ticker: str) -> list[dict]:
    row = find_issuer(rows, ticker)
    fail_count = int(row.get('activation_fail_count', 0)) + 1 if row else 1
    return update_issuer(rows, ticker, status='failed_activation',
                         activation_fail_count=str(fail_count))


# ---- Logging ----

def append_activation_event(ticker: str, cik: str, old_status: str,
                            new_status: str, filing_date: str = '',
                            form_type: str = '', reason: str = '') -> None:
    _append_csv(ACTIVATION_LOG_CSV, {
        'timestamp': _now_iso(),
        'ticker': ticker,
        'cik': cik,
        'old_status': old_status,
        'new_status': new_status,
        'filing_date': filing_date,
        'form_type': form_type,
        'reason': reason,
    }, ACTIVATION_LOG_COLUMNS)


def append_review_item(ticker: str, cik: str, reason: str,
                       severity: str = 'warning', filing_date: str = '',
                       form_type: str = '', config_path: str = '') -> None:
    _append_csv(REVIEW_QUEUE_CSV, {
        'timestamp': _now_iso(),
        'ticker': ticker,
        'cik': cik,
        'reason': reason,
        'severity': severity,
        'filing_date': filing_date,
        'form_type': form_type,
        'config_path': config_path,
        'status': 'open',
    }, REVIEW_QUEUE_COLUMNS)


# ---- Seeding ----

def seed_universe(cik_csv_path: Path, profiles_dir: Path) -> list[dict]:
    """Build universe.csv from CompanyCIKs.csv + existing profiles."""
    # Read CIK list (try utf-8, fall back to cp1252 for Windows-encoded files)
    for encoding in ('utf-8', 'cp1252', 'latin-1'):
        try:
            with open(cik_csv_path, 'r', encoding=encoding, newline='') as f:
                reader = csv.DictReader(f)
                cik_rows = list(reader)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f'Could not decode {cik_csv_path} with any supported encoding')

    # Read existing profiles to find active issuers
    active_tickers = {}  # ticker -> {cik, archetype, config_path, issuer_name, sector}
    archetypes_dir = profiles_dir / '_archetypes'
    for yaml_path in sorted(profiles_dir.glob('*.yaml')):
        if yaml_path.parent == archetypes_dir:
            continue
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
            ticker = cfg.get('ticker', yaml_path.stem.upper())
            active_tickers[ticker.upper()] = {
                'cik': str(cfg.get('cik', '')).lstrip('0'),
                'archetype': cfg.get('archetype', ''),
                'config_path': f'profiles/{yaml_path.name}',
                'issuer_name': cfg.get('issuer', ''),
                'sector': cfg.get('sector', ''),
            }
        except Exception as e:
            logger.warning(f'Failed to read profile {yaml_path}: {e}')

    # Build universe rows
    universe = []
    seen_tickers = set()

    for cik_row in cik_rows:
        ticker = cik_row.get('ticker', '').strip()
        if not ticker or ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)

        cik = str(cik_row.get('CIK', '')).strip()
        issuer_name = cik_row.get('company_name', '').strip()
        sector = cik_row.get('gics_sector', '').strip()

        active_info = active_tickers.get(ticker.upper())
        if active_info:
            universe.append({
                'ticker': ticker,
                'issuer_name': active_info.get('issuer_name') or issuer_name,
                'cik': cik,
                'sector': active_info.get('sector') or sector,
                'status': 'active',
                'archetype_guess': active_info.get('archetype', ''),
                'config_path': active_info.get('config_path', ''),
                'last_checked_at': '',
                'last_filing_date_seen': '',
                'last_processed_period': '',
                'activation_fail_count': '0',
                'notes': '',
            })
        else:
            universe.append({
                'ticker': ticker,
                'issuer_name': issuer_name,
                'cik': cik,
                'sector': sector,
                'status': 'registered',
                'archetype_guess': '',
                'config_path': '',
                'last_checked_at': '',
                'last_filing_date_seen': '',
                'last_processed_period': '',
                'activation_fail_count': '0',
                'notes': '',
            })

    # Check for active issuers not in the CIK CSV (shouldn't happen, but handle it)
    for ticker, info in active_tickers.items():
        if ticker not in seen_tickers:
            universe.append({
                'ticker': ticker,
                'issuer_name': info.get('issuer_name', ''),
                'cik': info.get('cik', ''),
                'sector': info.get('sector', ''),
                'status': 'active',
                'archetype_guess': info.get('archetype', ''),
                'config_path': info.get('config_path', ''),
                'last_checked_at': '',
                'last_filing_date_seen': '',
                'last_processed_period': '',
                'activation_fail_count': '0',
                'notes': '',
            })

    save_universe(universe)

    # Create empty log files with headers
    if not ACTIVATION_LOG_CSV.exists():
        _save_csv(ACTIVATION_LOG_CSV, [], ACTIVATION_LOG_COLUMNS)
    if not REVIEW_QUEUE_CSV.exists():
        _save_csv(REVIEW_QUEUE_CSV, [], REVIEW_QUEUE_COLUMNS)

    active_count = sum(1 for r in universe if r['status'] == 'active')
    registered_count = sum(1 for r in universe if r['status'] == 'registered')
    logger.info(f'Seeded universe: {len(universe)} total ({active_count} active, {registered_count} registered)')

    return universe


def main():
    parser = argparse.ArgumentParser(description='Registry management')
    sub = parser.add_subparsers(dest='command')

    seed_p = sub.add_parser('seed', help='Seed universe from CIK CSV')
    seed_p.add_argument('--cik-csv', required=True, type=Path, help='Path to CompanyCIKs.csv')
    seed_p.add_argument('--profiles-dir', type=Path,
                        default=Path(__file__).resolve().parent.parent / 'profiles',
                        help='Path to profiles directory')
    seed_p.add_argument('--seed-profiles', action='store_true',
                        help='Also seed filer profiles for active issuers')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    if args.command == 'seed':
        universe = seed_universe(args.cik_csv, args.profiles_dir)
        print(f'Seeded {len(universe)} issuers into {UNIVERSE_CSV}')

        if args.seed_profiles:
            from .filer_profile import seed_existing_profiles
            output_dir = Path(__file__).resolve().parent.parent / 'output'
            count = seed_existing_profiles(args.profiles_dir, output_dir)
            print(f'Seeded {count} filer profiles')
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
