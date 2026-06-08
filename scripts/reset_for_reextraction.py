"""Remove target filing periods from tracking CSVs so the scheduler re-extracts them.

Usage:
    python scripts/reset_for_reextraction.py [--dry-run] [--tickers AAPL MSFT ...]

By default targets all filers with missing/bad FX notional data.
Removes their most recent filing period from tracking.csv so the
scheduler treats them as unprocessed on the next run.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / 'output'


def has_bad_fx(row: dict) -> bool:
    year_values = {'2023.0', '2024.0', '2025.0', '2026.0', '2023', '2024', '2025', '2026'}
    has_d = row.get('has_derivatives', '')
    fx = row.get('fx_derivatives_notional', '') or row.get('fx_designated_notional', '')
    return (
        has_d in ('', 'No mention') or
        fx in year_values or
        not fx
    )


def main():
    parser = argparse.ArgumentParser(description='Reset filing periods for re-extraction')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be removed, do not modify files')
    parser.add_argument('--tickers', nargs='+', help='Only reset these tickers (default: all bad FX filers)')
    parser.add_argument('--periods', nargs='+', default=['2026-03-31', '2025-12-31'],
                        help='Period end dates to remove (default: 2026-03-31 2025-12-31)')
    args = parser.parse_args()

    target_periods = set(args.periods)
    target_tickers = {t.lower() for t in args.tickers} if args.tickers else None

    reset_count = 0
    row_count = 0

    for ticker_dir in sorted(os.listdir(OUTPUT_DIR)):
        if target_tickers and ticker_dir not in target_tickers:
            continue

        csv_path = OUTPUT_DIR / ticker_dir / 'tracking.csv'
        if not csv_path.exists():
            continue

        with open(csv_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = [f for f in (reader.fieldnames or []) if f is not None]
            rows = [{k: v for k, v in row.items() if k is not None} for row in reader]

        if not rows:
            continue

        # Check if latest row has bad FX data
        latest = rows[-1]
        if not has_bad_fx(latest):
            continue

        # Find rows matching target periods
        rows_to_keep = [r for r in rows if r.get('period_end_date', '') not in target_periods]
        rows_removed = [r for r in rows if r.get('period_end_date', '') in target_periods]

        if not rows_removed:
            continue

        ticker = ticker_dir.upper()
        for r in rows_removed:
            print(f'  {ticker}: removing {r["period_end_date"]} ({r.get("form_type", "")})')
            row_count += 1

        if not args.dry_run:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows_to_keep)

        reset_count += 1

    print(f'\n{"[DRY RUN] " if args.dry_run else ""}Reset {row_count} rows across {reset_count} filers.')
    if args.dry_run:
        print('Run without --dry-run to apply.')
    else:
        print(f'Run: python -m src.scheduler --provider openai --since 2025-01-01 --max-activations 0 --verbose')


if __name__ == '__main__':
    main()
