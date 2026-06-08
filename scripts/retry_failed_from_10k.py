"""Reset failed_activation issuers to registered so the scheduler retries them.

The scheduler's activation pipeline already prefers 10-K for bootstrap when
a 10-Q triggers the activation. But the initial check_new_filing only looks
at recent filings. By resetting activation_fail_count to 0 and status to
registered, the next scheduler run will re-check, find the 10-K, and use it.

Usage:
    python scripts/retry_failed_from_10k.py [--dry-run]
"""

import csv
import sys
from pathlib import Path

REGISTRY = Path(__file__).resolve().parent.parent / 'registry' / 'universe.csv'


def main():
    dry_run = '--dry-run' in sys.argv

    rows = []
    with open(REGISTRY, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    reset_count = 0
    for row in rows:
        if row.get('status') == 'failed_activation':
            ticker = row.get('ticker', '?')
            old_count = row.get('activation_fail_count', '0')
            if dry_run:
                print(f'  [dry-run] {ticker}: would reset to registered')
            else:
                row['status'] = 'registered'
                row['activation_fail_count'] = '0'
                print(f'  {ticker}: reset to registered')
            reset_count += 1

    if not dry_run and reset_count > 0:
        with open(REGISTRY, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print(f'\n{"Would reset" if dry_run else "Reset"} {reset_count} issuers')


if __name__ == '__main__':
    main()
