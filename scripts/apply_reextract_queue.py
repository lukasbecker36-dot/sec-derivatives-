"""Apply the re-extraction queue: remove flagged rows from tracking.csv so
the daily scheduler picks them up on its next run.

The daily engine dedups on period_end_date (filing_fetcher.get_unprocessed_
filings). While a bad row sits in tracking.csv, the filing is considered
"processed" and never retried, so the AT&T $36B fair-value figure and the
51 blank Q2 rows would stay indefinitely.

Removing the row is destructive; the diff is committed to git, and every
removal is logged to backfill/reextract_removed_<timestamp>.csv so the
original values can be recovered without archaeology.

Usage:

    python scripts/apply_reextract_queue.py                # dry run
    python scripts/apply_reextract_queue.py --apply        # actually change files

Run generate_reextract_queue.py first to populate the queue.
"""

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
QUEUE_PATH = REPO / 'backfill' / 'reextract_queue.csv'
STATE_PATH = REPO / 'backfill' / 'state.csv'
OUTPUT_DIR = REPO / 'output'


def _flatten(v):
    if isinstance(v, list):
        return ' '.join(str(x) for x in v if x)
    return str(v or '')


def load_queue(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding='utf-8') as f:
        return list(csv.DictReader(f))


def apply_queue(queue: list[dict], apply_changes: bool) -> tuple[int, int, list]:
    """Returns (rows_removed_from_tracking, state_entries_removed, removed_backup_rows)."""
    # Group queue entries by ticker so we open each tracking.csv once
    by_ticker: dict[str, set[tuple[str, str]]] = {}
    for q in queue:
        by_ticker.setdefault(q['ticker'].upper(), set()).add(
            (q['period_end'], q['form_type']))

    removed_rows = []
    tracking_removed = 0
    for ticker, keys in sorted(by_ticker.items()):
        csv_path = OUTPUT_DIR / ticker.lower() / 'tracking.csv'
        if not csv_path.exists():
            continue
        with open(csv_path, newline='', encoding='utf-8') as f:
            rd = csv.DictReader(f)
            columns = rd.fieldnames or []
            rows = list(rd)
        keep = []
        for r in rows:
            key = (_flatten(r.get('period_end_date')).strip(),
                   _flatten(r.get('form_type')).strip())
            if key in keys:
                tracking_removed += 1
                removed_rows.append({'ticker': ticker, **{c: _flatten(r.get(c))
                                                          for c in columns}})
            else:
                keep.append(r)
        if len(keep) == len(rows):
            continue
        if apply_changes:
            tmp = csv_path.with_suffix('.csv.tmp')
            with open(tmp, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
                w.writeheader()
                for r in keep:
                    w.writerow({c: _flatten(r.get(c)) for c in columns})
            tmp.replace(csv_path)

    # Also drop matching entries from state.csv so backfill's ticker-level gate
    # doesn't keep the ticker in "committed" state.
    state_removed = 0
    if STATE_PATH.exists():
        with open(STATE_PATH, newline='', encoding='utf-8') as f:
            rd = csv.DictReader(f)
            columns = rd.fieldnames or []
            rows = list(rd)
        keep = []
        for r in rows:
            key = (r.get('ticker', '').upper(),
                   r.get('period_end', '').strip(),
                   r.get('form_type', '').strip())
            wanted = by_ticker.get(key[0], set())
            if (key[1], key[2]) in wanted:
                state_removed += 1
            else:
                keep.append(r)
        if state_removed and apply_changes:
            tmp = STATE_PATH.with_suffix('.csv.tmp')
            with open(tmp, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=columns)
                w.writeheader()
                w.writerows(keep)
            tmp.replace(STATE_PATH)

    return tracking_removed, state_removed, removed_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true',
                    help='Actually modify files (default is dry-run).')
    ap.add_argument('--queue', type=Path, default=QUEUE_PATH)
    args = ap.parse_args()

    queue = load_queue(args.queue)
    if not queue:
        print(f'Queue is empty ({args.queue}) — nothing to do.')
        return

    print(f'{"APPLYING" if args.apply else "DRY RUN"}: {len(queue)} queue entries')
    tracking, state, removed = apply_queue(queue, args.apply)

    print(f'  rows to remove from tracking.csv: {tracking}')
    print(f'  entries to remove from state.csv: {state}')

    if args.apply and removed:
        ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        backup = REPO / 'backfill' / f'reextract_removed_{ts}.csv'
        cols = ['ticker'] + sorted(
            {k for r in removed for k in r if k != 'ticker'})
        with open(backup, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
            w.writeheader()
            w.writerows(removed)
        print(f'\n  Original values backed up to {backup.relative_to(REPO)}')
    elif not args.apply:
        print('\n  (dry-run — re-run with --apply to actually remove rows)')


if __name__ == '__main__':
    main()
