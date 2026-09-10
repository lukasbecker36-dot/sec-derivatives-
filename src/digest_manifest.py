"""Build a machine-readable manifest of what changed on master in the last
24 hours, so the daily digest routine can write from a small structured
input instead of re-scanning the whole corpus every day.

The routine calls:

    python -m src.digest_manifest --since 2026-09-02T05:00:00Z --out manifest.json

and reads `manifest.json` to decide what to write about. The manifest is
grouped by asset class so the routine's prompt can map straight to
sections without doing the classification itself.

Structure:

    {
      "since": "...",
      "as_of": "...",
      "new_filings": [
        {
          "ticker": "MSFT",
          "period_end_date": "2026-06-30",
          "form_type": "10-K",
          "prior_period_end_date": "2026-03-31",
          "audit_flags": [],
          "moves": {
            "fx": [{"field": "fx_derivatives_notional", "current": 64957, "prior": 60013, "pct": 8.24}],
            "ir": [...],
            "commodity": [...],
            "equity": [...],
            "credit": [...]
          },
          "notes_categories": {"FX exposure": [...], "Interest rate risk": [...]},
          "accession_number": "0000..."
        },
        ...
      ],
      "extraction_gaps": [
        {"ticker": "IBM", "period_end_date": "2026-06-30", "form_type": "10-Q", "attempts": 2}
      ],
      "counts": {"total_new_rows": 14, "gated_defects": 226, ...}
    }
"""

import argparse
import csv
import io
import json
import re
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .audit import run_audit  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / 'output'
META = {
    'period_end_date', 'form_type', 'accession_number', 'filing_date',
    'processed_at', 'extraction_version', 'extraction_attempts',
}
NUM_RE = re.compile(r'^-?\d+(?:\.\d+)?$')

# A "fresh" filing is one whose filing_date (as recorded by SEC EDGAR)
# is within this many days of the digest run. Older extractions are
# backfills — the reader has already had (or missed) the news on those
# and does not want them re-surfaced in the daily email. Kept in
# `backfill_filings` for auditability and so the routine can show a
# single-line "N historical periods were backfilled overnight" note if
# it wants to, but never in `new_filings`.
DEFAULT_RECENT_DAYS = 7

# Asset-class classification of extraction fields. Order matters: credit
# patterns come BEFORE ir so credit_default_swap goes to credit, not ir
# (both match "swap"). Anything not matched lands in 'other' so the routine
# can still surface it.
ASSET_PATTERNS = {
    'fx': re.compile(
        r'(?:^|_)(?:fx|foreign|currency|cross_currency)(?:_|$)|forward_notional|'
        r'principal_currency|net_investment_hedge', re.I),
    'credit': re.compile(r'credit_default|cds|credit_spread|cva|dva|xva', re.I),
    'commodity': re.compile(
        r'commodit|fuel|natural_gas|crude|oil|electricity|energy|power|'
        r'coal|corn|wheat|cocoa|coffee|copper|aluminum|nickel|gold|silver',
        re.I),
    'ir': re.compile(
        r'(?:^|_)(?:ir|interest_rate|rate_lock|swap|swaption|treasury_lock|'
        r'floating_rate|fixed_rate)(?:_|$)', re.I),
    'equity': re.compile(r'equity|share_repurchase|stock_warrant|convertible', re.I),
}

NOTES_CATEGORY_HDR = re.compile(r'^\s*\[(?P<cat>[^\]]+)\]\s*$')


def flat(v):
    if isinstance(v, list):
        return ' '.join(str(x) for x in v if x)
    return str(v or '')


def numeric(v):
    s = flat(v).strip().replace(',', '')
    if not s or not NUM_RE.match(s):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _classify(field: str) -> str:
    fl = field.lower()
    for cls, pat in ASSET_PATTERNS.items():
        if pat.search(fl):
            return cls
    return 'other'


def _read_csv_text(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def _tracking_at(ref: str, path: str) -> list[dict]:
    r = subprocess.run(['git', 'show', f'{ref}:{path}'],
                       capture_output=True, text=True, cwd=REPO)
    if r.returncode:
        return []
    return _read_csv_text(r.stdout)


def _first_commit_at_or_after(since_iso: str) -> str | None:
    """Return the first commit SHA on master whose author-date is >= since_iso.
    Used as the baseline ref to compare 'today's' tracking.csv against.
    """
    r = subprocess.run(
        ['git', 'log', 'origin/master', f'--until={since_iso}',
         '-1', '--format=%H'],
        capture_output=True, text=True, cwd=REPO,
    )
    return r.stdout.strip() or None


def _held_review_branches(since_iso: str) -> list[dict]:
    """Return remote review/YYYY-MM-DD-* branches created since since_iso.

    The scheduler pushes gate-blocked work to these branches instead of
    master. The daily digest surfaces them so the reader knows "the
    pipeline is not quiet — N filings are held for review" rather than
    seeing an ambiguous silent day.

    Each returned entry carries a `content_preview`: up to 10 tickers on
    the branch whose tracking.csv has populated rows master lacks, with a
    handful of key derivative values pulled out so the reader can decide
    whether the held content is worth chasing without checking out the
    branch. Without this, the daily email says only "N filings held" and
    the reader has no editorial signal to act on.
    """
    subprocess.run(['git', 'fetch', '--prune', 'origin'],
                   capture_output=True, text=True, cwd=REPO)
    r = subprocess.run(
        ['git', 'for-each-ref',
         '--format=%(refname:short)|%(committerdate:iso-strict)|%(subject)',
         'refs/remotes/origin/review/'],
        capture_output=True, text=True, cwd=REPO,
    )
    if r.returncode:
        return []
    since_dt = datetime.fromisoformat(since_iso.replace('Z', '+00:00'))
    out = []
    for line in r.stdout.splitlines():
        parts = line.split('|', 2)
        if len(parts) != 3:
            continue
        ref, dt_str, subject = parts
        try:
            dt = datetime.fromisoformat(dt_str)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt < since_dt:
            continue
        branch = ref.replace('origin/', '')
        out.append({
            'branch': branch,
            'committed_at': dt_str,
            'commit_subject': subject.strip(),
            'content_preview': _branch_content_preview(ref),
        })
    out.sort(key=lambda x: x['committed_at'])
    return out


# Notable fields to include in the review-branch preview, ordered by
# editorial priority. First N with populated values per row are surfaced.
_PREVIEW_FIELDS = (
    'fx_derivatives_notional', 'ir_swap_notional',
    'commodity_derivatives_notional', 'equity_derivatives_notional',
    'total_derivative_asset', 'total_derivative_liability',
    'net_derivative_position', 'fx_designated_notional',
    'fx_not_designated_notional', 'cash_flow_hedge_aoci',
)


def _branch_content_preview(ref: str, max_tickers: int = 10) -> list[dict]:
    """For a review branch, list the tickers whose tracking.csv has
    populated rows master doesn't, with a snapshot of the top derivative
    values so the reader can eyeball whether the held content is worth
    triaging.
    """
    r = subprocess.run(
        ['git', 'diff', '--name-only', 'origin/master', ref, '--', 'output/'],
        capture_output=True, text=True, cwd=REPO,
    )
    if r.returncode:
        return []
    preview = []
    for path in r.stdout.splitlines():
        if not path.endswith('/tracking.csv'):
            continue
        parts = path.split('/')
        if len(parts) < 3:
            continue
        ticker = parts[1].upper()
        branch_rows = _tracking_at(ref, path)
        master_rows = _tracking_at('origin/master', path)
        master_keys = {(r.get('period_end_date', ''), r.get('form_type', ''))
                       for r in master_rows}
        for row in branch_rows:
            key = (row.get('period_end_date', ''), row.get('form_type', ''))
            if not key[0] or key in master_keys:
                continue
            populated = {
                f: flat(row.get(f)).strip()
                for f in _PREVIEW_FIELDS
                if flat(row.get(f)).strip()
            }
            if not populated:
                continue
            preview.append({
                'ticker': ticker,
                'period_end_date': key[0],
                'form_type': key[1],
                'values': populated,
            })
            break  # one entry per ticker keeps the preview scannable
        if len(preview) >= max_tickers:
            break
    return preview




def _notes_categories_for_period(ticker: str, period: str,
                                  form_type: str) -> dict[str, list[str]]:
    """Extract categorised notes for a specific period-block from notes.txt.
    The block header is '--- {form_type} | Period ending {period} ---'.
    """
    notes_path = OUTPUT_DIR / ticker.lower() / 'notes.txt'
    if not notes_path.exists():
        return {}
    text = notes_path.read_text(encoding='utf-8', errors='replace')
    header = f'--- {form_type} | Period ending {period} ---'
    lines = text.split('\n')
    block = []
    inside = False
    for line in lines:
        if line.startswith('---'):
            if inside:
                break
            if line.strip() == header:
                inside = True
                continue
        elif inside:
            block.append(line)
    result: dict[str, list[str]] = {}
    current = None
    for line in block:
        m = NOTES_CATEGORY_HDR.match(line)
        if m:
            current = m.group('cat').strip()
            result.setdefault(current, [])
        elif current and line.strip().startswith('- '):
            result[current].append(line.strip()[2:].strip())
    return result


def _row_moves(prior: dict, curr: dict) -> dict[str, list[dict]]:
    """Grouped by asset class, the fields whose values differ from prior."""
    out: dict[str, list[dict]] = {'fx': [], 'ir': [], 'commodity': [],
                                   'equity': [], 'credit': [], 'other': []}
    for k, v in curr.items():
        if k in META or k is None:
            continue
        cv = numeric(v)
        pv = numeric(prior.get(k)) if prior else None
        if cv is None:
            continue
        entry = {'field': k, 'current': cv, 'prior': pv}
        if pv is not None and pv != 0:
            entry['pct'] = round((cv - pv) / abs(pv) * 100, 2)
        cls = _classify(k)
        out[cls].append(entry)
    return {k: v for k, v in out.items() if v}


def _filing_age_days(filing_date_str: str, as_of: datetime) -> int | None:
    """Days between filing_date (as recorded in tracking.csv) and as_of.
    Returns None when the field is blank or unparseable — the caller
    must decide how to treat unknown-age filings (currently: treat as
    NOT fresh, since a genuinely-new filing always carries a date)."""
    if not filing_date_str:
        return None
    s = filing_date_str.strip()
    if not s:
        return None
    try:
        # tracking.csv stores YYYY-MM-DD from EDGAR
        d = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (as_of - d).days


def build_manifest(since_iso: str,
                   recent_days: int = DEFAULT_RECENT_DAYS) -> dict:
    baseline_sha = _first_commit_at_or_after(since_iso)
    if not baseline_sha:
        # No commit before that time — treat as "everything is new".
        baseline_sha = None
    as_of_dt = datetime.now(timezone.utc)
    as_of = as_of_dt.isoformat()

    # Snapshot audit for the counts block
    report = run_audit(OUTPUT_DIR)
    defects_lookup: dict[tuple, list[str]] = {}
    for d in report['defects']:
        defects_lookup.setdefault(
            (d['ticker'].upper(), d['period']), []
        ).append(d['type'])

    new_filings: list[dict] = []
    backfill_filings: list[dict] = []
    extraction_gaps: list[dict] = []

    for csv_path in sorted(OUTPUT_DIR.glob('*/tracking.csv')):
        ticker = csv_path.parent.name.upper()
        rel = f'output/{ticker.lower()}/tracking.csv'
        current_rows = _read_csv_text(csv_path.read_text(encoding='utf-8'))
        baseline_rows = _tracking_at(baseline_sha, rel) if baseline_sha else []
        baseline_keys = {(r.get('period_end_date', ''), r.get('form_type', ''))
                         for r in baseline_rows}
        baseline_by_key = {(r.get('period_end_date', ''), r.get('form_type', '')): r
                           for r in baseline_rows}

        for i, row in enumerate(current_rows):
            key = (row.get('period_end_date', ''), row.get('form_type', ''))
            if not key[0]:
                continue
            is_new = key not in baseline_keys
            baseline_row = baseline_by_key.get(key)
            # A row that existed in baseline but was blank there and is
            # populated now also counts as new content for the digest.
            populated_now = any(flat(v).strip() for k, v in row.items() if k not in META)
            populated_before = baseline_row and any(
                flat(v).strip() for k, v in baseline_row.items() if k not in META)
            if not is_new and populated_before:
                continue
            if not populated_now:
                # Skip persistent blanks — a row that was blank in baseline
                # AND is still blank now is not a daily-digest event.
                # Historically the digest listed every blank row in the
                # corpus, so a reader saw 186 tickers of noise instead of
                # the handful of actual failures introduced today.
                #
                # Surface only the two categories that actually count as
                # "news the reader needs to know":
                #   - is_new  → a fresh row landed today and it's blank
                #   - populated_before && !populated_now → REGRESSION
                #     (a previously-populated row went blank on retry)
                if not (is_new or populated_before):
                    continue
                attempts_raw = row.get('extraction_attempts', '') or '0'
                try:
                    attempts = int(str(attempts_raw).strip())
                except (ValueError, TypeError):
                    attempts = 0
                extraction_gaps.append({
                    'ticker': ticker,
                    'period_end_date': key[0],
                    'form_type': key[1],
                    'attempts': attempts,
                    'is_regression': bool(populated_before),
                })
                continue
            # Prior row for delta computation is the one immediately before
            # the new row chronologically, populated or not.
            prior_row = None
            for j in range(i - 1, -1, -1):
                if any(flat(v).strip() for k, v in current_rows[j].items() if k not in META):
                    prior_row = current_rows[j]
                    break
            filing_date = flat(row.get('filing_date'))
            age_days = _filing_age_days(filing_date, as_of_dt)
            entry = {
                'ticker': ticker,
                'period_end_date': key[0],
                'form_type': key[1],
                'accession_number': flat(row.get('accession_number')),
                'filing_date': filing_date,
                'filing_date_age_days': age_days,
                'prior_period_end_date': (flat(prior_row.get('period_end_date'))
                                           if prior_row else None),
                'audit_flags': defects_lookup.get((ticker, key[0]), []),
                'moves': _row_moves(prior_row or {}, row),
                'notes_categories': _notes_categories_for_period(
                    ticker, key[0], key[1]),
            }
            # Filings whose EDGAR filing_date is older than the freshness
            # window (or missing) are treated as backfills, not news. The
            # reader has already had the chance to see those; putting
            # them in the daily email buries the actually-new content.
            if age_days is None or age_days > recent_days:
                backfill_filings.append(entry)
            else:
                new_filings.append(entry)

    held_branches = _held_review_branches(since_iso)

    return {
        'since': since_iso,
        'as_of': as_of,
        'baseline_sha': baseline_sha,
        'new_filings': new_filings,
        'backfill_filings': backfill_filings,
        'recent_days': recent_days,
        'extraction_gaps': extraction_gaps,
        'held_for_review': held_branches,
        'counts': {
            'total_new_rows': len(new_filings),
            'backfill_rows': len(backfill_filings),
            'extraction_gaps': len(extraction_gaps),
            'held_review_branches': len(held_branches),
            'total_defects': report['defect_count'],
            'defects_by_type': report['defects_by_type'],
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    parser.add_argument('--since', help='ISO timestamp; default = 24h ago.')
    parser.add_argument('--out', type=Path, default=Path('digest_manifest.json'))
    parser.add_argument('--recent-days', type=int, default=DEFAULT_RECENT_DAYS,
                        help='Only surface filings whose EDGAR filing_date '
                             'is within this many days as new_filings; older '
                             'ones become backfill_filings.')
    args = parser.parse_args(argv)

    if args.since:
        since_iso = args.since
    else:
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    manifest = build_manifest(since_iso, recent_days=args.recent_days)
    args.out.write_text(json.dumps(manifest, indent=2, default=str),
                        encoding='utf-8')
    print(f'Wrote {args.out}: {manifest["counts"]["total_new_rows"]} new rows, '
          f'{manifest["counts"]["backfill_rows"]} backfill rows, '
          f'{manifest["counts"]["extraction_gaps"]} extraction gaps, '
          f'baseline={manifest["baseline_sha"] or "none"}',
          file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
