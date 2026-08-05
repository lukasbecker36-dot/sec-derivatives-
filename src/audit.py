"""Dataset-wide integrity audit over committed output/.

The per-filing checks in validate.py run at extraction time, on one row, with
whatever config was loaded. Nothing re-examined the committed corpus as a
whole — so a filing that produced an all-null row, or notionals that
contradict each other, sat in output/{ticker}/tracking.csv indefinitely and
was read back by the weekly digest as if it were sound data.

This module scans the committed CSVs and reports three defects:

  empty_row      every extracted field blank. Distinct from a genuine
                 non-discloser, which still records has_derivatives=No and
                 market-risk sensitivities. An all-blank row means the
                 extraction failed, and there is nothing to write about.

  reconciliation notional total != sum of its components (validate.py's
                 RECONCILIATION_RULES). The values are mutually
                 contradictory, so at least one is wrong.

  stale_null     the row is blank here but populated for the same
                 period on another git ref. Catches branch divergence,
                 where good data exists but never reached live output.

  misaligned_row the row carries more values than the header has columns,
                 so values have shifted out of their intended fields. Every
                 number in such a row is suspect regardless of how sound it
                 looks. 287 of these sat undetected across 178 tickers.

Run before generating any digest:

    python -m src.audit                      # human summary, exit 1 on defects
    python -m src.audit --json report.json   # machine-readable
    python -m src.audit --compare-ref backfill-local
    python -m src.audit --ticker MSFT --ticker CVX
"""

import argparse
import csv
import io
import json
import logging
import subprocess
import sys
from pathlib import Path

from .validate import check_reconciliations

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'output'

# Bookkeeping columns — present or absent depending on which pipeline wrote
# the row, and never evidence that extraction actually found anything.
META_COLUMNS = {
    'period_end_date', 'form_type', 'accession_number', 'filing_date',
    'processed_at', 'extraction_version',
}


def _flatten(value) -> str:
    """csv.DictReader yields a list when a header name repeats."""
    if isinstance(value, list):
        return ' '.join(str(v) for v in value if v)
    return str(value or '')


def _row_key(row: dict) -> tuple[str, str]:
    return (_flatten(row.get('period_end_date')).strip(),
            _flatten(row.get('form_type')).strip())


def is_misaligned_row(row: dict) -> bool:
    """True when the row has more values than the header has columns.

    csv.DictReader collects the surplus under a None key, so its presence
    means the columns and values no longer line up and the named fields may
    hold values belonging to their neighbours.
    """
    surplus = row.get(None)
    if not surplus:
        return False
    return any(str(v or '').strip() for v in surplus)


def is_empty_row(row: dict) -> bool:
    """True when no extracted (non-bookkeeping) field holds a value."""
    return not any(
        _flatten(v).strip()
        for k, v in row.items()
        if k not in META_COLUMNS
    )


def _normalise(row: dict) -> dict:
    """Flatten duplicate-header lists so validate.py sees plain scalars."""
    return {k: _flatten(v).strip() for k, v in row.items()}


def read_tracking(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def _git_show(ref: str, path: str) -> str | None:
    result = subprocess.run(
        ['git', 'show', f'{ref}:{path}'],
        capture_output=True, text=True,
    )
    return result.stdout if result.returncode == 0 else None


def audit_issuer(ticker: str, csv_path: Path,
                 compare_ref: str | None = None) -> list[dict]:
    """Audit one issuer's tracking.csv. Returns a list of defect dicts."""
    defects: list[dict] = []
    try:
        rows = read_tracking(csv_path.read_text(encoding='utf-8'))
    except (OSError, csv.Error) as e:
        return [{'ticker': ticker, 'type': 'unreadable', 'period': '',
                 'form_type': '', 'detail': f'cannot read {csv_path}: {e}'}]

    reference: dict[tuple[str, str], dict] = {}
    if compare_ref:
        rel = csv_path.relative_to(csv_path.parents[2])
        text = _git_show(compare_ref, str(rel))
        if text:
            reference = {_row_key(r): r for r in read_tracking(text)}

    for row in rows:
        period, form_type = _row_key(row)
        if is_misaligned_row(row):
            surplus = [str(v).strip() for v in (row.get(None) or []) if str(v or '').strip()]
            defects.append({
                'ticker': ticker, 'type': 'misaligned_row', 'period': period,
                'form_type': form_type,
                'detail': f'row has {len(surplus)} value(s) past the last header '
                          f'column ({", ".join(surplus[:3])}) — columns and '
                          f'values are out of step, all fields suspect',
            })
            continue
        if is_empty_row(row):
            defect = {'ticker': ticker, 'type': 'empty_row', 'period': period,
                      'form_type': form_type,
                      'detail': 'all extracted fields blank — extraction failed'}
            ref_row = reference.get((period, form_type))
            if ref_row is not None and not is_empty_row(ref_row):
                defect['type'] = 'stale_null'
                defect['detail'] = (f'blank here but populated on {compare_ref} '
                                    f'— data exists, never merged')
            defects.append(defect)
            continue

        for result in check_reconciliations(_normalise(row)):
            defects.append({
                'ticker': ticker, 'type': 'reconciliation', 'period': period,
                'form_type': form_type, 'detail': result['message'],
            })

    return defects


def run_audit(output_dir: Path = OUTPUT_DIR,
              tickers: list[str] | None = None,
              compare_ref: str | None = None) -> dict:
    """Audit every issuer under output_dir. Returns a structured report."""
    wanted = {t.lower() for t in tickers} if tickers else None
    defects: list[dict] = []
    issuers = 0

    for csv_path in sorted(output_dir.glob('*/tracking.csv')):
        ticker = csv_path.parent.name
        if wanted and ticker not in wanted:
            continue
        issuers += 1
        defects.extend(audit_issuer(ticker, csv_path, compare_ref=compare_ref))

    by_type: dict[str, int] = {}
    for d in defects:
        by_type[d['type']] = by_type.get(d['type'], 0) + 1

    return {
        'issuers_audited': issuers,
        'defect_count': len(defects),
        'defects_by_type': by_type,
        'tickers_affected': sorted({d['ticker'] for d in defects}),
        'defects': defects,
    }


def format_report(report: dict, limit: int = 40) -> str:
    lines = [
        f"Audited {report['issuers_audited']} issuers — "
        f"{report['defect_count']} defects across "
        f"{len(report['tickers_affected'])} tickers",
    ]
    if not report['defect_count']:
        lines.append('No integrity defects found.')
        return '\n'.join(lines)

    for defect_type, count in sorted(report['defects_by_type'].items(),
                                     key=lambda kv: -kv[1]):
        lines.append(f'  {defect_type}: {count}')

    lines.append('')
    for d in report['defects'][:limit]:
        lines.append(f"  [{d['type']}] {d['ticker'].upper():8} "
                     f"{d['period']} {d['form_type']}: {d['detail']}")
    if report['defect_count'] > limit:
        lines.append(f'  ... +{report["defect_count"] - limit} more')
    return '\n'.join(lines)


def check_against_baseline(report: dict, baseline: dict,
                           zero_tolerance: list[str]) -> list[str]:
    """Compare a report to a recorded baseline. Returns failure messages.

    The corpus carries known defects that need re-extraction to clear, so a
    gate demanding zero defects would block every digest indefinitely. This
    fails on regression instead: the total must not grow, no single defect
    type may grow, and types listed in zero_tolerance must stay at zero.
    """
    failures = []
    base_total = baseline.get('defect_count', 0)
    if report['defect_count'] > base_total:
        failures.append(
            f"total defects rose {base_total} -> {report['defect_count']}")

    base_by_type = baseline.get('defects_by_type', {})
    for defect_type, count in sorted(report['defects_by_type'].items()):
        was = base_by_type.get(defect_type, 0)
        if count > was:
            failures.append(f'{defect_type} rose {was} -> {count}')

    for defect_type in zero_tolerance:
        count = report['defects_by_type'].get(defect_type, 0)
        if count:
            failures.append(
                f'{defect_type} must stay at zero, found {count}')

    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Audit committed output/ for integrity defects.')
    parser.add_argument('--output', type=Path, default=OUTPUT_DIR,
                        help='Output directory to audit')
    parser.add_argument('--ticker', action='append', dest='tickers',
                        help='Restrict to these tickers (repeatable)')
    parser.add_argument('--compare-ref',
                        help='Git ref to check blank rows against, so '
                             'divergence is reported as stale_null')
    parser.add_argument('--json', type=Path,
                        help='Write the full report as JSON to this path')
    parser.add_argument('--limit', type=int, default=40,
                        help='Max defects to print')
    parser.add_argument('--exit-zero', action='store_true',
                        help='Always exit 0, even when defects are found')
    parser.add_argument('--baseline', type=Path,
                        help='Baseline JSON to compare against. Fails only on '
                             'regression rather than on any defect at all.')
    parser.add_argument('--zero-tolerance', default='misaligned_row,stale_null',
                        help='Comma-separated defect types that must stay at '
                             'zero when --baseline is used')
    parser.add_argument('--write-baseline', type=Path,
                        help='Write the current report as a baseline and exit 0')
    args = parser.parse_args(argv)

    report = run_audit(args.output, tickers=args.tickers,
                       compare_ref=args.compare_ref)
    print(format_report(report, limit=args.limit))

    if args.json:
        args.json.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f'\nWrote {args.json}')

    if args.write_baseline:
        summary = {
            'defect_count': report['defect_count'],
            'defects_by_type': report['defects_by_type'],
            'issuers_audited': report['issuers_audited'],
        }
        args.write_baseline.write_text(json.dumps(summary, indent=2) + '\n',
                                      encoding='utf-8')
        print(f'\nWrote baseline {args.write_baseline}')
        return 0

    if args.baseline:
        if not args.baseline.exists():
            print(f'\nBaseline {args.baseline} not found — cannot gate.')
            return 0 if args.exit_zero else 1
        baseline = json.loads(args.baseline.read_text(encoding='utf-8'))
        zero_tolerance = [t.strip() for t in args.zero_tolerance.split(',')
                          if t.strip()]
        failures = check_against_baseline(report, baseline, zero_tolerance)
        if failures:
            print('\nDATA INTEGRITY REGRESSION:')
            for f in failures:
                print(f'  - {f}')
            return 0 if args.exit_zero else 1
        print(f"\nNo regression against baseline "
              f"({baseline.get('defect_count', 0)} known defects).")
        return 0

    if report['defect_count'] and not args.exit_zero:
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
