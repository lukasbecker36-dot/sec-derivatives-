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
    args = parser.parse_args(argv)

    report = run_audit(args.output, tickers=args.tickers,
                       compare_ref=args.compare_ref)
    print(format_report(report, limit=args.limit))

    if args.json:
        args.json.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f'\nWrote {args.json}')

    if report['defect_count'] and not args.exit_zero:
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
