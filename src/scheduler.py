"""Scheduled orchestrator — two-pass run for active + registered issuers."""

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import anthropic

from .activation import check_new_filing, activate_issuer, ActivationResult
from .config import load_config
from .engine import OUTPUT_DIR
from .monitor import run_from_configs
from .registry import (
    load_universe, save_universe, get_active, get_registered, get_failed,
    update_last_checked, mark_activating, mark_active,
    mark_active_needs_review, mark_failed,
    append_activation_event, append_review_item,
)

logger = logging.getLogger(__name__)


@dataclass
class RunSummary:
    started_at: str = ''
    finished_at: str = ''
    active_checked: int = 0
    active_new_filings: int = 0
    active_errors: int = 0
    registered_checked: int = 0
    registered_skipped: int = 0
    activations_attempted: int = 0
    activations_succeeded: int = 0
    activations_needs_review: int = 0
    activations_failed: int = 0
    review_items_added: int = 0
    issuer_results: list[dict] = field(default_factory=list)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _pass_active(universe: list[dict], output_dir: Path, since: str,
                 summary: RunSummary) -> list[dict]:
    """Pass 1: Process all active/active_needs_review issuers."""
    active_rows = get_active(universe)
    summary.active_checked = len(active_rows)

    if not active_rows:
        logger.info('Pass 1: No active issuers to process')
        return universe

    logger.info(f'Pass 1: Processing {len(active_rows)} active issuers...')

    # Load configs
    configs = []
    config_map = {}  # ticker -> config
    for row in active_rows:
        config_path = row.get('config_path', '')
        if not config_path:
            logger.warning(f"Active issuer {row['ticker']} has no config_path")
            continue
        try:
            config = load_config(Path(config_path))
            configs.append(config)
            config_map[config.ticker] = config
        except Exception as e:
            logger.error(f"Failed to load config for {row['ticker']}: {e}")
            summary.active_errors += 1

    # Run extraction
    results = run_from_configs(configs, output_dir, since=since)

    # Update universe with results
    now = _now_iso()
    for result in results:
        ticker = result.get('ticker', '')
        if result.get('processed', 0) > 0:
            summary.active_new_filings += result['processed']
        if result.get('errors'):
            summary.active_errors += len(result['errors'])

        try:
            universe = update_last_checked(universe, ticker, now)
        except KeyError:
            pass

        summary.issuer_results.append({
            'ticker': ticker,
            'phase': 'active',
            'processed': result.get('processed', 0),
            'errors': len(result.get('errors', [])),
        })

    return universe


def _pass_registered(universe: list[dict], cutoff_date: str,
                     max_activations: int, output_dir: Path,
                     dry_run: bool, summary: RunSummary,
                     check_interval_days: int = 3) -> list[dict]:
    """Pass 2: Check registered and failed_activation issuers for new filings, trigger activation."""
    registered_rows = get_registered(universe)
    failed_rows = get_failed(universe)
    candidate_rows = registered_rows + failed_rows

    if not candidate_rows:
        logger.info('Pass 2: No registered/failed issuers to check')
        return universe

    logger.info(f'Pass 2: Checking {len(candidate_rows)} issuers ({len(registered_rows)} registered, {len(failed_rows)} failed)...')

    client = None
    activations_done = 0
    now = _now_iso()
    skip_before = (datetime.now(timezone.utc) - timedelta(days=check_interval_days)).strftime('%Y-%m-%dT%H:%M:%SZ')

    for row in candidate_rows:
        ticker = row.get('ticker', '')
        cik = row.get('cik', '')
        is_failed = row.get('status') == 'failed_activation'

        # Skip if recently checked (failed issuers always retry)
        last_checked = row.get('last_checked_at', '')
        if not is_failed and last_checked and last_checked >= skip_before:
            summary.registered_skipped += 1
            continue

        summary.registered_checked += 1

        # Check for new filing — skip cutoff for failed retries so previously
        # detected filings aren't filtered out after aging past the window
        last_seen = row.get('last_filing_date_seen', '')
        effective_cutoff = '' if is_failed else cutoff_date
        new_filing = check_new_filing(cik, last_known_date=last_seen,
                                      cutoff_date=effective_cutoff)

        # Update last checked
        filing_date_seen = new_filing['period_end'] if new_filing else ''
        try:
            universe = update_last_checked(universe, ticker, now,
                                           filing_date_seen=filing_date_seen)
        except KeyError:
            pass

        if not new_filing:
            continue

        logger.info(f'  {ticker}: new {new_filing["form_type"]} ({new_filing["period_end"]})')

        if dry_run:
            summary.issuer_results.append({
                'ticker': ticker,
                'phase': 'registered',
                'new_filing': new_filing['period_end'],
                'form_type': new_filing['form_type'],
                'action': 'dry_run_skip',
            })
            continue

        if activations_done >= max_activations:
            logger.info(f'  {ticker}: skipped (max_activations={max_activations} reached)')
            summary.issuer_results.append({
                'ticker': ticker,
                'phase': 'registered',
                'new_filing': new_filing['period_end'],
                'action': 'max_activations_reached',
            })
            continue

        # Trigger activation
        try:
            old_status = row.get('status', 'registered')
            universe = mark_activating(universe, ticker)
            append_activation_event(ticker, cik, old_status, 'activating',
                                    filing_date=new_filing['period_end'],
                                    form_type=new_filing['form_type'],
                                    reason='New filing detected')

            if client is None:
                client = anthropic.Anthropic()

            result = activate_issuer(
                ticker=ticker,
                cik=cik,
                issuer_name=row.get('issuer_name', ''),
                sector=row.get('sector', ''),
                filing_meta=new_filing,
                client=client,
                output_dir=output_dir,
            )

            summary.activations_attempted += 1
            activations_done += 1

            # Update universe based on result
            if result.new_status == 'active':
                universe = mark_active(universe, ticker, result.config_path)
                summary.activations_succeeded += 1
            elif result.new_status == 'active_needs_review':
                universe = mark_active_needs_review(universe, ticker, result.config_path)
                summary.activations_needs_review += 1
                append_review_item(ticker, cik,
                                   reason='; '.join(result.reasons[:3]),
                                   severity='warning',
                                   filing_date=new_filing['period_end'],
                                   form_type=new_filing['form_type'],
                                   config_path=result.config_path)
                summary.review_items_added += 1
            else:  # failed_activation
                universe = mark_failed(universe, ticker)
                summary.activations_failed += 1
                append_review_item(ticker, cik,
                                   reason='; '.join(result.errors[:3] or result.reasons[:3]),
                                   severity='error',
                                   filing_date=new_filing['period_end'],
                                   form_type=new_filing['form_type'])
                summary.review_items_added += 1

            append_activation_event(ticker, cik, 'activating', result.new_status,
                                    filing_date=new_filing['period_end'],
                                    form_type=new_filing['form_type'],
                                    reason=f'score={result.score:.2f}')

            summary.issuer_results.append({
                'ticker': ticker,
                'phase': 'activation',
                'status': result.new_status,
                'score': result.score,
            })

        except Exception as e:
            logger.error(f'Activation error for {ticker}: {e}')
            try:
                universe = mark_failed(universe, ticker)
                append_activation_event(ticker, cik, 'activating', 'failed_activation',
                                        reason=str(e))
            except Exception:
                pass
            summary.activations_attempted += 1
            summary.activations_failed += 1
            summary.issuer_results.append({
                'ticker': ticker,
                'phase': 'activation',
                'status': 'failed_activation',
                'error': str(e),
            })

    return universe


def run_scheduled(
    output_dir: Path = OUTPUT_DIR,
    since: str = '',
    cutoff_date: str = '',
    max_activations: int = 10,
    dry_run: bool = False,
    check_interval_days: int = 3,
) -> RunSummary:
    """Main scheduler entry point.

    Args:
        since: Date cutoff for active issuers' filing processing.
        cutoff_date: Only trigger activation for filings after this date.
                     Defaults to 30 days ago if not set.
        max_activations: Max new activations per run.
        dry_run: Check but don't activate.
        check_interval_days: Skip registered issuers checked within this many days.
    """
    summary = RunSummary(started_at=_now_iso())

    if not cutoff_date:
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')

    # Load universe
    universe = load_universe()
    if not universe:
        logger.error('Universe is empty — run "python -m src.registry seed" first')
        summary.finished_at = _now_iso()
        return summary

    logger.info(f'Loaded universe: {len(universe)} issuers')

    # Pass 1: Active issuers
    universe = _pass_active(universe, output_dir, since, summary)

    # Pass 2: Registered issuers
    universe = _pass_registered(universe, cutoff_date, max_activations,
                                output_dir, dry_run, summary,
                                check_interval_days=check_interval_days)

    # Save updated universe
    save_universe(universe)

    summary.finished_at = _now_iso()
    return summary


def print_run_summary(summary: RunSummary) -> None:
    """Print formatted run summary."""
    print(f'\n{"=" * 60}')
    print(f'Scheduler run: {summary.started_at} — {summary.finished_at}')
    print(f'{"=" * 60}')
    print(f'\nPass 1 (Active):')
    print(f'  Checked: {summary.active_checked}')
    print(f'  New filings processed: {summary.active_new_filings}')
    print(f'  Errors: {summary.active_errors}')
    print(f'\nPass 2 (Registered):')
    print(f'  Checked: {summary.registered_checked}')
    print(f'  Skipped (recently checked): {summary.registered_skipped}')
    print(f'  Activations attempted: {summary.activations_attempted}')
    print(f'  -> Succeeded: {summary.activations_succeeded}')
    print(f'  -> Needs review: {summary.activations_needs_review}')
    print(f'  -> Failed: {summary.activations_failed}')
    print(f'  Review items added: {summary.review_items_added}')

    # Detail on activations
    activations = [r for r in summary.issuer_results if r.get('phase') == 'activation']
    if activations:
        print(f'\nActivation details:')
        for a in activations:
            status = a.get('status', '?')
            score = a.get('score', 0)
            print(f"  {a['ticker']}: {status} (score={score:.2f})")

    print()


def summary_to_dict(summary: RunSummary) -> dict:
    """Convert to JSON-serializable dict."""
    return {
        'started_at': summary.started_at,
        'finished_at': summary.finished_at,
        'active_checked': summary.active_checked,
        'active_new_filings': summary.active_new_filings,
        'active_errors': summary.active_errors,
        'registered_checked': summary.registered_checked,
        'registered_skipped': summary.registered_skipped,
        'activations_attempted': summary.activations_attempted,
        'activations_succeeded': summary.activations_succeeded,
        'activations_needs_review': summary.activations_needs_review,
        'activations_failed': summary.activations_failed,
        'review_items_added': summary.review_items_added,
        'issuer_results': summary.issuer_results,
    }


def main():
    parser = argparse.ArgumentParser(
        description='SEC Derivatives Scheduler — two-pass orchestrator',
    )
    parser.add_argument('--since', '-s', default='',
                        help='Date cutoff for active issuer processing (YYYY-MM-DD)')
    parser.add_argument('--cutoff', default='',
                        help='Activation cutoff date (default: 30 days ago)')
    parser.add_argument('--max-activations', type=int, default=10,
                        help='Max new activations per run (default: 10)')
    parser.add_argument('--check-interval', type=int, default=3,
                        help='Skip registered issuers checked within N days (default: 3)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check for new filings but do not activate')
    parser.add_argument('--output', '-o', type=Path, default=OUTPUT_DIR,
                        help='Output directory')
    parser.add_argument('--json-summary', type=Path, default=None,
                        help='Write JSON summary to file')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose logging')
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    summary = run_scheduled(
        output_dir=args.output,
        since=args.since,
        cutoff_date=args.cutoff,
        max_activations=args.max_activations,
        dry_run=args.dry_run,
        check_interval_days=args.check_interval,
    )

    print_run_summary(summary)

    if args.json_summary:
        with open(args.json_summary, 'w', encoding='utf-8') as f:
            json.dump(summary_to_dict(summary), f, indent=2)
        print(f'Summary written to {args.json_summary}')


if __name__ == '__main__':
    main()
