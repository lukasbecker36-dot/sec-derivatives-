"""Incremental processing loop + CLI entry point."""

import argparse
import logging
import sys
import time
from pathlib import Path

from .config import load_config, list_issuers
from .engine import run_issuer, OUTPUT_DIR

logger = logging.getLogger(__name__)


def run_all(output_dir: Path = OUTPUT_DIR, since: str = '') -> list[dict]:
    """Run extraction for all configured issuers."""
    issuer_paths = list_issuers()
    results = []

    for yaml_path in issuer_paths:
        try:
            config = load_config(yaml_path)
            logger.info(f'Processing {config.issuer} ({config.ticker})...')
            result = run_issuer(config, output_dir, since=since)
            results.append(result)

            if result['processed'] > 0:
                logger.info(f"  Processed {result['processed']} new filings")
            else:
                logger.info('  No new filings')

            if result['errors']:
                for err in result['errors']:
                    logger.error(f"  Error: {err['period_end']}: {err['error']}")

        except Exception as e:
            logger.error(f'Failed to process {yaml_path.stem}: {e}')
            results.append({
                'issuer': yaml_path.stem,
                'ticker': yaml_path.stem.upper(),
                'processed': 0,
                'errors': [{'period_end': 'N/A', 'error': str(e)}],
                'total_available': 0,
            })

    return results


def run_from_configs(configs, output_dir: Path = OUTPUT_DIR, since: str = '') -> list[dict]:
    """Run extraction for a pre-supplied list of IssuerConfig objects.

    Same as run_all() but accepts configs directly rather than
    discovering from profiles/*.yaml. Used by scheduler.py.
    """
    results = []
    for config in configs:
        try:
            logger.info(f'Processing {config.issuer} ({config.ticker})...')
            result = run_issuer(config, output_dir, since=since)
            results.append(result)

            if result['processed'] > 0:
                logger.info(f"  Processed {result['processed']} new filings")
            else:
                logger.info('  No new filings')

            if result['errors']:
                for err in result['errors']:
                    logger.error(f"  Error: {err['period_end']}: {err['error']}")

        except Exception as e:
            logger.error(f'Failed to process {config.ticker}: {e}')
            results.append({
                'issuer': config.issuer,
                'ticker': config.ticker,
                'processed': 0,
                'errors': [{'period_end': 'N/A', 'error': str(e)}],
                'total_available': 0,
            })

    return results


def run_single(ticker: str, output_dir: Path = OUTPUT_DIR, since: str = '') -> dict:
    """Run extraction for a single issuer by ticker or filename."""
    issuer_paths = list_issuers()
    for yaml_path in issuer_paths:
        config = load_config(yaml_path)
        if (yaml_path.stem.lower() == ticker.lower()
                or config.ticker.lower() == ticker.lower()):
            return run_issuer(config, output_dir, since=since)

    raise ValueError(f'No config found for ticker: {ticker}')


def print_summary(results: list[dict]):
    """Print a summary of the run."""
    total_processed = sum(r['processed'] for r in results)
    total_errors = sum(len(r['errors']) for r in results)
    issuers_with_new = [r for r in results if r['processed'] > 0]

    print(f'\n{"=" * 60}')
    print(f'Run complete: {total_processed} filings processed across {len(results)} issuers')

    if issuers_with_new:
        print(f'\nIssuers with new filings:')
        for r in issuers_with_new:
            print(f"  {r['ticker']}: {r['processed']} new")

    if total_errors > 0:
        print(f'\nErrors: {total_errors}')
        for r in results:
            for err in r['errors']:
                print(f"  {r['ticker']} {err['period_end']}: {err['error']}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description='SEC Derivatives & Market Risk Extractor',
    )
    parser.add_argument('--issuer', '-i', help='Single issuer ticker to process')
    parser.add_argument('--since', '-s', default='', help='Only process filings on or after this date (YYYY-MM-DD)')
    parser.add_argument('--watch', '-w', action='store_true', help='Poll continuously')
    parser.add_argument('--interval', type=int, default=3600, help='Poll interval in seconds (default: 3600)')
    parser.add_argument('--output', '-o', type=Path, default=OUTPUT_DIR, help='Output directory')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.watch:
        logger.info(f'Watching for new filings every {args.interval}s...')
        while True:
            try:
                if args.issuer:
                    result = run_single(args.issuer, args.output, since=args.since)
                    print_summary([result])
                else:
                    results = run_all(args.output, since=args.since)
                    print_summary(results)
            except KeyboardInterrupt:
                logger.info('Stopped.')
                break
            except Exception as e:
                logger.error(f'Run failed: {e}')

            logger.info(f'Sleeping {args.interval}s...')
            try:
                time.sleep(args.interval)
            except KeyboardInterrupt:
                logger.info('Stopped.')
                break
    else:
        if args.issuer:
            result = run_single(args.issuer, args.output, since=args.since)
            print_summary([result])
        else:
            results = run_all(args.output, since=args.since)
            print_summary(results)


if __name__ == '__main__':
    main()
