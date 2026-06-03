"""Claude Code bridge — prepare extraction requests and finalize with cached results.

Usage (called by the Claude Code scheduled routine):

  # Phase 1: discover work, write extraction requests
  python -m src.cc_bridge prepare --since 2025-01-01 --max-activations 50

  # Phase 2: (Claude Code reads cc_work/*.json, produces cc_results/*.json)

  # Phase 3: run pipeline with cached LLM results
  python -m src.cc_bridge finalize --since 2025-01-01 --max-activations 50 --json-summary summary.json
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
WORK_DIR = REPO_ROOT / 'cc_work'
RESULTS_DIR = REPO_ROOT / 'cc_results'


# ---------------------------------------------------------------------------
# Prepare phase — discover what needs extraction, write request files
# ---------------------------------------------------------------------------

def _prepare_active(universe, since, output_dir):
    """Prepare extraction requests for active issuers."""
    from .config import load_config
    from .engine import _build_schema_for_section, _get_prior_row, _prior_values_for_section, OUTPUT_DIR
    from .filer_profile import get_or_create_profile, build_prompt_context
    from .filing_fetcher import fetch_filing_text, get_unprocessed_filings
    from .registry import get_active
    from .section_extract import extract_all_sections

    if output_dir is None:
        output_dir = OUTPUT_DIR

    active_rows = get_active(universe)
    requests = []

    for row in active_rows:
        config_path = row.get('config_path', '')
        if not config_path:
            continue
        try:
            config = load_config(Path(config_path))
        except Exception as e:
            logger.error(f"Failed to load config for {row['ticker']}: {e}")
            continue

        csv_path = output_dir / config.ticker.lower() / 'tracking.csv'
        unprocessed = get_unprocessed_filings(config.cik, csv_path, since=since)
        if not unprocessed:
            continue

        profile = get_or_create_profile(config.cik, config.ticker, config.issuer)
        filer_context = build_prompt_context(profile)

        for filing_meta in unprocessed:
            try:
                filing_text = fetch_filing_text(
                    config.cik,
                    filing_meta['accession_number'],
                    filing_meta['primary_document'],
                )
                sections = extract_all_sections(filing_text, config)
                prior_row = _get_prior_row(csv_path)

                for section_name, section_text in sections.items():
                    if not section_text:
                        continue
                    schema = _build_schema_for_section(config, section_name)
                    if not schema:
                        continue
                    prior_vals = _prior_values_for_section(prior_row, config, section_name)

                    request_id = f"{config.ticker}|{filing_meta['period_end']}|{section_name}"
                    requests.append({
                        'id': request_id,
                        'type': 'extraction',
                        'issuer': config.issuer,
                        'ticker': config.ticker,
                        'period_end': filing_meta['period_end'],
                        'form_type': filing_meta['form_type'],
                        'section_name': section_name,
                        'section_text': section_text,
                        'schema': schema,
                        'prior_values': prior_vals,
                        'filer_context': filer_context,
                    })

            except Exception as e:
                logger.error(f"Error preparing {config.ticker} {filing_meta['period_end']}: {e}")

    return requests


def _prepare_registered(universe, cutoff_date, max_activations, check_interval_days):
    """Prepare bootstrap + extraction requests for registered issuers."""
    from .activation import check_new_filing
    from .bootstrap import _extract_analysis_sections, _find_note_headings, ANALYSIS_PROMPT
    from .filing_fetcher import discover_filings, fetch_filing_text
    from .registry import get_registered, get_failed

    registered_rows = get_registered(universe)
    failed_rows = get_failed(universe)
    candidates = registered_rows + failed_rows
    requests = []
    activations_queued = 0
    skip_before = (datetime.now(timezone.utc) - timedelta(days=check_interval_days)).strftime('%Y-%m-%dT%H:%M:%SZ')

    for row in candidates:
        if activations_queued >= max_activations:
            break

        ticker = row.get('ticker', '')
        cik = row.get('cik', '')
        is_failed = row.get('status') == 'failed_activation'

        last_checked = row.get('last_checked_at', '')
        if not is_failed and last_checked and last_checked >= skip_before:
            continue

        last_seen = row.get('last_filing_date_seen', '')
        effective_cutoff = '' if is_failed else cutoff_date
        new_filing = check_new_filing(cik, last_known_date=last_seen,
                                      cutoff_date=effective_cutoff)
        if not new_filing:
            continue

        logger.info(f"  {ticker}: new {new_filing['form_type']} ({new_filing['period_end']})")

        try:
            filing_text = fetch_filing_text(
                cik, new_filing['accession_number'], new_filing['primary_document']
            )

            # Check for 10-K for bootstrap
            bootstrap_text = filing_text
            bootstrap_filing = new_filing
            if new_filing.get('form_type') == '10-Q':
                all_filings = discover_filings(cik)
                ten_ks = [f for f in all_filings if f['form_type'] == '10-K']
                if ten_ks:
                    latest_10k = ten_ks[-1]
                    bootstrap_text = fetch_filing_text(
                        cik, latest_10k['accession_number'], latest_10k['primary_document']
                    )
                    bootstrap_filing = latest_10k

            note_headings = _find_note_headings(bootstrap_text)
            analysis_sections = _extract_analysis_sections(bootstrap_text)
            combined_text = '\n\n---\n\n'.join(
                f'[{name}]\n{content}' for name, content in analysis_sections.items()
            )
            issuer_name = row.get('issuer_name', '')
            company_info = f'{issuer_name} ({ticker})' if issuer_name else f'CIK {cik}'

            prompt = ANALYSIS_PROMPT.format(
                company_info=company_info,
                note_headings=json.dumps(note_headings[:20]),
                section_text=combined_text[:12000],
            )

            request_id = f"{ticker}|bootstrap"
            requests.append({
                'id': request_id,
                'type': 'bootstrap',
                'ticker': ticker,
                'cik': cik,
                'issuer_name': issuer_name,
                'sector': row.get('sector', ''),
                'prompt': prompt,
                'filing_meta': new_filing,
                'bootstrap_filing_meta': bootstrap_filing,
            })
            activations_queued += 1

        except Exception as e:
            logger.error(f"Error preparing bootstrap for {ticker}: {e}")

    return requests


def prepare(args):
    """Phase 1: discover work and write extraction request files."""
    from .registry import load_universe

    universe = load_universe()
    if not universe:
        logger.error('Universe is empty')
        return

    WORK_DIR.mkdir(exist_ok=True)
    # Clean previous run
    for f in WORK_DIR.glob('*.json'):
        f.unlink()

    cutoff_date = args.cutoff or (
        datetime.now(timezone.utc) - timedelta(days=120)
    ).strftime('%Y-%m-%d')

    output_dir = Path(args.output) if args.output else None

    extraction_requests = _prepare_active(universe, args.since, output_dir)
    bootstrap_requests = _prepare_registered(
        universe, cutoff_date, args.max_activations,
        args.check_interval,
    )

    all_requests = extraction_requests + bootstrap_requests

    if not all_requests:
        logger.info('No new work to process.')
        manifest = {'count': 0, 'extraction': 0, 'bootstrap': 0, 'requests': []}
        (WORK_DIR / 'manifest.json').write_text(json.dumps(manifest, indent=2))
        return

    # Write individual request files + manifest
    manifest_entries = []
    for req in all_requests:
        safe_id = re.sub(r'[|/\\]', '_', req['id'])
        filename = f"{req['type']}_{safe_id}.json"
        (WORK_DIR / filename).write_text(json.dumps(req, indent=2), encoding='utf-8')
        manifest_entries.append({
            'id': req['id'],
            'type': req['type'],
            'file': filename,
        })

    manifest = {
        'count': len(all_requests),
        'extraction': len(extraction_requests),
        'bootstrap': len(bootstrap_requests),
        'requests': manifest_entries,
    }
    (WORK_DIR / 'manifest.json').write_text(json.dumps(manifest, indent=2))

    logger.info(f'Prepared {len(extraction_requests)} extraction + {len(bootstrap_requests)} bootstrap requests in {WORK_DIR}')


# ---------------------------------------------------------------------------
# Finalize phase — inject cached results, run the real pipeline
# ---------------------------------------------------------------------------

def finalize(args):
    """Phase 3: run the scheduler with cached Claude Code results."""
    from . import llm_extract, bootstrap
    from .scheduler import run_scheduled, print_run_summary, summary_to_dict

    # Load manifest
    manifest_path = WORK_DIR / 'manifest.json'
    if not manifest_path.exists():
        logger.error('No manifest.json found — run prepare first')
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text())
    if manifest['count'] == 0:
        logger.info('No work was prepared — nothing to finalize.')
        return

    # Load all results into caches
    extraction_cache = {}
    bootstrap_cache = {}

    for entry in manifest['requests']:
        result_path = RESULTS_DIR / entry['file']
        if not result_path.exists():
            logger.warning(f"Missing result for {entry['id']} — will fall back to API")
            continue

        result = json.loads(result_path.read_text(encoding='utf-8'))

        if entry['type'] == 'extraction':
            extraction_cache[entry['id']] = result
        elif entry['type'] == 'bootstrap':
            bootstrap_cache[entry['id']] = result

    logger.info(f'Loaded {len(extraction_cache)} extraction + {len(bootstrap_cache)} bootstrap results')

    # Install overrides
    def cached_extractor(section_text, schema, context, filer_context=''):
        request_id = f"{context['issuer']}|{context['period_end']}|{context['section_name']}"
        # Try ticker-based ID too (prepare uses ticker, engine passes issuer name)
        if request_id not in extraction_cache:
            # Search by period_end and section_name across all keys
            for key, val in extraction_cache.items():
                parts = key.split('|')
                if len(parts) == 3 and parts[1] == context['period_end'] and parts[2] == context['section_name']:
                    return val
            logger.warning(f'No cached result for {request_id} — returning empty')
            return {
                'fields': {name: {'value': None, 'confidence': 'not_found', 'source_quote': ''} for name in schema},
                'flags': ['cc_bridge: no cached result available'],
                'notes': '',
            }
        return extraction_cache[request_id]

    def cached_analyser(prompt):
        # Match by ticker in the prompt
        for key, val in bootstrap_cache.items():
            ticker = key.split('|')[0]
            if ticker in prompt:
                return json.dumps(val) if isinstance(val, dict) else val
        logger.warning('No cached bootstrap result found — returning empty')
        return json.dumps({'key_fields': [], 'unusual_features': []})

    llm_extract.set_override(cached_extractor)
    bootstrap.set_analysis_override(cached_analyser)

    try:
        output_dir = Path(args.output) if args.output else None
        kwargs = dict(
            since=args.since,
            cutoff_date=args.cutoff or '',
            max_activations=args.max_activations,
            check_interval_days=args.check_interval,
        )
        if output_dir:
            kwargs['output_dir'] = output_dir

        summary = run_scheduled(**kwargs)
        print_run_summary(summary)

        if args.json_summary:
            with open(args.json_summary, 'w', encoding='utf-8') as f:
                json.dump(summary_to_dict(summary), f, indent=2)
            logger.info(f'Summary written to {args.json_summary}')

    finally:
        llm_extract.set_override(None)
        bootstrap.set_analysis_override(None)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Claude Code bridge for SEC Derivatives pipeline')
    sub = parser.add_subparsers(dest='command', required=True)

    # Shared arguments
    for name, sp in [
        ('prepare', sub.add_parser('prepare', help='Discover work, write extraction requests')),
        ('finalize', sub.add_parser('finalize', help='Run pipeline with cached results')),
    ]:
        sp.add_argument('--since', '-s', default='')
        sp.add_argument('--cutoff', default='')
        sp.add_argument('--max-activations', type=int, default=50)
        sp.add_argument('--check-interval', type=int, default=3)
        sp.add_argument('--output', '-o', default=None)
        sp.add_argument('--verbose', '-v', action='store_true')

    finalize_parser = sub.choices['finalize']
    finalize_parser.add_argument('--json-summary', type=Path, default=None)

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.command == 'prepare':
        prepare(args)
    elif args.command == 'finalize':
        finalize(args)


if __name__ == '__main__':
    main()
