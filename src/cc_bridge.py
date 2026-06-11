"""Claude Code bridge — split pipeline into prepare/finalize with CC as the LLM."""

import argparse
import csv
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .activation import check_new_filing, score_bootstrap, score_extraction, compute_final_status
from .bootstrap import (
    _classify_archetype_with_confidence, _find_note_headings,
    _extract_analysis_sections, _build_config_yaml, ANALYSIS_PROMPT, PROFILES_DIR,
)
from .config import load_config, IssuerConfig
from .engine import (
    _build_schema_for_section, _get_prior_row, _prior_values_for_section,
    process_filing, append_csv_row, append_notes, append_alerts, OUTPUT_DIR,
)
from .filer_profile import (
    get_or_create_profile, build_prompt_context, create_initial_profile,
    update_profile_after_extraction, save_profile,
)
from .filing_fetcher import fetch_filing_text, get_unprocessed_filings, discover_filings
from .llm_extract import build_extraction_prompt
from .registry import (
    load_universe, save_universe, get_active, get_registered, get_failed,
    update_last_checked, mark_activating, mark_active,
    mark_active_needs_review, mark_failed,
    append_activation_event, append_review_item,
)
from .section_extract import extract_all_sections

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORK_DIR = PROJECT_ROOT / 'cc_work'
RESULTS_DIR = PROJECT_ROOT / 'cc_results'


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _resolve_config_path(raw_path: str) -> Path:
    """Resolve config_path from universe CSV to an actual local path."""
    raw = Path(raw_path)
    # If relative and exists, use directly
    if not raw.is_absolute():
        local = PROJECT_ROOT / raw
        if local.exists():
            return local
    # If absolute and exists, use it
    if raw.exists():
        return raw
    # Extract profiles/xxx.yaml from any absolute path
    parts = raw.parts
    for i, part in enumerate(parts):
        if part == 'profiles' and i + 1 < len(parts):
            candidate = PROJECT_ROOT / 'profiles' / parts[i + 1]
            if candidate.exists():
                return candidate
    # Last resort: try profiles/<stem>.yaml
    candidate = PROJECT_ROOT / 'profiles' / raw.name
    if candidate.exists():
        return candidate
    return raw


def _safe_filename(ticker: str, section: str, period: str, idx: int) -> str:
    safe = re.sub(r'[^a-zA-Z0-9_.-]', '_', f'{ticker}_{section}_{period}')
    return f'{safe}_{idx:03d}.json'


def prepare(since: str, max_activations: int, verbose: bool = False):
    """Phase 1: Fetch filings, extract sections, write request files."""
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    universe = load_universe()
    if not universe:
        logger.error('Universe is empty')
        _write_manifest([])
        return

    logger.info(f'Loaded universe: {len(universe)} issuers')

    requests = []
    request_idx = 0
    now = _now_iso()
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=120)).strftime('%Y-%m-%d')

    # === Pass 1: Active issuers ===
    active_rows = get_active(universe)
    logger.info(f'Pass 1: {len(active_rows)} active issuers')

    for row in active_rows:
        ticker = row.get('ticker', '')
        cik = row.get('cik', '')
        config_path_raw = row.get('config_path', '')

        if not config_path_raw:
            logger.warning(f'{ticker}: no config_path, skipping')
            continue

        config_path = _resolve_config_path(config_path_raw)
        if not config_path.exists():
            logger.warning(f'{ticker}: config not found at {config_path}, skipping')
            continue

        try:
            config = load_config(config_path)
        except Exception as e:
            logger.error(f'{ticker}: failed to load config: {e}')
            continue

        csv_path = OUTPUT_DIR / config.ticker.lower() / 'tracking.csv'
        try:
            unprocessed = get_unprocessed_filings(config.cik, csv_path, since=since)
        except Exception as e:
            logger.error(f'{ticker}: EDGAR fetch failed: {e}')
            continue

        if not unprocessed:
            logger.debug(f'{ticker}: no unprocessed filings')
            continue

        logger.info(f'{ticker}: {len(unprocessed)} unprocessed filings')
        profile = get_or_create_profile(config.cik, config.ticker, config.issuer)
        filer_context = build_prompt_context(profile)
        prior_row = _get_prior_row(csv_path)

        for filing_meta in unprocessed:
            try:
                logger.info(f'  Fetching {ticker} {filing_meta["form_type"]} {filing_meta["period_end"]}...')
                filing_text = fetch_filing_text(
                    config.cik,
                    filing_meta['accession_number'],
                    filing_meta['primary_document'],
                )

                sections = extract_all_sections(filing_text, config)

                for section_name, section_text in sections.items():
                    if not section_text:
                        continue
                    schema = _build_schema_for_section(config, section_name)
                    if not schema:
                        continue
                    prior_vals = _prior_values_for_section(prior_row, config, section_name)

                    filename = _safe_filename(ticker, section_name, filing_meta['period_end'], request_idx)
                    request_data = {
                        'type': 'extraction',
                        'ticker': ticker,
                        'cik': cik,
                        'issuer': config.issuer,
                        'period_end': filing_meta['period_end'],
                        'form_type': filing_meta['form_type'],
                        'accession_number': filing_meta['accession_number'],
                        'primary_document': filing_meta['primary_document'],
                        'section_name': section_name,
                        'section_text': section_text,
                        'schema': schema,
                        'prior_values': prior_vals,
                        'filer_context': filer_context,
                        'config_path': str(config_path),
                        'filing_text_length': len(filing_text),
                    }

                    req_path = WORK_DIR / filename
                    with open(req_path, 'w', encoding='utf-8') as f:
                        json.dump(request_data, f, indent=2, ensure_ascii=False)

                    requests.append({
                        'file': filename,
                        'type': 'extraction',
                        'ticker': ticker,
                        'section': section_name,
                        'period_end': filing_meta['period_end'],
                    })
                    request_idx += 1

            except Exception as e:
                logger.error(f'  Error preparing {ticker} {filing_meta["period_end"]}: {e}')

    # === Pass 2: Registered / failed issuers ===
    registered_rows = get_registered(universe)
    failed_rows = get_failed(universe)
    candidate_rows = registered_rows + failed_rows
    activations_written = 0

    logger.info(f'Pass 2: {len(candidate_rows)} registered/failed issuers')

    for row in candidate_rows:
        if activations_written >= max_activations:
            break

        ticker = row.get('ticker', '')
        cik = row.get('cik', '')
        is_failed = row.get('status') == 'failed_activation'
        last_seen = row.get('last_filing_date_seen', '')

        effective_cutoff = '' if is_failed else cutoff_date
        try:
            new_filing = check_new_filing(cik, last_known_date=last_seen, cutoff_date=effective_cutoff)
        except Exception as e:
            logger.debug(f'{ticker}: check failed: {e}')
            continue

        if not new_filing:
            continue

        logger.info(f'{ticker}: new {new_filing["form_type"]} ({new_filing["period_end"]})')

        try:
            filing_text = fetch_filing_text(
                cik, new_filing['accession_number'], new_filing['primary_document']
            )

            # Also try to get 10-K for better bootstrap
            bootstrap_text = filing_text
            bootstrap_meta = new_filing
            if new_filing.get('form_type') == '10-Q':
                try:
                    all_filings = discover_filings(cik)
                    ten_ks = [f for f in all_filings if f['form_type'] == '10-K']
                    if ten_ks:
                        latest_10k = ten_ks[-1]
                        bootstrap_text = fetch_filing_text(
                            cik, latest_10k['accession_number'], latest_10k['primary_document']
                        )
                        bootstrap_meta = latest_10k
                except Exception:
                    pass

            analysis_sections = _extract_analysis_sections(bootstrap_text)
            note_headings = _find_note_headings(bootstrap_text)

            issuer_name = row.get('issuer_name', '')
            company_info = f'{issuer_name} ({ticker})' if issuer_name else f'CIK {cik}'
            combined_text = '\n\n---\n\n'.join(
                f'[{name}]\n{content}' for name, content in analysis_sections.items()
            )

            prompt = ANALYSIS_PROMPT.format(
                company_info=company_info,
                note_headings=json.dumps(note_headings[:20]),
                section_text=combined_text[:12000],
            )

            archetype, archetype_confidence = _classify_archetype_with_confidence(bootstrap_text)

            filename = _safe_filename(ticker, 'bootstrap', new_filing['period_end'], request_idx)
            request_data = {
                'type': 'bootstrap',
                'ticker': ticker,
                'cik': cik,
                'issuer_name': issuer_name,
                'sector': row.get('sector', ''),
                'period_end': new_filing['period_end'],
                'form_type': new_filing['form_type'],
                'accession_number': new_filing['accession_number'],
                'primary_document': new_filing['primary_document'],
                'prompt': prompt,
                'archetype': archetype,
                'archetype_confidence': archetype_confidence,
                'note_headings': note_headings,
                'sections_found': list(analysis_sections.keys()),
                'bootstrap_filing_text': bootstrap_text[:60000],
                'original_filing_text': filing_text[:60000] if filing_text != bootstrap_text else '',
                'original_filing_meta': new_filing if filing_text != bootstrap_text else None,
            }

            req_path = WORK_DIR / filename
            with open(req_path, 'w', encoding='utf-8') as f:
                json.dump(request_data, f, indent=2, ensure_ascii=False)

            requests.append({
                'file': filename,
                'type': 'bootstrap',
                'ticker': ticker,
                'period_end': new_filing['period_end'],
            })
            request_idx += 1
            activations_written += 1

        except Exception as e:
            logger.error(f'{ticker}: bootstrap prep failed: {e}')

    _write_manifest(requests)
    logger.info(f'Wrote {len(requests)} request files to {WORK_DIR}')


def _write_manifest(requests: list[dict]):
    manifest = {
        'timestamp': _now_iso(),
        'count': len(requests),
        'requests': requests,
    }
    with open(WORK_DIR / 'manifest.json', 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)


def finalize(since: str, max_activations: int, json_summary: str = '', verbose: bool = False):
    """Phase 3: Read CC results, validate, write outputs, update registry."""
    manifest_path = WORK_DIR / 'manifest.json'
    if not manifest_path.exists():
        logger.error('No manifest found — was prepare run?')
        return

    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)

    universe = load_universe()
    now = _now_iso()

    summary = {
        'started_at': now,
        'extractions_processed': 0,
        'extractions_failed': 0,
        'bootstraps_processed': 0,
        'bootstraps_failed': 0,
        'activations_succeeded': 0,
        'activations_needs_review': 0,
        'activations_failed': 0,
        'active_filings_processed': 0,
        'issuer_results': [],
    }

    # Group extraction requests by (ticker, period_end) to reassemble full filing results
    extraction_groups = {}
    bootstrap_items = []

    for req_info in manifest.get('requests', []):
        result_path = RESULTS_DIR / req_info['file']
        if not result_path.exists():
            logger.warning(f'Missing result: {req_info["file"]}')
            if req_info['type'] == 'extraction':
                summary['extractions_failed'] += 1
            else:
                summary['bootstraps_failed'] += 1
            continue

        with open(result_path, 'r', encoding='utf-8') as f:
            result_data = json.load(f)

        if req_info['type'] == 'extraction':
            key = (req_info['ticker'], req_info['period_end'])
            if key not in extraction_groups:
                extraction_groups[key] = []
            extraction_groups[key].append({
                'req_info': req_info,
                'result': result_data,
            })
        else:
            bootstrap_items.append({
                'req_info': req_info,
                'result': result_data,
            })

    # === Process extraction results ===
    for (ticker, period_end), group in extraction_groups.items():
        try:
            first_req = group[0]
            req_path = WORK_DIR / first_req['req_info']['file']
            with open(req_path, 'r', encoding='utf-8') as f:
                req_data = json.load(f)

            config_path = _resolve_config_path(req_data['config_path'])
            config = load_config(config_path)

            csv_path = OUTPUT_DIR / config.ticker.lower() / 'tracking.csv'
            prior_row = _get_prior_row(csv_path)

            # Reassemble row from all section results
            row = {
                'period_end_date': period_end,
                'form_type': req_data['form_type'],
            }
            all_llm_results = {}
            all_flags = []

            for item in group:
                section_name = item['req_info']['section']
                llm_result = item['result']
                all_llm_results[section_name] = llm_result

                for field_name, field_data in llm_result.get('fields', {}).items():
                    row[field_name] = field_data.get('value')
                all_flags.extend(llm_result.get('flags', []))

            # Load sections text for qualitative extraction
            sections = {}
            for item in group:
                sec_name = item['req_info']['section']
                sec_req_path = WORK_DIR / item['req_info']['file']
                with open(sec_req_path, 'r', encoding='utf-8') as f:
                    sec_req = json.load(f)
                sections[sec_name] = sec_req.get('section_text', '')

            # Qualitative extraction
            from .qualitative import extract_qualitative
            notes = extract_qualitative(sections, config, prior_row)

            # Validation
            from .validate import validate_row
            validation = validate_row(row, prior_row, config)

            # Change detection
            from .change_detect import detect_changes
            alerts = detect_changes(row, prior_row, config)
            alerts.extend(f'[VALIDATION] {v["message"]}' for v in validation if v['level'] == 'error')
            if all_flags:
                for flag in all_flags:
                    if flag:
                        alerts.append(f'[LLM_FLAG] {flag}')

            # Add metadata columns
            row['accession_number'] = req_data.get('accession_number', '')
            row['filing_date'] = req_data.get('filing_date', '')
            row['processed_at'] = datetime.now(timezone.utc).isoformat()
            row['extraction_version'] = 1

            # Write outputs
            append_csv_row(csv_path, row, config)
            from .engine import _write_to_db
            _write_to_db(row, config)
            notes_path = OUTPUT_DIR / config.ticker.lower() / 'notes.txt'
            alert_path = OUTPUT_DIR / config.ticker.lower() / 'alert_log.txt'
            append_notes(notes_path, period_end, req_data['form_type'], notes)
            append_alerts(alert_path, period_end, req_data['form_type'], alerts)

            # Update filer profile
            profile = get_or_create_profile(config.cik, config.ticker, config.issuer)
            filing_meta = {
                'period_end': period_end,
                'form_type': req_data['form_type'],
            }
            profile = update_profile_after_extraction(
                profile, filing_meta, '',
                sections, all_llm_results, config,
            )
            save_profile(profile)

            summary['extractions_processed'] += 1
            summary['active_filings_processed'] += 1
            summary['issuer_results'].append({
                'ticker': ticker,
                'phase': 'active',
                'period_end': period_end,
                'status': 'ok',
                'fields_extracted': sum(
                    1 for sr in all_llm_results.values()
                    for fd in sr.get('fields', {}).values()
                    if fd.get('value') is not None
                ),
            })
            logger.info(f'{ticker} {period_end}: extraction finalized')

        except Exception as e:
            logger.error(f'Finalize extraction failed for {ticker} {period_end}: {e}')
            summary['extractions_failed'] += 1
            summary['issuer_results'].append({
                'ticker': ticker,
                'phase': 'active',
                'period_end': period_end,
                'status': 'error',
                'error': str(e),
            })

    # === Process bootstrap results ===
    for item in bootstrap_items:
        ticker = item['req_info']['ticker']
        period_end = item['req_info']['period_end']

        try:
            req_path = WORK_DIR / item['req_info']['file']
            with open(req_path, 'r', encoding='utf-8') as f:
                req_data = json.load(f)

            analysis = item['result']
            cik = req_data['cik']
            issuer_name = req_data.get('issuer_name', '')
            sector = req_data.get('sector', '')
            archetype = req_data.get('archetype', 'minimal_hedger')
            archetype_confidence = req_data.get('archetype_confidence', 0.0)

            # Build config YAML
            config_yaml = _build_config_yaml(
                cik=cik, ticker=ticker, issuer_name=issuer_name,
                archetype=archetype, analysis=analysis,
            )

            filename = ticker.lower() if ticker else cik.lstrip('0')
            config_path = PROFILES_DIR / f'{filename}.yaml'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(config_yaml)
            logger.info(f'{ticker}: wrote config to {config_path}')

            # Build bootstrap result for scoring
            bootstrap_result = {
                'config_path': config_path,
                'archetype': archetype,
                'archetype_confidence': archetype_confidence,
                'note_headings_found': req_data.get('note_headings', []),
                'sections_found': req_data.get('sections_found', []),
                'llm_analysis': analysis,
                'llm_analysis_failed': False,
                'warnings': [],
            }

            bs_score, bs_reasons = score_bootstrap(bootstrap_result)
            logger.info(f'{ticker}: bootstrap score = {bs_score:.2f}')

            if bs_score < 0.15:
                config_path.unlink(missing_ok=True)
                try:
                    universe = mark_failed(universe, ticker)
                except (KeyError, ValueError):
                    pass
                summary['activations_failed'] += 1
                summary['issuer_results'].append({
                    'ticker': ticker, 'phase': 'activation',
                    'status': 'failed_activation', 'score': bs_score,
                })
                continue

            # Try extraction
            config = load_config(config_path)
            bootstrap_text = req_data.get('bootstrap_filing_text', '')
            filing_meta = {
                'period_end': period_end,
                'form_type': req_data.get('form_type', '10-Q'),
                'accession_number': req_data.get('accession_number', ''),
                'primary_document': req_data.get('primary_document', ''),
            }

            if bootstrap_text:
                sections = extract_all_sections(bootstrap_text, config)

                # Build a mock LLM result from the extraction fields in the analysis
                all_llm_results = {}
                row = {
                    'period_end_date': period_end,
                    'form_type': filing_meta['form_type'],
                }

                non_empty_sections = {k: v for k, v in sections.items() if v}

                process_result = {
                    'row': row,
                    'notes': {},
                    'alerts': [],
                    'validation': [],
                    'llm_results': all_llm_results,
                    'sections': sections,
                }

                ext_score, ext_reasons = score_extraction(process_result, config)
            else:
                ext_score = 0.3
                ext_reasons = ['No filing text available for extraction']
                sections = {}
                process_result = {'row': {}, 'notes': {}, 'alerts': [], 'validation': [], 'llm_results': {}, 'sections': {}}

            final_status = compute_final_status(bs_score, ext_score)
            combined_score = 0.4 * bs_score + 0.6 * ext_score
            logger.info(f'{ticker}: extraction score = {ext_score:.2f}, final = {final_status} ({combined_score:.2f})')

            try:
                old_status = None
                for r in universe:
                    if r.get('ticker', '').lower() == ticker.lower():
                        old_status = r.get('status', 'registered')
                        break

                if old_status in ('registered', 'failed_activation'):
                    universe = mark_activating(universe, ticker)
                    append_activation_event(ticker, cik, old_status, 'activating',
                                            filing_date=period_end,
                                            form_type=filing_meta['form_type'],
                                            reason='New filing detected (cc_bridge)')

                if final_status == 'active':
                    universe = mark_active(universe, ticker, str(config_path))
                    summary['activations_succeeded'] += 1
                elif final_status == 'active_needs_review':
                    universe = mark_active_needs_review(universe, ticker, str(config_path))
                    summary['activations_needs_review'] += 1
                    reasons_str = '; '.join((bs_reasons + ext_reasons)[:3])
                    append_review_item(ticker, cik, reason=reasons_str, severity='warning',
                                       filing_date=period_end, form_type=filing_meta['form_type'],
                                       config_path=str(config_path))
                else:
                    universe = mark_failed(universe, ticker)
                    summary['activations_failed'] += 1
                    config_path.unlink(missing_ok=True)

                append_activation_event(ticker, cik, 'activating', final_status,
                                        filing_date=period_end,
                                        form_type=filing_meta['form_type'],
                                        reason=f'score={combined_score:.2f}')

                universe = update_last_checked(universe, ticker, _now_iso(),
                                               filing_date_seen=period_end)

            except (KeyError, ValueError) as e:
                logger.warning(f'{ticker}: registry update failed: {e}')

            # Write initial output if activated
            if final_status in ('active', 'active_needs_review') and bootstrap_text:
                issuer_dir = OUTPUT_DIR / ticker.lower()
                csv_path_out = issuer_dir / 'tracking.csv'
                notes_path = issuer_dir / 'notes.txt'
                alert_path = issuer_dir / 'alert_log.txt'

                if process_result.get('row'):
                    append_csv_row(csv_path_out, process_result['row'], config)
                append_notes(notes_path, period_end, filing_meta['form_type'],
                             process_result.get('notes', {}))
                append_alerts(alert_path, period_end, filing_meta['form_type'],
                              process_result.get('alerts', []))

                profile = create_initial_profile(cik, ticker, issuer_name)
                profile = update_profile_after_extraction(
                    profile, filing_meta, bootstrap_text,
                    sections, all_llm_results if 'all_llm_results' in dir() else {}, config,
                )
                save_profile(profile)

            summary['bootstraps_processed'] += 1
            summary['issuer_results'].append({
                'ticker': ticker, 'phase': 'activation',
                'status': final_status, 'score': combined_score,
            })

        except Exception as e:
            logger.error(f'Finalize bootstrap failed for {ticker}: {e}')
            summary['bootstraps_failed'] += 1
            summary['issuer_results'].append({
                'ticker': ticker, 'phase': 'activation',
                'status': 'error', 'error': str(e),
            })

    # Save universe
    save_universe(universe)

    summary['finished_at'] = _now_iso()

    # Write summary
    if json_summary:
        with open(json_summary, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        logger.info(f'Summary written to {json_summary}')

    # Print summary
    print(f'\n{"=" * 60}')
    print(f'Finalize complete')
    print(f'  Extractions: {summary["extractions_processed"]} ok, {summary["extractions_failed"]} failed')
    print(f'  Bootstraps: {summary["bootstraps_processed"]} ok, {summary["bootstraps_failed"]} failed')
    print(f'  Activations: {summary["activations_succeeded"]} active, '
          f'{summary["activations_needs_review"]} needs_review, '
          f'{summary["activations_failed"]} failed')
    print(f'  Active filings processed: {summary["active_filings_processed"]}')
    print()


def main():
    parser = argparse.ArgumentParser(description='Claude Code Bridge')
    sub = parser.add_subparsers(dest='command')

    prep = sub.add_parser('prepare', help='Prepare extraction request files')
    prep.add_argument('--since', default='', help='Date cutoff (YYYY-MM-DD)')
    prep.add_argument('--max-activations', type=int, default=10)
    prep.add_argument('--verbose', '-v', action='store_true')

    fin = sub.add_parser('finalize', help='Process results and write outputs')
    fin.add_argument('--since', default='', help='Date cutoff (YYYY-MM-DD)')
    fin.add_argument('--max-activations', type=int, default=10)
    fin.add_argument('--json-summary', default='', help='Write JSON summary')
    fin.add_argument('--verbose', '-v', action='store_true')

    args = parser.parse_args()

    level = logging.DEBUG if getattr(args, 'verbose', False) else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.command == 'prepare':
        prepare(since=args.since, max_activations=args.max_activations, verbose=args.verbose)
    elif args.command == 'finalize':
        finalize(since=args.since, max_activations=args.max_activations,
                 json_summary=args.json_summary, verbose=args.verbose)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
