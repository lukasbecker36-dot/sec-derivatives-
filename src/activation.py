"""Lazy activation pipeline — detect new filing, bootstrap config, extract, promote."""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import anthropic

from .bootstrap import bootstrap_issuer_for_activation, PROFILES_DIR
from .config import load_config
from .engine import process_filing, append_csv_row, append_notes, append_alerts, OUTPUT_DIR
from .filer_profile import (
    create_initial_profile, update_profile_after_extraction, save_profile,
)
from .filing_fetcher import discover_filings, fetch_filing_text
from .section_extract import is_likely_cross_reference

logger = logging.getLogger(__name__)


@dataclass
class ActivationResult:
    ticker: str
    cik: str
    success: bool
    new_status: str  # 'active' | 'active_needs_review' | 'failed_activation'
    config_path: str = ''
    filing_date: str = ''
    form_type: str = ''
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def check_new_filing(cik: str, last_known_date: str = '',
                     cutoff_date: str = '') -> dict | None:
    """Check EDGAR for a new 10-Q/10-K filing after the last known date.

    Returns the newest filing metadata dict, or None if no new filing exists.
    Only considers 10-Q and 10-K forms (not amendments).
    """
    try:
        filings = discover_filings(cik)
    except Exception as e:
        logger.warning(f'EDGAR check failed for CIK {cik}: {e}')
        return None

    # Filter to 10-Q and 10-K only (not amendments)
    filings = [f for f in filings if f['form_type'] in ('10-Q', '10-K')]

    if cutoff_date:
        filings = [f for f in filings if f['period_end'] >= cutoff_date]

    if last_known_date:
        filings = [f for f in filings if f['period_end'] > last_known_date]

    if not filings:
        return None

    # Return the newest
    return filings[-1]


def score_bootstrap(bootstrap_result: dict) -> tuple[float, list[str]]:
    """Score the quality of a bootstrapped config. Returns (score 0-1, reasons)."""
    score = 0.5  # baseline
    reasons = []

    archetype_conf = bootstrap_result.get('archetype_confidence', 0.0)
    if archetype_conf > 0.3:
        score += 0.15
        reasons.append(f'Archetype confidence: {archetype_conf:.2f}')
    elif archetype_conf == 0.0:
        score -= 0.15
        reasons.append('No archetype keyword matches — defaulted')

    sections_found = bootstrap_result.get('sections_found', [])
    if sections_found:
        score += 0.20
        reasons.append(f'Sections found: {", ".join(sections_found)}')
    else:
        score -= 0.25
        reasons.append('No derivatives/market risk sections found')

    note_headings = bootstrap_result.get('note_headings_found', [])
    if len(note_headings) >= 3:
        score += 0.10
        reasons.append(f'{len(note_headings)} note headings found')

    if bootstrap_result.get('llm_analysis_failed'):
        score -= 0.30
        reasons.append('LLM analysis failed entirely')
    else:
        key_fields = bootstrap_result.get('llm_analysis', {}).get('key_fields', [])
        if len(key_fields) >= 2:
            score += 0.15
            reasons.append(f'LLM identified {len(key_fields)} key fields')

    warnings = bootstrap_result.get('warnings', [])
    if warnings:
        for w in warnings:
            reasons.append(f'Warning: {w}')

    return max(0.0, min(1.0, score)), reasons


def score_extraction(process_result: dict, config) -> tuple[float, list[str]]:
    """Score the quality of a first extraction run. Returns (score 0-1, reasons)."""
    score = 0.5  # baseline
    reasons = []

    # Field fill rate
    llm_results = process_result.get('llm_results', {})
    total_fields = 0
    non_null_fields = 0
    for section_result in llm_results.values():
        for field_data in section_result.get('fields', {}).values():
            total_fields += 1
            if field_data.get('value') is not None:
                non_null_fields += 1

    if total_fields > 0:
        fill_rate = non_null_fields / total_fields
        if fill_rate >= 0.5:
            score += 0.30
            reasons.append(f'Field fill rate: {fill_rate:.0%} ({non_null_fields}/{total_fields})')
        elif fill_rate < 0.2:
            score -= 0.40
            reasons.append(f'Very low fill rate: {fill_rate:.0%} ({non_null_fields}/{total_fields})')
        else:
            reasons.append(f'Moderate fill rate: {fill_rate:.0%}')
    else:
        score -= 0.30
        reasons.append('No fields extracted at all')

    # Validation errors
    validation = process_result.get('validation', [])
    val_errors = [v for v in validation if v.get('level') == 'error']
    if not val_errors:
        score += 0.10
        reasons.append('No validation errors')
    else:
        score -= 0.15
        reasons.append(f'{len(val_errors)} validation errors')

    # Section text length (cross-reference check)
    sections = process_result.get('sections', {})
    for section_name, text in sections.items():
        if text and is_likely_cross_reference(text):
            score -= 0.20
            reasons.append(f'Section "{section_name}" appears to be a cross-reference stub')

    # Non-empty sections
    non_empty = sum(1 for t in sections.values() if t)
    if non_empty >= 2:
        score += 0.10
        reasons.append(f'{non_empty} non-empty sections extracted')
    elif non_empty == 0:
        score -= 0.20
        reasons.append('No non-empty sections')

    return max(0.0, min(1.0, score)), reasons


def compute_final_status(bootstrap_score: float, extraction_score: float) -> str:
    """Combine scores into final status decision."""
    combined = 0.4 * bootstrap_score + 0.6 * extraction_score

    if combined >= 0.60:
        return 'active'
    elif combined >= 0.35:
        return 'active_needs_review'
    else:
        return 'failed_activation'


def activate_issuer(
    ticker: str,
    cik: str,
    issuer_name: str,
    sector: str,
    filing_meta: dict,
    client: anthropic.Anthropic | None = None,
    output_dir: Path = OUTPUT_DIR,
) -> ActivationResult:
    """Full activation pipeline for one issuer.

    1. Fetch filing text
    2. Bootstrap config
    3. Score bootstrap
    4. Run extraction
    5. Score extraction
    6. Determine final status
    7. Write outputs or clean up
    """
    if client is None:
        client = anthropic.Anthropic()

    result = ActivationResult(
        ticker=ticker,
        cik=cik,
        filing_date=filing_meta.get('period_end', ''),
        form_type=filing_meta.get('form_type', ''),
    )

    try:
        # Step 1: Fetch filing text
        logger.info(f'Activating {ticker}: fetching {filing_meta["form_type"]} {filing_meta["period_end"]}...')
        filing_text = fetch_filing_text(
            cik, filing_meta['accession_number'], filing_meta['primary_document']
        )

        # Step 2: Bootstrap config
        logger.info(f'Activating {ticker}: bootstrapping config...')
        bootstrap_result = bootstrap_issuer_for_activation(
            cik=cik, ticker=ticker, issuer_name=issuer_name,
            filing_text=filing_text, client=client,
        )

        # Step 3: Score bootstrap
        bs_score, bs_reasons = score_bootstrap(bootstrap_result)
        result.reasons.extend(bs_reasons)
        logger.info(f'Activating {ticker}: bootstrap score = {bs_score:.2f}')

        if bs_score < 0.15:
            # Catastrophic bootstrap failure — don't even try extraction
            result.new_status = 'failed_activation'
            result.score = bs_score
            result.reasons.append('Bootstrap score too low to attempt extraction')
            _cleanup_failed(bootstrap_result.get('config_path'))
            return result

        # Step 4: Load config and run extraction
        config_path = bootstrap_result['config_path']
        config = load_config(config_path)

        logger.info(f'Activating {ticker}: running extraction...')
        process_result = process_filing(
            config, filing_meta, filing_text, prior_row=None, client=client,
        )

        # Step 5: Score extraction
        ext_score, ext_reasons = score_extraction(process_result, config)
        result.reasons.extend(ext_reasons)
        logger.info(f'Activating {ticker}: extraction score = {ext_score:.2f}')

        # Step 6: Final status
        final_status = compute_final_status(bs_score, ext_score)
        result.new_status = final_status
        result.score = 0.4 * bs_score + 0.6 * ext_score
        result.config_path = str(config_path)

        # Step 7: Write outputs or clean up
        if final_status in ('active', 'active_needs_review'):
            result.success = True
            issuer_dir = output_dir / ticker.lower()
            csv_path = issuer_dir / 'tracking.csv'
            notes_path = issuer_dir / 'notes.txt'
            alert_path = issuer_dir / 'alert_log.txt'

            append_csv_row(csv_path, process_result['row'], config)
            append_notes(notes_path, filing_meta['period_end'],
                         filing_meta['form_type'], process_result['notes'])
            append_alerts(alert_path, filing_meta['period_end'],
                          filing_meta['form_type'], process_result['alerts'])

            # Create filer profile
            profile = create_initial_profile(cik, ticker, issuer_name)
            profile = update_profile_after_extraction(
                profile, filing_meta, filing_text,
                process_result.get('sections', {}),
                process_result.get('llm_results', {}), config,
            )
            save_profile(profile)

            logger.info(f'Activating {ticker}: -> {final_status} (score {result.score:.2f})')
        else:
            _cleanup_failed(config_path)
            logger.warning(f'Activating {ticker}: -> failed_activation (score {result.score:.2f})')

    except Exception as e:
        result.new_status = 'failed_activation'
        result.errors.append(str(e))
        logger.error(f'Activation failed for {ticker}: {e}')

    return result


def _cleanup_failed(config_path) -> None:
    """Remove a generated config file on failed activation."""
    if config_path:
        path = Path(config_path)
        if path.exists():
            path.unlink()
            logger.info(f'Cleaned up failed config: {path}')
