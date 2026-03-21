"""Core pipeline -- wire section extraction + LLM extraction + qualitative + validation."""

import csv
import logging
from pathlib import Path
from datetime import datetime, timezone

from .config import IssuerConfig, load_config
from .filing_fetcher import fetch_filing_text, get_unprocessed_filings
from .section_extract import extract_all_sections
from .llm_extract import extract_fields_llm
from .qualitative import extract_qualitative
from .change_detect import detect_changes
from .validate import validate_row

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'output'


def _build_schema_for_section(config: IssuerConfig, section_name: str) -> dict[str, str]:
    """Get {field_name: description} for fields belonging to a section."""
    return {
        name: fld.description
        for name, fld in config.fields.items()
        if fld.section == section_name
    }


def _get_prior_row(csv_path: Path) -> dict | None:
    """Read the last row from the tracking CSV."""
    if not csv_path.exists():
        return None
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows[-1] if rows else None


def _prior_values_for_section(prior_row: dict | None, config: IssuerConfig, section_name: str) -> dict:
    """Extract prior period values for fields in a given section."""
    if not prior_row:
        return {}
    result = {}
    for name, fld in config.fields.items():
        if fld.section == section_name and name in prior_row:
            val = prior_row[name]
            if val and val != '':
                try:
                    result[name] = float(val)
                except (ValueError, TypeError):
                    result[name] = val
    return result


def process_filing(
    config: IssuerConfig,
    filing_meta: dict,
    filing_text: str,
    prior_row: dict | None = None,
    client=None,
) -> dict:
    """Process a single filing through the full pipeline.

    Returns dict with:
        - row: dict of extracted field values for CSV
        - notes: qualitative findings
        - alerts: list of alert strings
        - validation: list of validation results
        - llm_results: raw LLM results per section
    """
    # Stage 1: Section extraction
    sections = extract_all_sections(filing_text, config)

    # Stage 2: LLM extraction per section
    row = {
        'period_end_date': filing_meta['period_end'],
        'form_type': filing_meta['form_type'],
    }
    all_llm_results = {}
    all_flags = []

    for section_name, section_text in sections.items():
        if not section_text:
            logger.warning(f"Empty section '{section_name}' for {config.issuer}")
            continue

        schema = _build_schema_for_section(config, section_name)
        if not schema:
            continue

        prior_vals = _prior_values_for_section(prior_row, config, section_name)
        context = {
            'issuer': config.issuer,
            'period_end': filing_meta['period_end'],
            'form_type': filing_meta['form_type'],
            'section_name': section_name,
            'prior_values': prior_vals,
        }

        llm_result = extract_fields_llm(section_text, schema, context, client=client)
        all_llm_results[section_name] = llm_result

        # Flatten LLM fields into row
        for field_name, field_data in llm_result.get('fields', {}).items():
            row[field_name] = field_data.get('value')

        all_flags.extend(llm_result.get('flags', []))

    # Qualitative extraction
    notes = extract_qualitative(sections, config, prior_row)

    # Validation
    validation = validate_row(row, prior_row, config)

    # Change detection
    alerts = detect_changes(row, prior_row, config)
    alerts.extend(f'[VALIDATION] {v["message"]}' for v in validation if v['level'] == 'error')

    if all_flags:
        for flag in all_flags:
            if flag:
                alerts.append(f'[LLM_FLAG] {flag}')

    return {
        'row': row,
        'notes': notes,
        'alerts': alerts,
        'validation': validation,
        'llm_results': all_llm_results,
    }


def _get_csv_columns(config: IssuerConfig) -> list[str]:
    """Build ordered list of CSV columns."""
    cols = ['period_end_date', 'form_type']
    cols.extend(config.fields.keys())
    return cols


def append_csv_row(csv_path: Path, row: dict, config: IssuerConfig):
    """Append a row to the tracking CSV, creating the file if needed."""
    columns = _get_csv_columns(config)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    write_header = not csv_path.exists()
    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def append_notes(notes_path: Path, period_end: str, form_type: str, notes: dict):
    """Append qualitative notes to the notes file."""
    if not notes:
        return
    notes_path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing content to prepend (newest first)
    existing = ''
    if notes_path.exists():
        existing = notes_path.read_text(encoding='utf-8')

    lines = []
    lines.append(f'--- {form_type} | Period ending {period_end} ---')
    for category, items in notes.items():
        lines.append(f'\n  [{category}]')
        for item in items:
            display = item if len(item) <= 300 else item[:297] + '...'
            lines.append(f'    - {display}')
    lines.append('')

    new_content = '\n'.join(lines) + '\n'
    notes_path.write_text(new_content + existing, encoding='utf-8')


def append_alerts(alert_path: Path, period_end: str, form_type: str, alerts: list[str]):
    """Append alerts to the alert log file."""
    if not alerts:
        return
    alert_path.parent.mkdir(parents=True, exist_ok=True)

    existing = ''
    if alert_path.exists():
        existing = alert_path.read_text(encoding='utf-8')

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    lines = [f'=== {form_type} | Period ending {period_end} | Processed {now} ===']
    lines.extend(alerts)
    lines.append('')

    new_content = '\n'.join(lines) + '\n'
    alert_path.write_text(new_content + existing, encoding='utf-8')


def run_issuer(config: IssuerConfig, output_dir: Path = OUTPUT_DIR, client=None, since: str = '') -> dict:
    """Run the full pipeline for one issuer.

    Args:
        since: Optional cutoff date (YYYY-MM-DD). Only process filings on or after this date.

    Returns summary dict with counts and any errors.
    """
    issuer_dir = output_dir / config.ticker.lower()
    csv_path = issuer_dir / 'tracking.csv'
    notes_path = issuer_dir / 'notes.txt'
    alert_path = issuer_dir / 'alert_log.txt'

    unprocessed = get_unprocessed_filings(config.cik, csv_path, since=since)
    logger.info(f'{config.issuer}: {len(unprocessed)} unprocessed filings')

    processed = 0
    errors = []

    for filing_meta in unprocessed:
        try:
            logger.info(f"  Fetching {filing_meta['form_type']} {filing_meta['period_end']}...")
            filing_text = fetch_filing_text(
                config.cik,
                filing_meta['accession_number'],
                filing_meta['primary_document'],
            )

            prior_row = _get_prior_row(csv_path)
            result = process_filing(config, filing_meta, filing_text, prior_row, client=client)

            append_csv_row(csv_path, result['row'], config)
            append_notes(notes_path, filing_meta['period_end'], filing_meta['form_type'], result['notes'])
            append_alerts(alert_path, filing_meta['period_end'], filing_meta['form_type'], result['alerts'])

            processed += 1

        except Exception as e:
            logger.error(f"  Error processing {filing_meta['period_end']}: {e}")
            errors.append({'period_end': filing_meta['period_end'], 'error': str(e)})

    return {
        'issuer': config.issuer,
        'ticker': config.ticker,
        'processed': processed,
        'errors': errors,
        'total_available': len(unprocessed),
    }
