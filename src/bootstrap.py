"""Onboarding tool: analyse one filing from a CIK, draft a YAML config."""

import json
import re
import logging
from pathlib import Path

import anthropic
import yaml

from .filing_fetcher import discover_filings, fetch_filing_text
from .utils import extract_sentences, log_llm_usage

logger = logging.getLogger(__name__)

PROFILES_DIR = Path(__file__).resolve().parent.parent / 'profiles'
ARCHETYPES_DIR = PROFILES_DIR / '_archetypes'
LLM_LOG = Path(__file__).resolve().parent.parent / 'output' / 'llm_usage.log'

SONNET_MODEL = 'claude-sonnet-4-20250514'

# Keywords for archetype classification
ARCHETYPE_SIGNALS = {
    'active_fx_commodity_hedger': [
        r'commodity\s+(?:derivative|contract|hedge)',
        r'foreign\s+(?:exchange|currency)\s+(?:derivative|contract|hedge)',
        r'designated\s+as\s+hedging',
    ],
    'active_ir_fx_hedger': [
        r'interest\s+rate\s+swap',
        r'foreign\s+(?:exchange|currency)\s+(?:derivative|contract|forward)',
        r'designated\s+as\s+(?:fair\s+value|cash\s+flow)\s+hedge',
    ],
    'minimal_hedger': [
        r'financial\s+instruments',
        r'fair\s+value',
        r'(?:no|not)\s+(?:use|enter|utilize)\s+derivative',
    ],
    'no_derivatives': [
        r'(?:do|does)\s+not\s+(?:use|enter|utilize)\s+derivative',
        r'no\s+derivative\s+(?:instrument|financial)',
    ],
}


def _classify_archetype_with_confidence(text: str) -> tuple[str, float]:
    """Classify archetype from filing text, returning (archetype, confidence 0-1)."""
    scores = {}
    for archetype, patterns in ARCHETYPE_SIGNALS.items():
        score = 0
        for pat in patterns:
            matches = re.findall(pat, text[:50000], re.I)
            score += len(matches)
        scores[archetype] = score

    if not any(scores.values()):
        return 'minimal_hedger', 0.0

    best = max(scores, key=scores.get)
    confidence = min(1.0, scores[best] / 5.0)
    return best, confidence


def _classify_archetype(text: str) -> str:
    """Classify archetype from filing text using keyword signals."""
    archetype, _ = _classify_archetype_with_confidence(text)
    return archetype


def _find_note_headings(text: str) -> list[str]:
    """Find all Note headings in the filing."""
    pattern = r'Note\s+\d+\s*[.\u2013\u2014\u2015\ufffd\u2012\u2014\u2013-]\s*([A-Z][^.]{5,80})'
    return re.findall(pattern, text)


def _filter_toc_matches(text: str, matches) -> list[re.Match]:
    """Filter out table-of-contents matches (heading followed by a page number)."""
    all_matches = list(matches)
    real = []
    for m in all_matches:
        after = text[m.end():m.end() + 20].strip()
        # ToC entries look like "...Market Risk 47 ITEM 4..." (page number right after)
        if re.match(r'^\d{1,3}\s', after):
            continue
        real.append(m)
    # If all matches were filtered (only ToC entries), return last one as fallback
    return real if real else all_matches


def _extract_analysis_sections(text: str) -> dict[str, str]:
    """Extract derivatives note and market risk section for LLM analysis."""
    sections = {}

    # Derivatives note
    for heading_pat in [
        r'Note\s+\d+\s*[.\u2013\u2014\u2015\ufffd\u2012\u2014\u2013-]\s*Derivative',
        r'Note\s+\d+\s*[.\u2013\u2014\u2015\ufffd\u2012\u2014\u2013-]\s*Financial Instruments',
        r'Note\s+\d+\s*[.\u2013\u2014\u2015\ufffd\u2012\u2014\u2013-]\s*Hedging',
    ]:
        matches = _filter_toc_matches(text, re.finditer(heading_pat, text, re.I))
        if matches:
            start = matches[-1].start()
            end_m = re.search(r'Note\s+\d+\s*[.\u2013\u2014\u2015\ufffd\u2012\u2014\u2013-]\s*(?!Derivative|Financial Instruments|Hedging)', text[start + 50:], re.I)
            end = start + 50 + end_m.start() if end_m else start + 10000
            sections['derivatives_or_instruments'] = text[start:end][:8000]
            break

    # Market risk
    matches = _filter_toc_matches(
        text,
        re.finditer(r'Quantitative\s+and\s+Qualitat\s*ive\s+Disclosures\s+About\s+Market\s+Risk', text, re.I),
    )
    if matches:
        start = matches[-1].start()
        end_m = re.search(r'Item\s*[\s\xa0]*[489]', text[start + 80:], re.I)
        end = start + 80 + end_m.start() if end_m else start + 8000
        sections['market_risk'] = text[start:end][:8000]

    return sections


ANALYSIS_PROMPT = """Analyse this SEC filing section for a derivatives/market risk data extraction config.

Company: {company_info}

Note headings found: {note_headings}

Section text:
{section_text}

Provide a JSON analysis:
{{
  "instrument_types": ["list of derivative instrument types found"],
  "has_designated_hedges": true/false,
  "has_notional_table": true/false,
  "has_fair_value_table": true/false,
  "key_fields": [
    {{"name": "field_name", "description": "what to extract", "section": "which section"}}
  ],
  "section_heading_pattern": "regex for the note heading",
  "end_boundary_pattern": "regex for where the section ends",
  "unusual_features": ["anything non-standard about this disclosure"]
}}

Return JSON only."""


def bootstrap_issuer(
    cik: str,
    ticker: str = '',
    issuer_name: str = '',
    client: anthropic.Anthropic | None = None,
) -> Path:
    """Bootstrap a new issuer config from a CIK.

    Fetches most recent 10-Q, analyses it, generates draft YAML.
    Returns path to the generated config file.
    """
    if client is None:
        client = anthropic.Anthropic()

    # Fetch most recent 10-Q
    filings = discover_filings(cik)
    ten_qs = [f for f in filings if f['form_type'] == '10-Q']
    if not ten_qs:
        ten_qs = [f for f in filings if f['form_type'] == '10-K']
    if not ten_qs:
        raise ValueError(f'No 10-Q or 10-K filings found for CIK {cik}')

    latest = ten_qs[-1]
    logger.info(f'Bootstrapping from {latest["form_type"]} {latest["period_end"]}')

    text = fetch_filing_text(cik, latest['accession_number'], latest['primary_document'])

    # Find note headings
    note_headings = _find_note_headings(text)

    # Classify archetype
    archetype = _classify_archetype(text)
    logger.info(f'Classified archetype: {archetype}')

    # Extract sections for LLM analysis
    analysis_sections = _extract_analysis_sections(text)

    # Send to Sonnet for analysis
    combined_text = '\n\n---\n\n'.join(
        f'[{name}]\n{content}' for name, content in analysis_sections.items()
    )
    company_info = f'{issuer_name} ({ticker})' if issuer_name else f'CIK {cik}'

    prompt = ANALYSIS_PROMPT.format(
        company_info=company_info,
        note_headings=json.dumps(note_headings[:20]),
        section_text=combined_text[:12000],
    )

    try:
        response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=2048,
            messages=[{'role': 'user', 'content': prompt}],
        )
        raw = response.content[0].text
        log_llm_usage(
            LLM_LOG, issuer_name or cik, 'bootstrap', SONNET_MODEL,
            response.usage.input_tokens, response.usage.output_tokens,
            (response.usage.input_tokens * 3.0 + response.usage.output_tokens * 15.0) / 1_000_000,
        )

        # Parse analysis
        raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
        raw = re.sub(r'\s*```$', '', raw)
        analysis = json.loads(raw)
    except Exception as e:
        logger.warning(f'LLM analysis failed: {e}. Using archetype defaults only.')
        analysis = {'key_fields': [], 'unusual_features': []}

    # Generate YAML config
    config = _build_config_yaml(
        cik=cik,
        ticker=ticker,
        issuer_name=issuer_name,
        archetype=archetype,
        analysis=analysis,
    )

    # Write to profiles/
    filename = ticker.lower() if ticker else cik.lstrip('0')
    config_path = PROFILES_DIR / f'{filename}.yaml'
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(config)

    logger.info(f'Wrote draft config to {config_path}')
    return config_path


def _build_config_yaml(cik, ticker, issuer_name, archetype, analysis):
    """Build YAML config string from analysis results."""
    lines = [
        f'issuer: {issuer_name or "TODO: Company Name"}',
        f'ticker: {ticker or "TODO"}',
        f'cik: "{cik}"',
        f'archetype: {archetype}',
        f'sector: TODO  # e.g., technology, industrials, financials',
        f'extraction_mode: llm',
        '',
    ]

    # Add extra fields from LLM analysis
    # Map LLM-invented section names to archetype sections
    VALID_SECTIONS = {'financial_instruments', 'market_risk', 'derivatives_note'}
    extra_fields = analysis.get('key_fields', [])
    if extra_fields:
        lines.append('# Additional fields identified by LLM analysis (review and adjust)')
        lines.append('fields:')
        for fld in extra_fields:
            name = fld.get('name', 'unknown_field').replace(' ', '_').lower()
            desc = fld.get('description', 'TODO')
            section = fld.get('section', 'market_risk')
            # Remap non-standard section names to valid archetype sections
            if section not in VALID_SECTIONS:
                section = 'market_risk'
            lines.append(f'  {name}:')
            lines.append(f'    description: "{desc}"')
            lines.append(f'    section: {section}')
        lines.append('')

    # Add notes about unusual features
    unusual = analysis.get('unusual_features', [])
    if unusual:
        lines.append('# Unusual features identified:')
        for feat in unusual:
            lines.append(f'#   - {feat}')
        lines.append('')

    return '\n'.join(lines) + '\n'


def bootstrap_issuer_for_activation(
    cik: str,
    ticker: str,
    issuer_name: str,
    filing_text: str,
    client: anthropic.Anthropic | None = None,
) -> dict:
    """Activation-mode bootstrap: generate config from pre-fetched filing text.

    Unlike bootstrap_issuer(), this accepts filing_text directly and returns
    a metadata dict with confidence signals instead of just a Path.

    Returns dict with: config_path, archetype, archetype_confidence,
    note_headings_found, sections_found, llm_analysis, llm_analysis_failed, warnings
    """
    if client is None:
        client = anthropic.Anthropic()

    warnings = []

    # Find note headings
    note_headings = _find_note_headings(filing_text)

    # Classify archetype with confidence
    archetype, archetype_confidence = _classify_archetype_with_confidence(filing_text)
    if archetype_confidence == 0.0:
        warnings.append('No archetype keyword matches; defaulting to minimal_hedger')

    # Extract sections for LLM analysis
    analysis_sections = _extract_analysis_sections(filing_text)
    sections_found = list(analysis_sections.keys())

    if not analysis_sections:
        warnings.append('No derivatives/market risk sections found in filing text')

    # Send to Sonnet for analysis
    llm_analysis_failed = False
    combined_text = '\n\n---\n\n'.join(
        f'[{name}]\n{content}' for name, content in analysis_sections.items()
    )
    company_info = f'{issuer_name} ({ticker})' if issuer_name else f'CIK {cik}'

    prompt = ANALYSIS_PROMPT.format(
        company_info=company_info,
        note_headings=json.dumps(note_headings[:20]),
        section_text=combined_text[:12000],
    )

    try:
        response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=2048,
            messages=[{'role': 'user', 'content': prompt}],
        )
        raw = response.content[0].text
        log_llm_usage(
            LLM_LOG, issuer_name or cik, 'bootstrap', SONNET_MODEL,
            response.usage.input_tokens, response.usage.output_tokens,
            (response.usage.input_tokens * 3.0 + response.usage.output_tokens * 15.0) / 1_000_000,
        )

        raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
        raw = re.sub(r'\s*```$', '', raw)
        analysis = json.loads(raw)
    except Exception as e:
        logger.warning(f'LLM analysis failed for {ticker}: {e}. Using archetype defaults only.')
        analysis = {'key_fields': [], 'unusual_features': []}
        llm_analysis_failed = True
        warnings.append(f'LLM analysis failed: {e}')

    # Generate YAML config
    config_yaml = _build_config_yaml(
        cik=cik,
        ticker=ticker,
        issuer_name=issuer_name,
        archetype=archetype,
        analysis=analysis,
    )

    # Write to profiles/
    filename = ticker.lower() if ticker else cik.lstrip('0')
    config_path = PROFILES_DIR / f'{filename}.yaml'
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(config_yaml)

    logger.info(f'Wrote activation draft config to {config_path}')

    return {
        'config_path': config_path,
        'archetype': archetype,
        'archetype_confidence': archetype_confidence,
        'note_headings_found': note_headings,
        'sections_found': sections_found,
        'llm_analysis': analysis,
        'llm_analysis_failed': llm_analysis_failed,
        'warnings': warnings,
    }


def bootstrap_batch(cik_list_path: Path, client: anthropic.Anthropic | None = None):
    """Bootstrap configs for a list of CIKs from a text file."""
    with open(cik_list_path, 'r') as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    results = []
    for line in lines:
        parts = line.split(',')
        cik = parts[0].strip()
        ticker = parts[1].strip() if len(parts) > 1 else ''
        name = parts[2].strip() if len(parts) > 2 else ''

        try:
            path = bootstrap_issuer(cik, ticker, name, client)
            results.append({'cik': cik, 'status': 'ok', 'path': str(path)})
        except Exception as e:
            logger.error(f'Failed to bootstrap CIK {cik}: {e}')
            results.append({'cik': cik, 'status': 'error', 'error': str(e)})

    return results
