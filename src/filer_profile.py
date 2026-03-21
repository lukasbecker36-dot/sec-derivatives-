"""Per-CIK filer profile — persistent memory of company-specific reporting patterns."""

import csv
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

FILER_PROFILES_DIR = Path(__file__).resolve().parent.parent / 'filer_profiles'

# Patterns for detecting hedging/derivatives language
HEDGING_PHRASES = [
    r'currency headwinds?',
    r'net investment hedg(?:e|es|ing)',
    r'cash flow hedg(?:e|es|ing)',
    r'fair value hedg(?:e|es|ing)',
    r'economic hedg(?:e|es|ing)',
    r'natural hedg(?:e|es|ing)',
    r'cross[- ]currency',
    r'interest rate swap',
    r'foreign exchange forward',
    r'commodity (?:derivative|contract|swap)',
    r'notional (?:amount|value)',
    r'mark[- ]to[- ]market',
    r'unrealized (?:gain|loss)',
    r'hedge ineffectiveness',
    r'designated as (?:a )?hedging',
    r'de-designated',
    r'AOCI',
    r'accumulated other comprehensive',
]

NON_GAAP_PATTERNS = [
    r'adjusted (?:EBITDA|earnings|EPS|operating income)',
    r'non[- ]GAAP',
    r'organic (?:revenue|growth|sales)',
    r'constant[- ]currency',
    r'free cash flow',
    r'core (?:earnings|revenue)',
]


def _pad_cik(cik: str) -> str:
    return cik.lstrip('0').zfill(10)


def _profile_path(cik: str) -> Path:
    return FILER_PROFILES_DIR / f'{_pad_cik(cik)}.json'


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _empty_profile(cik: str, ticker: str = '', issuer_name: str = '') -> dict:
    return {
        'cik': _pad_cik(cik),
        'ticker': ticker,
        'issuer_name': issuer_name,
        'aliases': [],
        'document_structure': {
            'derivatives_note_heading': '',
            'market_risk_heading': '',
            'section_locations': {},
        },
        'filing_patterns': {
            'typical_note_numbering': '',
            'heading_variations_seen': [],
            'uses_cross_reference_in_10q': False,
            'market_risk_in_10k_only': False,
        },
        'idiosyncrasies': {
            'recurring_phrases': [],
            'non_gaap_metrics': [],
            'unusual_disclosure_patterns': [],
            'known_issues': [],
        },
        'history': [],
    }


def load_profile(cik: str) -> dict | None:
    """Load profile JSON for a CIK. Returns None if no profile exists."""
    path = _profile_path(cik)
    if not path.exists():
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_profile(profile: dict) -> None:
    """Write profile JSON atomically."""
    cik = profile.get('cik', '')
    path = _profile_path(cik)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(profile, f, indent=2, ensure_ascii=False)
            f.write('\n')
        os.replace(tmp, str(path))
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def create_initial_profile(cik: str, ticker: str = '', issuer_name: str = '') -> dict:
    """Create a new empty profile with default structure."""
    return _empty_profile(cik, ticker, issuer_name)


def get_or_create_profile(cik: str, ticker: str = '', issuer_name: str = '') -> dict:
    """Load existing profile or create a new one."""
    profile = load_profile(cik)
    if profile is None:
        profile = create_initial_profile(cik, ticker, issuer_name)
    return profile


def extract_structural_features(filing_text: str, sections: dict, config) -> dict:
    """Extract document_structure and filing_patterns from a processed filing.

    Args:
        filing_text: Full cleaned filing text.
        sections: Dict of {section_name: extracted_text} from section extraction.
        config: IssuerConfig with section heading patterns.

    Returns dict with 'document_structure' and 'filing_patterns' keys.
    """
    doc_structure = {
        'derivatives_note_heading': '',
        'market_risk_heading': '',
        'section_locations': {},
    }
    filing_patterns = {
        'heading_variations_seen': [],
        'uses_cross_reference_in_10q': False,
    }

    # Find actual note headings that matched
    note_heading_pat = r'(Note\s+\d+\s*[.\u2013\u2014\u2015\u2012-]\s*[A-Z][^\n]{5,80})'
    heading_matches = re.findall(note_heading_pat, filing_text[:50000])

    for section_name, section_text in sections.items():
        if not section_text:
            continue
        # Find the heading at the start of extracted section
        for heading in heading_matches:
            if heading in section_text[:200]:
                doc_structure['section_locations'][section_name] = heading
                filing_patterns['heading_variations_seen'].append(heading)
                if 'derivative' in heading.lower() or 'financial instrument' in heading.lower():
                    doc_structure['derivatives_note_heading'] = heading
                if 'market risk' in heading.lower():
                    doc_structure['market_risk_heading'] = heading
                break

    # Check for cross-reference patterns in short sections
    from .section_extract import XREF_PATTERN
    for section_name, section_text in sections.items():
        if section_text and len(section_text.strip()) < 300 and XREF_PATTERN.search(section_text):
            filing_patterns['uses_cross_reference_in_10q'] = True

    return {
        'document_structure': doc_structure,
        'filing_patterns': filing_patterns,
    }


def extract_language_patterns(filing_text: str, sections: dict, llm_results: dict) -> dict:
    """Extract idiosyncrasies from filing text and LLM results.

    Returns dict with 'idiosyncrasies' key containing recurring_phrases and non_gaap_metrics.
    """
    # Search sections text for hedging phrases
    combined_text = ' '.join(text for text in sections.values() if text)
    recurring = []
    for pat in HEDGING_PHRASES:
        matches = re.findall(pat, combined_text, re.I)
        if matches:
            # Normalize to lowercase
            phrase = matches[0].lower().strip()
            if phrase not in recurring:
                recurring.append(phrase)

    # Search for non-GAAP metrics
    non_gaap = []
    for pat in NON_GAAP_PATTERNS:
        matches = re.findall(pat, combined_text, re.I)
        if matches:
            metric = matches[0].lower().strip()
            if metric not in non_gaap:
                non_gaap.append(metric)

    # Check LLM notes for unusual patterns
    unusual = []
    for section_name, result in llm_results.items():
        notes = result.get('notes', '')
        if notes and 'unusual' in notes.lower():
            unusual.append(f'{section_name}: {notes[:200]}')

    return {
        'idiosyncrasies': {
            'recurring_phrases': recurring,
            'non_gaap_metrics': non_gaap,
            'unusual_disclosure_patterns': unusual,
        },
    }


def _merge_list_dedup(existing: list, new_items: list) -> list:
    """Merge two lists, deduplicating by lowercase comparison."""
    seen = {str(item).lower() for item in existing}
    result = list(existing)
    for item in new_items:
        if str(item).lower() not in seen:
            result.append(item)
            seen.add(str(item).lower())
    return result


def update_profile_after_extraction(
    profile: dict,
    filing_meta: dict,
    filing_text: str,
    sections: dict,
    llm_results: dict,
    config,
) -> dict:
    """Update profile with observations from a processed filing. Idempotent."""
    period_end = filing_meta.get('period_end', '')

    # Check if this period is already in history (idempotency)
    existing_periods = {h.get('period_end') for h in profile.get('history', [])}
    if period_end in existing_periods:
        return profile

    # Extract features
    structural = extract_structural_features(filing_text, sections, config)
    language = extract_language_patterns(filing_text, sections, llm_results)

    # Merge document_structure
    ds = profile.setdefault('document_structure', {})
    new_ds = structural['document_structure']
    if new_ds.get('derivatives_note_heading'):
        ds['derivatives_note_heading'] = new_ds['derivatives_note_heading']
    if new_ds.get('market_risk_heading'):
        ds['market_risk_heading'] = new_ds['market_risk_heading']
    existing_locs = ds.setdefault('section_locations', {})
    existing_locs.update(new_ds.get('section_locations', {}))

    # Merge filing_patterns
    fp = profile.setdefault('filing_patterns', {})
    new_fp = structural['filing_patterns']
    existing_headings = fp.setdefault('heading_variations_seen', [])
    fp['heading_variations_seen'] = _merge_list_dedup(
        existing_headings, new_fp.get('heading_variations_seen', [])
    )
    if new_fp.get('uses_cross_reference_in_10q'):
        fp['uses_cross_reference_in_10q'] = True

    # Merge idiosyncrasies
    idio = profile.setdefault('idiosyncrasies', {})
    new_idio = language['idiosyncrasies']
    idio['recurring_phrases'] = _merge_list_dedup(
        idio.get('recurring_phrases', []), new_idio.get('recurring_phrases', [])
    )
    idio['non_gaap_metrics'] = _merge_list_dedup(
        idio.get('non_gaap_metrics', []), new_idio.get('non_gaap_metrics', [])
    )
    idio['unusual_disclosure_patterns'] = _merge_list_dedup(
        idio.get('unusual_disclosure_patterns', []),
        new_idio.get('unusual_disclosure_patterns', [])
    )

    # Append to history
    fields_extracted = 0
    fields_null = 0
    for section_result in llm_results.values():
        for field_data in section_result.get('fields', {}).values():
            if field_data.get('value') is not None:
                fields_extracted += 1
            else:
                fields_null += 1

    history = profile.setdefault('history', [])
    history.append({
        'filing_date': filing_meta.get('period_end', ''),
        'form_type': filing_meta.get('form_type', ''),
        'period_end': period_end,
        'processed_at': _now_iso(),
        'sections_found': [s for s, t in sections.items() if t],
        'fields_extracted': fields_extracted,
        'fields_null': fields_null,
    })

    return profile


def build_prompt_context(profile: dict | None) -> str:
    """Format profile data as a prompt context block for the LLM.

    Returns empty string if profile is None or has no useful data.
    """
    if not profile:
        return ''

    lines = []
    issuer = profile.get('issuer_name') or profile.get('ticker') or 'this company'

    # Heading info
    ds = profile.get('document_structure', {})
    if ds.get('derivatives_note_heading'):
        lines.append(f'- Derivatives note typically under: {ds["derivatives_note_heading"]}')
    if ds.get('market_risk_heading'):
        lines.append(f'- Market risk section: {ds["market_risk_heading"]}')

    # Recurring phrases
    idio = profile.get('idiosyncrasies', {})
    phrases = idio.get('recurring_phrases', [])
    if phrases:
        lines.append(f'- Recurring terms: {", ".join(phrases[:10])}')

    # Non-GAAP metrics
    non_gaap = idio.get('non_gaap_metrics', [])
    if non_gaap:
        lines.append(f'- Non-GAAP metrics used: {", ".join(non_gaap[:5])}')

    # Filing patterns
    fp = profile.get('filing_patterns', {})
    if fp.get('uses_cross_reference_in_10q'):
        lines.append('- Note: This company uses cross-references in 10-Qs for some sections')
    if fp.get('market_risk_in_10k_only'):
        lines.append('- Note: Market risk sensitivity data only available in 10-Ks')

    # Known issues
    issues = idio.get('known_issues', [])
    if issues:
        for issue in issues[:3]:
            lines.append(f'- Known issue: {issue}')

    if not lines:
        return ''

    return f'Known company-specific patterns for {issuer}:\n' + '\n'.join(lines)


def resolve_alias(ticker: str, profiles_dir: Path = FILER_PROFILES_DIR) -> str | None:
    """Check if ticker is an alias in any profile. Returns the canonical CIK if found."""
    if not profiles_dir.exists():
        return None
    for json_path in profiles_dir.glob('*.json'):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                profile = json.load(f)
            aliases = profile.get('aliases', [])
            if ticker.upper() in [a.upper() for a in aliases]:
                return profile.get('cik')
            if profile.get('ticker', '').upper() == ticker.upper():
                return profile.get('cik')
        except Exception:
            continue
    return None


def seed_existing_profiles(profiles_dir: Path, output_dir: Path) -> int:
    """Backfill filer profiles for existing active issuers.

    Reads YAML configs from profiles_dir and tracking CSVs from output_dir
    to build initial profiles with history and structural info.

    Returns count of profiles created.
    """
    archetypes_dir = profiles_dir / '_archetypes'
    count = 0

    for yaml_path in sorted(profiles_dir.glob('*.yaml')):
        if yaml_path.parent == archetypes_dir:
            continue
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)

            cik = str(cfg.get('cik', '')).lstrip('0')
            ticker = cfg.get('ticker', yaml_path.stem.upper())
            issuer_name = cfg.get('issuer', '')

            if not cik:
                continue

            # Don't overwrite existing profiles
            if load_profile(cik) is not None:
                continue

            profile = create_initial_profile(cik, ticker, issuer_name)

            # Populate document_structure from YAML sections
            sections = cfg.get('sections', {})
            for section_name, section_cfg in sections.items():
                heading = section_cfg.get('heading', '')
                if heading:
                    profile['document_structure']['section_locations'][section_name] = heading
                    if 'derivative' in section_name.lower() or 'instrument' in section_name.lower():
                        profile['document_structure']['derivatives_note_heading'] = heading
                    if 'market_risk' in section_name.lower():
                        profile['document_structure']['market_risk_heading'] = heading

            # Populate history from tracking.csv
            tracking_csv = output_dir / ticker.lower() / 'tracking.csv'
            if tracking_csv.exists():
                with open(tracking_csv, 'r', encoding='utf-8', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Count non-empty fields
                        fields_extracted = 0
                        fields_null = 0
                        for key, val in row.items():
                            if key in ('period_end_date', 'form_type'):
                                continue
                            if val and val.strip():
                                fields_extracted += 1
                            else:
                                fields_null += 1

                        profile['history'].append({
                            'filing_date': row.get('period_end_date', ''),
                            'form_type': row.get('form_type', ''),
                            'period_end': row.get('period_end_date', ''),
                            'processed_at': '',
                            'sections_found': list(sections.keys()),
                            'fields_extracted': fields_extracted,
                            'fields_null': fields_null,
                        })

            save_profile(profile)
            count += 1
            logger.info(f'Seeded profile for {ticker} (CIK {cik})')

        except Exception as e:
            logger.warning(f'Failed to seed profile from {yaml_path}: {e}')

    return count
