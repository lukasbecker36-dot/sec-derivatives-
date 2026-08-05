"""Section location with scoring and LLM-assisted fallback.

Wraps the deterministic regex slicing in section_extract.py with:
  1. a quality verdict per section (ok / empty / stub / keyword_miss), and
  2. locate-request payloads for an LLM to find sections the regex missed,
     plus appliers that turn locate results back into sliced text and a
     learned heading regex persisted to the issuer's YAML config.

Used by the backfill pipeline; designed to be reusable as a daily-ingest
fallback later.
"""

import logging
import re
from pathlib import Path

import yaml

from .config import IssuerConfig, SectionConfig
from .section_extract import extract_all_sections, is_likely_cross_reference

logger = logging.getLogger(__name__)

MIN_SECTION_LENGTH = 300

# Keyword anchors used to pick candidate windows shown to the locate LLM.
# Falls back to the section's validation_keywords when not listed here.
_SECTION_ANCHOR_KEYWORDS = {
    'derivatives_note': ['notional', 'derivative instruments', 'hedging activities',
                         'cash flow hedge', 'forward contracts'],
    'market_risk': ['market risk', 'sensitivity analysis', 'hypothetical',
                    'value at risk', 'interest rate risk'],
    'financial_instruments': ['fair value', 'marketable securities',
                              'fair value hierarchy', 'level 2'],
}

LOCATE_PROMPT_TEMPLATE = """You are locating a section inside an SEC {form_type} filing for {issuer}.

Target section: "{section_name}"
Purpose: this section should contain the data needed for these fields:
{field_list}

The filing's note headings (extracted automatically, may be incomplete):
{note_headings}

Below are text windows from the filing around candidate keyword anchors.
Each window is labelled [WINDOW n].

{windows}

Decide where the target section is in this filing.

Respond with ONLY a JSON object, no markdown fences:
- If you can locate the section:
  {{"found": true,
    "heading_text": "<the exact heading text as it appears in the filing>",
    "start_anchor": "<a short UNIQUE verbatim substring (20-60 chars) marking where the section starts>",
    "end_anchor": "<a short verbatim substring marking where the section ends (e.g. the next note heading), or null to take a fixed window>"}}
- If the filing genuinely does not contain this section (e.g. it cross-references
  a prior 10-K, or the company has no such disclosure):
  {{"found": false, "reason": "not_disclosed", "note": "<one-sentence explanation>"}}
- If you cannot tell from the windows provided:
  {{"found": false, "reason": "insufficient_context", "note": "<what you would need>"}}

The anchors must be verbatim substrings of the filing text shown or implied by
the windows — they are used for literal string search."""


def score_section(text: str, section_cfg: SectionConfig) -> str:
    """Return a verdict for a regex-sliced section: ok / empty / stub / keyword_miss."""
    if not text or not text.strip():
        return 'empty'
    if is_likely_cross_reference(text):
        return 'stub'
    if len(text.strip()) < MIN_SECTION_LENGTH:
        return 'stub'
    if section_cfg.validation_keywords:
        found = sum(1 for kw in section_cfg.validation_keywords
                    if kw.lower() in text.lower())
        if found == 0:
            return 'keyword_miss'
    return 'ok'


def assess_sections(filing_text: str, config: IssuerConfig) -> dict[str, dict]:
    """Run regex extraction and score every field-bearing section.

    Returns {section_name: {'status': verdict, 'text': sliced_text}}.
    Sections with no fields mapped to them are skipped.
    """
    field_sections = {f.section for f in config.fields.values()}
    sections = extract_all_sections(filing_text, config)
    out = {}
    for name, cfg in config.sections.items():
        if name not in field_sections:
            continue
        text = sections.get(name, '')
        out[name] = {'status': score_section(text, cfg), 'text': text}
    return out


def _candidate_windows(filing_text: str, section_name: str,
                       section_cfg: SectionConfig,
                       window: int = 1500, max_windows: int = 6) -> list[str]:
    """Pull text windows around keyword anchors for the locate prompt."""
    keywords = _SECTION_ANCHOR_KEYWORDS.get(section_name) or section_cfg.validation_keywords
    if not keywords:
        return []

    spans = []
    for kw in keywords:
        for m in re.finditer(re.escape(kw), filing_text, re.IGNORECASE):
            spans.append((max(0, m.start() - window // 3), m.start() + window))
            if len(spans) > 40:
                break

    # Merge overlapping spans, keep the first few
    spans.sort()
    merged = []
    for start, end in spans:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
        if len(merged) >= max_windows:
            break

    return [filing_text[s:e] for s, e in merged[:max_windows]]


def build_locate_request(filing_text: str, config: IssuerConfig,
                         section_name: str, filing_meta: dict,
                         note_headings: list[str]) -> dict:
    """Build a locate request payload for Claude Code processing."""
    section_cfg = config.sections[section_name]
    fields = {name: f.description for name, f in config.fields.items()
              if f.section == section_name}
    field_list = '\n'.join(f'- {n}: {d}' for n, d in fields.items()) or '- (none listed)'

    windows = _candidate_windows(filing_text, section_name, section_cfg)
    windows_text = '\n\n'.join(f'[WINDOW {i + 1}]\n{w}' for i, w in enumerate(windows)) \
        or '(no keyword anchors found in filing)'

    prompt = LOCATE_PROMPT_TEMPLATE.format(
        form_type=filing_meta.get('form_type', '10-Q'),
        issuer=config.issuer,
        section_name=section_name,
        field_list=field_list,
        note_headings='\n'.join(f'- {h}' for h in note_headings[:30]) or '(none found)',
        windows=windows_text,
    )

    return {
        'type': 'locate',
        'ticker': config.ticker,
        'cik': config.cik,
        'issuer': config.issuer,
        'section_name': section_name,
        'period_end': filing_meta.get('period_end', ''),
        'form_type': filing_meta.get('form_type', ''),
        'accession_number': filing_meta.get('accession_number', ''),
        'prompt': prompt,
    }


def apply_locate_result(filing_text: str, locate_result: dict,
                        section_cfg: SectionConfig) -> tuple[str, str]:
    """Slice filing text using a locate result's anchors.

    Returns (section_text, status) where status is one of
    ok / not_disclosed / locate_failed.
    """
    if not locate_result.get('found'):
        reason = locate_result.get('reason', 'insufficient_context')
        return '', 'not_disclosed' if reason == 'not_disclosed' else 'locate_failed'

    start_anchor = (locate_result.get('start_anchor') or '').strip()
    if not start_anchor:
        return '', 'locate_failed'

    start = filing_text.find(start_anchor)
    if start == -1:
        # Tolerate whitespace differences between the model's quote and the text
        loose = r'\s+'.join(re.escape(w) for w in start_anchor.split())
        m = re.search(loose, filing_text)
        if not m:
            return '', 'locate_failed'
        start = m.start()

    end = start + section_cfg.max_length
    end_anchor = (locate_result.get('end_anchor') or '').strip()
    if end_anchor:
        pos = filing_text.find(end_anchor, start + len(start_anchor))
        if pos != -1:
            end = min(end, pos)

    text = filing_text[start:end]
    verdict = score_section(text, section_cfg)
    # A located section that still fails keywords is suspect; treat as failed
    return (text, 'ok') if verdict == 'ok' else ('', 'locate_failed')


def heading_to_regex(heading_text: str) -> str:
    """Generalise a verbatim heading into a reusable regex.

    'Note 12 — Derivative Financial Instruments' becomes
    'Note\\s+\\d+\\s*[.–—―‒-]?\\s*Derivative\\s+Financial\\s+Instruments'
    so the pattern survives note renumbering and punctuation drift.
    """
    text = heading_text.strip()
    m = re.match(r'^(?:Note|NOTE)\s+\d+\s*[.–—―‒:\-]*\s*(.+)$', text)
    if m:
        title = m.group(1).strip()
        title_pat = r'\s+'.join(re.escape(w) for w in title.split())
        return r'Note\s+\d+\s*[.–—―‒:\-]?\s*' + title_pat
    return r'\s+'.join(re.escape(w) for w in text.split())


def persist_learned_heading(config_path: Path, section_name: str,
                            heading_regex: str) -> bool:
    """Write a learned heading regex into the issuer's YAML config.

    Sets sections.<name>.heading in the issuer file (overriding the archetype
    via the normal deep merge). Returns True if the file was updated.
    """
    if not config_path.exists():
        return False
    with open(config_path, 'r', encoding='utf-8') as f:
        raw = yaml.safe_load(f) or {}

    sections = raw.setdefault('sections', {}) or {}
    sec = sections.setdefault(section_name, {}) or {}
    if sec.get('heading') == heading_regex:
        return False
    sec['heading'] = heading_regex
    sections[section_name] = sec
    raw['sections'] = sections

    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(raw, f, sort_keys=False, allow_unicode=True, width=120)
    logger.info(f'{config_path.name}: learned heading for {section_name}: {heading_regex}')
    return True
