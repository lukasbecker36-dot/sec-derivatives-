"""Stage 1: Deterministic section slicing from filing text using regex."""

import re
from .config import SectionConfig, IssuerConfig

# Expanded cross-reference pattern
XREF_PATTERN = re.compile(
    r',\s*Note\s+\d+'
    r'|(?:in |to )(?:the |our )?(?:accompanying )?notes(?:\s+to)?'
    r'|for (?:disclosures|further (?:details|information)|a discussion)'
    r'|(?:see|refer\s+to)\s+Note\s+\d+'
    r'|(?:included|discussed|described)\s+in\s+(?:the\s+)?(?:notes|Note)'
    r'|for (?:additional|more) (?:information|detail)',
    re.IGNORECASE,
)


def is_likely_cross_reference(text: str, min_length: int = 300) -> bool:
    """Check if extracted section text is just a cross-reference stub.

    Returns True if the text is short AND contains cross-reference patterns.
    """
    if len(text.strip()) >= min_length:
        return False
    return bool(XREF_PATTERN.search(text))


def extract_section(text: str, section_cfg: SectionConfig,
                    reject_stubs: bool = False) -> str:
    """Extract a single section from filing text using heading regex.

    Args:
        text: Full cleaned filing text.
        section_cfg: Section configuration with heading pattern, strategy, etc.

    Returns:
        Extracted section text, or empty string if not found.
    """
    if not section_cfg.heading:
        return ''

    # Convert literal spaces in heading to \s+ so patterns match tabs, line
    # breaks, and OCR artifacts (e.g. "Qualitat ive") in filing text.
    heading_pat = re.sub(r' +', r'\\s+', section_cfg.heading)

    # Also build an OCR-tolerant pattern (allow optional whitespace within
    # words to handle mid-word splits from HTML extraction)
    words = section_cfg.heading.split()
    if all(w.isalpha() for w in words):
        fuzzy_words = [r'\s*'.join(w) for w in words]
        fuzzy_pat = r'\s+'.join(fuzzy_words)
    else:
        fuzzy_pat = heading_pat  # keep original for regex-heavy headings

    # Try both patterns and combine matches
    raw_matches = list(re.finditer(heading_pat, text, re.IGNORECASE))
    if fuzzy_pat != heading_pat:
        fuzzy_matches = list(re.finditer(fuzzy_pat, text, re.IGNORECASE))
        seen_positions = {m.start() for m in raw_matches}
        for m in fuzzy_matches:
            if m.start() not in seen_positions:
                raw_matches.append(m)
        raw_matches.sort(key=lambda m: m.start())

    if not raw_matches:
        return ''

    # Filter out cross-references and table-of-contents entries
    matches = []
    for m in raw_matches:
        after = text[m.end():m.end() + 100]
        after_stripped = text[m.end():m.end() + 20].strip()
        # Skip ToC entries (heading followed immediately by a page number)
        if re.match(r'^\d{1,3}\s', after_stripped):
            continue
        if not XREF_PATTERN.search(after):
            matches.append(m)

    # Fall back to all matches if filtering removed everything
    if not matches:
        matches = raw_matches

    # Pick match based on strategy
    if section_cfg.match_strategy == 'first':
        match = matches[0]
    else:  # 'last' is default
        match = matches[-1]

    start = match.start()

    # Find end boundary
    end = start + section_cfg.max_length
    if section_cfg.end_boundary:
        # Search for end boundary starting after the heading match
        end_match = re.search(
            section_cfg.end_boundary,
            text[match.end():],
            re.IGNORECASE,
        )
        if end_match:
            end = match.end() + end_match.start()

    section_text = text[start:end]

    # Validate — check that required keywords are present
    if section_cfg.validation_keywords:
        found = sum(
            1 for kw in section_cfg.validation_keywords
            if kw.lower() in section_text.lower()
        )
        if found == 0:
            return ''  # No keywords found — likely wrong section

    # Optional stub rejection for activation mode
    if reject_stubs and is_likely_cross_reference(section_text):
        return ''

    return section_text


# Anchor phrases that reliably mark the derivatives notional table. Require a
# derivative-context word nearby so we don't latch onto unrelated "notional"
# mentions (e.g. a passing reference in a debt footnote).
_NOTIONAL_ANCHOR = re.compile(
    r'(?:'
    r'notional\s+amounts?\s+of'                          # "notional amounts of our derivatives"
    r'|(?:aggregate|total|outstanding|combined)\s+notional'  # qualified notional
    r'|notional\s+amounts?\b'                            # bare "notional amount(s)"
    r'|hedging\s+activities\W{0,30}we\s+had'            # GD-style "Hedging Activities. We had"
    r'|\bnotional\b'                                    # bare "notional" (guarded by _DERIV_CONTEXT)
    r')',
    re.IGNORECASE,
)
_DERIV_CONTEXT = re.compile(
    r'derivative|forward\s+contract|interest\s+rate\s+swap|cross-currency|'
    r'currency\s+(?:forward|contract|swap)|hedg|commodity\s+contract|'
    r'swap\s+agreement|rate\s+swap|forward\s+exchange|'
    r'credit\s+default\s+swap|CDS|strike\s+price|collar',
    re.IGNORECASE,
)


def extract_derivatives_by_content(text: str, max_length: int = 10000,
                                   lookback: int = 600) -> str:
    """Locate the derivatives notional disclosure by content, not heading.

    Fallback for filers whose derivatives note heading doesn't match the
    configured pattern (e.g. "5. Derivative Instruments", bare tables, or
    all-caps variants). Finds the first 'notional amount(s) of ...' anchor that
    has derivative context nearby and returns a window around it.

    Returns '' if no confident anchor is found.
    """
    for m in _NOTIONAL_ANCHOR.finditer(text):
        pos = m.start()
        # Require derivative-context within a window around the anchor
        ctx = text[max(0, pos - 200):pos + 300]
        if _DERIV_CONTEXT.search(ctx):
            start = max(0, pos - lookback)
            return text[start:start + max_length]
    return ''


def extract_all_sections(text: str, config: IssuerConfig) -> dict[str, str]:
    """Extract all configured sections from filing text.

    Returns:
        Dict of {section_name: extracted_text}
    """
    sections = {}
    for name, section_cfg in config.sections.items():
        sections[name] = extract_section(text, section_cfg)

    # Content fallback: if the derivatives_note section is missing or was found
    # by heading but contains no notional data (heading matched a stub/cross-ref),
    # try to locate the notional table by content instead.
    if 'derivatives_note' in sections:
        dn = sections['derivatives_note']
        if not dn or 'notional' not in dn.lower():
            cfg = config.sections['derivatives_note']
            fallback = extract_derivatives_by_content(text, max_length=cfg.max_length)
            if fallback and 'notional' in fallback.lower():
                sections['derivatives_note'] = fallback

    return sections

