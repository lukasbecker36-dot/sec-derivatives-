"""Stage 1: Deterministic section slicing from filing text using regex."""

import re
from .config import SectionConfig, IssuerConfig

# Expanded cross-reference pattern
XREF_PATTERN = re.compile(
    r',\s*Note\s+\d+'
    r'|(?:in |to )(?:the |our )?(?:notes|condensed|consolidated|accompanying)'
    r'|for (?:disclosures|further details|a discussion)'
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


def extract_all_sections(text: str, config: IssuerConfig) -> dict[str, str]:
    """Extract all configured sections from filing text.

    Returns:
        Dict of {section_name: extracted_text}
    """
    sections = {}
    for name, section_cfg in config.sections.items():
        sections[name] = extract_section(text, section_cfg)
    return sections
