"""Stage 1: Deterministic section slicing from filing text using regex."""

import re
from .config import SectionConfig, IssuerConfig


def extract_section(text: str, section_cfg: SectionConfig) -> str:
    """Extract a single section from filing text using heading regex.

    Args:
        text: Full cleaned filing text.
        section_cfg: Section configuration with heading pattern, strategy, etc.

    Returns:
        Extracted section text, or empty string if not found.
    """
    if not section_cfg.heading:
        return ''

    raw_matches = list(re.finditer(section_cfg.heading, text, re.IGNORECASE))
    if not raw_matches:
        return ''

    # Filter out cross-references ("in the notes to...", "in the consolidated...")
    XREF_PATTERN = re.compile(
        r',\s*Note\s+\d|(?:in |to )(?:the |our )?(?:notes|condensed|consolidated|accompanying)|for (?:disclosures|further details|a discussion)',
        re.IGNORECASE,
    )
    matches = []
    for m in raw_matches:
        after = text[m.end():m.end() + 100]
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
