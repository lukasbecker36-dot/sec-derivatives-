"""Keyword-based qualitative sentence extraction — no LLM needed."""

import re
from .config import IssuerConfig
from .utils import extract_sentences, normalise_for_comparison


def extract_qualitative(
    sections: dict[str, str],
    config: IssuerConfig,
    prior_row: dict | None = None,
) -> dict[str, list[str]]:
    """Extract categorised qualitative findings from section texts.

    Args:
        sections: Dict of {section_name: extracted_text} from section_extract.
        config: Issuer configuration with qualitative categories.
        prior_row: Previous filing's row (unused here, but prior_sections used for [NEW]).

    Returns:
        Dict of {category: [sentence, ...]} with [NEW] tags.
    """
    qual_cfg = config.qualitative
    if not qual_cfg.categories:
        return {}

    # Combine text from configured sections
    combined_parts = []
    for sec_name in qual_cfg.sections_to_search:
        if sec_name in sections and sections[sec_name]:
            combined_parts.append(sections[sec_name])
    combined = ' '.join(combined_parts)

    if not combined.strip():
        return {}

    sentences = extract_sentences(combined)

    # Build normalised set of prior sentences for [NEW] detection
    # We store the prior combined text in the notes file, but for simplicity
    # we pass prior_sections if available
    prior_normalised = set()
    # Prior sentences would come from a stored combined text — for now,
    # we just tag based on whether the raw sentence appeared before.
    # TODO: In a future enhancement, store normalised prior text.

    findings = {}
    for category, patterns in qual_cfg.categories.items():
        cat_finds = []
        seen = set()
        for sent in sentences:
            for pat in patterns:
                try:
                    if re.search(pat, sent, re.I):
                        # De-duplicate by first 80 chars
                        short = sent[:80]
                        if short not in seen:
                            seen.add(short)
                            cat_finds.append(sent)
                        break
                except re.error:
                    # Skip invalid regex patterns
                    continue
        if cat_finds:
            findings[category] = cat_finds

    return findings


def tag_new_sentences(
    current_findings: dict[str, list[str]],
    prior_combined_text: str,
) -> dict[str, list[str]]:
    """Add [NEW] tags to sentences not present in prior period.

    Uses normalised comparison so date/amount changes don't trigger false [NEW].
    """
    if not prior_combined_text:
        return current_findings

    prior_sents = extract_sentences(prior_combined_text)
    prior_normalised = {normalise_for_comparison(s) for s in prior_sents}

    tagged = {}
    for category, items in current_findings.items():
        tagged_items = []
        for sent in items:
            normalised = normalise_for_comparison(sent)
            if normalised not in prior_normalised:
                tagged_items.append(f'[NEW] {sent}')
            else:
                tagged_items.append(sent)
        tagged[category] = tagged_items

    return tagged
