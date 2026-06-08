"""Tests for src.section_extract — regex-based section slicing."""

import pytest
from src.config import SectionConfig, IssuerConfig
from src.section_extract import (
    extract_section, extract_all_sections, extract_derivatives_by_content,
)


SAMPLE_FILING = """
Some preamble text about the company.

Note 5 - Financial Instruments

This note discusses cash equivalents and fair value of securities.
Total cash equivalents were $5,000 million. The fair value of marketable
securities was $10,000 million.

Note 6 - Derivative Financial Instruments

The Company uses derivative instruments to manage exposures to foreign
currency exchange and commodity price risks. The notional amount of
outstanding derivative instruments was $15,000 million. Designated hedge
instruments had fair value assets of $200 million.

Note 7 - Fair Value Measurements

This note discusses fair value hierarchy and measurements.

Item 3. Quantitative and Qualitative Disclosures About Market Risk

We are exposed to market risk from changes in foreign currency exchange
rates and interest rates. A hypothetical 10% decrease in foreign currency
rates would result in a $500 million decline. A 100 basis point increase
would change fair value by $300 million. Sensitivity analysis shows
moderate exposure.

Item 4. Controls and Procedures
"""


class TestExtractSection:
    def test_derivatives_note_last_match(self):
        cfg = SectionConfig(
            heading=r'Note\s+\d+\s*[-–—.]\s*Derivative Financial Instruments',
            match_strategy='last',
            validation_keywords=['notional', 'hedge', 'derivative'],
            end_boundary=r'Note\s+\d+\s*[-–—.]\s*(?!Derivative)',
            max_length=10000,
        )
        result = extract_section(SAMPLE_FILING, cfg)
        assert 'notional amount' in result.lower()
        assert 'Fair Value Measurements' not in result

    def test_market_risk(self):
        cfg = SectionConfig(
            heading='Quantitative and Qualitative Disclosures About Market Risk',
            match_strategy='last',
            end_boundary=r'Item\s*[\s]*[489]',
            max_length=8000,
        )
        result = extract_section(SAMPLE_FILING, cfg)
        assert 'foreign currency' in result.lower()
        assert 'Controls and Procedures' not in result

    def test_financial_instruments_first_match(self):
        cfg = SectionConfig(
            heading=r'Note\s+\d+\s*[-–—.]\s*Financial Instruments',
            match_strategy='first',
            validation_keywords=['fair value', 'securities', 'cash'],
            end_boundary=r'Note\s+\d+\s*[-–—.]\s*(?!Financial Instruments)',
            max_length=10000,
        )
        result = extract_section(SAMPLE_FILING, cfg)
        assert 'cash equivalents' in result.lower()

    def test_no_match_returns_empty(self):
        cfg = SectionConfig(
            heading='NONEXISTENT SECTION HEADING',
            max_length=5000,
        )
        assert extract_section(SAMPLE_FILING, cfg) == ''

    def test_validation_keywords_filter(self):
        cfg = SectionConfig(
            heading=r'Note\s+\d+\s*[-–—.]\s*Financial Instruments',
            match_strategy='first',
            validation_keywords=['ZZZNONEXISTENT'],
            max_length=10000,
        )
        # No keywords match -> returns empty
        assert extract_section(SAMPLE_FILING, cfg) == ''

    def test_max_length_truncates(self):
        cfg = SectionConfig(
            heading='Quantitative and Qualitative Disclosures About Market Risk',
            match_strategy='last',
            max_length=50,
        )
        result = extract_section(SAMPLE_FILING, cfg)
        assert len(result) <= 50


class TestDerivativesContentFallback:
    """Content-anchored fallback for diversely-titled derivatives notes."""

    def test_finds_notional_without_note_heading(self):
        # Heading is "5. Derivative Instruments" — no "Note" prefix, so the
        # standard heading regex misses it, but content anchoring should catch it.
        text = (
            'Some unrelated preamble about revenue recognition policies. '
            '5. Derivative Instruments The Company uses forward contracts. '
            'The notional amounts of our outstanding derivative instruments '
            'were $4,237 million for purchased forwards and $1,200 million sold.'
        )
        result = extract_derivatives_by_content(text, max_length=5000)
        assert 'notional amounts of our outstanding derivative' in result
        assert '4,237' in result

    def test_ignores_notional_without_derivative_context(self):
        # "notional" appearing in an unrelated debt context must not match.
        text = (
            'The bonds were issued at a notional amount of $500 million '
            'and mature in 2030. Interest is payable semi-annually.'
        )
        assert extract_derivatives_by_content(text) == ''

    def test_returns_empty_when_no_notional(self):
        text = 'This filing discusses only revenue and operating expenses.'
        assert extract_derivatives_by_content(text) == ''

    def test_fallback_wired_into_extract_all_sections(self):
        text = (
            'Item 1. Financial Statements. '
            '5. Derivative Instruments and Hedging. '
            'The aggregate notional of our foreign currency forward contracts '
            'was $9,500 million as of the period end.'
        )
        config = IssuerConfig(
            issuer='Test', ticker='TST', cik='1', archetype='minimal_hedger',
            sections={
                'derivatives_note': SectionConfig(
                    heading=r'Note\s+\d+\s*[-–—.]\s*Derivatives',  # won't match
                    match_strategy='last',
                    max_length=8000,
                ),
            },
        )
        sections = extract_all_sections(text, config)
        assert sections['derivatives_note']
        assert '9,500' in sections['derivatives_note']
