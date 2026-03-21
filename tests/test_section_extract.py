"""Tests for src.section_extract — regex-based section slicing."""

import pytest
from src.config import SectionConfig, IssuerConfig
from src.section_extract import extract_section, extract_all_sections


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
