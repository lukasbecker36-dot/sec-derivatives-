"""Tests for src.engine -- full pipeline with mocked dependencies."""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.config import load_config
from src.engine import process_filing, _build_schema_for_section


class TestBuildSchema:
    def test_filters_by_section(self):
        meta_path = Path(__file__).resolve().parent.parent / 'profiles' / 'meta.yaml'
        if not meta_path.exists():
            pytest.skip('meta.yaml not found')
        config = load_config(meta_path)
        schema = _build_schema_for_section(config, 'market_risk')
        # Should only have market_risk fields
        for name in schema:
            assert config.fields[name].section == 'market_risk'


class TestProcessFiling:
    @patch('src.engine.extract_fields_llm')
    def test_basic_pipeline(self, mock_llm):
        meta_path = Path(__file__).resolve().parent.parent / 'profiles' / 'meta.yaml'
        if not meta_path.exists():
            pytest.skip('meta.yaml not found')
        config = load_config(meta_path)

        mock_llm.return_value = {
            'fields': {
                'has_derivatives': {'value': 'No', 'confidence': 'high', 'source_quote': 'test'},
                'ir_sensitivity_100bp': {'value': 300, 'confidence': 'high', 'source_quote': 'test'},
            },
            'flags': [],
            'notes': '',
        }

        filing_meta = {'period_end': '2025-03-31', 'form_type': '10-Q'}
        filing_text = """
        Note 5 - Financial Instruments
        Cash equivalents fair value was $5,000 million. Securities fair value $10,000.
        Note 6 - Something Else
        Item 3. Quantitative and Qualitative Disclosures About Market Risk
        Interest rate sensitivity of 100 basis points is $300 million.
        Item 4. Controls
        """

        result = process_filing(config, filing_meta, filing_text)
        assert result['row']['period_end_date'] == '2025-03-31'
        assert 'alerts' in result
        assert 'notes' in result
        assert 'validation' in result
