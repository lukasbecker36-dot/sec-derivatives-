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


class TestRetryableFailure:
    @patch('src.engine.extract_fields_llm')
    def test_retryable_failure_surfaced(self, mock_llm):
        meta_path = Path(__file__).resolve().parent.parent / 'profiles' / 'meta.yaml'
        if not meta_path.exists():
            pytest.skip('meta.yaml not found')
        config = load_config(meta_path)

        mock_llm.return_value = {
            'fields': {},
            'flags': ['api_error: Error code: 529 overloaded'],
            'notes': 'LLM API error',
            'retryable': True,
        }

        filing_meta = {'period_end': '2025-03-31', 'form_type': '10-Q'}
        filing_text = """
        Item 3. Quantitative and Qualitative Disclosures About Market Risk
        Interest rate sensitivity of 100 basis points is $300 million.
        Item 4. Controls
        """
        result = process_filing(config, filing_meta, filing_text)
        assert result['retryable_failure'] is True

    @patch('src.engine.extract_fields_llm')
    def test_clean_extraction_not_retryable(self, mock_llm):
        meta_path = Path(__file__).resolve().parent.parent / 'profiles' / 'meta.yaml'
        if not meta_path.exists():
            pytest.skip('meta.yaml not found')
        config = load_config(meta_path)

        mock_llm.return_value = {
            'fields': {'ir_sensitivity_100bp': {'value': 300, 'confidence': 'high', 'source_quote': '$300 million'}},
            'flags': [],
            'notes': '',
        }
        filing_meta = {'period_end': '2025-03-31', 'form_type': '10-Q'}
        filing_text = """
        Item 3. Quantitative and Qualitative Disclosures About Market Risk
        Interest rate sensitivity of 100 basis points is $300 million.
        Item 4. Controls
        """
        result = process_filing(config, filing_meta, filing_text)
        assert result['retryable_failure'] is False


class TestAppendCsvRow:
    def _cfg(self, field_names):
        from src.config import IssuerConfig, FieldConfig
        return IssuerConfig(
            issuer='T', ticker='TST', cik='1',
            fields={n: FieldConfig(description='d', section='s') for n in field_names},
        )

    def test_creates_file_with_header(self, tmp_path):
        from src.engine import append_csv_row
        p = tmp_path / 'tracking.csv'
        append_csv_row(p, {'period_end_date': '2025-03-31', 'form_type': '10-Q', 'a': 1}, self._cfg(['a']))
        lines = p.read_text().splitlines()
        assert lines[0].startswith('period_end_date,form_type,a,')
        assert '2025-03-31' in lines[1]

    def test_plain_append_when_header_matches(self, tmp_path):
        from src.engine import append_csv_row
        import csv
        p = tmp_path / 'tracking.csv'
        cfg = self._cfg(['a', 'b'])
        append_csv_row(p, {'period_end_date': 'p1', 'form_type': '10-Q', 'a': 1, 'b': 2}, cfg)
        append_csv_row(p, {'period_end_date': 'p2', 'form_type': '10-Q', 'a': 3, 'b': 4}, cfg)
        rows = list(csv.DictReader(open(p)))
        assert len(rows) == 2
        assert rows[0]['a'] == '1' and rows[1]['b'] == '4'

    def test_migrates_when_columns_added(self, tmp_path):
        # Row written under old 2-field schema, then config gains a field
        # inserted before the metadata tail. Reading back must keep values
        # aligned to the right names, not shifted.
        from src.engine import append_csv_row
        import csv
        p = tmp_path / 'tracking.csv'
        append_csv_row(p, {'period_end_date': 'p1', 'form_type': '10-Q', 'a': 10, 'b': 20},
                       self._cfg(['a', 'b']))
        # config now has a new field 'a2' between a and b
        append_csv_row(p, {'period_end_date': 'p2', 'form_type': '10-Q', 'a': 30, 'a2': 99, 'b': 40},
                       self._cfg(['a', 'a2', 'b']))
        rows = list(csv.DictReader(open(p)))
        assert len(rows) == 2
        # old row's values still map to the correct names, a2 blank
        assert rows[0]['a'] == '10' and rows[0]['b'] == '20' and rows[0]['a2'] == ''
        # new row intact
        assert rows[1]['a'] == '30' and rows[1]['a2'] == '99' and rows[1]['b'] == '40'
