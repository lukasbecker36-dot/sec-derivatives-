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


class TestAppendCsvRowUpsert:
    """append_csv_row is now an upsert keyed on (period_end_date, form_type),
    not a bare append. This is what makes the retry-on-blank story safe:
    a retry writes over the prior blank row instead of duplicating it, and
    extraction_attempts increments so the fetcher's retry cap eventually
    stops the loop."""

    def _make_config(self):
        from src.config import IssuerConfig, FieldConfig
        return IssuerConfig(
            issuer='Test', ticker='TEST', cik='0000000001',
            fields={
                'notional': FieldConfig(description='Notional', section='m'),
            },
        )

    def test_first_write_records_attempt_one(self, tmp_path):
        from src.engine import append_csv_row
        import csv
        csv_path = tmp_path / 'tracking.csv'
        append_csv_row(csv_path, {
            'period_end_date': '2025-03-31', 'form_type': '10-Q',
            'notional': 100,
        }, self._make_config())
        rows = list(csv.DictReader(open(csv_path)))
        assert len(rows) == 1
        assert rows[0]['extraction_attempts'] == '1'

    def test_retry_replaces_prior_row(self, tmp_path):
        """The retry writes over the blank predecessor. Duplicate rows for the
        same period would appear as extraction failures in the audit and would
        also break the QoQ / YoY comparisons in the report generator."""
        from src.engine import append_csv_row
        import csv
        csv_path = tmp_path / 'tracking.csv'
        cfg = self._make_config()
        # First write: blank (extraction failed)
        append_csv_row(csv_path, {'period_end_date': '2025-03-31',
                                  'form_type': '10-Q'}, cfg)
        # Retry: populated
        append_csv_row(csv_path, {'period_end_date': '2025-03-31',
                                  'form_type': '10-Q', 'notional': 250}, cfg)
        rows = list(csv.DictReader(open(csv_path)))
        assert len(rows) == 1
        assert rows[0]['notional'] == '250'
        assert rows[0]['extraction_attempts'] == '2'

    def test_attempts_increments_across_retries(self, tmp_path):
        from src.engine import append_csv_row
        import csv
        csv_path = tmp_path / 'tracking.csv'
        cfg = self._make_config()
        for _ in range(3):
            append_csv_row(csv_path, {'period_end_date': '2025-03-31',
                                      'form_type': '10-Q'}, cfg)
        rows = list(csv.DictReader(open(csv_path)))
        assert len(rows) == 1
        assert rows[0]['extraction_attempts'] == '3'

    def test_different_periods_coexist(self, tmp_path):
        """Upsert must key on (period, form_type), NOT collapse everything."""
        from src.engine import append_csv_row
        import csv
        csv_path = tmp_path / 'tracking.csv'
        cfg = self._make_config()
        append_csv_row(csv_path, {'period_end_date': '2025-03-31',
                                  'form_type': '10-Q', 'notional': 100}, cfg)
        append_csv_row(csv_path, {'period_end_date': '2025-06-30',
                                  'form_type': '10-Q', 'notional': 200}, cfg)
        rows = list(csv.DictReader(open(csv_path)))
        assert len(rows) == 2
        assert {r['period_end_date'] for r in rows} == {'2025-03-31', '2025-06-30'}

    def test_different_form_types_same_period_coexist(self, tmp_path):
        """A 10-Q/A restatement is a distinct row from the original 10-Q."""
        from src.engine import append_csv_row
        import csv
        csv_path = tmp_path / 'tracking.csv'
        cfg = self._make_config()
        append_csv_row(csv_path, {'period_end_date': '2025-12-31',
                                  'form_type': '10-K', 'notional': 100}, cfg)
        append_csv_row(csv_path, {'period_end_date': '2025-12-31',
                                  'form_type': '10-K/A', 'notional': 105}, cfg)
        rows = list(csv.DictReader(open(csv_path)))
        assert len(rows) == 2
