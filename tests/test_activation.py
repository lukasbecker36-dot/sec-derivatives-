"""Tests for src/activation.py — lazy activation pipeline and scoring."""

import pytest
from unittest.mock import patch, MagicMock

from src.activation import (
    check_new_filing, score_bootstrap, score_extraction,
    compute_final_status, ActivationResult,
)
from src.section_extract import is_likely_cross_reference


class TestCheckNewFiling:
    @patch('src.activation.discover_filings')
    def test_new_filing_found(self, mock_discover):
        mock_discover.return_value = [
            {'period_end': '2024-12-31', 'form_type': '10-K', 'accession_number': 'a1', 'primary_document': 'd1'},
            {'period_end': '2025-03-31', 'form_type': '10-Q', 'accession_number': 'a2', 'primary_document': 'd2'},
        ]
        result = check_new_filing('12927', last_known_date='2024-12-31')
        assert result is not None
        assert result['period_end'] == '2025-03-31'

    @patch('src.activation.discover_filings')
    def test_no_new_filing(self, mock_discover):
        mock_discover.return_value = [
            {'period_end': '2024-12-31', 'form_type': '10-K', 'accession_number': 'a1', 'primary_document': 'd1'},
        ]
        result = check_new_filing('12927', last_known_date='2024-12-31')
        assert result is None

    @patch('src.activation.discover_filings')
    def test_cutoff_date_filter(self, mock_discover):
        mock_discover.return_value = [
            {'period_end': '2024-06-30', 'form_type': '10-Q', 'accession_number': 'a1', 'primary_document': 'd1'},
            {'period_end': '2025-03-31', 'form_type': '10-Q', 'accession_number': 'a2', 'primary_document': 'd2'},
        ]
        result = check_new_filing('12927', cutoff_date='2025-01-01')
        assert result is not None
        assert result['period_end'] == '2025-03-31'

    @patch('src.activation.discover_filings')
    def test_excludes_amendments(self, mock_discover):
        mock_discover.return_value = [
            {'period_end': '2025-03-31', 'form_type': '10-Q/A', 'accession_number': 'a1', 'primary_document': 'd1'},
        ]
        result = check_new_filing('12927')
        assert result is None

    @patch('src.activation.discover_filings')
    def test_edgar_error_returns_none(self, mock_discover):
        mock_discover.side_effect = Exception('Network error')
        result = check_new_filing('12927')
        assert result is None


class TestScoreBootstrap:
    def test_strong_result(self):
        bootstrap_result = {
            'archetype_confidence': 0.8,
            'sections_found': ['derivatives_or_instruments', 'market_risk'],
            'note_headings_found': ['Derivatives', 'Revenue', 'Taxes', 'Leases'],
            'llm_analysis': {'key_fields': [{'name': 'fx'}, {'name': 'ir'}]},
            'llm_analysis_failed': False,
            'warnings': [],
        }
        score, reasons = score_bootstrap(bootstrap_result)
        assert score >= 0.80

    def test_weak_result(self):
        bootstrap_result = {
            'archetype_confidence': 0.0,
            'sections_found': [],
            'note_headings_found': [],
            'llm_analysis': {'key_fields': []},
            'llm_analysis_failed': True,
            'warnings': ['No archetype keyword matches'],
        }
        score, reasons = score_bootstrap(bootstrap_result)
        assert score < 0.30

    def test_moderate_result(self):
        bootstrap_result = {
            'archetype_confidence': 0.2,
            'sections_found': ['derivatives_or_instruments'],
            'note_headings_found': ['Note 1', 'Note 2'],
            'llm_analysis': {'key_fields': [{'name': 'fx'}]},
            'llm_analysis_failed': False,
            'warnings': [],
        }
        score, reasons = score_bootstrap(bootstrap_result)
        assert 0.30 <= score <= 0.80


class TestScoreExtraction:
    def test_strong_result(self):
        process_result = {
            'llm_results': {
                'derivatives_note': {
                    'fields': {
                        'fx_notional': {'value': 1000},
                        'ir_notional': {'value': 500},
                        'commodity_notional': {'value': 200},
                    },
                },
                'market_risk': {
                    'fields': {
                        'ir_sensitivity': {'value': 50},
                    },
                },
            },
            'validation': [],
            'sections': {
                'derivatives_note': 'A' * 500,
                'market_risk': 'B' * 400,
            },
        }
        config = MagicMock()
        score, reasons = score_extraction(process_result, config)
        assert score >= 0.70

    def test_weak_result(self):
        process_result = {
            'llm_results': {
                'derivatives_note': {
                    'fields': {
                        'fx_notional': {'value': None},
                        'ir_notional': {'value': None},
                    },
                },
            },
            'validation': [{'level': 'error', 'message': 'bad'}],
            'sections': {
                'derivatives_note': 'See Note 5 for details.',
            },
        }
        config = MagicMock()
        score, reasons = score_extraction(process_result, config)
        assert score < 0.40

    def test_empty_result(self):
        process_result = {
            'llm_results': {},
            'validation': [],
            'sections': {},
        }
        config = MagicMock()
        score, reasons = score_extraction(process_result, config)
        assert score < 0.30


class TestComputeFinalStatus:
    def test_active(self):
        assert compute_final_status(0.8, 0.9) == 'active'

    def test_needs_review(self):
        assert compute_final_status(0.5, 0.4) == 'active_needs_review'

    def test_failed(self):
        assert compute_final_status(0.1, 0.2) == 'failed_activation'

    def test_boundary_active(self):
        # 0.4 * 0.8 + 0.6 * 0.5 = 0.32 + 0.30 = 0.62
        assert compute_final_status(0.8, 0.5) == 'active'

    def test_boundary_needs_review(self):
        # 0.4 * 0.5 + 0.6 * 0.25 = 0.20 + 0.15 = 0.35
        assert compute_final_status(0.5, 0.25) == 'active_needs_review'


class TestCrossReferenceStub:
    def test_short_with_xref(self):
        text = "See Note 5 in the condensed consolidated financial statements."
        assert is_likely_cross_reference(text) is True

    def test_long_text(self):
        text = "A" * 500
        assert is_likely_cross_reference(text) is False

    def test_short_no_xref(self):
        text = "The company uses derivatives."
        assert is_likely_cross_reference(text) is False

    def test_refer_to_pattern(self):
        text = "Refer to Note 12 for additional information about derivatives."
        assert is_likely_cross_reference(text) is True
