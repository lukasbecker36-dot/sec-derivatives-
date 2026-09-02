"""Tests for src.filing_fetcher — retry-on-blank behaviour.

The fix motivated by these tests: a blank row (row_has_extracted_data == False)
used to silently mark a period as 'processed', permanently blocking retries.
That failure mode blanked AAPL's 2025 quarters for months and made a YoY
comparison impossible against a corpus that had every incentive to look
'complete' — the rows were there, just empty.

get_unprocessed_filings now includes such periods in the returned work list,
capped at MAX_BLANK_RETRIES so legitimately-blank filings (incorporation-by-
reference filers) don't loop forever burning extraction budget.
"""

import csv
from pathlib import Path
from unittest.mock import patch

import pytest

from src.filing_fetcher import (
    get_unprocessed_filings,
    _row_has_extracted_data,
    _row_attempts,
    MAX_BLANK_RETRIES,
)


def _write_csv(path: Path, header: list[str], rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


HEADER = [
    'period_end_date', 'form_type', 'has_derivatives', 'fx_derivatives_notional',
    'accession_number', 'filing_date', 'processed_at', 'extraction_version',
    'extraction_attempts',
]


class TestRowHasExtractedData:
    def test_populated_field_counts(self):
        assert _row_has_extracted_data({
            'period_end_date': '2025-03-31', 'form_type': '10-Q',
            'fx_derivatives_notional': '10000',
        }) is True

    def test_metadata_only_does_not_count(self):
        """The exact failure that let AAPL's blank rows lock the period."""
        assert _row_has_extracted_data({
            'period_end_date': '2025-03-29',
            'form_type': '10-Q',
            'accession_number': '0000320193-25-000057',
            'filing_date': '2025-05-02',
            'processed_at': '2025-05-03T00:00:00Z',
            'extraction_version': '1',
            'extraction_attempts': '1',
        }) is False

    def test_has_derivatives_no_counts_as_data(self):
        """Genuine non-disclosers (has_derivatives=No) are a real finding,
        not an extraction failure — they must count as 'processed' so the
        pipeline doesn't retry them forever."""
        assert _row_has_extracted_data({
            'period_end_date': '2025-03-31', 'form_type': '10-Q',
            'has_derivatives': 'No',
        }) is True

    def test_list_valued_cell_is_correctly_flattened(self):
        assert _row_has_extracted_data({
            'period_end_date': '2025-03-31',
            'unexpected_extra': ['', '', 'stray text'],
        }) is True


class TestRowAttempts:
    def test_reads_integer(self):
        assert _row_attempts({'extraction_attempts': '2'}) == 2

    def test_missing_defaults_zero(self):
        assert _row_attempts({}) == 0

    def test_blank_defaults_zero(self):
        assert _row_attempts({'extraction_attempts': ''}) == 0

    def test_malformed_defaults_zero(self):
        assert _row_attempts({'extraction_attempts': 'not-a-number'}) == 0


class TestGetUnprocessedFilings:
    ALL_FILINGS = [
        {'period_end': '2025-03-31', 'form_type': '10-Q', 'accession_number': 'a1'},
        {'period_end': '2025-06-30', 'form_type': '10-Q', 'accession_number': 'a2'},
        {'period_end': '2025-09-30', 'form_type': '10-Q', 'accession_number': 'a3'},
    ]

    @patch('src.filing_fetcher.discover_filings')
    def test_no_existing_csv_returns_all(self, mock_discover, tmp_path):
        mock_discover.return_value = list(self.ALL_FILINGS)
        result = get_unprocessed_filings('0000000000', tmp_path / 'nope.csv')
        assert len(result) == 3

    @patch('src.filing_fetcher.discover_filings')
    def test_populated_rows_are_processed(self, mock_discover, tmp_path):
        mock_discover.return_value = list(self.ALL_FILINGS)
        csv_path = tmp_path / 'tracking.csv'
        _write_csv(csv_path, HEADER, [
            {'period_end_date': '2025-03-31', 'form_type': '10-Q',
             'fx_derivatives_notional': '10000', 'extraction_attempts': '1'},
        ])
        result = get_unprocessed_filings('0000000000', csv_path)
        assert {f['period_end'] for f in result} == {'2025-06-30', '2025-09-30'}

    @patch('src.filing_fetcher.discover_filings')
    def test_blank_row_under_retry_cap_is_retried(self, mock_discover, tmp_path):
        """The AAPL failure: blank rows must be returned as work to do."""
        mock_discover.return_value = list(self.ALL_FILINGS)
        csv_path = tmp_path / 'tracking.csv'
        _write_csv(csv_path, HEADER, [
            {'period_end_date': '2025-03-31', 'form_type': '10-Q',
             'accession_number': 'a1', 'extraction_attempts': '1'},
        ])
        result = get_unprocessed_filings('0000000000', csv_path)
        assert '2025-03-31' in {f['period_end'] for f in result}

    @patch('src.filing_fetcher.discover_filings')
    def test_blank_row_at_retry_cap_is_not_retried(self, mock_discover, tmp_path):
        """Legitimate non-discloser filers (incorporation-by-reference)
        would produce blanks forever; the cap stops the retry loop."""
        mock_discover.return_value = list(self.ALL_FILINGS)
        csv_path = tmp_path / 'tracking.csv'
        _write_csv(csv_path, HEADER, [
            {'period_end_date': '2025-03-31', 'form_type': '10-Q',
             'accession_number': 'a1',
             'extraction_attempts': str(MAX_BLANK_RETRIES)},
        ])
        result = get_unprocessed_filings('0000000000', csv_path)
        assert '2025-03-31' not in {f['period_end'] for f in result}

    @patch('src.filing_fetcher.discover_filings')
    def test_blank_row_without_attempts_field_is_retried(self, mock_discover, tmp_path):
        """Historical rows written before extraction_attempts existed default
        to 0 and get one shot at being backfilled."""
        mock_discover.return_value = list(self.ALL_FILINGS)
        csv_path = tmp_path / 'tracking.csv'
        # Simulate an old-format row (no extraction_attempts column):
        old_header = [c for c in HEADER if c != 'extraction_attempts']
        _write_csv(csv_path, old_header, [
            {'period_end_date': '2025-03-31', 'form_type': '10-Q',
             'accession_number': 'a1'},
        ])
        result = get_unprocessed_filings('0000000000', csv_path)
        assert '2025-03-31' in {f['period_end'] for f in result}

    @patch('src.filing_fetcher.discover_filings')
    def test_mixed_state_partitions_correctly(self, mock_discover, tmp_path):
        mock_discover.return_value = list(self.ALL_FILINGS)
        csv_path = tmp_path / 'tracking.csv'
        _write_csv(csv_path, HEADER, [
            # populated: done
            {'period_end_date': '2025-03-31', 'form_type': '10-Q',
             'fx_derivatives_notional': '10000', 'extraction_attempts': '1'},
            # blank under cap: retry
            {'period_end_date': '2025-06-30', 'form_type': '10-Q',
             'accession_number': 'a2', 'extraction_attempts': '2'},
            # blank at cap: give up
            {'period_end_date': '2025-09-30', 'form_type': '10-Q',
             'accession_number': 'a3', 'extraction_attempts': '3'},
        ])
        result = get_unprocessed_filings('0000000000', csv_path)
        assert {f['period_end'] for f in result} == {'2025-06-30'}
