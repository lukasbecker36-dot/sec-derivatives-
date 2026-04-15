"""Tests for src/scheduler.py — two-pass orchestrator."""

import csv
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from src.scheduler import (
    _pass_active, _pass_registered, run_scheduled,
    RunSummary, summary_to_dict,
)
from src.registry import UNIVERSE_COLUMNS


def _make_row(ticker, status='registered', **overrides):
    row = {col: '' for col in UNIVERSE_COLUMNS}
    row['ticker'] = ticker
    row['status'] = status
    row['activation_fail_count'] = '0'
    row.update(overrides)
    return row


class TestPassActive:
    @patch('src.scheduler.run_from_configs')
    @patch('src.scheduler.load_config')
    def test_processes_all_active(self, mock_load, mock_run):
        mock_config = MagicMock()
        mock_config.ticker = 'BA'
        mock_config.issuer = 'Boeing'
        mock_load.return_value = mock_config
        mock_run.return_value = [{'ticker': 'BA', 'processed': 1, 'errors': [], 'total_available': 1}]

        universe = [
            _make_row('BA', 'active', config_path='profiles/boeing.yaml'),
            _make_row('AAPL', 'registered'),
        ]
        summary = RunSummary()

        result = _pass_active(universe, Path('/tmp/out'), '', summary)
        assert summary.active_checked == 1
        assert summary.active_new_filings == 1
        mock_run.assert_called_once()

    @patch('src.scheduler.run_from_configs')
    @patch('src.scheduler.load_config')
    def test_isolates_config_load_failure(self, mock_load, mock_run):
        mock_load.side_effect = Exception('Bad YAML')
        mock_run.return_value = []

        universe = [_make_row('BA', 'active', config_path='profiles/boeing.yaml')]
        summary = RunSummary()

        result = _pass_active(universe, Path('/tmp/out'), '', summary)
        assert summary.active_errors == 1


class TestPassRegistered:
    @patch('src.scheduler.activate_issuer')
    @patch('src.scheduler.check_new_filing')
    @patch('src.scheduler.append_activation_event')
    @patch('src.scheduler.append_review_item')
    def test_triggers_activation(self, mock_review, mock_log, mock_check, mock_activate):
        from src.activation import ActivationResult
        mock_check.return_value = {
            'period_end': '2025-03-31', 'form_type': '10-Q',
            'accession_number': 'a1', 'primary_document': 'd1',
        }
        mock_activate.return_value = ActivationResult(
            ticker='AAPL', cik='320193', success=True,
            new_status='active', config_path='profiles/aapl.yaml',
            score=0.75, reasons=['Good'],
        )

        universe = [_make_row('AAPL', 'registered', cik='320193')]
        summary = RunSummary()

        result = _pass_registered(universe, '2025-01-01', 10,
                                  Path('/tmp/out'), False, summary,
                                  check_interval_days=0)
        assert summary.activations_attempted == 1
        assert summary.activations_succeeded == 1

    @patch('src.scheduler.check_new_filing')
    def test_skips_no_new_filing(self, mock_check):
        mock_check.return_value = None

        universe = [_make_row('AAPL', 'registered', cik='320193')]
        summary = RunSummary()

        result = _pass_registered(universe, '2025-01-01', 10,
                                  Path('/tmp/out'), False, summary,
                                  check_interval_days=0)
        assert summary.activations_attempted == 0
        assert summary.registered_checked == 1

    @patch('src.scheduler.check_new_filing')
    def test_max_activations_cap(self, mock_check):
        mock_check.return_value = {
            'period_end': '2025-03-31', 'form_type': '10-Q',
            'accession_number': 'a1', 'primary_document': 'd1',
        }

        universe = [
            _make_row('AAPL', 'registered', cik='320193'),
            _make_row('MSFT', 'registered', cik='789019'),
        ]
        summary = RunSummary()

        # max_activations=0 means none can be activated
        result = _pass_registered(universe, '2025-01-01', 0,
                                  Path('/tmp/out'), False, summary,
                                  check_interval_days=0)
        assert summary.activations_attempted == 0

    @patch('src.scheduler.check_new_filing')
    def test_dry_run(self, mock_check):
        mock_check.return_value = {
            'period_end': '2025-03-31', 'form_type': '10-Q',
            'accession_number': 'a1', 'primary_document': 'd1',
        }

        universe = [_make_row('AAPL', 'registered', cik='320193')]
        summary = RunSummary()

        result = _pass_registered(universe, '2025-01-01', 10,
                                  Path('/tmp/out'), True, summary,
                                  check_interval_days=0)
        assert summary.activations_attempted == 0
        dry_run_results = [r for r in summary.issuer_results if r.get('action') == 'dry_run_skip']
        assert len(dry_run_results) == 1

    @patch('src.scheduler.check_new_filing')
    def test_skip_recently_checked(self, mock_check):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        universe = [_make_row('AAPL', 'registered', cik='320193', last_checked_at=now)]
        summary = RunSummary()

        result = _pass_registered(universe, '2025-01-01', 10,
                                  Path('/tmp/out'), False, summary,
                                  check_interval_days=3)
        assert summary.registered_skipped == 1
        assert summary.registered_checked == 0
        mock_check.assert_not_called()

    @patch('src.scheduler.activate_issuer')
    @patch('src.scheduler.check_new_filing')
    @patch('src.scheduler.append_activation_event')
    @patch('src.scheduler.append_review_item')
    def test_retries_failed_activation(self, mock_review, mock_log, mock_check, mock_activate):
        from src.activation import ActivationResult
        mock_check.return_value = {
            'period_end': '2025-03-31', 'form_type': '10-Q',
            'accession_number': 'a1', 'primary_document': 'd1',
        }
        mock_activate.return_value = ActivationResult(
            ticker='ACN', cik='1467373', success=True,
            new_status='active', config_path='profiles/acn.yaml',
            score=0.75, reasons=['Good'],
        )

        universe = [_make_row('ACN', 'failed_activation', cik='1467373',
                              activation_fail_count='1')]
        summary = RunSummary()

        result = _pass_registered(universe, '2025-01-01', 10,
                                  Path('/tmp/out'), False, summary,
                                  check_interval_days=0)
        assert summary.activations_attempted == 1
        assert summary.activations_succeeded == 1

    @patch('src.scheduler.check_new_filing')
    def test_failed_issuers_skip_recency_check(self, mock_check):
        """Failed issuers should always be retried regardless of last_checked_at."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        mock_check.return_value = None

        universe = [_make_row('ACN', 'failed_activation', cik='1467373',
                              last_checked_at=now, activation_fail_count='1')]
        summary = RunSummary()

        result = _pass_registered(universe, '2025-01-01', 10,
                                  Path('/tmp/out'), False, summary,
                                  check_interval_days=3)
        assert summary.registered_skipped == 0
        assert summary.registered_checked == 1
        mock_check.assert_called_once()


    @patch('src.scheduler.activate_issuer')
    @patch('src.scheduler.check_new_filing')
    @patch('src.scheduler.append_activation_event')
    @patch('src.scheduler.append_review_item')
    def test_failed_issuer_skips_cutoff(self, mock_review, mock_log, mock_check, mock_activate):
        """Failed issuers should bypass the cutoff_date filter so previously
        detected filings aren't filtered out after aging past the window."""
        from src.activation import ActivationResult
        mock_check.return_value = {
            'period_end': '2025-02-22', 'form_type': '10-Q',
            'accession_number': 'a1', 'primary_document': 'd1',
        }
        mock_activate.return_value = ActivationResult(
            ticker='GIS', cik='40704', success=True,
            new_status='active', config_path='profiles/gis.yaml',
            score=0.75, reasons=['Good'],
        )

        universe = [_make_row('GIS', 'failed_activation', cik='40704',
                              activation_fail_count='1')]
        summary = RunSummary()

        # cutoff_date is AFTER the filing date — would normally filter it out
        result = _pass_registered(universe, '2025-02-23', 10,
                                  Path('/tmp/out'), False, summary,
                                  check_interval_days=0)
        # check_new_filing should be called with empty cutoff for failed issuers
        mock_check.assert_called_once()
        _, kwargs = mock_check.call_args
        assert kwargs.get('cutoff_date') == ''
        assert summary.activations_attempted == 1


class TestRunSummary:
    def test_summary_to_dict(self):
        summary = RunSummary(
            started_at='2025-03-21T00:00:00Z',
            active_checked=5,
            activations_succeeded=2,
        )
        d = summary_to_dict(summary)
        assert d['active_checked'] == 5
        assert d['activations_succeeded'] == 2
        assert 'issuer_results' in d
