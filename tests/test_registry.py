"""Tests for src/registry.py — universe CSV management and status transitions."""

import csv
from pathlib import Path

import pytest

from src.registry import (
    load_universe, save_universe, get_by_status, get_active, get_registered,
    find_issuer, update_issuer, update_last_checked, mark_activating,
    mark_active, mark_active_needs_review, mark_failed,
    append_activation_event, append_review_item, seed_universe,
    UNIVERSE_COLUMNS, ACTIVATION_LOG_COLUMNS, REVIEW_QUEUE_COLUMNS,
)


def _make_row(ticker='TEST', status='registered', **overrides):
    row = {col: '' for col in UNIVERSE_COLUMNS}
    row['ticker'] = ticker
    row['status'] = status
    row['activation_fail_count'] = '0'
    row.update(overrides)
    return row


def _write_universe(tmp_path, rows):
    csv_path = tmp_path / 'universe.csv'
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=UNIVERSE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


class TestLoadSave:
    def test_load_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr('src.registry.UNIVERSE_CSV', tmp_path / 'nope.csv')
        assert load_universe() == []

    def test_roundtrip(self, tmp_path, monkeypatch):
        csv_path = tmp_path / 'universe.csv'
        monkeypatch.setattr('src.registry.UNIVERSE_CSV', csv_path)

        rows = [_make_row('AAPL', 'registered'), _make_row('BA', 'active')]
        save_universe(rows)

        loaded = load_universe()
        assert len(loaded) == 2
        assert loaded[0]['ticker'] == 'AAPL'
        assert loaded[1]['ticker'] == 'BA'
        assert loaded[1]['status'] == 'active'


class TestFiltering:
    def test_get_by_status(self):
        rows = [_make_row('A', 'registered'), _make_row('B', 'active'), _make_row('C', 'registered')]
        assert len(get_by_status(rows, 'registered')) == 2

    def test_get_active_includes_needs_review(self):
        rows = [_make_row('A', 'active'), _make_row('B', 'active_needs_review'), _make_row('C', 'registered')]
        active = get_active(rows)
        assert len(active) == 2
        assert {r['ticker'] for r in active} == {'A', 'B'}

    def test_get_registered(self):
        rows = [_make_row('A', 'registered'), _make_row('B', 'active')]
        assert len(get_registered(rows)) == 1

    def test_find_issuer_case_insensitive(self):
        rows = [_make_row('AAPL', 'registered')]
        assert find_issuer(rows, 'aapl') is not None
        assert find_issuer(rows, 'AAPL') is not None
        assert find_issuer(rows, 'MSFT') is None


class TestStatusTransitions:
    def test_valid_registered_to_activating(self):
        rows = [_make_row('A', 'registered')]
        result = mark_activating(rows, 'A')
        assert result[0]['status'] == 'activating'

    def test_valid_activating_to_active(self):
        rows = [_make_row('A', 'activating')]
        result = mark_active(rows, 'A', 'profiles/a.yaml')
        assert result[0]['status'] == 'active'
        assert result[0]['config_path'] == 'profiles/a.yaml'

    def test_valid_activating_to_needs_review(self):
        rows = [_make_row('A', 'activating')]
        result = mark_active_needs_review(rows, 'A', 'profiles/a.yaml')
        assert result[0]['status'] == 'active_needs_review'

    def test_valid_activating_to_failed(self):
        rows = [_make_row('A', 'activating', activation_fail_count='0')]
        result = mark_failed(rows, 'A')
        assert result[0]['status'] == 'failed_activation'
        assert result[0]['activation_fail_count'] == '1'

    def test_failed_to_activating_retry(self):
        rows = [_make_row('A', 'failed_activation', activation_fail_count='1')]
        result = mark_activating(rows, 'A')
        assert result[0]['status'] == 'activating'

    def test_invalid_registered_to_active(self):
        rows = [_make_row('A', 'registered')]
        with pytest.raises(ValueError, match='Invalid status transition'):
            mark_active(rows, 'A', 'profiles/a.yaml')

    def test_invalid_active_to_activating(self):
        rows = [_make_row('A', 'active')]
        with pytest.raises(ValueError, match='Invalid status transition'):
            mark_activating(rows, 'A')

    def test_mark_failed_increments_count(self):
        rows = [_make_row('A', 'activating', activation_fail_count='2')]
        result = mark_failed(rows, 'A')
        assert result[0]['activation_fail_count'] == '3'

    def test_ticker_not_found(self):
        rows = [_make_row('A', 'registered')]
        with pytest.raises(KeyError, match='Ticker not found'):
            mark_activating(rows, 'NOPE')


class TestUpdateLastChecked:
    def test_updates_fields(self):
        rows = [_make_row('A', 'registered')]
        result = update_last_checked(rows, 'A', '2025-03-21T00:00:00Z', '2025-03-15')
        assert result[0]['last_checked_at'] == '2025-03-21T00:00:00Z'
        assert result[0]['last_filing_date_seen'] == '2025-03-15'


class TestAppendLogs:
    def test_append_activation_event(self, tmp_path, monkeypatch):
        log_path = tmp_path / 'activation_log.csv'
        monkeypatch.setattr('src.registry.ACTIVATION_LOG_CSV', log_path)

        append_activation_event('AAPL', '320193', 'registered', 'activating',
                                filing_date='2025-03-15', form_type='10-Q')
        append_activation_event('AAPL', '320193', 'activating', 'active')

        rows = list(csv.DictReader(open(log_path, 'r', encoding='utf-8')))
        assert len(rows) == 2
        assert rows[0]['old_status'] == 'registered'
        assert rows[1]['new_status'] == 'active'

    def test_append_review_item(self, tmp_path, monkeypatch):
        log_path = tmp_path / 'review_queue.csv'
        monkeypatch.setattr('src.registry.REVIEW_QUEUE_CSV', log_path)

        append_review_item('AAPL', '320193', 'weak extraction', severity='warning')

        rows = list(csv.DictReader(open(log_path, 'r', encoding='utf-8')))
        assert len(rows) == 1
        assert rows[0]['reason'] == 'weak extraction'
        assert rows[0]['status'] == 'open'


class TestSeedUniverse:
    def test_seed(self, tmp_path, monkeypatch):
        # Create a mini CIK CSV
        cik_csv = tmp_path / 'ciks.csv'
        with open(cik_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ticker', 'CIK', 'company_name', 'gics_sector', 'gics_sub_industry'])
            writer.writerow(['AAPL', '320193', 'Apple Inc.', 'Information Technology', 'Tech Hardware'])
            writer.writerow(['BA', '12927', 'Boeing', 'Industrials', 'Aerospace'])
            writer.writerow(['MSFT', '789019', 'Microsoft', 'Information Technology', 'Software'])

        # Create a mock profile for BA
        profiles_dir = tmp_path / 'profiles'
        profiles_dir.mkdir()
        (profiles_dir / '_archetypes').mkdir()
        boeing_yaml = profiles_dir / 'boeing.yaml'
        boeing_yaml.write_text(
            'issuer: Boeing\nticker: BA\ncik: "0000012927"\n'
            'archetype: active_fx_commodity_hedger\nsector: industrials\n'
        )

        monkeypatch.setattr('src.registry.UNIVERSE_CSV', tmp_path / 'registry' / 'universe.csv')
        monkeypatch.setattr('src.registry.ACTIVATION_LOG_CSV', tmp_path / 'registry' / 'activation_log.csv')
        monkeypatch.setattr('src.registry.REVIEW_QUEUE_CSV', tmp_path / 'registry' / 'review_queue.csv')

        universe = seed_universe(cik_csv, profiles_dir)

        assert len(universe) == 3
        ba = find_issuer(universe, 'BA')
        assert ba['status'] == 'active'
        assert ba['config_path'] == 'profiles/boeing.yaml'
        assert ba['archetype_guess'] == 'active_fx_commodity_hedger'

        aapl = find_issuer(universe, 'AAPL')
        assert aapl['status'] == 'registered'

        # Check log files were created
        assert (tmp_path / 'registry' / 'activation_log.csv').exists()
        assert (tmp_path / 'registry' / 'review_queue.csv').exists()
