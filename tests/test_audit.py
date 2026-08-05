"""Tests for src.audit — dataset-wide integrity audit over committed output."""

import csv

from src.audit import (
    is_empty_row, is_misaligned_row, audit_issuer, run_audit, format_report,
    META_COLUMNS,
)

HEADER = [
    'period_end_date', 'form_type', 'has_derivatives',
    'fx_derivatives_notional', 'fx_designated_notional',
    'fx_not_designated_notional', 'accession_number', 'processed_at',
]


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=HEADER, extrasaction='ignore')
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


class TestIsEmptyRow:
    def test_all_blank_is_empty(self):
        row = {c: '' for c in HEADER}
        row['period_end_date'] = '2025-06-30'
        row['form_type'] = '10-K'
        assert is_empty_row(row)

    def test_bookkeeping_only_is_still_empty(self):
        """An accession number is not evidence extraction found anything."""
        row = {c: '' for c in HEADER}
        row.update({'period_end_date': '2025-06-30', 'form_type': '10-K',
                    'accession_number': '0000950170-25-100235',
                    'processed_at': '2026-06-11T17:14:03Z'})
        assert is_empty_row(row)

    def test_genuine_non_discloser_is_not_empty(self):
        """has_derivatives=No is a real finding, not a failure."""
        row = {c: '' for c in HEADER}
        row.update({'period_end_date': '2025-06-30', 'form_type': '10-K',
                    'has_derivatives': 'No'})
        assert not is_empty_row(row)

    def test_populated_row_is_not_empty(self):
        row = {c: '' for c in HEADER}
        row.update({'period_end_date': '2025-06-30', 'form_type': '10-K',
                    'fx_derivatives_notional': '60013'})
        assert not is_empty_row(row)


class TestIsMisalignedRow:
    def test_surplus_values_are_misaligned(self):
        row = {'period_end_date': '2025-12-31', 'form_type': '10-Q',
               None: ['', '', 'Item 2']}
        assert is_misaligned_row(row)

    def test_blank_surplus_is_not_misaligned(self):
        """Trailing commas produce empty surplus — harmless."""
        row = {'period_end_date': '2025-12-31', 'form_type': '10-Q',
               None: ['', '', '']}
        assert not is_misaligned_row(row)

    def test_well_formed_row(self):
        row = {'period_end_date': '2025-12-31', 'form_type': '10-Q'}
        assert not is_misaligned_row(row)


class TestAuditIssuer:
    def test_flags_empty_row(self, tmp_path):
        path = tmp_path / 'msft' / 'tracking.csv'
        _write(path, [{'period_end_date': '2025-06-30', 'form_type': '10-K'}])
        defects = audit_issuer('msft', path)
        assert len(defects) == 1
        assert defects[0]['type'] == 'empty_row'

    def test_flags_reconciliation_failure(self, tmp_path):
        path = tmp_path / 'msft' / 'tracking.csv'
        _write(path, [{
            'period_end_date': '2025-12-31', 'form_type': '10-Q',
            'fx_derivatives_notional': '52784',
            'fx_designated_notional': '1492',
            'fx_not_designated_notional': '8994',
        }])
        defects = audit_issuer('msft', path)
        assert [d['type'] for d in defects] == ['reconciliation']

    def test_clean_issuer_has_no_defects(self, tmp_path):
        path = tmp_path / 'msft' / 'tracking.csv'
        _write(path, [{
            'period_end_date': '2025-12-31', 'form_type': '10-Q',
            'fx_derivatives_notional': '54086',
            'fx_designated_notional': '1492',
            'fx_not_designated_notional': '52594',
        }])
        assert audit_issuer('msft', path) == []

    def test_empty_row_short_circuits_reconciliation(self, tmp_path):
        """A blank row is one defect, not one per rule."""
        path = tmp_path / 'msft' / 'tracking.csv'
        _write(path, [{'period_end_date': '2025-06-30', 'form_type': '10-K'}])
        defects = audit_issuer('msft', path)
        assert len(defects) == 1


class TestRunAudit:
    def test_aggregates_across_issuers(self, tmp_path):
        _write(tmp_path / 'aaa' / 'tracking.csv',
               [{'period_end_date': '2025-06-30', 'form_type': '10-K'}])
        _write(tmp_path / 'bbb' / 'tracking.csv', [{
            'period_end_date': '2025-06-30', 'form_type': '10-K',
            'fx_derivatives_notional': '100',
            'fx_designated_notional': '10',
            'fx_not_designated_notional': '10',
        }])
        report = run_audit(tmp_path)
        assert report['issuers_audited'] == 2
        assert report['defect_count'] == 2
        assert report['defects_by_type'] == {'empty_row': 1, 'reconciliation': 1}
        assert report['tickers_affected'] == ['aaa', 'bbb']

    def test_ticker_filter(self, tmp_path):
        _write(tmp_path / 'aaa' / 'tracking.csv',
               [{'period_end_date': '2025-06-30', 'form_type': '10-K'}])
        _write(tmp_path / 'bbb' / 'tracking.csv',
               [{'period_end_date': '2025-06-30', 'form_type': '10-K'}])
        report = run_audit(tmp_path, tickers=['AAA'])
        assert report['issuers_audited'] == 1
        assert report['tickers_affected'] == ['aaa']

    def test_clean_corpus_reports_nothing(self, tmp_path):
        _write(tmp_path / 'aaa' / 'tracking.csv', [{
            'period_end_date': '2025-06-30', 'form_type': '10-K',
            'has_derivatives': 'No',
        }])
        report = run_audit(tmp_path)
        assert report['defect_count'] == 0
        assert 'No integrity defects' in format_report(report)
