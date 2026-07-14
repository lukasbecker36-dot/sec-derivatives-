"""Tests for backfill: ledger, row assembly, fill rates, gate, rebuild."""

import csv
import json

import pytest

from src import backfill
from src.config import IssuerConfig, SectionConfig, FieldConfig


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """Redirect all backfill paths into a tmp dir."""
    bf = tmp_path / 'backfill'
    monkeypatch.setattr(backfill, 'BACKFILL_DIR', bf)
    monkeypatch.setattr(backfill, 'STATE_CSV', bf / 'state.csv')
    monkeypatch.setattr(backfill, 'REQUESTS_DIR', bf / 'requests')
    monkeypatch.setattr(backfill, 'RESULTS_DIR', bf / 'results')
    monkeypatch.setattr(backfill, 'UNITS_DIR', bf / 'units')
    monkeypatch.setattr(backfill, 'CACHE_DIR', bf / 'cache')
    monkeypatch.setattr(backfill, 'STAGING_DIR', bf / 'staging')
    monkeypatch.setattr(backfill, 'OUTPUT_DIR', tmp_path / 'output')
    return tmp_path


def _config():
    return IssuerConfig(
        issuer='Test Co', ticker='TST', cik='0000123456',
        sections={
            'derivatives_note': SectionConfig(heading='Note'),
            'market_risk': SectionConfig(heading='Market Risk'),
        },
        fields={
            'fx_notional': FieldConfig(description='d', section='derivatives_note'),
            'ir_notional': FieldConfig(description='d', section='derivatives_note'),
            'rate_sensitivity': FieldConfig(description='d', section='market_risk'),
        },
    )


def _detail(accession='acc-1', period='2025-03-31', sections=None):
    return {
        'ticker': 'TST', 'cik': '0000123456',
        'accession_number': accession, 'period_end': period,
        'form_type': '10-Q', 'filing_date': '2025-05-01',
        'config_path': 'unused',
        'sections': sections or {},
    }


def _write_extraction_pair(accession, section, fields, flags=None):
    """Write matching request/result files; return the request filename."""
    key = backfill._unit_key('TST', accession)
    name = f'{key}_{section}_extract.json'
    backfill.REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
    backfill.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(backfill.REQUESTS_DIR / name, 'w') as f:
        json.dump({'section_text': f'text for {section}'}, f)
    with open(backfill.RESULTS_DIR / name, 'w') as f:
        json.dump({'fields': {k: {'value': v} for k, v in fields.items()},
                   'flags': flags or []}, f)
    return name


class TestLedger:
    def test_round_trip(self, workspace):
        units = {}
        detail = _detail()
        detail['sections'] = {'derivatives_note': {'status': 'ok'},
                              'market_risk': {'status': 'empty'}}
        key = backfill._unit_key('TST', 'acc-1')
        backfill._update_ledger(units, key, detail, 'locate_pending')
        backfill.save_state(units)

        loaded = backfill.load_state()
        assert loaded[key]['status'] == 'locate_pending'
        assert loaded[key]['sections_located'] == '1'
        assert loaded[key]['sections_total'] == '2'

    def test_unit_detail_round_trip(self, workspace):
        key = backfill._unit_key('TST', 'acc-1')
        backfill._save_unit_detail(key, _detail())
        assert backfill._load_unit_detail(key)['ticker'] == 'TST'
        assert backfill._load_unit_detail('missing') is None


class TestArtifactsPresent:
    def test_missing_detail(self, workspace):
        assert not backfill._unit_artifacts_present('nope')

    def test_missing_request_file(self, workspace):
        key = backfill._unit_key('TST', 'acc-1')
        backfill._save_unit_detail(key, _detail(sections={
            'derivatives_note': {'status': 'ok', 'request_file': 'gone.json'},
        }))
        assert not backfill._unit_artifacts_present(key)

    def test_present(self, workspace):
        req = _write_extraction_pair('acc-1', 'derivatives_note', {'fx_notional': 1})
        key = backfill._unit_key('TST', 'acc-1')
        backfill._save_unit_detail(key, _detail(sections={
            'derivatives_note': {'status': 'ok', 'request_file': req},
            'market_risk': {'status': 'not_disclosed'},
        }))
        assert backfill._unit_artifacts_present(key)


class TestAssembleRow:
    def test_full_assembly(self, workspace):
        req = _write_extraction_pair('acc-1', 'derivatives_note',
                                     {'fx_notional': 5200, 'ir_notional': None},
                                     flags=['units unclear'])
        detail = _detail(sections={
            'derivatives_note': {'status': 'ok', 'request_file': req},
            'market_risk': {'status': 'not_disclosed'},
        })
        staged = backfill._assemble_row('k', detail, _config())
        assert staged['row']['fx_notional'] == 5200
        assert staged['row']['extraction_version'] == backfill.EXTRACTION_VERSION
        assert staged['row']['accession_number'] == 'acc-1'
        assert staged['provenance'] == {
            'fx_notional': 'extracted',
            'ir_notional': 'not_disclosed',
            'rate_sensitivity': 'not_disclosed',
        }
        assert staged['flags'] == ['units unclear']
        assert staged['section_texts']['derivatives_note'] == 'text for derivatives_note'

    def test_missing_result_returns_none(self, workspace):
        detail = _detail(sections={
            'derivatives_note': {'status': 'ok', 'request_file': 'nonexistent.json'},
        })
        assert backfill._assemble_row('k', detail, _config()) is None

    def test_locate_failed_marks_section_missing(self, workspace):
        req = _write_extraction_pair('acc-1', 'derivatives_note',
                                     {'fx_notional': 100, 'ir_notional': 200})
        detail = _detail(sections={
            'derivatives_note': {'status': 'ok', 'request_file': req},
            'market_risk': {'status': 'locate_failed'},
        })
        staged = backfill._assemble_row('k', detail, _config())
        assert staged['provenance']['rate_sensitivity'] == 'section_missing'


class TestFillRate:
    def test_not_disclosed_excluded_from_denominator(self):
        staged = {'provenance': {
            'a': 'extracted', 'b': 'not_disclosed', 'c': 'extracted',
        }}
        assert backfill._row_fill_rate(staged) == 1.0

    def test_section_missing_counts_against(self):
        staged = {'provenance': {
            'a': 'extracted', 'b': 'section_missing',
        }}
        assert backfill._row_fill_rate(staged) == 0.5

    def test_all_not_disclosed_is_none(self):
        staged = {'provenance': {'a': 'not_disclosed'}}
        assert backfill._row_fill_rate(staged) is None


class TestRebuildIssuer:
    def _staged(self, period, fx, accession, missing=False, form_type='10-Q'):
        prov = {'fx_notional': 'extracted' if fx is not None else 'not_disclosed',
                'ir_notional': 'not_disclosed',
                'rate_sensitivity': 'section_missing' if missing else 'not_disclosed'}
        row = {'period_end_date': period, 'form_type': form_type,
               'accession_number': accession, 'filing_date': '',
               'processed_at': '', 'extraction_version': 2}
        if fx is not None:
            row['fx_notional'] = fx
        return {'row': row, 'provenance': prov, 'sections_status': {},
                'section_texts': {}, 'flags': []}

    def test_chronological_alerts_and_gate(self, workspace):
        staged_rows = [
            self._staged('2025-06-30', 12000, 'acc-2'),  # out of order on purpose
            self._staged('2025-03-31', 5000, 'acc-1'),
        ]
        verdict = backfill._rebuild_issuer('TST', staged_rows, _config())
        assert verdict['gate_passed']
        assert verdict['median_fill_rate'] == 1.0
        assert verdict['rows'] == 2

        # Rows were sorted oldest-first
        assert staged_rows[0]['row']['period_end_date'] == '2025-03-31'

        # Q2 vs Q1 is a >20% move -> NUMERIC alert in regenerated log
        alerts = (backfill.STAGING_DIR / 'tst' / 'alerts.txt').read_text()
        assert '[NUMERIC]' in alerts
        assert '2025-06-30' in alerts

    def test_historical_tagging_against_live_coverage(self, workspace):
        live_dir = backfill.OUTPUT_DIR / 'tst'
        live_dir.mkdir(parents=True)
        with open(live_dir / 'tracking.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['period_end_date', 'form_type'])
            writer.writeheader()
            writer.writerow({'period_end_date': '2025-06-30', 'form_type': '10-Q'})

        staged_rows = [
            self._staged('2025-03-31', 5000, 'acc-1'),
            self._staged('2025-06-30', 12000, 'acc-2'),
            self._staged('2025-09-30', 4000, 'acc-3'),
        ]
        backfill._rebuild_issuer('TST', staged_rows, _config())
        alerts = (backfill.STAGING_DIR / 'tst' / 'alerts.txt').read_text()

        # Alerts for periods <= live coverage (2025-06-30) are historical
        assert '[HISTORICAL] [NUMERIC]' in alerts
        # The genuinely new 2025-09-30 period is not tagged
        sept_blocks = [b for b in alerts.split('\n\n')
                       if 'Period ending 2025-09-30' in b]
        assert len(sept_blocks) == 1
        assert '[NUMERIC]' in sept_blocks[0]
        assert '[HISTORICAL]' not in sept_blocks[0]

    def test_gate_fails_on_low_fill(self, workspace):
        staged_rows = [
            self._staged('2025-03-31', None, 'acc-1', missing=True),
            self._staged('2025-06-30', None, 'acc-2', missing=True),
        ]
        verdict = backfill._rebuild_issuer('TST', staged_rows, _config())
        assert not verdict['gate_passed']
        assert verdict['median_fill_rate'] == 0.0

    def test_honest_minimal_filer_passes(self, workspace):
        staged_rows = [self._staged('2025-03-31', None, 'acc-1')]
        verdict = backfill._rebuild_issuer('TST', staged_rows, _config())
        assert verdict['median_fill_rate'] is None
        assert verdict['gate_passed']

    def test_annual_only_discloser_passes_on_10k_alone(self, workspace):
        # ITW-style pattern: the 10-K fully discloses notionals but each
        # 10-Q just cross-references it ("See Note 8. Debt for additional
        # information...") and has nothing extractable. The overall median
        # is dragged to 0 by the four empty quarters, but the well-extracted
        # annual filing alone should be enough to pass the gate and commit.
        staged_rows = [
            self._staged('2025-03-31', None, 'acc-q1', missing=True, form_type='10-Q'),
            self._staged('2025-06-30', None, 'acc-q2', missing=True, form_type='10-Q'),
            self._staged('2025-09-30', None, 'acc-q3', missing=True, form_type='10-Q'),
            self._staged('2025-12-31', 1600, 'acc-10k', form_type='10-K'),
            self._staged('2026-03-31', None, 'acc-q4', missing=True, form_type='10-Q'),
        ]
        verdict = backfill._rebuild_issuer('TST', staged_rows, _config())
        assert verdict['median_fill_rate'] == 0.0
        assert verdict['annual_median_fill_rate'] == 1.0
        assert verdict['gate_passed']

    def test_genuine_non_discloser_still_fails_despite_annual_check(self, workspace):
        # A company with no derivatives at all (like ROP/UNP): the 10-K also
        # has nothing extractable, so neither the overall nor the annual-only
        # median should rescue it, and any_missing correctly fails the gate.
        staged_rows = [
            self._staged('2025-03-31', None, 'acc-q1', missing=True, form_type='10-Q'),
            self._staged('2025-12-31', None, 'acc-10k', missing=True, form_type='10-K'),
        ]
        verdict = backfill._rebuild_issuer('TST', staged_rows, _config())
        assert verdict['annual_median_fill_rate'] == 0.0
        assert not verdict['gate_passed']


class TestCommitIssuer:
    def test_writes_sorted_tracking_csv(self, workspace):
        config = _config()
        staged_rows = [
            {'row': {'period_end_date': '2025-03-31', 'form_type': '10-Q',
                     'fx_notional': 5000, 'accession_number': 'acc-1',
                     'filing_date': '2025-05-01', 'processed_at': 'x',
                     'extraction_version': 2},
             'provenance': {}, 'sections_status': {}, 'section_texts': {}, 'flags': []},
            {'row': {'period_end_date': '2025-06-30', 'form_type': '10-Q',
                     'fx_notional': 6000, 'accession_number': 'acc-2',
                     'filing_date': '2025-08-01', 'processed_at': 'x',
                     'extraction_version': 2},
             'provenance': {}, 'sections_status': {}, 'section_texts': {}, 'flags': []},
        ]
        staging = backfill.STAGING_DIR / 'tst'
        staging.mkdir(parents=True)
        (staging / 'alerts.txt').write_text('alerts content')
        (staging / 'notes.txt').write_text('notes content')

        universe = [{'ticker': 'TST', 'status': 'active_needs_review',
                     'config_path': 'profiles/tst.yaml'}]
        universe = backfill._commit_issuer('TST', staged_rows, config, universe)

        out = backfill.OUTPUT_DIR / 'tst'
        with open(out / 'tracking.csv') as f:
            rows = list(csv.DictReader(f))
        assert [r['period_end_date'] for r in rows] == ['2025-03-31', '2025-06-30']
        assert rows[0]['accession_number'] == 'acc-1'
        assert rows[0]['extraction_version'] == '2'
        assert (out / 'alert_log.txt').read_text() == 'alerts content'
        assert (out / 'notes.txt').read_text() == 'notes content'
        assert universe[0]['status'] == 'active'

    def test_daily_append_stays_aligned(self, workspace):
        """Daily ingester appends with fewer columns; they must align with the
        leading columns of the extended backfill header."""
        config = _config()
        cols = backfill._live_columns(config)
        # New provenance columns must come after all config-driven columns
        assert cols[:2] == ['period_end_date', 'form_type']
        assert cols[-4:] == ['accession_number', 'filing_date',
                             'processed_at', 'extraction_version']
        assert cols[2:-4] == list(config.fields.keys())
