"""Tests for src/cme_bulletin.py — CME IR bulletin parsing and storage.

Runs entirely offline against a committed sample PDF fixture; no network.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest

import src.cme_bulletin as cme

FIXTURE = Path(__file__).parent / 'fixtures' / 'cme_section02a_sample.pdf'


@pytest.fixture(scope='module')
def parsed():
    trade_date, status, rows = cme.parse_bulletin(FIXTURE.read_bytes())
    return trade_date, status, rows


def _find(rows, code, section, option_type=''):
    hits = [r for r in rows
            if r['product_code'] == code
            and r['report_section'] == section
            and r['option_type'] == option_type]
    return hits[0] if hits else None


class TestParse:
    def test_trade_date_and_status(self, parsed):
        trade_date, status, _ = parsed
        assert trade_date == '2026-07-02'
        assert status == 'PRELIMINARY'

    def test_both_sections_present(self, parsed):
        _, _, rows = parsed
        sections = {r['report_section'] for r in rows}
        assert sections == {'FUTURES', 'OPTIONS'}

    def test_headline_futures_row(self, parsed):
        """SR3 (3-month SOFR) — the most-traded IR future — parses exactly."""
        _, _, rows = parsed
        sr3 = _find(rows, 'SR3', 'FUTURES')
        assert sr3 is not None
        assert sr3['product_name'] == 'THREE-MONTH SOFR FUTURES'
        assert sr3['globex_volume'] == 4068575
        assert sr3['pnt_volume'] == 8700
        assert sr3['total_volume'] == 4086781
        assert sr3['open_interest'] == 13103154
        assert sr3['oi_change'] == 122457

    def test_glued_name_and_number_split(self, parsed):
        """'...NOTE FUT465011' must split into name + globex volume."""
        _, _, rows = parsed
        tn = _find(rows, 'TN', 'FUTURES')
        assert tn['product_name'] == 'ULTRA 10-YEAR U S TREASURY NOTE FUT'
        assert tn['globex_volume'] == 465011
        assert tn['total_volume'] == 471414

    def test_negative_oi_change_sign(self, parsed):
        _, _, rows = parsed
        ff = _find(rows, 'FF', 'FUTURES')
        assert ff['oi_change'] == -241949

    def test_option_call_put_split(self, parsed):
        _, _, rows = parsed
        call = _find(rows, 'TC', 'OPTIONS', 'C')
        put = _find(rows, 'TC', 'OPTIONS', 'P')
        assert call['total_volume'] == 263162
        assert put['total_volume'] == 179797
        assert call['product_name'] == '10-YR NOTE OPTIONS'

    def test_option_total_rows_flagged(self, parsed):
        _, _, rows = parsed
        tot = [r for r in rows
               if r['report_section'] == 'OPTIONS' and r['is_total']
               and r['product_name'] == '10-YR NOTE OPTIONS']
        assert tot and tot[0]['total_volume'] == 442959

    def test_all_rows_reconcile(self, parsed):
        """Every parsed row must satisfy globex+outcry+pnt == total_volume."""
        _, _, rows = parsed
        assert all(cme._reconciles(r) for r in rows)

    def test_futures_sum_matches_cme_aggregate(self, parsed):
        """Per-product FUTURES volumes must sum to CME's own 'FUTURES ONLY /
        INTEREST RATES' grand total printed in the same bulletin."""
        _, _, rows = parsed
        total = sum(r['total_volume'] or 0
                    for r in rows if r['report_section'] == 'FUTURES')
        assert total == 10299111

    def test_bad_pdf_raises(self):
        with pytest.raises(cme.BulletinParseError):
            cme.parse_bulletin(b'%PDF-1.4 not really a bulletin')


class TestStore:
    def _prep(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cme, 'CSV_PATH', tmp_path / 'ir_volume_oi.csv')
        monkeypatch.setattr(cme, 'RAW_DIR', tmp_path / 'raw')

    def _rows(self):
        td, status, rows = cme.parse_bulletin(FIXTURE.read_bytes())
        fa = datetime.now(timezone.utc).isoformat(timespec='seconds')
        for r in rows:
            r.update(trade_date=td, report_status=status,
                     source_pdf=f'{td}.pdf', fetched_at=fa)
        return rows

    def test_upsert_then_idempotent(self, tmp_path, monkeypatch):
        self._prep(tmp_path, monkeypatch)
        rows = self._rows()
        n1 = cme.upsert_rows(rows)
        assert n1 == len(rows)
        assert len(cme._load_csv()) == len(rows)

        # Re-running the same day replaces, does not duplicate.
        cme.upsert_rows(self._rows())
        assert len(cme._load_csv()) == len(rows)

    def test_second_day_appends(self, tmp_path, monkeypatch):
        self._prep(tmp_path, monkeypatch)
        day1 = self._rows()
        cme.upsert_rows(day1)
        day2 = self._rows()
        for r in day2:
            r['trade_date'] = '2026-07-03'
        cme.upsert_rows(day2)
        stored = cme._load_csv()
        assert len(stored) == 2 * len(day1)
        assert {'2026-07-02', '2026-07-03'} == {r['trade_date'] for r in stored}
