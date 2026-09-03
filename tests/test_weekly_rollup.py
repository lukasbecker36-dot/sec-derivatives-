"""Tests for src.weekly_rollup — the mechanical assembler that stitches
the week's daily digests into a single weekly rollup HTML.

No LLM involved by design — the substance already got verified for each
daily send, so the rollup just carries that verified content forward
under a per-day heading rather than re-summarising it (which would be
another chance for the wrong-endpoint-percentage failure mode).
"""

from datetime import date
from pathlib import Path

import pytest

from src.weekly_rollup import build_rollup, _extract_body, _weekday_files


def _write(path: Path, body: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<!DOCTYPE html><html><head><title>x</title></head>'
        f'<body>{body}</body></html>',
        encoding='utf-8',
    )


class TestExtractBody:
    def test_extracts_between_body_tags(self):
        html = '<html><body><p>Hello</p></body></html>'
        assert _extract_body(html) == '<p>Hello</p>'

    def test_case_insensitive(self):
        html = '<HTML><BODY><p>Hello</p></BODY></HTML>'
        assert _extract_body(html) == '<p>Hello</p>'

    def test_multiline(self):
        html = '<html><body>\nline 1\nline 2\n</body></html>'
        assert 'line 1' in _extract_body(html)
        assert 'line 2' in _extract_body(html)

    def test_no_body_returns_full_content(self):
        """Some routines emit fragments; don't drop them."""
        html = '<p>Fragment only</p>'
        assert _extract_body(html) == '<p>Fragment only</p>'


class TestWeekdayFiles:
    def test_only_weekdays_returned(self, tmp_path, monkeypatch):
        monkeypatch.setattr('src.weekly_rollup.DIGESTS_DIR', tmp_path)
        # Create seven files, Mon-Sun of a specific week
        # Mon 2026-08-31 through Sun 2026-09-06
        for d in range(31, 32):
            _write(tmp_path / f'2026-08-{d}.html', f'<p>Aug {d}</p>')
        for d in range(1, 7):
            _write(tmp_path / f'2026-09-{d:02d}.html', f'<p>Sep {d}</p>')
        # Ask as of Fri 2026-09-04
        result = _weekday_files(as_of=date(2026, 9, 4))
        stems = [p.stem for p in result]
        # Should have Mon-Fri only: 08-31, 09-01, 09-02, 09-03, 09-04
        assert stems == ['2026-08-31', '2026-09-01', '2026-09-02',
                         '2026-09-03', '2026-09-04']

    def test_missing_days_silently_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setattr('src.weekly_rollup.DIGESTS_DIR', tmp_path)
        # Only Wed and Fri
        _write(tmp_path / '2026-09-02.html', '<p>Wed</p>')
        _write(tmp_path / '2026-09-04.html', '<p>Fri</p>')
        result = _weekday_files(as_of=date(2026, 9, 4))
        stems = [p.stem for p in result]
        assert stems == ['2026-09-02', '2026-09-04']

    def test_no_files_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr('src.weekly_rollup.DIGESTS_DIR', tmp_path)
        assert _weekday_files(as_of=date(2026, 9, 4)) == []

    def test_ordering_is_chronological(self, tmp_path, monkeypatch):
        """The reader gets Mon-first-Friday-last, not newest-first."""
        monkeypatch.setattr('src.weekly_rollup.DIGESTS_DIR', tmp_path)
        for d in range(31, 32):
            _write(tmp_path / f'2026-08-{d}.html', 'x')
        for d in range(1, 5):
            _write(tmp_path / f'2026-09-0{d}.html', 'x')
        result = _weekday_files(as_of=date(2026, 9, 4))
        stems = [p.stem for p in result]
        assert stems == sorted(stems)


class TestBuildRollup:
    def test_empty_input_returns_empty(self):
        assert build_rollup([]) == ''

    def test_wraps_each_day_in_section(self, tmp_path):
        _write(tmp_path / '2026-09-01.html', '<p>Mon content</p>')
        _write(tmp_path / '2026-09-02.html', '<p>Tue content</p>')
        result = build_rollup([tmp_path / '2026-09-01.html',
                                tmp_path / '2026-09-02.html'])
        assert 'Mon content' in result
        assert 'Tue content' in result
        assert 'id="day-2026-09-01"' in result
        assert 'id="day-2026-09-02"' in result

    def test_has_table_of_contents(self, tmp_path):
        _write(tmp_path / '2026-09-01.html', '<p>x</p>')
        _write(tmp_path / '2026-09-02.html', '<p>y</p>')
        result = build_rollup([tmp_path / '2026-09-01.html',
                                tmp_path / '2026-09-02.html'])
        # TOC entries link to each day
        assert '#day-2026-09-01' in result
        assert '#day-2026-09-02' in result

    def test_daily_headings_are_human_readable(self, tmp_path):
        _write(tmp_path / '2026-09-01.html', '<p>x</p>')
        result = build_rollup([tmp_path / '2026-09-01.html'])
        # e.g. "Tuesday, 1 September 2026"
        assert 'September 2026' in result

    def test_does_not_re_narrate(self, tmp_path):
        """The rollup is a concatenation, not a resummary. Nothing that
        looks like a re-computed change should appear — the original
        daily's numbers must survive verbatim in that day's section."""
        _write(tmp_path / '2026-09-01.html',
               '<p>Microsoft FX notional 60,013M</p>')
        result = build_rollup([tmp_path / '2026-09-01.html'])
        assert '60,013M' in result
