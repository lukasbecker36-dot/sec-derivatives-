"""Tests for src.change_detect — threshold logic and alert types."""

import pytest
from src.change_detect import detect_changes, _parse_numeric
from src.config import IssuerConfig, FieldConfig


def _make_config(fields, thresholds=None):
    fld_configs = {
        name: FieldConfig(description=desc, section='test')
        for name, desc in fields.items()
    }
    return IssuerConfig(
        issuer='Test', ticker='TEST', cik='0000000001',
        fields=fld_configs,
        alert_thresholds=thresholds or {},
    )


class TestParseNumeric:
    def test_float(self):
        assert _parse_numeric(100.5) == 100.5

    def test_string_float(self):
        assert _parse_numeric('100.5') == 100.5

    def test_none(self):
        assert _parse_numeric(None) is None

    def test_empty(self):
        assert _parse_numeric('') is None

    def test_invalid(self):
        assert _parse_numeric('not a number') is None


class TestDetectChanges:
    def test_no_prior(self):
        config = _make_config({'x': 'test'})
        alerts = detect_changes({'x': 100}, None, config)
        assert alerts == []

    def test_numeric_change_above_threshold(self):
        config = _make_config({'x': 'Notional amount'})
        prior = {'x': '100'}
        current = {'x': 130}
        alerts = detect_changes(current, prior, config)
        assert any('[NUMERIC]' in a for a in alerts)

    def test_no_alert_below_threshold(self):
        config = _make_config({'x': 'Notional amount'})
        prior = {'x': '100'}
        current = {'x': 110}
        alerts = detect_changes(current, prior, config)
        assert not any('[NUMERIC]' in a for a in alerts)

    def test_dropped_to_zero(self):
        config = _make_config({'x': 'Notional amount'})
        prior = {'x': '500'}
        current = {'x': 0}
        alerts = detect_changes(current, prior, config)
        assert any('[DROPPED_TO_ZERO]' in a for a in alerts)

    def test_new_field(self):
        config = _make_config({'x': 'Notional amount'})
        prior = {'x': None}
        current = {'x': 500}
        alerts = detect_changes(current, prior, config)
        assert any('[NEW_FIELD]' in a for a in alerts)

    def test_disappeared_field(self):
        config = _make_config({'x': 'Notional amount'})
        prior = {'x': '500'}
        current = {'x': None}
        alerts = detect_changes(current, prior, config)
        assert any('[DISAPPEARED_FIELD]' in a for a in alerts)

    def test_custom_threshold(self):
        config = _make_config({'x': 'Notional amount'}, thresholds={'x': 0.10})
        prior = {'x': '100'}
        current = {'x': 112}
        alerts = detect_changes(current, prior, config)
        assert any('[NUMERIC]' in a for a in alerts)  # 12% > 10%
