"""Tests for src.validate — sanity checks on extracted data."""

import pytest
from src.validate import validate_row, _parse_numeric
from src.config import IssuerConfig, FieldConfig


def _make_config(fields):
    fld_configs = {
        name: FieldConfig(description=desc, section='test')
        for name, desc in fields.items()
    }
    return IssuerConfig(
        issuer='Test', ticker='TEST', cik='0000000001',
        fields=fld_configs,
    )


class TestValidateRow:
    def test_completeness_failure(self):
        config = _make_config({
            'a': 'notional a', 'b': 'notional b', 'c': 'fair value c',
            'd': 'notional d', 'e': 'fair value e',
        })
        prior = {'a': '100', 'b': '200', 'c': '300', 'd': '400', 'e': '500'}
        # >30% null: 3/5 = 60%
        current = {'a': 100, 'b': None, 'c': None, 'd': None, 'e': 500}
        results = validate_row(current, prior, config)
        assert any(r['level'] == 'error' and 'completeness' in r['field'] for r in results)

    def test_negative_notional(self):
        config = _make_config({'x': 'Notional amount of FX derivatives'})
        current = {'x': -500}
        results = validate_row(current, None, config)
        assert any('Negative notional' in r['message'] for r in results)

    def test_units_mismatch(self):
        config = _make_config({'x': 'some field'})
        prior = {'x': '100'}
        current = {'x': 100000}  # 1000x
        results = validate_row(current, prior, config)
        assert any('units mismatch' in r['message'] for r in results)

    def test_plausibility_notional_swing(self):
        config = _make_config({'x': 'Notional amount'})
        prior = {'x': '1000'}
        current = {'x': 1600}  # 60% swing
        results = validate_row(current, prior, config)
        assert any('Large swing' in r['message'] for r in results)

    def test_clean_row_no_errors(self):
        config = _make_config({'x': 'Notional amount', 'y': 'Fair value'})
        prior = {'x': '1000', 'y': '500'}
        current = {'x': 1050, 'y': 520}
        results = validate_row(current, prior, config)
        errors = [r for r in results if r['level'] == 'error']
        assert len(errors) == 0
