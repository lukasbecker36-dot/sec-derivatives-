"""Tests for src.validate — sanity checks on extracted data."""

import pytest
from src.validate import (
    validate_row, validate_source_quotes, check_reconciliations, _parse_numeric,
)
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


class TestCheckReconciliations:
    def test_minimal_hedger_total_is_checked(self):
        """The MSFT case: total is fx_derivatives_notional, not total_notional.

        The old check only looked for a field named 'total_notional', so this
        contradiction shipped undetected for five consecutive quarters.
        """
        row = {
            'fx_derivatives_notional': '52784',
            'fx_designated_notional': '1492',
            'fx_not_designated_notional': '8994',
        }
        results = check_reconciliations(row)
        assert any(r['level'] == 'error' for r in results)
        assert any('Reconciliation failure' in r['message'] for r in results)

    def test_reconciling_row_passes(self):
        row = {
            'fx_derivatives_notional': '54086',
            'fx_designated_notional': '1492',
            'fx_not_designated_notional': '52594',
        }
        assert check_reconciliations(row) == []

    def test_within_tolerance_passes(self):
        row = {
            'fx_derivatives_notional': '1000',
            'fx_designated_notional': '500',
            'fx_not_designated_notional': '520',  # 2% over
        }
        assert check_reconciliations(row) == []

    def test_missing_components_are_not_flagged(self):
        """Partial disclosure must not generate noise."""
        row = {'fx_derivatives_notional': '54086'}
        assert check_reconciliations(row) == []

    def test_absent_total_is_not_flagged(self):
        row = {'fx_designated_notional': '1492',
               'fx_not_designated_notional': '52594'}
        assert check_reconciliations(row) == []

    def test_surfaces_through_validate_row(self):
        config = _make_config({
            'fx_derivatives_notional': 'Total FX notional',
            'fx_designated_notional': 'Designated FX notional',
            'fx_not_designated_notional': 'Non-designated FX notional',
        })
        row = {'fx_derivatives_notional': 52784,
               'fx_designated_notional': 1492,
               'fx_not_designated_notional': 8994}
        results = validate_row(row, None, config)
        assert any('Reconciliation failure' in r['message'] for r in results)


class TestValidateSourceQuotes:
    _schema = {
        'ir_swap_notional': 'Interest rate swap notional amount in millions',
        'cash_flow_hedge_aoci': 'Cash flow hedge fair value in AOCI, millions',
        'fx_notional': 'FX derivatives notional in millions',
        'usd_revenue_pct': 'USD revenue as a percentage of total',
    }

    def test_flags_year_as_notional(self):
        result = {'fields': {'ir_swap_notional':
                  {'value': 2025.0, 'source_quote': 'as of March 31, 2025'}}}
        flags = validate_source_quotes(result, self._schema)
        assert any('calendar year' in f for f in flags)

    def test_flags_day_of_month_with_date_context(self):
        result = {'fields': {'cash_flow_hedge_aoci':
                  {'value': 31.0, 'source_quote': 'balance at March 31, 2026'}}}
        flags = validate_source_quotes(result, self._schema)
        assert any('day-of-month' in f for f in flags)

    def test_flags_value_unsupported_by_quote(self):
        result = {'fields': {'fx_notional':
                  {'value': 500.0, 'source_quote': 'the company uses forward contracts'}}}
        flags = validate_source_quotes(result, self._schema)
        assert any('not supported' in f for f in flags)

    def test_legit_values_not_flagged(self):
        result = {'fields': {
            'ir_swap_notional': {'value': 4080.0, 'source_quote': 'notional totaling $4.08 billion'},
            'fx_notional': {'value': 30.0, 'source_quote': 'forward contracts of $30 million'},
        }}
        assert validate_source_quotes(result, self._schema) == []

    def test_percent_field_year_not_flagged(self):
        # A percentage field is exempt from the year/date checks.
        result = {'fields': {'usd_revenue_pct':
                  {'value': 2025.0, 'source_quote': 'fiscal year 2025'}}}
        assert validate_source_quotes(result, self._schema) == []

    def test_null_and_string_values_skipped(self):
        result = {'fields': {
            'ir_swap_notional': {'value': None, 'source_quote': ''},
            'fx_notional': {'value': 'Yes', 'source_quote': 'uses derivatives'},
        }}
        assert validate_source_quotes(result, self._schema) == []
