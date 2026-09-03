"""Tests for src.digest_manifest — the diff helper the daily digest routine
reads to know what changed on master in the last 24 hours.

Kept narrow on unit scope: the helper functions (_classify, _row_moves,
numeric parsing) get direct tests; the full build_manifest path is covered
by an end-to-end sanity check in a git-repo tmp env.
"""

import pytest

from src.digest_manifest import (
    _classify, _row_moves, numeric, flat, ASSET_PATTERNS,
)


class TestClassify:
    def test_fx_fields(self):
        assert _classify('fx_derivatives_notional') == 'fx'
        assert _classify('fx_designated_notional') == 'fx'
        assert _classify('cross_currency_swaps_notional') == 'fx'
        assert _classify('foreign_exchange_forwards_outstanding') == 'fx'

    def test_ir_fields(self):
        assert _classify('ir_swap_notional') == 'ir'
        assert _classify('interest_rate_swaps_outstanding') == 'ir'
        assert _classify('ir_swaption_notional') == 'ir'
        assert _classify('treasury_lock_notional') == 'ir'

    def test_commodity_fields(self):
        assert _classify('commodity_derivatives_notional') == 'commodity'
        assert _classify('fuel_hedge_pct_by_year') == 'commodity'
        assert _classify('natural_gas_notional_bcf') == 'commodity'
        assert _classify('crude_oil_call_options_barrels') == 'commodity'

    def test_equity_fields(self):
        assert _classify('equity_derivatives_notional') == 'equity'

    def test_credit_fields(self):
        assert _classify('credit_default_swap_notional') == 'credit'
        assert _classify('credit_spread_sensitivity') == 'credit'

    def test_unknown_falls_to_other(self):
        """Uncatalogued fields must not silently be reclassified — they go
        to 'other' so the routine can decide whether to surface them."""
        assert _classify('has_derivatives') == 'other'
        assert _classify('processed_at') == 'other'


class TestNumeric:
    def test_parses_valid_number(self):
        assert numeric('100') == 100.0
        assert numeric('1,000') == 1000.0
        assert numeric('-500.5') == -500.5

    def test_string_returns_none(self):
        assert numeric('Yes') is None
        assert numeric('') is None

    def test_none_returns_none(self):
        assert numeric(None) is None


class TestFlat:
    def test_string_passes_through(self):
        assert flat('hello') == 'hello'

    def test_list_joined(self):
        assert flat(['a', '', 'b']) == 'a b'

    def test_none_becomes_empty(self):
        assert flat(None) == ''


class TestRowMoves:
    def test_groups_by_asset_class(self):
        curr = {
            'fx_derivatives_notional': '60000',
            'ir_swap_notional': '5000',
            'commodity_derivatives_notional': '300',
        }
        prior = {
            'fx_derivatives_notional': '54000',
            'ir_swap_notional': '5000',
        }
        moves = _row_moves(prior, curr)
        assert 'fx' in moves
        assert 'ir' in moves
        assert 'commodity' in moves
        assert moves['fx'][0]['field'] == 'fx_derivatives_notional'

    def test_computes_pct_when_prior_exists(self):
        moves = _row_moves(
            {'fx_derivatives_notional': '10000'},
            {'fx_derivatives_notional': '15000'},
        )
        assert moves['fx'][0]['pct'] == 50.0

    def test_omits_pct_when_no_prior(self):
        moves = _row_moves({}, {'fx_derivatives_notional': '15000'})
        assert 'pct' not in moves['fx'][0]

    def test_metadata_fields_skipped(self):
        """extraction_attempts, processed_at etc must not surface as moves."""
        moves = _row_moves(
            {'extraction_attempts': '1'},
            {'extraction_attempts': '2', 'processed_at': '2026-09-03'},
        )
        assert moves == {}

    def test_string_fields_skipped(self):
        moves = _row_moves(
            {'has_derivatives': 'No'},
            {'has_derivatives': 'Yes'},
        )
        assert moves == {}

    def test_empty_result_prunes_empty_classes(self):
        """The output only contains asset classes that actually have moves.
        A digest section with 'no moves' would be redundant with the
        routine's own 'quiet in this asset class' handling."""
        moves = _row_moves({}, {'fx_derivatives_notional': '100'})
        assert list(moves.keys()) == ['fx']
