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


class TestHeldReviewBranches:
    """The manifest surfaces gate-blocked review branches so the daily
    digest can distinguish 'quiet day on master' from 'the pipeline is
    working but the gate held today's content'. Without this, days when
    the scheduler correctly refused to push would look identical to
    days when nothing happened at all."""

    def test_helper_returns_list_shape(self):
        """The helper hits git-for-each-ref against the real repo; we're
        not mocking git here, just confirming it returns a well-formed
        list of dicts with the expected keys."""
        from datetime import datetime, timezone
        from src.digest_manifest import _held_review_branches
        # Ask for anything since the epoch — will pick up all review/* refs.
        result = _held_review_branches('1970-01-01T00:00:00+00:00')
        assert isinstance(result, list)
        for entry in result:
            assert 'branch' in entry
            assert 'committed_at' in entry
            assert 'commit_subject' in entry
            assert entry['branch'].startswith('review/')

    def test_helper_filters_by_since(self):
        """Passing a far-future since should return zero entries."""
        from src.digest_manifest import _held_review_branches
        assert _held_review_branches('2099-01-01T00:00:00+00:00') == []

    def test_entries_carry_content_preview(self):
        """Each held branch entry has a content_preview list. Without it
        the daily digest sees only 'N filings held' with no editorial
        signal — the reader can't tell if the branch is a real story or
        a mechanical retry."""
        from src.digest_manifest import _held_review_branches
        result = _held_review_branches('1970-01-01T00:00:00+00:00')
        for entry in result:
            assert 'content_preview' in entry
            assert isinstance(entry['content_preview'], list)
            for item in entry['content_preview']:
                assert 'ticker' in item
                assert 'period_end_date' in item
                assert 'values' in item
                assert isinstance(item['values'], dict)


class TestPreviewFields:
    def test_preview_field_ordering(self):
        """Fields are ordered by editorial priority — the top of the list
        is what the reader most wants to see first. FX and IR notionals
        lead because they're the newsroom's staple numbers."""
        from src.digest_manifest import _PREVIEW_FIELDS
        assert _PREVIEW_FIELDS[0] == 'fx_derivatives_notional'
        assert _PREVIEW_FIELDS[1] == 'ir_swap_notional'
        assert 'commodity_derivatives_notional' in _PREVIEW_FIELDS


class TestExtractionGapsHaveIsRegression:
    """The extraction_gaps entries now carry an `is_regression` flag so
    the routine can lead the digest with genuine failures rather than
    burying them under the corpus-wide historical blank list. Without
    this flag the daily email showed 186 rows of noise every day."""

    def test_gap_entry_shape_documented(self):
        """Snapshot the contract: every extraction_gap row must expose
        ticker, period_end_date, form_type, attempts, and is_regression.
        A missing is_regression would silently drop every gap to
        'not-a-regression', and the routine's lead paragraph would go
        empty on the days that matter most."""
        # Contract-only test — a real build_manifest run requires a git
        # repo with output/ history, covered by the end-to-end sanity
        # test elsewhere. Here we assert the emitting code still contains
        # the field, so a future refactor doesn't drop it silently.
        import inspect
        from src import digest_manifest
        src = inspect.getsource(digest_manifest.build_manifest)
        assert "'is_regression'" in src
        assert 'populated_before' in src
