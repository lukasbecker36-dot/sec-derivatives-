"""Tests for src/filer_profile.py — per-CIK filer profile management."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.filer_profile import (
    load_profile, save_profile, create_initial_profile, get_or_create_profile,
    extract_structural_features, extract_language_patterns,
    update_profile_after_extraction, build_prompt_context,
    resolve_alias, _merge_list_dedup, _pad_cik,
)


@pytest.fixture
def tmp_profiles(tmp_path, monkeypatch):
    profiles_dir = tmp_path / 'filer_profiles'
    profiles_dir.mkdir()
    monkeypatch.setattr('src.filer_profile.FILER_PROFILES_DIR', profiles_dir)
    return profiles_dir


class TestLoadSave:
    def test_load_nonexistent(self, tmp_profiles):
        assert load_profile('999999') is None

    def test_create_and_load(self, tmp_profiles):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        assert profile['cik'] == '0000012927'
        assert profile['ticker'] == 'BA'
        assert profile['history'] == []

        save_profile(profile)
        loaded = load_profile('12927')
        assert loaded is not None
        assert loaded['cik'] == '0000012927'
        assert loaded['issuer_name'] == 'Boeing'

    def test_roundtrip(self, tmp_profiles):
        profile = create_initial_profile('320193', 'AAPL', 'Apple Inc.')
        profile['idiosyncrasies']['recurring_phrases'] = ['currency headwinds']
        save_profile(profile)

        loaded = load_profile('320193')
        assert loaded['idiosyncrasies']['recurring_phrases'] == ['currency headwinds']


class TestGetOrCreate:
    def test_creates_new(self, tmp_profiles):
        profile = get_or_create_profile('12927', 'BA', 'Boeing')
        assert profile['ticker'] == 'BA'

    def test_loads_existing(self, tmp_profiles):
        original = create_initial_profile('12927', 'BA', 'Boeing')
        original['idiosyncrasies']['recurring_phrases'] = ['test phrase']
        save_profile(original)

        loaded = get_or_create_profile('12927', 'BA', 'Boeing')
        assert loaded['idiosyncrasies']['recurring_phrases'] == ['test phrase']


class TestMergeListDedup:
    def test_deduplicates(self):
        assert _merge_list_dedup(['a', 'b'], ['B', 'c']) == ['a', 'b', 'c']

    def test_empty(self):
        assert _merge_list_dedup([], ['a']) == ['a']
        assert _merge_list_dedup(['a'], []) == ['a']


class TestExtractStructuralFeatures:
    def test_finds_headings(self):
        filing_text = """
        Note 12 — Derivative Financial Instruments
        The company uses FX forwards...
        """
        sections = {
            'derivatives_note': 'Note 12 — Derivative Financial Instruments\nThe company uses FX forwards...',
        }
        config = MagicMock()

        result = extract_structural_features(filing_text, sections, config)
        ds = result['document_structure']
        assert 'Derivative Financial Instruments' in ds['derivatives_note_heading']

    def test_empty_sections(self):
        result = extract_structural_features('', {}, MagicMock())
        assert result['document_structure']['derivatives_note_heading'] == ''


class TestExtractLanguagePatterns:
    def test_finds_hedging_phrases(self):
        sections = {
            'derivatives_note': 'The company uses cash flow hedging and net investment hedges to manage risk.',
        }
        result = extract_language_patterns('', sections, {})
        phrases = result['idiosyncrasies']['recurring_phrases']
        assert any('cash flow' in p for p in phrases)
        assert any('net investment' in p for p in phrases)

    def test_finds_non_gaap(self):
        sections = {
            'market_risk': 'We report adjusted EBITDA and constant-currency revenue growth.',
        }
        result = extract_language_patterns('', sections, {})
        non_gaap = result['idiosyncrasies']['non_gaap_metrics']
        assert any('adjusted ebitda' in m for m in non_gaap)


class TestUpdateProfileAfterExtraction:
    def test_appends_history(self, tmp_profiles):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        filing_meta = {'period_end': '2025-03-31', 'form_type': '10-Q'}
        sections = {'derivatives_note': 'Some text about derivatives'}
        llm_results = {
            'derivatives_note': {
                'fields': {
                    'fx_notional': {'value': 1000, 'confidence': 'high'},
                    'ir_notional': {'value': None, 'confidence': 'not_found'},
                },
                'notes': '',
            }
        }

        updated = update_profile_after_extraction(
            profile, filing_meta, '', sections, llm_results, MagicMock()
        )
        assert len(updated['history']) == 1
        assert updated['history'][0]['period_end'] == '2025-03-31'
        assert updated['history'][0]['fields_extracted'] == 1
        assert updated['history'][0]['fields_null'] == 1

    def test_idempotent(self, tmp_profiles):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        filing_meta = {'period_end': '2025-03-31', 'form_type': '10-Q'}
        sections = {'derivatives_note': 'text'}
        llm_results = {'derivatives_note': {'fields': {}, 'notes': ''}}

        updated = update_profile_after_extraction(
            profile, filing_meta, '', sections, llm_results, MagicMock()
        )
        assert len(updated['history']) == 1

        # Process same period again — should not duplicate
        updated2 = update_profile_after_extraction(
            updated, filing_meta, '', sections, llm_results, MagicMock()
        )
        assert len(updated2['history']) == 1

    def test_merges_phrases_dedup(self, tmp_profiles):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        profile['idiosyncrasies']['recurring_phrases'] = ['cash flow hedging']

        filing_meta = {'period_end': '2025-03-31', 'form_type': '10-Q'}
        sections = {'derivatives_note': 'Uses cash flow hedging and net investment hedges.'}
        llm_results = {'derivatives_note': {'fields': {}, 'notes': ''}}

        updated = update_profile_after_extraction(
            profile, filing_meta, '', sections, llm_results, MagicMock()
        )
        phrases = updated['idiosyncrasies']['recurring_phrases']
        # Should not duplicate 'cash flow hedging'
        assert phrases.count('cash flow hedging') == 1


class TestBuildPromptContext:
    def test_none_profile(self):
        assert build_prompt_context(None) == ''

    def test_empty_profile(self):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        assert build_prompt_context(profile) == ''

    def test_with_data(self):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        profile['document_structure']['derivatives_note_heading'] = 'Note 12 — Derivatives'
        profile['idiosyncrasies']['recurring_phrases'] = ['cash flow hedging', 'net investment hedges']

        context = build_prompt_context(profile)
        assert 'Boeing' in context
        assert 'Note 12' in context
        assert 'cash flow hedging' in context


class TestResolveAlias:
    def test_finds_by_alias(self, tmp_profiles):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        profile['aliases'] = ['BOE']
        save_profile(profile)

        assert resolve_alias('BOE', tmp_profiles) == '0000012927'

    def test_finds_by_ticker(self, tmp_profiles):
        profile = create_initial_profile('12927', 'BA', 'Boeing')
        save_profile(profile)

        assert resolve_alias('BA', tmp_profiles) == '0000012927'

    def test_not_found(self, tmp_profiles):
        assert resolve_alias('NOPE', tmp_profiles) is None
