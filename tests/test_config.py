"""Tests for src.config — YAML loading, archetype merge, config parsing."""

import pytest
from pathlib import Path
from src.config import load_config, list_issuers, _deep_merge, IssuerConfig


class TestDeepMerge:
    def test_simple_override(self):
        base = {'a': 1, 'b': 2}
        override = {'b': 3, 'c': 4}
        result = _deep_merge(base, override)
        assert result == {'a': 1, 'b': 3, 'c': 4}

    def test_nested_merge(self):
        base = {'sections': {'a': {'heading': 'foo', 'max_length': 100}}}
        override = {'sections': {'a': {'max_length': 200}, 'b': {'heading': 'bar'}}}
        result = _deep_merge(base, override)
        assert result['sections']['a']['heading'] == 'foo'
        assert result['sections']['a']['max_length'] == 200
        assert result['sections']['b']['heading'] == 'bar'

    def test_override_wins_on_scalar(self):
        base = {'x': 'old'}
        override = {'x': 'new'}
        assert _deep_merge(base, override)['x'] == 'new'


class TestLoadConfig:
    def test_load_meta(self):
        meta_path = Path(__file__).resolve().parent.parent / 'profiles' / 'meta.yaml'
        if not meta_path.exists():
            pytest.skip('meta.yaml not found')
        config = load_config(meta_path)
        assert config.issuer == 'Meta Platforms'
        assert config.cik == '0001326801'
        assert config.extraction_mode == 'llm'
        # Should have fields from archetype + overrides
        assert 'has_derivatives' in config.fields
        assert 'market_risk' in config.sections

    def test_load_boeing(self):
        boeing_path = Path(__file__).resolve().parent.parent / 'profiles' / 'boeing.yaml'
        if not boeing_path.exists():
            pytest.skip('boeing.yaml not found')
        config = load_config(boeing_path)
        assert config.issuer == 'Boeing'
        assert config.ticker == 'BA'
        assert 'derivatives_note' in config.sections
        assert 'fx_designated_notional' in config.fields


class TestListIssuers:
    def test_excludes_archetypes(self):
        profiles = Path(__file__).resolve().parent.parent / 'profiles'
        if not profiles.exists():
            pytest.skip('profiles dir not found')
        issuers = list_issuers(profiles)
        for p in issuers:
            assert not p.name.startswith('_')
