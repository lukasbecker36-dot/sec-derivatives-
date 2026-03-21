"""Tests for src.qualitative — keyword matching and [NEW] tagging."""

import pytest
from src.qualitative import extract_qualitative, tag_new_sentences
from src.config import IssuerConfig, QualitativeConfig, SectionConfig, FieldConfig


def _make_config(categories):
    return IssuerConfig(
        issuer='Test',
        ticker='TEST',
        cik='0000000001',
        qualitative=QualitativeConfig(
            sections_to_search=['market_risk'],
            categories=categories,
        ),
        sections={'market_risk': SectionConfig(heading='test')},
    )


class TestExtractQualitative:
    def test_matches_keywords(self):
        config = _make_config({
            'Interest rate risk': ['interest rate', 'basis point'],
        })
        sections = {
            'market_risk': 'The interest rate exposure is significant. '
                           'A 100 basis point move would cost $300 million.',
        }
        result = extract_qualitative(sections, config)
        assert 'Interest rate risk' in result
        assert len(result['Interest rate risk']) >= 1

    def test_no_matches(self):
        config = _make_config({
            'Exotic': ['swaption', 'barrier option'],
        })
        sections = {'market_risk': 'Simple text with no exotic instruments.'}
        result = extract_qualitative(sections, config)
        assert 'Exotic' not in result

    def test_dedup_by_80_chars(self):
        config = _make_config({
            'FX': ['foreign currency'],
        })
        # Same first 80 chars, different ending
        sections = {
            'market_risk': (
                'The foreign currency exposure arising from our international operations '
                'is managed through natural hedges and forward contracts in various currencies. '
                'The foreign currency exposure arising from our international operations '
                'is managed differently in 2025.'
            ),
        }
        result = extract_qualitative(sections, config)
        assert 'FX' in result
        # Should be deduped to 1 sentence
        assert len(result['FX']) == 1


class TestTagNewSentences:
    def test_tags_new(self):
        current = {
            'FX': ['The company entered into new FX forward contracts in 2025.'],
        }
        prior_text = 'The company had no derivative instruments outstanding.'
        result = tag_new_sentences(current, prior_text)
        assert result['FX'][0].startswith('[NEW]')

    def test_no_tag_if_same(self):
        sent = 'The company uses interest rate swaps to hedge its exposure.'
        current = {'IR': [sent]}
        prior_text = sent
        result = tag_new_sentences(current, prior_text)
        assert not result['IR'][0].startswith('[NEW]')

    def test_no_tag_if_no_prior(self):
        current = {'FX': ['Some sentence about FX.']}
        result = tag_new_sentences(current, '')
        assert not result['FX'][0].startswith('[NEW]')
