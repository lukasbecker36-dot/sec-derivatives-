"""Tests for src.utils — dollar parsing, text cleaning, normalisation."""

import pytest
from src.utils import parse_dollar, extract_first_dollar, clean_filing_text, normalise_for_comparison, extract_sentences


class TestParseDollar:
    def test_simple(self):
        assert parse_dollar('$2,800') == 2800.0

    def test_negative_parens(self):
        assert parse_dollar('($39)') == -39.0

    def test_em_dash(self):
        assert parse_dollar('—') == 0
        assert parse_dollar('–') == 0
        assert parse_dollar('\u2014') == 0

    def test_none(self):
        assert parse_dollar(None) is None

    def test_empty(self):
        assert parse_dollar('') == 0
        assert parse_dollar('  ') == 0

    def test_non_breaking_space(self):
        assert parse_dollar('$\xa01,500') == 1500.0

    def test_negative_with_dollar(self):
        assert parse_dollar('($ 39)') == -39.0

    def test_float_value(self):
        assert parse_dollar('$1,234.56') == 1234.56

    def test_invalid_string(self):
        assert parse_dollar('abc') is None

    def test_plain_number(self):
        assert parse_dollar('500') == 500.0

    def test_hyphen_dash(self):
        assert parse_dollar('-') == 0


class TestExtractFirstDollar:
    def test_finds_first(self):
        assert extract_first_dollar('The total was $5,000 and $3,000') == 5000.0

    def test_no_match(self):
        assert extract_first_dollar('no dollars here') is None


class TestCleanFilingText:
    def test_strips_html(self):
        html = '<html><body><p>Hello</p><p>World</p></body></html>'
        assert 'Hello' in clean_filing_text(html)
        assert '<p>' not in clean_filing_text(html)

    def test_removes_toc_artifacts(self):
        html = '<html><body>42 Table of Contents Some text</body></html>'
        result = clean_filing_text(html)
        assert 'Table of Contents' not in result

    def test_non_breaking_space(self):
        html = '<html><body>Item\xa03</body></html>'
        assert '\xa0' not in clean_filing_text(html)


class TestNormalise:
    def test_dates_replaced(self):
        text = 'As of March 31, 2025, the company had...'
        assert '[DATE]' in normalise_for_comparison(text)

    def test_amounts_replaced(self):
        text = 'The total was $5,000 million.'
        assert '$[AMT]' in normalise_for_comparison(text)

    def test_quarter_replaced(self):
        text = 'In Q1 2025, revenue grew.'
        assert '[DATE]' in normalise_for_comparison(text)


class TestExtractSentences:
    def test_splits_sentences(self):
        text = 'This is a reasonably long first sentence here. This is a reasonably long second sentence too. And this third one is also long enough to pass the filter.'
        sents = extract_sentences(text)
        assert len(sents) >= 2

    def test_filters_short(self):
        text = 'OK. This is a longer sentence that should be kept.'
        sents = extract_sentences(text)
        assert all(len(s) > 20 for s in sents)
