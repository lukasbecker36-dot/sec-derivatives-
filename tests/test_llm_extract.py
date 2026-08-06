"""Tests for src.llm_extract -- prompt construction, JSON parsing, retry."""

import json
import pytest
from unittest.mock import MagicMock, patch

from src.llm_extract import (
    build_extraction_prompt,
    parse_llm_response,
    extract_fields_llm,
    _infer_field_kind,
    enrich_schema,
    FIELD_KIND_HINTS,
)


class TestBuildExtractionPrompt:
    def test_contains_schema(self):
        schema = {'fx_notional': 'FX derivative notional, in millions'}
        context = {'issuer': 'Boeing', 'period_end': '2025-03-31', 'form_type': '10-Q'}
        prompt = build_extraction_prompt('Some filing text', schema, context)
        assert 'fx_notional' in prompt
        assert 'Boeing' in prompt
        assert '2025-03-31' in prompt
        assert 'Some filing text' in prompt

    def test_includes_prior_values(self):
        schema = {'total': 'Total amount'}
        context = {'prior_values': {'total': 500.0}, 'issuer': 'Test', 'period_end': '2025', 'form_type': '10-Q'}
        prompt = build_extraction_prompt('text', schema, context)
        assert '500.0' in prompt


class TestInferFieldKind:
    def test_notional_by_name(self):
        assert _infer_field_kind('fx_derivatives_notional') == 'notional'
        assert _infer_field_kind('ir_swap_notional') == 'notional'
        assert _infer_field_kind('fi_fx_designated_notional') == 'notional'

    def test_fair_value_by_name_suffix(self):
        assert _infer_field_kind('total_derivative_asset') == 'fair_value'
        assert _infer_field_kind('total_derivative_liability') == 'fair_value'
        assert _infer_field_kind('cash_equivalents_fv') == 'fair_value'

    def test_sensitivity_by_name(self):
        assert _infer_field_kind('fx_sensitivity_10pct') == 'sensitivity'
        assert _infer_field_kind('ir_sensitivity_100bp') == 'sensitivity'

    def test_aoci_by_name(self):
        assert _infer_field_kind('cash_flow_hedge_aoci') == 'aoci'

    def test_description_can_classify_when_name_is_ambiguous(self):
        assert _infer_field_kind('foo_metric',
                                 'Notional amount in millions') == 'notional'
        assert _infer_field_kind('bar_number',
                                 'Fair value of the position') == 'fair_value'

    def test_falls_back_to_other(self):
        assert _infer_field_kind('has_derivatives',
                                 'Yes/No indicator') == 'other'
        assert _infer_field_kind('principal_currency_exposures',
                                 'List of currencies') == 'other'


class TestEnrichSchema:
    def test_notional_gets_magnitude_hint(self):
        """The AT&T failure: a $36B notional lands in a fair-value field
        because the prompt gives Haiku no shape information. Enriched schema
        tells the model that fair values are ALWAYS smaller than notionals."""
        enriched = enrich_schema({'fx_derivatives_notional': 'Total FX notional'})
        e = enriched['fx_derivatives_notional']
        assert e['kind'] == 'notional'
        assert 'NOT a fair value' in e['expected_magnitude']

    def test_fair_value_hint_forbids_notional_substitution(self):
        enriched = enrich_schema({'total_derivative_asset': 'Total derivative asset'})
        e = enriched['total_derivative_asset']
        assert e['kind'] == 'fair_value'
        assert 'NEVER put a notional' in e['expected_magnitude']
        assert 'much smaller than any notional' in e['expected_magnitude']

    def test_other_kind_has_no_hint(self):
        """Fields we can't confidently classify get no magnitude expectation
        rather than a misleading one."""
        enriched = enrich_schema({'has_derivatives': 'Yes/No indicator'})
        assert enriched['has_derivatives']['kind'] == 'other'
        assert 'expected_magnitude' not in enriched['has_derivatives']

    def test_prompt_renders_kind_and_hint(self):
        schema = {'fx_derivatives_notional': 'FX notional in millions',
                  'total_derivative_asset': 'Total derivative asset'}
        prompt = build_extraction_prompt(
            'Some text', schema,
            {'issuer': 'X', 'period_end': '2026-06-30', 'form_type': '10-Q'})
        assert '"kind": "notional"' in prompt
        assert '"kind": "fair_value"' in prompt
        assert 'NOT a fair value' in prompt
        assert 'NEVER put a notional' in prompt

    def test_all_kinds_have_hints_except_other(self):
        for kind, hint in FIELD_KIND_HINTS.items():
            if kind == 'other':
                assert hint is None
            else:
                assert hint and 'NOT' in hint or 'not' in (hint or '')


class TestParseLlmResponse:
    def test_plain_json(self):
        raw = '{"fields": {"x": {"value": 100}}, "flags": [], "notes": ""}'
        result = parse_llm_response(raw)
        assert result['fields']['x']['value'] == 100

    def test_with_markdown_fences(self):
        raw = '```json\n{"fields": {"x": {"value": 42}}, "flags": [], "notes": ""}\n```'
        result = parse_llm_response(raw)
        assert result['fields']['x']['value'] == 42

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            parse_llm_response('not json at all')


class TestExtractFieldsLlm:
    def _mock_client(self, response_text):
        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(text=response_text)]
        msg.usage = MagicMock(input_tokens=100, output_tokens=50)
        client.messages.create.return_value = msg
        return client

    @patch('src.llm_extract.log_llm_usage')
    def test_successful_extraction(self, mock_log):
        response = json.dumps({
            'fields': {'fx_notional': {'value': 5000, 'confidence': 'high', 'source_quote': '$5,000'}},
            'flags': [],
            'notes': '',
        })
        client = self._mock_client(response)
        schema = {'fx_notional': 'FX notional in millions'}
        context = {'issuer': 'Test', 'period_end': '2025', 'form_type': '10-Q', 'section_name': 'deriv'}
        result = extract_fields_llm('filing text', schema, context, client=client)
        assert result['fields']['fx_notional']['value'] == 5000

    @patch('src.llm_extract.log_llm_usage')
    def test_retry_on_bad_json(self, mock_log):
        client = MagicMock()
        # First call returns bad JSON, second returns good
        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text='not json')]
        bad_msg.usage = MagicMock(input_tokens=100, output_tokens=50)
        good_msg = MagicMock()
        good_msg.content = [MagicMock(text='{"fields": {"x": {"value": 1}}, "flags": [], "notes": ""}')]
        good_msg.usage = MagicMock(input_tokens=100, output_tokens=50)
        client.messages.create.side_effect = [bad_msg, good_msg]

        schema = {'x': 'test'}
        context = {'issuer': 'Test', 'period_end': '2025', 'form_type': '10-Q', 'section_name': 'test'}
        result = extract_fields_llm('text', schema, context, client=client)
        assert result['fields']['x']['value'] == 1
        assert client.messages.create.call_count == 2

    @patch('src.llm_extract.log_llm_usage')
    def test_double_failure_returns_extraction_failed(self, mock_log):
        client = MagicMock()
        bad_msg = MagicMock()
        bad_msg.content = [MagicMock(text='garbage')]
        bad_msg.usage = MagicMock(input_tokens=100, output_tokens=50)
        client.messages.create.return_value = bad_msg

        schema = {'x': 'test', 'y': 'test2'}
        context = {'issuer': 'Test', 'period_end': '2025', 'form_type': '10-Q', 'section_name': 'test'}
        result = extract_fields_llm('text', schema, context, client=client)
        assert result['fields']['x']['confidence'] == 'extraction_failed'
        assert result['fields']['y']['confidence'] == 'extraction_failed'
