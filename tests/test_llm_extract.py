"""Tests for src.llm_extract -- prompt construction, JSON parsing, retry."""

import json
import pytest
from unittest.mock import MagicMock, patch

from src.llm_extract import (
    build_extraction_prompt,
    parse_llm_response,
    extract_fields_llm,
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
