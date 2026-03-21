"""Stage 2: LLM-based structured field extraction using Claude Haiku."""

import json
import re
import logging
from pathlib import Path

import anthropic

from .utils import log_llm_usage

logger = logging.getLogger(__name__)

MODEL = 'claude-haiku-4-5-20251001'
LLM_LOG = Path(__file__).resolve().parent.parent / 'output' / 'llm_usage.log'

SYSTEM_PROMPT = """You are a financial data extraction assistant. You extract structured \
data from SEC 10-Q and 10-K filing sections. Be precise. Use null for fields \
not found in the text. All dollar amounts in millions unless the text \
explicitly states otherwise (e.g. "billion" means multiply by 1000 to store \
in millions)."""

USER_TEMPLATE = """Extract the following fields from this {form_type} filing section for \
{issuer} (period ending {period_end}).

Fields to extract:
{schema}

Prior period values (for plausibility checking):
{prior_values}
{filer_context_block}
If any extracted value differs from the prior period by more than 50%, \
add a "flag" key for that field explaining why.

Return JSON only, no preamble, no markdown fences. Format:
{{
  "fields": {{
    "field_name": {{
      "value": <number or string or null>,
      "confidence": "high" | "medium" | "low" | "not_found",
      "source_quote": "<the exact phrase you extracted this from>"
    }}
  }},
  "flags": ["<any plausibility concerns>"],
  "notes": "<anything unusual about this filing's disclosure>"
}}

--- FILING TEXT ---
{section_text}"""

RETRY_SYSTEM = """You are a financial data extraction assistant. Return ONLY valid JSON. \
No preamble, no markdown fences, no explanation. Just the JSON object."""


def build_extraction_prompt(section_text: str, schema: dict, context: dict,
                           filer_context: str = '') -> str:
    """Build the extraction prompt from section text and schema."""
    schema_json = json.dumps(
        {name: desc for name, desc in schema.items()},
        indent=2,
    )
    prior_json = json.dumps(context.get('prior_values', {}), indent=2)
    filer_block = f'\nCompany-specific patterns from prior filings:\n{filer_context}\n' if filer_context else ''
    return USER_TEMPLATE.format(
        form_type=context.get('form_type', '10-Q'),
        issuer=context.get('issuer', 'Unknown'),
        period_end=context.get('period_end', 'Unknown'),
        schema=schema_json,
        prior_values=prior_json,
        filer_context_block=filer_block,
        section_text=section_text,
    )


def parse_llm_response(raw: str) -> dict:
    """Parse LLM response, stripping markdown fences if present."""
    text = raw.strip()
    # Strip markdown code fences
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return json.loads(text)


def _compute_cost(input_tokens: int, output_tokens: int) -> float:
    """Estimate cost for Haiku."""
    # Haiku pricing: $0.80/M input, $4.00/M output (as of 2025)
    return (input_tokens * 0.80 + output_tokens * 4.00) / 1_000_000


def extract_fields_llm(
    section_text: str,
    schema: dict[str, str],
    context: dict,
    client: anthropic.Anthropic | None = None,
    filer_context: str = '',
) -> dict:
    """Send section text + output schema to Claude, get structured JSON back.

    Args:
        section_text: The cleaned text of one section (1-8K tokens).
        schema: Dict of {field_name: description} from the YAML config.
        context: {issuer, period_end, form_type, prior_values}.
        client: Optional Anthropic client (for testing/injection).
        filer_context: Optional company-specific patterns from filer profile.

    Returns:
        Dict with 'fields', 'flags', 'notes' keys. On failure, fields have
        confidence='extraction_failed'.
    """
    if client is None:
        client = anthropic.Anthropic()

    prompt = build_extraction_prompt(section_text, schema, context, filer_context=filer_context)
    issuer = context.get('issuer', 'unknown')
    section_name = context.get('section_name', 'unknown')

    for attempt in range(2):
        try:
            sys_prompt = SYSTEM_PROMPT if attempt == 0 else RETRY_SYSTEM
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=sys_prompt,
                messages=[{'role': 'user', 'content': prompt}],
            )
            raw_text = response.content[0].text
            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens

            log_llm_usage(
                LLM_LOG, issuer, section_name, MODEL,
                input_tokens, output_tokens,
                _compute_cost(input_tokens, output_tokens),
            )

            result = parse_llm_response(raw_text)

            # Validate structure
            if 'fields' not in result:
                raise ValueError("Response missing 'fields' key")

            return result

        except (json.JSONDecodeError, ValueError) as e:
            if attempt == 0:
                logger.warning(f'LLM JSON parse failed for {issuer}/{section_name}, retrying: {e}')
                continue
            else:
                logger.error(f'LLM extraction failed for {issuer}/{section_name}: {e}')
                logger.error(f'Raw response: {raw_text[:500]}')
                # Return failure structure
                failed_fields = {
                    name: {'value': None, 'confidence': 'extraction_failed', 'source_quote': ''}
                    for name in schema
                }
                return {
                    'fields': failed_fields,
                    'flags': [f'extraction_failed: {e}'],
                    'notes': f'LLM extraction failed after retry: {e}',
                }

        except Exception as e:
            logger.error(f'LLM API error for {issuer}/{section_name}: {e}')
            failed_fields = {
                name: {'value': None, 'confidence': 'extraction_failed', 'source_quote': ''}
                for name in schema
            }
            return {
                'fields': failed_fields,
                'flags': [f'api_error: {e}'],
                'notes': f'LLM API error: {e}',
            }
