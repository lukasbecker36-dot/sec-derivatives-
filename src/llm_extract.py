"""Stage 2: LLM-based structured field extraction — Anthropic or OpenAI backend."""

import json
import re
import logging
from pathlib import Path
from typing import Any

from .utils import log_llm_usage

logger = logging.getLogger(__name__)

# Default models per provider
ANTHROPIC_MODEL = 'claude-haiku-4-5-20251001'
OPENAI_MODEL = 'gpt-4o-mini'

# Active provider: 'anthropic' or 'openai'
_provider = 'anthropic'
_client: Any = None  # shared client instance

# Pluggable override — when set, extract_fields_llm() delegates to this
# instead of calling the API.  Used by cc_bridge.py.
_override_fn = None


def set_override(fn):
    """Replace the LLM extraction backend.

    fn(section_text, schema, context, filer_context) -> dict with
    'fields', 'flags', 'notes' keys.  Pass None to restore API mode.
    """
    global _override_fn
    _override_fn = fn


def set_provider(provider: str, api_key: str | None = None, model: str | None = None):
    """Switch LLM backend. Call once at startup before any extractions.

    Args:
        provider: 'anthropic' or 'openai'
        api_key: Optional API key (falls back to env var ANTHROPIC_API_KEY / OPENAI_API_KEY)
        model: Optional model override
    """
    global _provider, _client, ANTHROPIC_MODEL, OPENAI_MODEL
    _provider = provider
    if provider == 'openai':
        import openai
        _client = openai.OpenAI(api_key=api_key) if api_key else openai.OpenAI()
        if model:
            OPENAI_MODEL = model
        logger.info(f'LLM backend: OpenAI ({OPENAI_MODEL})')
    else:
        import anthropic
        _client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()
        if model:
            ANTHROPIC_MODEL = model
        logger.info(f'LLM backend: Anthropic ({ANTHROPIC_MODEL})')


LLM_LOG = Path(__file__).resolve().parent.parent / 'output' / 'llm_usage.log'

SYSTEM_PROMPT = """You are a financial data extraction assistant working for a derivatives \
journalist at Risk.net. You extract structured data from SEC 10-Q and 10-K \
filing sections. Be precise. Use null for fields not found in the text. All \
dollar amounts in millions unless the text explicitly states otherwise \
(e.g. "billion" means multiply by 1000 to store in millions).

Beyond extracting the requested fields, you also watch for anything \
editorially interesting about the company's derivatives and hedging activity. \
Flag noteworthy items in the "flags" and "notes" fields of your response."""

USER_TEMPLATE = """Extract the following fields from this {form_type} filing section for \
{issuer} (period ending {period_end}).

Fields to extract:
{schema}

Prior period values (for plausibility checking):
{prior_values}
{filer_context_block}
If any extracted value differs from the prior period by more than 50%, \
add a "flag" key for that field explaining why.

Also flag anything a derivatives journalist at Risk.net would find \
newsworthy. In particular watch for:
- Deal-contingent hedges or M&A-linked derivatives
- New hedging programmes or instruments the company hasn't used before
- Programmes being wound down or discontinued
- Hedge accounting de-designations or ineffectiveness
- Novations, terminations, or restructuring of derivative positions
- Central clearing changes, margin calls, collateral disputes
- CVA/DVA/XVA adjustments that moved materially
- Unusual counterparty concentration or credit concerns
- Exotic or structured products (TRS, CDS, cross-currency swaps, knock-ins)
- Embedded derivatives being bifurcated
- Regulatory references (Dodd-Frank, EMIR, margin rules)
- Any management commentary explaining WHY hedging strategy changed

Return JSON only, no preamble, no markdown fences. Format:
{{
  "fields": {{
    "field_name": {{
      "value": <number or string or null>,
      "confidence": "high" | "medium" | "low" | "not_found",
      "source_quote": "<the exact phrase you extracted this from>"
    }}
  }},
  "flags": ["<any plausibility concerns or editorial flags>"],
  "notes": "<anything unusual or newsworthy about this filing's disclosure>"
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


def _is_retryable_error(exc: Exception) -> bool:
    """True for transient API failures worth retrying on a later run.

    Overload (529), rate-limit (429), and 5xx/timeout/connection errors clear
    on their own, so the caller should leave the filing unprocessed and retry
    rather than persisting a null row that blocks it forever. A 400/401/404 is
    a real problem with the request and should not loop.
    """
    status = getattr(exc, 'status_code', None) or getattr(exc, 'status', None)
    if status in (408, 409, 429, 500, 502, 503, 504, 529):
        return True
    msg = str(exc).lower()
    return any(k in msg for k in (
        'overload', 'rate limit', 'ratelimit', 'timeout', 'timed out',
        'temporarily', 'connection', 'unavailable',
        '429', '500', '502', '503', '504', '529',
    ))


def parse_llm_response(raw: str) -> dict:
    """Parse LLM response, stripping markdown fences if present."""
    text = raw.strip()
    # Strip markdown code fences
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return json.loads(text)


def _compute_cost(input_tokens: int, output_tokens: int) -> float:
    if _provider == 'openai':
        # gpt-4o-mini: $0.15/M input, $0.60/M output
        return (input_tokens * 0.15 + output_tokens * 0.60) / 1_000_000
    # Haiku: $0.80/M input, $4.00/M output
    return (input_tokens * 0.80 + output_tokens * 4.00) / 1_000_000


def _call_llm(sys_prompt: str, user_prompt: str, client: Any) -> tuple[str, int, int]:
    """Call the active provider. Returns (raw_text, input_tokens, output_tokens)."""
    if _provider == 'openai':
        active_client = client or _client
        if active_client is None:
            import openai
            active_client = openai.OpenAI()
        response = active_client.chat.completions.create(
            model=OPENAI_MODEL,
            max_tokens=4096,
            messages=[
                {'role': 'system', 'content': sys_prompt},
                {'role': 'user', 'content': user_prompt},
            ],
        )
        raw = response.choices[0].message.content
        inp = response.usage.prompt_tokens
        out = response.usage.completion_tokens
    else:
        import anthropic as _anthropic
        active_client = client or _client
        if active_client is None:
            active_client = _anthropic.Anthropic()
        response = active_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=4096,
            system=sys_prompt,
            messages=[{'role': 'user', 'content': user_prompt}],
        )
        raw = response.content[0].text
        inp = response.usage.input_tokens
        out = response.usage.output_tokens
    return raw, inp, out


def extract_fields_llm(
    section_text: str,
    schema: dict[str, str],
    context: dict,
    client: Any = None,
    filer_context: str = '',
) -> dict:
    """Send section text + output schema to the active LLM, get structured JSON back.

    Args:
        section_text: The cleaned text of one section (1-8K tokens).
        schema: Dict of {field_name: description} from the YAML config.
        context: {issuer, period_end, form_type, prior_values}.
        client: Optional client instance (Anthropic or OpenAI).
        filer_context: Optional company-specific patterns from filer profile.

    Returns:
        Dict with 'fields', 'flags', 'notes' keys. On failure, fields have
        confidence='extraction_failed'.
    """
    if _override_fn is not None:
        return _override_fn(section_text, schema, context, filer_context)

    prompt = build_extraction_prompt(section_text, schema, context, filer_context=filer_context)
    issuer = context.get('issuer', 'unknown')
    section_name = context.get('section_name', 'unknown')
    active_model = OPENAI_MODEL if _provider == 'openai' else ANTHROPIC_MODEL

    for attempt in range(2):
        try:
            sys_prompt = SYSTEM_PROMPT if attempt == 0 else RETRY_SYSTEM
            raw_text, input_tokens, output_tokens = _call_llm(sys_prompt, prompt, client)

            log_llm_usage(
                LLM_LOG, issuer, section_name, active_model,
                input_tokens, output_tokens,
                _compute_cost(input_tokens, output_tokens),
            )

            result = parse_llm_response(raw_text)

            if 'fields' not in result:
                raise ValueError("Response missing 'fields' key")

            return result

        except (json.JSONDecodeError, ValueError) as e:
            if attempt == 0:
                logger.warning(f'LLM JSON parse failed for {issuer}/{section_name}, retrying: {e}')
                continue
            else:
                logger.error(f'LLM extraction failed for {issuer}/{section_name}: {e}')
                failed_fields = {
                    name: {'value': None, 'confidence': 'extraction_failed', 'source_quote': ''}
                    for name in schema
                }
                return {
                    'fields': failed_fields,
                    'flags': [f'extraction_failed: {e}'],
                    'notes': f'LLM extraction failed after retry: {e}',
                    'retryable': True,
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
                'retryable': _is_retryable_error(e),
            }
