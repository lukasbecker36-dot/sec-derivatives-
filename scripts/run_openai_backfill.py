"""Run OpenAI (default gpt-4o-mini) against backfill request files.

Reads backfill/requests/*.json, writes results to backfill/results_openai/
by default (so it does not clobber existing ground truth in backfill/results/).
Use --output-dir backfill/results to overwrite the live results directory.

Requires OPENAI_API_KEY in the environment.

Usage:
    python scripts/run_openai_backfill.py --tickers ABBV,NUE
    python scripts/run_openai_backfill.py --all
    python scripts/run_openai_backfill.py --tickers ABBV --output-dir backfill/results
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import llm_extract

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('openai_backfill')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REQUESTS_DIR = PROJECT_ROOT / 'backfill' / 'requests'


def _load_request(path: Path) -> dict:
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def _save_result(out_dir: Path, name: str, result: dict):
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / name, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)


def process_locate(req: dict, client) -> tuple[dict, int, int]:
    """Send the prepared locate prompt to OpenAI, return parsed JSON + tokens."""
    sys_prompt = (
        'You locate sections inside SEC filings. Return ONLY a JSON object '
        '(no markdown fences, no preamble). The required keys are documented '
        'in the user prompt.'
    )
    response = client.chat.completions.create(
        model=llm_extract.OPENAI_MODEL,
        max_tokens=512,
        response_format={'type': 'json_object'},
        messages=[
            {'role': 'system', 'content': sys_prompt},
            {'role': 'user', 'content': req['prompt']},
        ],
    )
    raw = response.choices[0].message.content
    result = llm_extract.parse_llm_response(raw)
    return result, response.usage.prompt_tokens, response.usage.completion_tokens


def process_extraction(req: dict, client) -> tuple[dict, int, int]:
    """Build the standard extraction prompt and send to OpenAI."""
    schema = req['schema']
    context = {
        'form_type': req.get('form_type', '10-Q'),
        'issuer': req.get('issuer', 'Unknown'),
        'period_end': req.get('period_end', ''),
        'section_name': req.get('section_name', ''),
        'prior_values': req.get('prior_values', {}),
    }
    user_prompt = llm_extract.build_extraction_prompt(
        req['section_text'], schema, context,
        filer_context=req.get('filer_context', ''),
    )
    # Add a brief instruction to bias toward JSON-only output
    sys_prompt = llm_extract.SYSTEM_PROMPT + (
        '\n\nReturn ONLY a JSON object — no markdown fences, no preamble. '
        'Required top-level keys: "fields" (dict), "flags" (list), "notes" (string).'
    )
    response = client.chat.completions.create(
        model=llm_extract.OPENAI_MODEL,
        max_tokens=4096,
        response_format={'type': 'json_object'},
        messages=[
            {'role': 'system', 'content': sys_prompt},
            {'role': 'user', 'content': user_prompt},
        ],
    )
    raw = response.choices[0].message.content
    result = llm_extract.parse_llm_response(raw)
    if 'fields' not in result:
        raise ValueError("response missing 'fields'")
    return result, response.usage.prompt_tokens, response.usage.completion_tokens


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--tickers', default='', help='Comma-separated tickers (e.g. ABBV,NUE)')
    p.add_argument('--all', action='store_true', help='Process all request files')
    p.add_argument('--model', default='gpt-4o-mini')
    p.add_argument('--output-dir', default='backfill/results_openai',
                   help='Where to write result JSONs (default: backfill/results_openai)')
    p.add_argument('--skip-existing', action='store_true',
                   help='Skip request files that already have a result in --output-dir')
    p.add_argument('--limit', type=int, default=0, help='Stop after N requests (0 = no limit)')
    p.add_argument('--sleep', type=float, default=0.0, help='Seconds between API calls')
    args = p.parse_args()

    if not os.environ.get('OPENAI_API_KEY'):
        logger.error('OPENAI_API_KEY not set in environment')
        sys.exit(1)

    import openai
    client = openai.OpenAI()
    llm_extract.OPENAI_MODEL = args.model

    out_dir = PROJECT_ROOT / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers = {t.strip().upper() for t in args.tickers.split(',') if t.strip()}
    if not tickers and not args.all:
        logger.error('Pass --tickers or --all')
        sys.exit(2)

    requests = []
    for path in sorted(REQUESTS_DIR.glob('*.json')):
        if path.name == 'manifest.json':
            continue
        ticker = path.name.split('_', 1)[0]
        if tickers and ticker not in tickers:
            continue
        if args.skip_existing and (out_dir / path.name).exists():
            continue
        requests.append(path)
    logger.info(f'Processing {len(requests)} requests with {args.model} -> {out_dir}')

    cost_per_M_in = 0.15
    cost_per_M_out = 0.60
    total_in = total_out = 0
    locate_n = extract_n = 0
    errors = []

    for i, req_path in enumerate(requests):
        if args.limit and i >= args.limit:
            break
        req = _load_request(req_path)
        kind = req.get('type', '')
        try:
            if kind == 'locate':
                result, ti, to = process_locate(req, client)
                locate_n += 1
            else:
                result, ti, to = process_extraction(req, client)
                extract_n += 1
            total_in += ti
            total_out += to
            _save_result(out_dir, req_path.name, result)
            if (i + 1) % 10 == 0:
                cost = (total_in * cost_per_M_in + total_out * cost_per_M_out) / 1e6
                logger.info(f'  {i+1}/{len(requests)} | tok in={total_in:,} out={total_out:,} | ${cost:.4f}')
        except Exception as e:
            errors.append((req_path.name, str(e)))
            logger.error(f'{req_path.name}: {e}')
        if args.sleep:
            time.sleep(args.sleep)

    cost = (total_in * cost_per_M_in + total_out * cost_per_M_out) / 1e6
    logger.info('=' * 60)
    logger.info(f'Done. Locate: {locate_n}, Extract: {extract_n}, Errors: {len(errors)}')
    logger.info(f'Tokens: input={total_in:,}, output={total_out:,}')
    logger.info(f'Cost (gpt-4o-mini sync pricing): ${cost:.4f}')
    if errors:
        logger.error('Errors:')
        for name, e in errors:
            logger.error(f'  {name}: {e}')


if __name__ == '__main__':
    main()
