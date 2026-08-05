"""Compare two backfill result directories field-by-field.

The "truth" dir is the existing committed extraction (Claude/hand); the
"candidate" dir is whatever you want to validate (e.g. gpt-4o-mini).

Usage:
    python scripts/diff_backfill_results.py \
        --truth backfill/results \
        --candidate backfill/results_openai \
        --tickers ABBV,NUE,BA
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def _value_of(field: dict | None):
    """Extract the value from a field record, handling both shapes."""
    if field is None:
        return None
    if isinstance(field, dict):
        return field.get('value')
    return field


def _agree(truth_val, cand_val, tol=0.05) -> str:
    """exact / close / disagree / both_null / one_null."""
    if truth_val is None and cand_val is None:
        return 'both_null'
    if truth_val is None or cand_val is None:
        return 'one_null'
    if truth_val == cand_val:
        return 'exact'
    # Numeric tolerance
    try:
        t = float(truth_val)
        c = float(cand_val)
        if t == 0 and abs(c) < 1:
            return 'close'
        if t != 0 and abs(t - c) / abs(t) <= tol:
            return 'close'
        return 'disagree'
    except (TypeError, ValueError):
        # String compare
        if str(truth_val).strip().lower() == str(cand_val).strip().lower():
            return 'exact'
        return 'disagree'


def diff_locate(truth: dict, cand: dict) -> dict:
    """Compare locate results."""
    tf = bool(truth.get('found'))
    cf = bool(cand.get('found'))
    out = {'found_match': tf == cf, 'truth_found': tf, 'cand_found': cf}
    if tf and cf:
        out['heading_match'] = (truth.get('heading_text', '').strip().lower()
                                == cand.get('heading_text', '').strip().lower())
    elif (not tf) and (not cf):
        out['reason_match'] = truth.get('reason') == cand.get('reason')
    return out


def diff_extract(truth: dict, cand: dict) -> dict:
    """Compare extraction results field-by-field."""
    tf = truth.get('fields', {}) or {}
    cf = cand.get('fields', {}) or {}
    all_fields = set(tf) | set(cf)
    per_field = {}
    counts = defaultdict(int)
    for name in sorted(all_fields):
        tv = _value_of(tf.get(name))
        cv = _value_of(cf.get(name))
        verdict = _agree(tv, cv)
        per_field[name] = {'truth': tv, 'cand': cv, 'verdict': verdict}
        counts[verdict] += 1
    return {'counts': dict(counts), 'fields': per_field}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--truth', default='backfill/results')
    p.add_argument('--candidate', default='backfill/results_openai')
    p.add_argument('--tickers', default='', help='Comma-separated tickers to limit comparison')
    p.add_argument('--verbose', action='store_true', help='Print every disagreeing field')
    args = p.parse_args()

    truth_dir = PROJECT_ROOT / args.truth
    cand_dir = PROJECT_ROOT / args.candidate
    tickers = {t.strip().upper() for t in args.tickers.split(',') if t.strip()}

    files = []
    for path in sorted(cand_dir.glob('*.json')):
        ticker = path.name.split('_', 1)[0]
        if tickers and ticker not in tickers:
            continue
        files.append(path)

    if not files:
        print(f'No candidate files in {cand_dir}', file=sys.stderr)
        sys.exit(1)

    locate_total = 0
    locate_found_match = 0
    extract_counts = defaultdict(int)
    per_issuer = defaultdict(lambda: defaultdict(int))
    per_field_disagree = defaultdict(list)
    locate_disagreements = []
    extract_disagreements = []

    for cand_path in files:
        truth_path = truth_dir / cand_path.name
        truth = _load(truth_path)
        cand = _load(cand_path)
        if truth is None or cand is None:
            print(f'SKIP {cand_path.name}: missing pair')
            continue
        ticker = cand_path.name.split('_', 1)[0]

        if 'locate' in cand_path.name:
            d = diff_locate(truth, cand)
            locate_total += 1
            if d['found_match']:
                locate_found_match += 1
                per_issuer[ticker]['locate_match'] += 1
            else:
                per_issuer[ticker]['locate_mismatch'] += 1
                locate_disagreements.append((cand_path.name, d))
        else:
            d = diff_extract(truth, cand)
            for k, v in d['counts'].items():
                extract_counts[k] += v
                per_issuer[ticker][f'ext_{k}'] += v
            disagrees = [(f, info) for f, info in d['fields'].items()
                         if info['verdict'] == 'disagree']
            if disagrees:
                extract_disagreements.append((cand_path.name, disagrees))
                for f, info in disagrees:
                    per_field_disagree[f].append(
                        (cand_path.name, info['truth'], info['cand']))

    print('=' * 70)
    print(f'Files compared: {len(files)}')
    print()
    print(f'LOCATE: {locate_found_match}/{locate_total} agree on found/not-found')
    print()
    ext_total = sum(extract_counts.values())
    print(f'EXTRACT: {ext_total} field comparisons across all extraction requests')
    for k in ['exact', 'close', 'both_null', 'one_null', 'disagree']:
        v = extract_counts[k]
        pct = (100 * v / ext_total) if ext_total else 0
        print(f'  {k:12} {v:5} ({pct:5.1f}%)')

    agreed = extract_counts['exact'] + extract_counts['close'] + extract_counts['both_null']
    print()
    print(f'Agreement rate (exact+close+both_null): {agreed}/{ext_total} '
          f'= {100*agreed/ext_total:.1f}%' if ext_total else '')
    print(f'Disagreement on numbers:     {extract_counts["disagree"]}/{ext_total}')
    print(f'One side null, other not:    {extract_counts["one_null"]}/{ext_total}')

    print()
    print('Per-issuer breakdown:')
    for t in sorted(per_issuer):
        s = per_issuer[t]
        print(f'  {t}: ' + ', '.join(f'{k}={v}' for k, v in sorted(s.items())))

    print()
    print('Top disagreeing fields (numeric mismatches):')
    field_pairs = sorted(per_field_disagree.items(),
                         key=lambda kv: -len(kv[1]))[:15]
    for fname, occurrences in field_pairs:
        print(f'  {fname}: {len(occurrences)} files')
        for filename, t, c in occurrences[:3]:
            print(f'    {filename}: truth={t} cand={c}')

    if args.verbose and (locate_disagreements or extract_disagreements):
        print()
        print('=' * 70)
        print('All locate disagreements:')
        for name, d in locate_disagreements:
            print(f'  {name}: truth_found={d["truth_found"]} cand_found={d["cand_found"]}')
        print()
        print('All extract disagreements:')
        for name, fields in extract_disagreements:
            print(f'  {name}')
            for f, info in fields:
                print(f'    {f}: truth={info["truth"]} cand={info["cand"]}')


if __name__ == '__main__':
    main()
