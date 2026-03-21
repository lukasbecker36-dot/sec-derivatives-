"""Post-extraction sanity checks on extracted data."""

from .config import IssuerConfig


def _parse_numeric(val) -> float | None:
    if val is None or val == '' or val == 'None':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def validate_row(
    row: dict,
    prior_row: dict | None,
    config: IssuerConfig,
) -> list[dict]:
    """Run validation checks on an extracted row.

    Returns list of {level: 'error'|'warning'|'info', field: str, message: str}.
    """
    results = []

    # --- 1. Completeness ---
    if prior_row:
        prev_populated = [
            f for f in config.fields
            if _parse_numeric(prior_row.get(f)) is not None
        ]
        cur_null = [
            f for f in prev_populated
            if _parse_numeric(row.get(f)) is None
        ]
        if prev_populated and len(cur_null) / len(prev_populated) > 0.30:
            results.append({
                'level': 'error',
                'field': '_completeness',
                'message': f'Likely extraction failure: {len(cur_null)}/{len(prev_populated)} '
                           f'previously-populated fields are now null '
                           f'({", ".join(cur_null[:5])}{"..." if len(cur_null) > 5 else ""})',
            })

    # --- 2. Positivity (notionals >= 0) ---
    for field_name, fld_cfg in config.fields.items():
        val = _parse_numeric(row.get(field_name))
        if val is None:
            continue
        desc_lower = fld_cfg.description.lower()
        if 'notional' in desc_lower and val < 0:
            results.append({
                'level': 'error',
                'field': field_name,
                'message': f'Negative notional: {field_name} = {val}',
            })

    # --- 3. Summation checks ---
    # Check if total_notional ≈ sum of component notionals
    component_fields = [
        f for f in config.fields
        if 'notional' in config.fields[f].description.lower()
        and f != 'total_notional'
        and 'total' not in f
    ]
    total_val = _parse_numeric(row.get('total_notional'))
    if total_val is not None and component_fields:
        component_sum = sum(
            _parse_numeric(row.get(f)) or 0
            for f in component_fields
        )
        if component_sum > 0 and total_val > 0:
            diff_pct = abs(total_val - component_sum) / total_val
            if diff_pct > 0.05:
                results.append({
                    'level': 'warning',
                    'field': 'total_notional',
                    'message': f'Summation mismatch: total_notional={total_val:,.0f} vs '
                               f'sum of components={component_sum:,.0f} (diff {diff_pct:.1%})',
                })

    # --- 4. Units check (>100x swing) ---
    if prior_row:
        for field_name in config.fields:
            cur = _parse_numeric(row.get(field_name))
            prev = _parse_numeric(prior_row.get(field_name))
            if cur and prev and prev != 0:
                ratio = abs(cur / prev)
                if ratio > 100 or ratio < 0.01:
                    results.append({
                        'level': 'error',
                        'field': field_name,
                        'message': f'Likely units mismatch: {field_name} changed from '
                                   f'{prev:,.0f} to {cur:,.0f} ({ratio:.0f}x)',
                    })

    # --- 5. Duplicate check (handled at CSV level, not here) ---

    # --- 6. Plausibility ---
    if prior_row:
        for field_name, fld_cfg in config.fields.items():
            cur = _parse_numeric(row.get(field_name))
            prev = _parse_numeric(prior_row.get(field_name))
            if cur is None or prev is None or prev == 0:
                continue
            pct = abs(cur - prev) / abs(prev)
            desc_lower = fld_cfg.description.lower()
            if 'notional' in desc_lower and pct > 0.50:
                results.append({
                    'level': 'warning',
                    'field': field_name,
                    'message': f'Large swing: {field_name} changed {pct:.0%} '
                               f'({prev:,.0f} → {cur:,.0f}) — review recommended',
                })
            elif 'fair value' in desc_lower and pct > 1.00:
                results.append({
                    'level': 'warning',
                    'field': field_name,
                    'message': f'Large swing: {field_name} changed {pct:.0%} '
                               f'({prev:,.0f} → {cur:,.0f}) — review recommended',
                })

    return results
