"""Post-extraction sanity checks on extracted data."""

import re

from .config import IssuerConfig

# Field descriptions with any of these words carry dollar amounts, so a value
# that looks like a calendar year or a day-of-month is almost certainly a date
# fragment that leaked out of a table header rather than a real figure.
_DOLLAR_HINTS = (
    'notional', 'fair value', 'million', 'billion', 'debt', 'asset',
    'liability', 'collateral', 'sensitivity', 'var', 'proceeds', 'exposure',
    'gain', 'loss', 'reclass', 'aoci', 'notes',
)
_PERCENT_HINTS = ('percent', '%', 'basis point')

# A source_quote backing a dollar figure should contain a currency/number
# marker; if it has none, the value was likely fabricated or mis-sourced.
_DOLLAR_MARKER = re.compile(r'[\$\d]|million|billion|thousand', re.IGNORECASE)
_DATE_CONTEXT = re.compile(
    r'January|February|March|April|May|June|July|August|September|October'
    r'|November|December|\d{1,2}/\d{1,2}/\d{2,4}|20\d{2}',
    re.IGNORECASE,
)


def _parse_numeric(val) -> float | None:
    if val is None or val == '' or val == 'None':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def validate_source_quotes(llm_result: dict, schema: dict[str, str]) -> list[str]:
    """Cross-check each extracted numeric value against its source_quote.

    Catches the common failure where a date fragment ("March 31, 2026") leaks
    from a table header into a numeric field — e.g. an IR-swap notional of
    2026.0 or a cash-flow-hedge AOCI of 31.0. Returns a list of flag strings
    (empty if everything looks sane). Deliberately conservative: only fields
    whose description clearly denotes a dollar amount are checked, so a genuine
    $30M notional isn't spuriously flagged.
    """
    flags = []
    fields = llm_result.get('fields', {}) or {}
    for name, desc in schema.items():
        fd = fields.get(name) or {}
        value = _parse_numeric(fd.get('value'))
        if value is None:
            continue
        desc_l = desc.lower()
        is_dollar = any(h in desc_l for h in _DOLLAR_HINTS)
        is_percent = any(h in desc_l for h in _PERCENT_HINTS)
        if not is_dollar or is_percent:
            continue

        quote = str(fd.get('source_quote') or '')

        # Value looks like a calendar year.
        if value == int(value) and 1990 <= value <= 2035:
            flags.append(
                f'[QUOTE_CHECK] {name}={value:g} looks like a calendar year — '
                f'possible date contamination (source_quote: "{quote[:80]}")'
            )
            continue

        # Value looks like a day-of-month and its quote mentions a date.
        if value in (28, 29, 30, 31) and _DATE_CONTEXT.search(quote):
            flags.append(
                f'[QUOTE_CHECK] {name}={value:g} may be a day-of-month from a '
                f'date, not a figure (source_quote: "{quote[:80]}")'
            )
            continue

        # A dollar figure whose quote carries no number/currency marker.
        if value != 0 and quote and not _DOLLAR_MARKER.search(quote):
            flags.append(
                f'[QUOTE_CHECK] {name}={value:g} not supported by its '
                f'source_quote (no $/number: "{quote[:80]}")'
            )

    return flags


# Notional totals and the components that must sum to them. Keyed on the
# actual field names the archetypes emit — the previous version of this check
# only looked for a field literally named 'total_notional', so it silently
# no-opped for every minimal_hedger issuer (whose total is
# 'fx_derivatives_notional'). That is how MSFT shipped five quarters where
# designated + not-designated came to a fifth of the reported total.
RECONCILIATION_RULES: list[tuple[str, list[str]]] = [
    ('fx_derivatives_notional',
     ['fx_designated_notional', 'fx_not_designated_notional']),
    ('fi_fx_derivatives_notional',
     ['fi_fx_designated_notional', 'fi_fx_not_designated_notional']),
    ('total_notional',
     ['fx_designated_notional', 'fx_not_designated_notional',
      'commodity_designated_notional', 'commodity_not_designated_notional']),
]

# Relative gap above which a total/component mismatch is reported.
RECONCILIATION_TOLERANCE = 0.05


def check_reconciliations(row: dict,
                          tolerance: float = RECONCILIATION_TOLERANCE) -> list[dict]:
    """Check that notional totals equal the sum of their components.

    A mismatch means the extraction read the wrong table (or mixed two
    tables), so this is an 'error' rather than a soft warning: the numbers
    are mutually contradictory regardless of which one is right.

    Only fires when the total and at least one component are populated, so
    partially-disclosed filings don't generate noise.
    """
    results = []
    for total_field, component_fields in RECONCILIATION_RULES:
        total_val = _parse_numeric(row.get(total_field))
        if total_val is None or total_val == 0:
            continue
        components = {
            f: _parse_numeric(row.get(f))
            for f in component_fields
            if _parse_numeric(row.get(f)) is not None
        }
        if not components:
            continue
        component_sum = sum(components.values())
        diff_pct = abs(abs(total_val) - abs(component_sum)) / abs(total_val)
        if diff_pct > tolerance:
            detail = ' + '.join(f'{f}={v:,.0f}' for f, v in components.items())
            results.append({
                'level': 'error',
                'field': total_field,
                'message': f'Reconciliation failure: {total_field}={total_val:,.0f} '
                           f'but {detail} = {component_sum:,.0f} '
                           f'(diff {diff_pct:.1%}) — extraction likely read the '
                           f'wrong table',
            })
    return results


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
    results.extend(check_reconciliations(row))

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
