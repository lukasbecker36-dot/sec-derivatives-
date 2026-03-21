"""Period-over-period change detection and alerting."""

from .config import IssuerConfig

DEFAULT_THRESHOLD = 0.20  # 20%


def _parse_numeric(val) -> float | None:
    """Try to parse a value as float."""
    if val is None or val == '' or val == 'None':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def detect_changes(
    current_row: dict,
    prior_row: dict | None,
    config: IssuerConfig,
) -> list[str]:
    """Compare current row against prior row, return alert strings.

    Alert types:
        [NUMERIC] — field changed more than threshold
        [DROPPED_TO_ZERO] — field went from nonzero to zero
        [NEW_FIELD] — field appeared for first time
        [DISAPPEARED_FIELD] — field was present, now null/missing
    """
    if not prior_row:
        return []

    alerts = []

    for field_name in config.fields:
        cur_val = _parse_numeric(current_row.get(field_name))
        prev_val = _parse_numeric(prior_row.get(field_name))

        # Get threshold: per-field override or default
        threshold = config.alert_thresholds.get(field_name, DEFAULT_THRESHOLD)
        if config.fields[field_name].alert_threshold is not None:
            threshold = config.fields[field_name].alert_threshold

        # New field appeared
        if prev_val is None and cur_val is not None:
            alerts.append(
                f'[NEW_FIELD] {field_name}: appeared for first time (${cur_val:,.0f}M)'
                if isinstance(cur_val, (int, float)) else
                f'[NEW_FIELD] {field_name}: appeared for first time ({cur_val})'
            )
            continue

        # Field disappeared
        if prev_val is not None and cur_val is None:
            alerts.append(f'[DISAPPEARED_FIELD] {field_name}: was ${prev_val:,.0f}M, now missing')
            continue

        # Both None — skip
        if prev_val is None and cur_val is None:
            continue

        # Dropped to zero
        if prev_val != 0 and cur_val == 0:
            alerts.append(f'[DROPPED_TO_ZERO] {field_name}: ${prev_val:,.0f}M → $0M')
            continue

        # Numeric change
        if prev_val != 0:
            pct_change = abs(cur_val - prev_val) / abs(prev_val)
            if pct_change > threshold:
                direction = '+' if cur_val > prev_val else ''
                change_pct = ((cur_val - prev_val) / abs(prev_val)) * 100
                alerts.append(
                    f'[NUMERIC] {field_name}: ${prev_val:,.0f}M → ${cur_val:,.0f}M '
                    f'({direction}{change_pct:.1f}%) — exceeds {threshold:.0%} threshold'
                )

    return alerts
