"""Cross-issuer dashboard, trend detection, and story-lead generation."""

import csv
import logging
import re
import statistics
from pathlib import Path

import anthropic

from .config import load_config, list_issuers
from .utils import log_llm_usage

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'output'
CROSS_DIR = OUTPUT_DIR / '_cross_issuer'
LLM_LOG = OUTPUT_DIR / 'llm_usage.log'
SONNET_MODEL = 'claude-sonnet-4-20250514'


def build_dashboard(output_dir: Path = OUTPUT_DIR) -> list[dict]:
    """Build cross-issuer dashboard from all issuers' tracking CSVs.

    Returns list of normalised rows and writes to _cross_issuer/dashboard.csv.
    """
    issuer_paths = list_issuers()
    rows = []

    for yaml_path in issuer_paths:
        try:
            config = load_config(yaml_path)
            csv_path = output_dir / config.ticker.lower() / 'tracking.csv'
            if not csv_path.exists():
                continue

            mapping = config.dashboard_mapping
            if not mapping:
                continue

            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for raw_row in reader:
                    dash_row = {
                        'issuer': config.issuer,
                        'ticker': config.ticker,
                        'archetype': config.archetype,
                        'sector': config.sector,
                        'period_end': raw_row.get('period_end_date', ''),
                        'form_type': raw_row.get('form_type', ''),
                    }

                    for dash_col, source_expr in mapping.items():
                        val = _evaluate_mapping(source_expr, raw_row)
                        dash_row[dash_col] = val

                    rows.append(dash_row)
        except Exception as e:
            logger.error(f'Error building dashboard for {yaml_path.stem}: {e}')

    # Write dashboard CSV
    if rows:
        CROSS_DIR.mkdir(parents=True, exist_ok=True)
        # Collect all column names
        all_cols = ['issuer', 'ticker', 'archetype', 'sector', 'period_end', 'form_type']
        extra_cols = set()
        for row in rows:
            extra_cols.update(k for k in row if k not in all_cols)
        all_cols.extend(sorted(extra_cols))

        dash_path = CROSS_DIR / 'dashboard.csv'
        with open(dash_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction='ignore')
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        logger.info(f'Wrote {len(rows)} rows to {dash_path}')

    return rows


def _evaluate_mapping(expr: str, row: dict) -> float | None:
    """Evaluate a dashboard mapping expression.

    Supports:
        - Direct field name: "fx_designated_notional"
        - Addition: "field_a + field_b"
    """
    if '+' in expr:
        parts = [p.strip() for p in expr.split('+')]
        total = 0.0
        for part in parts:
            val = row.get(part)
            if val is not None and val != '':
                try:
                    total += float(val)
                except (ValueError, TypeError):
                    pass
        return total if total != 0 else None
    else:
        val = row.get(expr.strip())
        if val is not None and val != '':
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        return None


def detect_cross_issuer_alerts(dashboard_rows: list[dict]) -> list[dict]:
    """Detect cross-issuer trends and anomalies.

    Alert types:
        - trend_break: issuer deviates >2σ from own rolling mean
        - peer_outlier: issuer >2σ from peer group in same period
        - systemic_shift: >N issuers moving same direction
    """
    alerts = []

    # Group by issuer for trend analysis
    by_issuer = {}
    for row in dashboard_rows:
        ticker = row.get('ticker', '')
        if ticker not in by_issuer:
            by_issuer[ticker] = []
        by_issuer[ticker].append(row)

    # Group by period + archetype for peer comparison
    by_period_arch = {}
    for row in dashboard_rows:
        key = (row.get('period_end', ''), row.get('archetype', ''))
        if key not in by_period_arch:
            by_period_arch[key] = []
        by_period_arch[key].append(row)

    # Numeric dashboard columns
    meta_cols = {'issuer', 'ticker', 'archetype', 'sector', 'period_end', 'form_type'}
    numeric_cols = set()
    for row in dashboard_rows:
        for k, v in row.items():
            if k not in meta_cols and v is not None:
                try:
                    float(v)
                    numeric_cols.add(k)
                except (ValueError, TypeError):
                    pass

    # Trend breaks
    for ticker, rows in by_issuer.items():
        rows_sorted = sorted(rows, key=lambda r: r.get('period_end', ''))
        for col in numeric_cols:
            values = []
            for r in rows_sorted:
                v = r.get(col)
                if v is not None:
                    try:
                        values.append(float(v))
                    except (ValueError, TypeError):
                        pass

            if len(values) >= 4:
                mean = statistics.mean(values[:-1])
                stdev = statistics.stdev(values[:-1]) if len(values[:-1]) > 1 else 0
                latest = values[-1]
                if stdev > 0 and abs(latest - mean) > 2 * stdev:
                    alerts.append({
                        'type': 'trend_break',
                        'ticker': ticker,
                        'field': col,
                        'value': latest,
                        'mean': mean,
                        'stdev': stdev,
                        'period': rows_sorted[-1].get('period_end', ''),
                        'magnitude': abs(latest - mean) / stdev,
                    })

    # Peer outliers
    for (period, archetype), peer_rows in by_period_arch.items():
        if len(peer_rows) < 3:
            continue
        for col in numeric_cols:
            peer_vals = []
            for r in peer_rows:
                v = r.get(col)
                if v is not None:
                    try:
                        peer_vals.append((r.get('ticker', ''), float(v)))
                    except (ValueError, TypeError):
                        pass
            if len(peer_vals) < 3:
                continue

            vals = [v for _, v in peer_vals]
            mean = statistics.mean(vals)
            stdev = statistics.stdev(vals) if len(vals) > 1 else 0
            if stdev == 0:
                continue

            for ticker, val in peer_vals:
                if abs(val - mean) > 2 * stdev:
                    alerts.append({
                        'type': 'peer_outlier',
                        'ticker': ticker,
                        'field': col,
                        'value': val,
                        'peer_mean': mean,
                        'peer_stdev': stdev,
                        'period': period,
                        'archetype': archetype,
                        'magnitude': abs(val - mean) / stdev,
                    })

    # Sort by magnitude
    alerts.sort(key=lambda a: a.get('magnitude', 0), reverse=True)
    return alerts


def generate_story_leads(
    alerts: list[dict],
    client: anthropic.Anthropic | None = None,
    max_alerts: int = 15,
) -> str:
    """Feed top alerts to Sonnet, generate story leads in Risk.net style."""
    if not alerts:
        return 'No significant cross-issuer alerts this period.'

    if client is None:
        client = anthropic.Anthropic()

    top_alerts = alerts[:max_alerts]
    alerts_text = '\n'.join(
        f"- [{a['type']}] {a.get('ticker', '?')}: {a.get('field', '?')} = {a.get('value', '?')} "
        f"(magnitude: {a.get('magnitude', 0):.1f}\u03c3, period: {a.get('period', '?')})"
        for a in top_alerts
    )

    prompt = f"""You are a financial journalist at Risk.net covering derivatives markets.

Based on these cross-issuer alerts from SEC filing analysis, write 3-5 one-paragraph story leads.
Each lead should identify: what changed, who is affected, why it matters, and what the next step
for investigation would be.

Alerts:
{alerts_text}

Write in Risk.net house style: precise, data-driven, no hype. Lead with the news."""

    try:
        response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=2048,
            messages=[{'role': 'user', 'content': prompt}],
        )
        log_llm_usage(
            LLM_LOG, '_cross_issuer', 'story_leads', SONNET_MODEL,
            response.usage.input_tokens, response.usage.output_tokens,
            (response.usage.input_tokens * 3.0 + response.usage.output_tokens * 15.0) / 1_000_000,
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f'Story lead generation failed: {e}')
        return f'Story lead generation failed: {e}'


def run_cross_issuer_analysis(output_dir: Path = OUTPUT_DIR, client=None) -> dict:
    """Run full cross-issuer analysis pipeline."""
    dashboard_rows = build_dashboard(output_dir)
    alerts = detect_cross_issuer_alerts(dashboard_rows)

    # Write alerts
    CROSS_DIR.mkdir(parents=True, exist_ok=True)
    alerts_path = CROSS_DIR / 'alerts.txt'
    with open(alerts_path, 'w', encoding='utf-8') as f:
        f.write('Cross-Issuer Alerts\n')
        f.write('=' * 60 + '\n\n')

        for a in alerts[:30]:
            f.write(f"[{a['type'].upper()}] {a.get('ticker', '?')} | {a.get('field', '?')} | "
                    f"value={a.get('value', '?')} | {a.get('magnitude', 0):.1f}\u03c3 | "
                    f"period={a.get('period', '?')}\n")

        f.write('\n' + '=' * 60 + '\n')
        f.write('Story Leads\n')
        f.write('=' * 60 + '\n\n')

        if alerts:
            leads = generate_story_leads(alerts, client)
            f.write(leads)
        else:
            f.write('No significant alerts.\n')

    return {
        'dashboard_rows': len(dashboard_rows),
        'alerts': len(alerts),
        'alerts_path': str(alerts_path),
    }
