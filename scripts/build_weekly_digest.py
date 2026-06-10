"""Build a weekly HTML email digest from the pipeline's output files.

Scans output/*/tracking.csv and output/*/alert_log.txt for activity in
the last 7 days. Prints HTML to stdout; exits 0 with empty output if
there's nothing to report.

Usage:
    python scripts/build_weekly_digest.py [--days 7] > digest.html
"""

import argparse
import csv
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'output'
REGISTRY_DIR = Path(__file__).resolve().parent.parent / 'registry'


def _parse_date(s: str) -> datetime | None:
    for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _recent_filings(days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filings = []
    for csv_path in sorted(OUTPUT_DIR.glob('*/tracking.csv')):
        ticker = csv_path.parent.name.upper()
        with open(csv_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                dt = _parse_date(row.get('period_end_date', ''))
                if not dt:
                    continue
                # Check if this row was recently added by looking at file mtime
                # Instead, include all rows — we filter by git commit dates in the workflow
                filings.append({
                    'ticker': ticker,
                    'period_end': row.get('period_end_date', ''),
                    'form_type': row.get('form_type', ''),
                    'row': row,
                })
    return filings


def _recent_alerts(days: int) -> dict[str, list[str]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_str = cutoff.strftime('%Y-%m-%d')
    alerts_by_ticker = {}

    for alert_path in sorted(OUTPUT_DIR.glob('*/alert_log.txt')):
        ticker = alert_path.parent.name.upper()
        text = alert_path.read_text(encoding='utf-8')
        current_block = []
        current_date = None

        for line in text.splitlines():
            # Backfill-regenerated alerts for already-covered periods are tagged
            # historical and must not appear in the weekly digest.
            if '[HISTORICAL]' in line:
                continue
            header = re.match(r'^=== .+ \| Processed (\d{4}-\d{2}-\d{2}) ===$', line)
            if header:
                if current_block and current_date and current_date >= cutoff_str:
                    alerts_by_ticker.setdefault(ticker, []).extend(current_block)
                current_date = header.group(1)
                current_block = [line]
            elif line.strip():
                current_block.append(line)

        if current_block and current_date and current_date >= cutoff_str:
            alerts_by_ticker.setdefault(ticker, []).extend(current_block)

    return alerts_by_ticker


def _recent_activations(days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_str = cutoff.strftime('%Y-%m-%dT%H:%M:%SZ')
    log_path = REGISTRY_DIR / 'activation_log.csv'
    if not log_path.exists():
        return []

    events = []
    with open(log_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            ts = row.get('timestamp', '')
            if ts >= cutoff_str:
                events.append(row)
    return events


def _count_universe_status() -> dict[str, int]:
    universe_path = REGISTRY_DIR / 'universe.csv'
    if not universe_path.exists():
        return {}
    counts = {}
    with open(universe_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            status = row.get('status', 'unknown')
            counts[status] = counts.get(status, 0) + 1
    return counts


def build_html(days: int) -> str:
    alerts = _recent_alerts(days)
    activations = _recent_activations(days)
    status_counts = _count_universe_status()

    if not alerts and not activations:
        return ''

    parts = []
    parts.append(f'''<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body {{ font-family: -apple-system, Arial, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #333; }}
h1 {{ color: #1a1a2e; border-bottom: 2px solid #e94560; padding-bottom: 8px; font-size: 22px; }}
h2 {{ color: #1a1a2e; font-size: 17px; margin-top: 28px; }}
h3 {{ color: #444; font-size: 14px; margin: 16px 0 4px 0; }}
.stat {{ display: inline-block; background: #f0f0f0; padding: 6px 14px; border-radius: 4px; margin: 3px 4px 3px 0; font-size: 13px; }}
.stat b {{ color: #1a1a2e; }}
.alert {{ font-size: 13px; line-height: 1.5; color: #555; margin: 2px 0; font-family: Consolas, monospace; }}
.alert-numeric {{ color: #c44; }}
.alert-disappeared {{ color: #888; }}
.alert-new {{ color: #2a7; }}
.alert-flag {{ color: #b80; }}
.section-header {{ background: #f8f8f8; padding: 6px 10px; border-left: 3px solid #e94560; margin: 8px 0 4px 0; font-size: 13px; font-weight: bold; }}
.footer {{ margin-top: 30px; padding-top: 12px; border-top: 1px solid #ddd; font-size: 11px; color: #999; }}
</style></head><body>
<h1>SEC Derivatives — Weekly Digest</h1>
<p style="color:#666; font-size:13px;">Week ending {datetime.now(timezone.utc).strftime("%d %B %Y")} &middot; Last {days} days</p>''')

    # Universe status
    total = sum(status_counts.values())
    active = status_counts.get('active', 0) + status_counts.get('active_needs_review', 0)
    registered = status_counts.get('registered', 0)
    failed = status_counts.get('failed_activation', 0)
    parts.append(f'''
<h2>Universe</h2>
<div>
<span class="stat"><b>{active}</b> active</span>
<span class="stat"><b>{registered}</b> registered</span>
<span class="stat"><b>{failed}</b> failed</span>
<span class="stat"><b>{total}</b> total</span>
</div>''')

    # Activations
    if activations:
        succeeded = [a for a in activations if a.get('new_status') in ('active', 'active_needs_review')]
        failed_acts = [a for a in activations if a.get('new_status') == 'failed_activation']
        parts.append(f'<h2>Activations ({len(succeeded)} new, {len(failed_acts)} failed)</h2>')
        if succeeded:
            tickers = ', '.join(sorted(set(a['ticker'] for a in succeeded)))
            parts.append(f'<p style="font-size:13px;"><b>Newly active:</b> {tickers}</p>')
        if failed_acts:
            items = []
            for a in failed_acts:
                items.append(f"{a['ticker']}: {a.get('reason', 'unknown')}")
            parts.append('<p style="font-size:13px;"><b>Failed:</b></p><ul style="font-size:12px;">')
            for item in items[:20]:
                parts.append(f'<li>{item}</li>')
            parts.append('</ul>')

    # Alerts by ticker
    if alerts:
        total_alerts = sum(len(v) for v in alerts.values())
        parts.append(f'<h2>Alerts ({total_alerts} across {len(alerts)} issuers)</h2>')

        for ticker in sorted(alerts.keys()):
            ticker_alerts = alerts[ticker]
            parts.append(f'<h3>{ticker}</h3>')
            for line in ticker_alerts:
                css_class = 'alert'
                if '[NUMERIC]' in line:
                    css_class = 'alert alert-numeric'
                elif '[DISAPPEARED' in line:
                    css_class = 'alert alert-disappeared'
                elif '[NEW_FIELD]' in line:
                    css_class = 'alert alert-new'
                elif '[LLM_FLAG]' in line or '[VALIDATION]' in line:
                    css_class = 'alert alert-flag'
                elif line.startswith('==='):
                    parts.append(f'<div class="section-header">{line}</div>')
                    continue
                parts.append(f'<div class="{css_class}">{line}</div>')

    parts.append('''
<div class="footer">
SEC Derivatives &amp; Market Risk Extractor &middot; Automated weekly digest<br>
Data sourced from SEC EDGAR. Extraction via Claude Code scheduled routine.
</div>
</body></html>''')

    return '\n'.join(parts)


def main():
    parser = argparse.ArgumentParser(description='Build weekly HTML digest email')
    parser.add_argument('--days', type=int, default=7, help='Lookback window (default: 7)')
    args = parser.parse_args()

    html = build_html(args.days)
    if html:
        sys.stdout.buffer.write(html.encode('utf-8'))
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
