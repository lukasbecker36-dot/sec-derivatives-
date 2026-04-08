"""Build an HTML email summary from a scheduler run's summary.json.

Outputs HTML to stdout if there is news (new filings or activations).
Outputs nothing (empty file) if there is nothing to report.
"""

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def load_summary(path: str) -> dict:
    with open(path, 'r') as f:
        return json.load(f)


def has_news(summary: dict) -> bool:
    if summary.get('active_new_filings', 0) > 0:
        return True
    if summary.get('activations_attempted', 0) > 0:
        return True
    return False


def load_notes_for_period(ticker: str, period: str) -> str:
    """Load qualitative notes for a specific filing period."""
    notes_path = Path('output') / ticker.lower() / 'notes.txt'
    if not notes_path.exists():
        return ''

    text = notes_path.read_text(encoding='utf-8')
    # Find the section for this period
    marker = f'Period ending {period}'
    idx = text.find(marker)
    if idx == -1:
        return ''

    # Find the next period marker or end of file
    next_marker = text.find('--- 10-', idx + len(marker))
    section = text[idx:next_marker] if next_marker != -1 else text[idx:]

    # Clean up and extract key lines
    lines = []
    for line in section.strip().split('\n'):
        line = line.strip()
        if line.startswith('[') and line.endswith(']'):
            lines.append(f'<b>{line[1:-1]}</b>')
        elif line.startswith('- '):
            # Truncate long lines
            content = line[2:].strip()
            if len(content) > 200:
                content = content[:200] + '...'
            lines.append(f'&bull; {content}')
    return '<br>\n'.join(lines)


def load_tracking_row(ticker: str, period: str) -> dict:
    """Load the tracking CSV row for a specific period."""
    csv_path = Path('output') / ticker.lower() / 'tracking.csv'
    if not csv_path.exists():
        return {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row.get('period_end_date') == period:
                return row
    return {}


def build_filing_section(result: dict) -> str:
    """Build HTML for a single filing result."""
    ticker = result.get('ticker', '?')
    period = result.get('period_end', '')
    form_type = result.get('form_type', '')
    processed = result.get('processed', 0)

    if processed == 0 and result.get('phase') != 'activation':
        return ''

    # Load details
    row = load_tracking_row(ticker, period) if period else {}
    notes_html = load_notes_for_period(ticker, period) if period else ''

    has_derivatives = row.get('has_derivatives', '')

    html = f"""
    <div style="margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 8px;">
      <h3 style="margin-top: 0; color: #1a1a1a;">{ticker} &mdash; {form_type} ({period})</h3>
      <p style="color: #666;">Derivatives usage: <b>{has_derivatives or 'N/A'}</b></p>
    """

    # Add key numeric fields if present
    key_fields = []
    for field_name, value in row.items():
        if field_name in ('period_end_date', 'form_type', 'has_derivatives'):
            continue
        if value and value.strip():
            key_fields.append((field_name, value))

    if key_fields:
        html += '<table style="border-collapse: collapse; width: 100%; margin: 10px 0;">\n'
        for name, value in key_fields[:12]:
            display_name = name.replace('_', ' ').title()
            # Truncate long values
            display_value = value if len(value) <= 100 else value[:100] + '...'
            html += f'<tr><td style="padding: 4px 8px; border-bottom: 1px solid #eee; color: #555;">{display_name}</td>'
            html += f'<td style="padding: 4px 8px; border-bottom: 1px solid #eee;"><b>{display_value}</b></td></tr>\n'
        html += '</table>\n'

    if notes_html:
        html += f"""
      <details style="margin-top: 10px;">
        <summary style="cursor: pointer; color: #0066cc;">Qualitative notes</summary>
        <div style="margin-top: 8px; padding: 10px; background: #f9f9f9; border-radius: 4px; font-size: 13px;">
          {notes_html}
        </div>
      </details>
    """

    html += '</div>\n'
    return html


def build_activation_section(result: dict) -> str:
    """Build HTML for an activation result."""
    ticker = result.get('ticker', '?')
    status = result.get('status', '?')
    score = result.get('score', 0)

    color = '#28a745' if status == 'active' else '#ffc107' if status == 'active_needs_review' else '#dc3545'

    return f"""
    <div style="margin-bottom: 10px; padding: 10px; border-left: 4px solid {color}; background: #fafafa;">
      <b>{ticker}</b> &rarr; <span style="color: {color};">{status}</span> (score: {score:.2f})
    </div>
    """


def build_email(summary: dict) -> str:
    """Build the full HTML email."""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    new_filings = summary.get('active_new_filings', 0)
    activations_attempted = summary.get('activations_attempted', 0)
    activations_succeeded = summary.get('activations_succeeded', 0)
    activations_failed = summary.get('activations_failed', 0)
    activations_review = summary.get('activations_needs_review', 0)

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family: -apple-system, Arial, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; color: #333;">
  <h2 style="border-bottom: 2px solid #0066cc; padding-bottom: 10px;">SEC Derivatives &mdash; Daily Update</h2>
  <p style="color: #666;">{now}</p>

  <div style="background: #f0f7ff; padding: 12px; border-radius: 6px; margin-bottom: 20px;">
    <b>Summary:</b> {new_filings} new filing{'s' if new_filings != 1 else ''} processed"""

    if activations_attempted > 0:
        html += f""" | {activations_succeeded} activated, {activations_review} need review, {activations_failed} failed"""

    html += '</div>\n'

    # Active issuer filings
    active_results = [r for r in summary.get('issuer_results', [])
                      if r.get('phase') == 'active' and r.get('processed', 0) > 0]
    if active_results:
        html += '<h3>New Filings Processed</h3>\n'
        for result in active_results:
            # We need to find the actual filing details from the output
            ticker = result.get('ticker', '')
            # Read the latest row from tracking.csv
            csv_path = Path('output') / ticker.lower() / 'tracking.csv'
            if csv_path.exists():
                with open(csv_path, 'r', encoding='utf-8') as f:
                    rows = list(csv.DictReader(f))
                    if rows:
                        last_row = rows[-1]
                        filing_result = {
                            'ticker': ticker,
                            'period_end': last_row.get('period_end_date', ''),
                            'form_type': last_row.get('form_type', ''),
                            'processed': 1,
                        }
                        html += build_filing_section(filing_result)

    # Activations
    activation_results = [r for r in summary.get('issuer_results', [])
                          if r.get('phase') == 'activation']
    if activation_results:
        html += '<h3>New Activations</h3>\n'
        for result in activation_results:
            html += build_activation_section(result)

    html += """
  <hr style="border: none; border-top: 1px solid #eee; margin-top: 30px;">
  <p style="color: #999; font-size: 12px;">
    SEC Derivatives Monitoring &mdash; automated daily scan of S&amp;P 500 10-Q/10-K filings on EDGAR
  </p>
</body>
</html>"""

    return html


def main():
    if len(sys.argv) < 2:
        print('Usage: build_email_summary.py <summary.json>', file=sys.stderr)
        sys.exit(1)

    summary = load_summary(sys.argv[1])

    if not has_news(summary):
        # Output nothing — no email will be sent
        sys.exit(0)

    print(build_email(summary))


if __name__ == '__main__':
    main()
