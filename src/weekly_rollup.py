"""Assemble a weekly rollup HTML from the week's dated daily digests.

The daily digest routine writes to digests/YYYY-MM-DD.html each weekday.
This module reads the last five weekday files, extracts their <body>
content, and stitches them into a single HTML with per-day sections and
a top-of-page summary.

No LLM involved — it's a mechanical concatenation. That's deliberate:
the substantive editorial work is done in the dailies, and re-narrating
them would just introduce a fresh chance for the "quote a growth rate
from memory" failure the audit gate was built to prevent. The rollup
adds structure, dates, and navigation on top of the dailies you already
verified.

    python -m src.weekly_rollup --out WEEKLY_ROLLUP.html
"""

import argparse
import html
import re
import sys
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DIGESTS_DIR = REPO / 'digests'

BODY_RE = re.compile(r'<body[^>]*>(.*?)</body>', re.IGNORECASE | re.DOTALL)


def _weekday_files(as_of: date | None = None) -> list[Path]:
    """Return the paths of the five most-recent weekday digest files that
    exist. Missing days are silently skipped."""
    d = (as_of or datetime.now(timezone.utc).date())
    out = []
    # Walk back at most 10 calendar days to find 5 weekday files.
    scanned = 0
    while len(out) < 5 and scanned < 10:
        # Skip weekends
        if d.isoweekday() <= 5:
            p = DIGESTS_DIR / f'{d.isoformat()}.html'
            if p.exists() and p.stat().st_size > 0:
                out.append(p)
        d = d - timedelta(days=1)
        scanned += 1
    # Return oldest-first so the reader gets Mon → Fri chronology.
    return list(reversed(out))


def _extract_body(html_text: str) -> str:
    m = BODY_RE.search(html_text)
    if m:
        return m.group(1).strip()
    # If no <body> tag, treat the whole thing as body content — the daily
    # routine may emit fragment HTML rather than a full document.
    return html_text.strip()


def build_rollup(paths: list[Path]) -> str:
    if not paths:
        return ''

    week_start = paths[0].stem
    week_end = paths[-1].stem

    sections = []
    for p in paths:
        day = p.stem  # YYYY-MM-DD
        body = _extract_body(p.read_text(encoding='utf-8'))
        try:
            dt = datetime.strptime(day, '%Y-%m-%d')
            weekday = dt.strftime('%A, %-d %B %Y')
        except ValueError:
            weekday = day
        sections.append(
            f'<section id="day-{html.escape(day)}">'
            f'<h2 style="border-top:2px solid #d1d5db;padding-top:1.5em;'
            f'color:#1e3a8a;">{html.escape(weekday)}</h2>'
            f'{body}'
            f'</section>'
        )

    toc = '<ul>' + ''.join(
        f'<li><a href="#day-{html.escape(p.stem)}">{html.escape(p.stem)}</a></li>'
        for p in paths
    ) + '</ul>'

    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>SEC Derivatives — Weekly Rollup {html.escape(week_start)} to {html.escape(week_end)}</title>
</head>
<body style="font-family:Calibri,system-ui,sans-serif;color:#1F2937;max-width:900px;margin:20px auto;padding:0 20px;">
<h1 style="color:#1e3a8a;">SEC Derivatives — Weekly Rollup</h1>
<p style="color:#4B5563;">Daily digests from {html.escape(week_start)} to {html.escape(week_end)},
concatenated for the week's editorial view. Each section below is the
daily digest as originally sent — nothing has been re-computed or
re-worded, so any figure that was traceable to a cell when it was
verified for the daily is still traceable in the same way here.</p>
<h2 style="color:#1e3a8a;">Days included</h2>
{toc}
{''.join(sections)}
</body></html>
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    parser.add_argument('--out', type=Path, default=Path('WEEKLY_ROLLUP.html'))
    parser.add_argument('--as-of', help='ISO date (default: today UTC).')
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    paths = _weekday_files(as_of=as_of)
    html_out = build_rollup(paths)
    if not html_out:
        print('No daily digests found for the last 5 weekdays', file=sys.stderr)
        args.out.write_text('', encoding='utf-8')
        return 0

    args.out.write_text(html_out, encoding='utf-8')
    print(f'Wrote {args.out} from {len(paths)} daily digests: '
          f'{", ".join(p.stem for p in paths)}', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
