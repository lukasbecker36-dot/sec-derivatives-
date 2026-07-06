"""CME daily bulletin — Interest Rate futures & options volume and open interest.

Downloads CME Daily Bulletin *Section 02A — Summary Volume and Open Interest,
Interest Rate Futures and Options*, parses the per-product volume / open-interest
grid with pdfplumber, and appends the results to a git-friendly CSV that can be
searched with SQL via DuckDB.

The source PDF refreshes every trading day at a stable "current" URL:
https://www.cmegroup.com/daily_bulletin/current/Section02A_Summary_Volume_And_Open_Interest_Int_Rates_Futures_And_Options.pdf

Usage:
    python -m src.cme_bulletin pull  [--date YYYY-MM-DD] [--force] [--url URL] [-v]
    python -m src.cme_bulletin query "SELECT ... FROM data ..."

Parsing strategy
----------------
The bulletin is a fixed-width report with right-aligned numeric columns; empty
cells simply vanish from the extracted text, so positional token order is not
reliable.  Instead we cluster the *right edge* (x1) of every numeric token on a
page into column anchors, then map the five right-most anchors — which are always
present — to (total_volume, open_interest, oi_change, prior_year_volume,
prior_year_open_interest) and the remaining left anchors to the venue-volume
breakdown (globex / open_outcry / pnt).  Each numeric cell is bucketed to its
nearest anchor, so blanks are handled naturally.  A reconciliation check
(globex + open_outcry + pnt == total_volume) validates every row and makes the
parser fail loudly if CME changes the layout.
"""

import argparse
import csv
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber
import requests

from .utils import RateLimiter

logger = logging.getLogger(__name__)

BULLETIN_URL = (
    'https://www.cmegroup.com/daily_bulletin/current/'
    'Section02A_Summary_Volume_And_Open_Interest_Int_Rates_Futures_And_Options.pdf'
)
# CME's CDN (Akamai) rejects unfamiliar clients; present a browser-like profile.
DAILY_BULLETIN_PAGE = 'https://www.cmegroup.com/market-data/daily-bulletin.html'
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
)
HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/pdf,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': DAILY_BULLETIN_PAGE,
}

DATA_DIR = Path(__file__).resolve().parent.parent / 'data' / 'cme'
CSV_PATH = DATA_DIR / 'ir_volume_oi.csv'
RAW_DIR = DATA_DIR / 'raw'

COLUMNS = [
    'trade_date', 'report_status', 'report_section',
    'product_code', 'product_name', 'option_type', 'is_total',
    'globex_volume', 'open_outcry_volume', 'pnt_volume', 'total_volume',
    'open_interest', 'oi_change', 'prior_year_volume', 'prior_year_open_interest',
    'source_pdf', 'fetched_at',
]
# Idempotency key — one row per product line per section per day.
KEY = ('trade_date', 'report_section', 'product_code', 'option_type', 'product_name')

# Right-most five columns, in left-to-right order (always populated).
RIGHT5 = [
    'total_volume', 'open_interest', 'oi_change',
    'prior_year_volume', 'prior_year_open_interest',
]
# Venue-volume breakdown columns, left-to-right, that sit left of total_volume.
VENUE = ['globex_volume', 'open_outcry_volume', 'pnt_volume']

_NUM = re.compile(r'^[\d,]+$')
_CODE = re.compile(r'^[A-Z0-9]{1,5}$')
_GLUED = re.compile(r'^(?P<txt>.*[A-Za-z])(?P<num>\d{3,})$')  # e.g. "FUT465011"
_DATE = re.compile(r'[A-Z][a-z]{2}, ([A-Z][a-z]{2} \d{1,2}, \d{4})')
_XMIN = 150.0        # ignore tokens left of the first data column (page furniture)
_ANCHOR_GAP = 15.0   # min horizontal gap (pt) separating two columns
_BUCKET_TOL = 12.0   # max distance (pt) a token may sit from its column anchor

rate_limiter = RateLimiter(max_per_second=2.0)


class BulletinParseError(RuntimeError):
    """Raised when the PDF does not match the expected Section 02A layout."""


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

class DownloadBlockedError(RuntimeError):
    """Raised when CME refuses the download (HTTP 403 / non-PDF body)."""


def fetch_pdf(url: str = BULLETIN_URL, retries: int = 2) -> bytes:
    """Download the bulletin PDF with requests. Fast path; may be blocked by Akamai.

    Raises DownloadBlockedError on a 403 or a non-PDF body so the caller can fall
    back to a real browser; other network errors propagate.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    last_exc = None
    for attempt in range(retries + 1):
        rate_limiter.wait()
        logger.debug('Fetching %s (attempt %d)', url, attempt + 1)
        try:
            resp = session.get(url, timeout=60)
        except requests.RequestException as e:
            last_exc = e
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 403:
            raise DownloadBlockedError(
                f'CME returned 403 (bot/IP block): {resp.text[:200]}')
        resp.raise_for_status()
        if resp.content[:5] != b'%PDF-':
            raise DownloadBlockedError(
                f'URL did not return a PDF (got {resp.content[:40]!r})')
        return resp.content
    raise last_exc  # exhausted retries on network errors


def fetch_pdf_browser(url: str = BULLETIN_URL) -> bytes:
    """Download the bulletin using headless Chromium (Playwright).

    A real browser satisfies Akamai's cookie/JS challenge, so this succeeds where
    plain requests are blocked — provided the network egress itself isn't IP-banned.
    Requires a one-time `python -m playwright install chromium`.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise BulletinParseError(
            'Playwright is not installed. Run `pip install playwright` and '
            '`python -m playwright install chromium`.') from e

    logger.debug('Fetching %s via headless Chromium', url)
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
        except Exception as e:
            raise BulletinParseError(
                'Could not launch Chromium. Run `python -m playwright install '
                f'chromium`. ({e})') from e
        try:
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()
            # Prime Akamai cookies by visiting the bulletin landing page first
            # (best effort — the direct download can still work without it).
            try:
                page.goto(DAILY_BULLETIN_PAGE, wait_until='domcontentloaded', timeout=60000)
            except Exception as e:
                logger.debug('Landing-page prime failed (continuing): %s', e)
            resp = context.request.get(url, timeout=60000)
            if resp.status != 200:
                raise DownloadBlockedError(
                    f'CME returned HTTP {resp.status} to the browser download')
            body = resp.body()
        finally:
            browser.close()

    if body[:5] != b'%PDF-':
        raise DownloadBlockedError(f'browser download was not a PDF (got {body[:40]!r})')
    return body


def download_pdf(url: str = BULLETIN_URL, browser: bool = False) -> bytes:
    """Fetch the PDF: requests first, then fall back to a headless browser."""
    if browser:
        return fetch_pdf_browser(url)
    try:
        return fetch_pdf(url)
    except DownloadBlockedError as e:
        logger.info('Direct download blocked (%s); retrying via headless browser', e)
        return fetch_pdf_browser(url)


# ---------------------------------------------------------------------------
# Parse helpers
# ---------------------------------------------------------------------------

def _is_num(t: str) -> bool:
    return bool(_NUM.match(t))


def _is_code(t: str) -> bool:
    return bool(_CODE.match(t)) and any(c.isalpha() for c in t)


def _to_int(t: str) -> int:
    return int(t.replace(',', ''))


def _line_words(line: dict) -> list[tuple[str, float]]:
    """Rebuild (text, x1) words from a line's chars, splitting glued name+number.

    pdfplumber's word grouping drops some product names onto adjacent baselines,
    so we regroup this line's own chars by horizontal gaps instead.
    """
    words: list[tuple[str, float]] = []
    cur, x1 = '', None
    for c in sorted(line['chars'], key=lambda c: c['x0']):
        if cur and c['x0'] - x1 > 2.0:
            words.append((cur, x1))
            cur = ''
        cur += c['text']
        x1 = c['x1']
    if cur:
        words.append((cur, x1))

    out: list[tuple[str, float]] = []
    for text, edge in words:
        m = _GLUED.match(text)
        if m:  # "FUT465011" -> "FUT", "465011" (number keeps the right edge)
            out.append((m.group('txt'), edge))
            out.append((m.group('num'), edge))
        else:
            out.append((text, edge))
    return out


def _cluster(xs: list[float], gap: float = _ANCHOR_GAP) -> list[float]:
    """Cluster sorted x positions into column anchors (mean of each cluster)."""
    xs = sorted(xs)
    clusters: list[list[float]] = [[xs[0]]]
    for x in xs[1:]:
        if x - clusters[-1][-1] > gap:
            clusters.append([x])
        else:
            clusters[-1].append(x)
    return [sum(c) / len(c) for c in clusters]


def _is_data_line(words: list[tuple[str, float]]) -> bool:
    """A product/total row: starts with a code (or TOTAL) and reaches the total col."""
    if not words:
        return False
    head = words[0][0]
    if not (_is_code(head) or head == 'TOTAL'):
        return False
    return any(_is_num(t) and edge > 330 for t, edge in words)


def _build_anchors(datalines: list[list[tuple[str, float]]]) -> tuple[list[float], dict]:
    """Derive column anchors for a page and map them to column names."""
    xs = [edge for words in datalines for t, edge in words if _is_num(t) and edge > _XMIN]
    anchors = _cluster(xs)
    if len(anchors) < 5:
        raise BulletinParseError(f'expected >=5 numeric columns, found {len(anchors)}')
    names: dict[float, str] = {}
    for name, a in zip(RIGHT5, anchors[-5:]):
        names[a] = name
    for name, a in zip(VENUE, anchors[:-5]):
        names[a] = name
    return anchors, names


def _parse_row(words: list[tuple[str, float]], anchors: list[float],
               names: dict, section: str) -> dict:
    first_num = next(i for i, (t, _) in enumerate(words) if _is_num(t))
    head = words[:first_num]
    code = head[0][0]
    is_total = code == 'TOTAL'

    name_words = head[1:]
    option_type = ''
    if section == 'OPTIONS' and name_words and name_words[-1][0] in ('C', 'P'):
        option_type = name_words[-1][0]
        name_words = name_words[:-1]
    name = ' '.join(t for t, _ in name_words)

    row = {c: None for c in RIGHT5 + VENUE}
    sign = 1
    for t, edge in words[first_num:]:
        if t in ('+', '-'):
            sign = -1 if t == '-' else 1
            continue
        if not _is_num(t):
            continue
        anchor = min(anchors, key=lambda a: abs(a - edge))
        if abs(anchor - edge) > _BUCKET_TOL or anchor not in names:
            continue
        row[names[anchor]] = _to_int(t)
    if row['oi_change'] is not None:
        row['oi_change'] *= sign

    row.update(product_code=code, product_name=name,
               option_type=option_type, is_total=is_total)
    return row


def _reconciles(row: dict) -> bool:
    """Venue volumes must sum to total_volume when the breakdown is present."""
    parts = [row[v] for v in VENUE if row[v] is not None]
    if not parts or row['total_volume'] is None:
        return True
    return sum(parts) == row['total_volume']


def parse_bulletin(pdf_bytes: bytes) -> tuple[str, str, list[dict]]:
    """Parse the bulletin PDF.

    Returns (trade_date_iso, report_status, rows).  Raises BulletinParseError
    if the layout is not recognised or rows fail the reconciliation check.
    """
    import io

    rows: list[dict] = []
    trade_date = None
    report_status = 'FINAL'
    section = 'FUTURES'          # side 01 is futures; options follow
    seen_product_header = False  # futures products start after this header line

    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    except Exception as e:
        raise BulletinParseError(f'could not open PDF: {e}') from e

    with pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            head = text[:600].upper()
            if trade_date is None:
                m = _DATE.search(text)
                if m:
                    trade_date = datetime.strptime(m.group(1), '%b %d, %Y').date().isoformat()
            if 'PRELIMINARY' in head:
                report_status = 'PRELIMINARY'
            if 'OPTIONS' in head:
                section = 'OPTIONS'

            lines = page.extract_text_lines()
            all_words = [_line_words(ln) for ln in lines]
            datalines = [w for w in all_words if _is_data_line(w)]
            if not datalines:
                continue
            anchors, names = _build_anchors(datalines)

            for words in all_words:
                raw = ' '.join(t for t, _ in words)
                if section == 'FUTURES' and 'INTEREST RATE FUTURES' in raw.upper():
                    seen_product_header = True
                    continue
                if not _is_data_line(words):
                    continue
                # On the futures page, skip the exchange/OTC summary block that
                # precedes the per-product listing.
                if section == 'FUTURES' and not seen_product_header:
                    continue
                row = _parse_row(words, anchors, names, section)
                if not _reconciles(row):
                    raise BulletinParseError(
                        f'row failed volume reconciliation: {row["product_code"]} '
                        f'{row["product_name"]!r} venue={[row[v] for v in VENUE]} '
                        f'total={row["total_volume"]}'
                    )
                row['report_section'] = section
                rows.append(row)

    if trade_date is None:
        raise BulletinParseError('could not find a trade date in the PDF header')
    if not rows:
        raise BulletinParseError('no product rows parsed — layout may have changed')
    return trade_date, report_status, rows


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

def _load_csv() -> list[dict]:
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, encoding='utf-8', newline='') as f:
        return list(csv.DictReader(f))


def _atomic_write(rows: list[dict]) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=CSV_PATH.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp, CSV_PATH)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def upsert_rows(new_rows: list[dict]) -> int:
    """Replace any existing rows for the same day/section, then append new ones.

    Returns the number of rows written for this run.
    """
    if not new_rows:
        return 0
    new_keys = {tuple(r.get(k, '') for k in KEY) for r in new_rows}
    kept = [r for r in _load_csv() if tuple(r.get(k, '') for k in KEY) not in new_keys]
    _atomic_write(kept + new_rows)
    return len(new_rows)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def pull(url: str = BULLETIN_URL, date_override: str | None = None,
         force: bool = False, file: str | None = None,
         browser: bool = False) -> dict:
    """Fetch (or read) the bulletin, parse it, and upsert to the CSV.

    Source precedence: ``file`` (a local PDF you downloaded yourself) →
    ``force`` (re-read an archived PDF for ``date_override``) → download
    (requests, then a headless-browser fallback for Akamai-blocked requests).

    Note: CME's Data Terms of Use restrict automated access to the daily
    bulletin. The download path is intended for a machine/network you're
    entitled to use; ``--file`` (parse a PDF you obtained yourself) remains the
    most clearly compliant option.
    """
    if file:
        logger.info('Parsing local PDF %s', file)
        pdf_bytes = Path(file).read_bytes()
    elif force and date_override:
        raw_path = RAW_DIR / f'{date_override}.pdf'
        logger.info('Re-parsing cached %s', raw_path)
        pdf_bytes = raw_path.read_bytes()
    else:
        pdf_bytes = download_pdf(url, browser=browser)

    trade_date, report_status, rows = parse_bulletin(pdf_bytes)
    if date_override:
        trade_date = date_override

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = RAW_DIR / f'{trade_date}.pdf'
    if not (force and raw_path.exists()):
        raw_path.write_bytes(pdf_bytes)

    fetched_at = datetime.now(timezone.utc).isoformat(timespec='seconds')
    for r in rows:
        r.update(trade_date=trade_date, report_status=report_status,
                 source_pdf=raw_path.name, fetched_at=fetched_at)

    written = upsert_rows(rows)
    logger.info('Parsed %s (%s): %d rows written to %s',
                trade_date, report_status, written, CSV_PATH)
    return {'trade_date': trade_date, 'report_status': report_status,
            'rows': written, 'csv': str(CSV_PATH)}


def query(sql: str) -> None:
    """Run a DuckDB SQL query over the CSV. The table is exposed as `data`."""
    import duckdb

    if not CSV_PATH.exists():
        raise SystemExit(f'no data yet at {CSV_PATH} — run `pull` first')
    con = duckdb.connect()
    csv_literal = str(CSV_PATH).replace("'", "''")
    con.execute(
        f"CREATE VIEW data AS SELECT * FROM read_csv_auto('{csv_literal}', header=true)"
    )
    print(con.sql(sql))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='CME Interest-Rate futures & options volume / open-interest tracker')
    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('pull', help='parse and store a bulletin (download or local file)')
    p.add_argument('--file', default=None,
                   help='parse a local PDF you downloaded yourself (bypasses download)')
    p.add_argument('--browser', action='store_true',
                   help='force the headless-browser download (skip the requests attempt)')
    p.add_argument('--date', default=None, help='override trade date (YYYY-MM-DD)')
    p.add_argument('--force', action='store_true',
                   help='re-parse the stored raw PDF for --date instead of downloading')
    p.add_argument('--url', default=BULLETIN_URL, help='override source PDF URL')
    p.add_argument('--verbose', '-v', action='store_true')

    q = sub.add_parser('query', help='run DuckDB SQL over the stored data (table: data)')
    q.add_argument('sql', help='SQL query, e.g. "SELECT * FROM data LIMIT 5"')
    q.add_argument('--verbose', '-v', action='store_true')

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, 'verbose', False) else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.command == 'pull':
        pull(url=args.url, date_override=args.date, force=args.force,
             file=args.file, browser=args.browser)
    elif args.command == 'query':
        query(args.sql)


if __name__ == '__main__':
    main()
