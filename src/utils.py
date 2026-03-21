"""Shared helpers — dollar parsing, text cleaning, logging, rate limiting."""

import csv
import re
import time
import threading
from pathlib import Path
from datetime import datetime, timezone

from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Dollar-amount parsing (lifted from extract_derivatives_meta.py)
# ---------------------------------------------------------------------------

def parse_dollar(s):
    """Parse a dollar string like '$2,800', '($39)', '—' into a float (millions)."""
    if s is None:
        return None
    s = str(s).strip()
    if not s or s in ('\u2014', '\u2013', '\u2012', '-', '—', '–'):
        return 0
    neg = False
    if '(' in s and ')' in s:
        neg = True
        s = s.replace('(', '').replace(')', '')
    s = s.replace('$', '').replace(',', '').replace('\xa0', '').strip()
    if not s or s in ('\u2014', '\u2013', '\u2012', '-', '—', '–'):
        return 0
    try:
        val = float(s)
    except ValueError:
        return None
    return -val if neg else val


def extract_first_dollar(text):
    """Extract the first dollar amount from a text fragment."""
    m = re.search(r'\$\s*([\d,.]+)', text)
    if m:
        return parse_dollar('$' + m.group(1))
    return None


# ---------------------------------------------------------------------------
# Text cleaning
# ---------------------------------------------------------------------------

def clean_filing_text(html: str) -> str:
    """Clean raw filing HTML into plain text for extraction."""
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(' ', strip=True)
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\d+\s+Table of Contents', '', text)
    return text


def extract_sentences(text: str) -> list[str]:
    """Split text into sentences (rough heuristic)."""
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
    return [s.strip() for s in sentences if len(s.strip()) > 20]


def normalise_for_comparison(text: str) -> str:
    """Normalise text for [NEW] comparison — dates→[DATE], amounts→$[AMT]."""
    text = re.sub(
        r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
        '[DATE]', text
    )
    text = re.sub(r'\d{1,2}/\d{1,2}/\d{2,4}', '[DATE]', text)
    text = re.sub(r'Q[1-4]\s+\d{4}', '[DATE]', text)
    text = re.sub(r'(?:three|six|nine|twelve)\s+months\s+ended\s+[A-Z][a-z]+\s+\d{1,2},?\s+\d{4}', '[PERIOD]', text, flags=re.I)
    text = re.sub(r'\$\s*[\d,]+(?:\.\d+)?(?:\s*(?:million|billion))?', '$[AMT]', text)
    return text


# ---------------------------------------------------------------------------
# LLM usage logging
# ---------------------------------------------------------------------------

_LOG_LOCK = threading.Lock()

def log_llm_usage(log_path: Path, issuer: str, section: str, model: str,
                   input_tokens: int, output_tokens: int, cost_usd: float):
    """Append a row to the LLM usage log CSV."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with _LOG_LOCK:
        write_header = not log_path.exists()
        with open(log_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(['timestamp', 'issuer', 'section', 'model',
                                 'input_tokens', 'output_tokens', 'cost_usd'])
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                issuer, section, model,
                input_tokens, output_tokens, f'{cost_usd:.6f}',
            ])


# ---------------------------------------------------------------------------
# Rate limiter (for SEC EDGAR — max 10 req/s)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, max_per_second: float = 10.0):
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last = time.monotonic()


# Global rate limiter for SEC requests
sec_rate_limiter = RateLimiter(max_per_second=10.0)
