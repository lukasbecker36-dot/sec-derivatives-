"""Prototype: filing-wide derivatives table sweep.

Validates the hypothesis that we can find FX/derivatives notional tables by
scoring every <table> in a filing for derivative-context keywords + $ amounts,
WITHOUT relying on the Note-heading regex that currently misses sections.

Usage:
    python -m scripts.validate_table_sweep

Outputs a report per (ticker, filing) showing the top-scoring tables and
whether they contain plausible FX notional numbers.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup

from src.filing_fetcher import discover_filings, ARCHIVES, HEADERS
from src.utils import sec_rate_limiter


CASES = [
    ("BA",   "0000012927", "10-Q"),
    ("F",    "0000037996", "10-Q"),
    ("INTC", "0000050863", "10-Q"),
    ("PM",   "0001413329", "10-K"),  # the known-broken one
    ("MRK",  "0000310158", "10-K"),
    ("ORCL", "0001341439", "10-K"),
]

DERIV_KEYWORDS = [
    "notional", "designated", "not designated", "cash flow hedge",
    "fair value hedge", "net investment hedge", "forward", "swap",
    "option", "collar", "derivative", "hedge", "currency forward",
    "foreign currency", "interest rate", "commodity",
]
FX_KEYWORDS = ["foreign currency", "currency forward", "fx", "euro", "yen",
               "pound", "peso", "yuan", "renminbi", "real ", "rupee"]
ASSET_CLASS_TOKENS = {
    "fx": ["foreign currency", "currency forward", "fx forward"],
    "ir": ["interest rate swap", "interest rate contract"],
    "commodity": ["commodity", "fuel", "aluminum", "copper", "natural gas", "jet fuel"],
    "equity": ["equity contract", "equity forward"],
}


@dataclass
class TableScore:
    idx: int
    score: float
    rows: int
    cols: int
    derivative_kw_hits: int
    fx_kw_hits: int
    dollar_count: int
    has_notional_header: bool
    asset_classes: list[str] = field(default_factory=list)
    snippet: str = ""


def fetch_raw_html(cik: str, accession: str, document: str) -> str:
    cik_num = cik.lstrip("0")
    acc_nodash = accession.replace("-", "")
    url = f"{ARCHIVES}/{cik_num}/{acc_nodash}/{document}"
    sec_rate_limiter.wait()
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text


def render_table_text(tbl) -> str:
    rows = []
    for tr in tbl.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        cells = [c for c in cells if c]
        if cells:
            rows.append(" | ".join(cells))
    return "\n".join(rows)


def score_table(text: str, rows: int, cols: int) -> TableScore | None:
    if rows < 2 or len(text) < 40:
        return None
    low = text.lower()
    deriv_hits = sum(low.count(k) for k in DERIV_KEYWORDS)
    fx_hits = sum(low.count(k) for k in FX_KEYWORDS)
    dollar_count = len(re.findall(r"\$\s*[\d,]+", text)) + len(
        re.findall(r"\b\d{1,3}(?:,\d{3}){1,}\b", text)
    )
    has_notional = "notional" in low
    asset_classes = [ac for ac, toks in ASSET_CLASS_TOKENS.items()
                     if any(t in low for t in toks)]

    if deriv_hits == 0 and not has_notional:
        return None

    score = (
        4.0 * has_notional
        + 1.5 * deriv_hits
        + 1.0 * fx_hits
        + 0.3 * min(dollar_count, 20)
        + 1.0 * len(asset_classes)
    )
    # Penalise tiny tables and giant boilerplate tables
    if rows > 80:
        score *= 0.5
    if cols < 2:
        score *= 0.6

    snippet = text[:300].replace("\n", " ⏎ ")
    return TableScore(
        idx=-1, score=round(score, 2), rows=rows, cols=cols,
        derivative_kw_hits=deriv_hits, fx_kw_hits=fx_hits,
        dollar_count=dollar_count, has_notional_header=has_notional,
        asset_classes=asset_classes, snippet=snippet,
    )


def sweep(html: str) -> list[TableScore]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[TableScore] = []
    for i, tbl in enumerate(soup.find_all("table")):
        text = render_table_text(tbl)
        if not text:
            continue
        first_row = tbl.find("tr")
        cols = len(first_row.find_all(["td", "th"])) if first_row else 0
        rows = len(tbl.find_all("tr"))
        s = score_table(text, rows, cols)
        if s is None:
            continue
        s.idx = i
        results.append(s)
    results.sort(key=lambda x: x.score, reverse=True)
    return results


def latest_filing(cik: str, form: str) -> dict | None:
    filings = [f for f in discover_filings(cik) if f["form_type"] == form]
    return filings[-1] if filings else None


def main() -> int:
    for ticker, cik, form in CASES:
        print(f"\n{'='*72}\n{ticker}  CIK={cik}  form={form}")
        try:
            filing = latest_filing(cik, form)
            if not filing:
                print("  no filing found"); continue
            print(f"  period_end={filing['period_end']}  acc={filing['accession_number']}")
            html = fetch_raw_html(cik, filing["accession_number"], filing["primary_document"])
            print(f"  bytes={len(html):,}")
            ranked = sweep(html)
            print(f"  candidate tables: {len(ranked)}")
            for r in ranked[:5]:
                print(
                    f"   #{r.idx:>4}  score={r.score:>6.1f}  rows={r.rows:>3}  cols={r.cols:>2}  "
                    f"deriv_kw={r.derivative_kw_hits:>2}  fx_kw={r.fx_kw_hits:>2}  "
                    f"$={r.dollar_count:>3}  notional={r.has_notional_header}  "
                    f"classes={r.asset_classes}"
                )
                print(f"        snippet: {r.snippet[:200]}")
        except Exception as e:
            print(f"  ERROR: {e!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
