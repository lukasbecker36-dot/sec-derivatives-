"""Filing-wide derivatives table extractor.

Locates derivative notional tables in 10-Q/10-K filings by scoring every
<table>, then LLM-normalises the top candidates into long-format rows.

Designed to be robust to the Note-heading regex failures that the current
section_extract.py pipeline suffers from (e.g. PM 10-K, MRK 10-Q cross-refs).

Output schema (one row per (issuer, period_end, asset_class, designation,
currency, instrument_type)):

    ticker, cik, period_end, form_type, accession,
    asset_class            # fx | ir | commodity | equity | credit | other
    designation            # designated | not_designated | net_investment | total
    instrument_type        # forward | swap | option | collar | future | other
    currency               # ISO 4217 or "mixed" / None
    notional_usd_millions
    fair_value_asset_usd_millions
    fair_value_liability_usd_millions
    source_table_idx
    source_snippet
    extracted_at
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .filing_fetcher import discover_filings, ARCHIVES, HEADERS
from .utils import sec_rate_limiter

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
AGG_DIR = REPO_ROOT / "output" / "_aggregate"
LONG_CSV = AGG_DIR / "derivatives_long.csv"
LONG_FIELDS = [
    "ticker", "cik", "period_end", "form_type", "accession",
    "asset_class", "designation", "instrument_type", "currency",
    "notional_usd_millions",
    "fair_value_asset_usd_millions",
    "fair_value_liability_usd_millions",
    "source_table_idx", "source_snippet", "extracted_at",
]


DERIV_KEYWORDS = [
    "notional", "designated", "not designated", "cash flow hedge",
    "fair value hedge", "net investment hedge", "forward", "swap",
    "option", "collar", "derivative", "hedge", "currency forward",
    "foreign currency", "interest rate", "commodity",
]
FX_KEYWORDS = ["foreign currency", "currency forward", "fx forward", "euro",
               "yen", "pound", "peso", "yuan", "renminbi"]
ASSET_CLASS_TOKENS = {
    "fx": ["foreign currency", "currency forward", "fx forward"],
    "ir": ["interest rate swap", "interest rate contract"],
    "commodity": ["commodity", "fuel", "aluminum", "copper", "natural gas", "jet fuel"],
    "equity": ["equity contract", "equity forward"],
}
DEBT_SCHEDULE_TOKENS = ["due ", "maturity", "senior notes", "fixed-rate", "floating-rate"]


@dataclass
class TableCandidate:
    idx: int
    score: float
    rows: int
    cols: int
    deriv_kw: int
    fx_kw: int
    has_notional: bool
    asset_classes: list[str]
    text: str = ""


def _fetch_raw_html(cik: str, accession: str, document: str) -> str:
    cik_num = cik.lstrip("0")
    acc_nodash = accession.replace("-", "")
    url = f"{ARCHIVES}/{cik_num}/{acc_nodash}/{document}"
    sec_rate_limiter.wait()
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text


def _render_table(tbl) -> str:
    rows = []
    for tr in tbl.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        cells = [c for c in cells if c]
        if cells:
            rows.append(" | ".join(cells))
    return "\n".join(rows)


def _score_table(text: str, rows: int, cols: int) -> TableCandidate | None:
    if rows < 2 or len(text) < 40:
        return None
    low = text.lower()
    deriv_hits = sum(low.count(k) for k in DERIV_KEYWORDS)
    fx_hits = sum(low.count(k) for k in FX_KEYWORDS)
    has_notional = "notional" in low
    asset_classes = [ac for ac, toks in ASSET_CLASS_TOKENS.items()
                     if any(t in low for t in toks)]
    if deriv_hits == 0 and not has_notional:
        return None
    debt_schedule_hits = sum(low.count(t) for t in DEBT_SCHEDULE_TOKENS)
    is_debt_schedule = debt_schedule_hits >= 3 and "swap" not in low

    dollar_count = len(re.findall(r"\$\s*[\d,]+", text)) + \
                   len(re.findall(r"\b\d{1,3}(?:,\d{3}){1,}\b", text))
    score = (
        8.0 * has_notional
        + 1.5 * deriv_hits
        + 1.0 * fx_hits
        + 0.3 * min(dollar_count, 20)
        + 1.0 * len(asset_classes)
    )
    if rows > 80:
        score *= 0.5
    if cols < 2:
        score *= 0.6
    if is_debt_schedule:
        score *= 0.3
    return TableCandidate(
        idx=-1, score=round(score, 2), rows=rows, cols=cols,
        deriv_kw=deriv_hits, fx_kw=fx_hits,
        has_notional=has_notional, asset_classes=asset_classes,
        text=text,
    )


def sweep_filing(html: str, top_k: int = 3) -> list[TableCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[TableCandidate] = []
    for i, tbl in enumerate(soup.find_all("table")):
        text = _render_table(tbl)
        if not text:
            continue
        first_row = tbl.find("tr")
        cols = len(first_row.find_all(["td", "th"])) if first_row else 0
        rows = len(tbl.find_all("tr"))
        cand = _score_table(text, rows, cols)
        if cand is None:
            continue
        cand.idx = i
        out.append(cand)
    out.sort(key=lambda x: x.score, reverse=True)
    return out[:top_k]


LLM_SYSTEM = (
    "You normalise SEC derivatives notional tables into structured JSON rows. "
    "Read the table text and output one row per distinct combination of "
    "(asset_class, designation, instrument_type, currency). Use millions of USD. "
    "Do not invent values. If a cell is blank or unclear, return null. "
    "Do not include rows that aren't derivatives (e.g. debt schedules, AOCI rollforward)."
)

LLM_USER_TEMPLATE = """Issuer: {ticker}
Period end: {period_end}
Form: {form_type}

Table text:
---
{table_text}
---

Return ONLY JSON in this exact shape:
{{
  "rows": [
    {{
      "asset_class": "fx|ir|commodity|equity|credit|other",
      "designation": "designated|not_designated|net_investment|total",
      "instrument_type": "forward|swap|option|collar|future|other",
      "currency": "USD|EUR|GBP|... or null if not specified or mixed",
      "notional_usd_millions": number or null,
      "fair_value_asset_usd_millions": number or null,
      "fair_value_liability_usd_millions": number or null
    }}
  ]
}}

Rules:
- "Foreign currency exchange contracts" → asset_class="fx", instrument_type="forward" unless otherwise stated.
- "Interest rate swaps" / "Interest rate contracts" → asset_class="ir", instrument_type="swap".
- A "Total" row should have designation="total".
- Convert all amounts to USD millions (if "billion" used, multiply by 1000).
- Pick the MOST RECENT period column if the table shows multiple periods (i.e. {period_end}).
- If a row has no notional and no fair value, skip it.
- Empty rows list is acceptable if the table has no extractable derivative rows.
"""


def _call_openai(table_text: str, ticker: str, period_end: str, form_type: str) -> dict:
    import openai
    client = openai.OpenAI()
    resp = client.chat.completions.create(
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        messages=[
            {"role": "system", "content": LLM_SYSTEM},
            {"role": "user", "content": LLM_USER_TEMPLATE.format(
                ticker=ticker, period_end=period_end, form_type=form_type,
                table_text=table_text[:12000],
            )},
        ],
        response_format={"type": "json_object"},
        max_tokens=2048,
        temperature=0,
    )
    raw = resp.choices[0].message.content
    return json.loads(raw)


def extract_long_rows(
    ticker: str, cik: str, filing: dict, top_k: int = 3,
) -> list[dict]:
    """Run the full sweep + LLM normalisation for one filing.

    Returns a list of long-format row dicts (LONG_FIELDS).
    """
    html = _fetch_raw_html(cik, filing["accession_number"], filing["primary_document"])
    candidates = sweep_filing(html, top_k=top_k)
    if not candidates:
        logger.info(f"{ticker}: no candidate derivative tables found")
        return []

    rows: list[dict] = []
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for cand in candidates:
        try:
            result = _call_openai(cand.text, ticker, filing["period_end"], filing["form_type"])
        except Exception as e:
            logger.warning(f"{ticker} table #{cand.idx}: LLM call failed: {e!r}")
            continue
        for r in result.get("rows", []):
            if r.get("notional_usd_millions") is None and \
               r.get("fair_value_asset_usd_millions") is None and \
               r.get("fair_value_liability_usd_millions") is None:
                continue
            rows.append({
                "ticker": ticker,
                "cik": cik.lstrip("0"),
                "period_end": filing["period_end"],
                "form_type": filing["form_type"],
                "accession": filing["accession_number"],
                "asset_class": r.get("asset_class"),
                "designation": r.get("designation"),
                "instrument_type": r.get("instrument_type"),
                "currency": r.get("currency"),
                "notional_usd_millions": r.get("notional_usd_millions"),
                "fair_value_asset_usd_millions": r.get("fair_value_asset_usd_millions"),
                "fair_value_liability_usd_millions": r.get("fair_value_liability_usd_millions"),
                "source_table_idx": cand.idx,
                "source_snippet": cand.text[:200].replace("\n", " | "),
                "extracted_at": ts,
            })
    return rows


def append_rows(rows: list[dict], path: Path = LONG_CSV) -> None:
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LONG_FIELDS, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def pick_filings_for_growth(cik: str) -> list[dict]:
    """Pick the current latest filing + the right baseline.

    - If latest is a 10-Q, baseline = same-quarter prior-year 10-Q (or latest
      10-Q from ~12 months prior).
    - If latest is a 10-K, baseline = prior 10-K.
    - If latest 10-Q exists but no same-quarter prior, fall back to the most
      recent 10-K before the 10-Q.
    """
    all_filings = discover_filings(cik)
    if not all_filings:
        return []
    qs = [f for f in all_filings if f["form_type"] in ("10-Q", "10-Q/A")]
    ks = [f for f in all_filings if f["form_type"] in ("10-K", "10-K/A")]
    latest = all_filings[-1]
    if latest["form_type"].startswith("10-Q") and qs:
        cur = qs[-1]
        cur_month = cur["period_end"][5:7]
        prior_same_q = [f for f in qs[:-1] if f["period_end"][5:7] == cur_month]
        baseline = prior_same_q[-1] if prior_same_q else (ks[-1] if ks else None)
        return [b for b in (cur, baseline) if b]
    if latest["form_type"].startswith("10-K") and ks:
        cur = ks[-1]
        baseline = ks[-2] if len(ks) >= 2 else None
        return [b for b in (cur, baseline) if b]
    return [latest]


def run_for_issuer(ticker: str, cik: str, mode: str = "growth") -> int:
    """Extract derivative rows for an issuer. Returns row count written."""
    if mode == "growth":
        filings = pick_filings_for_growth(cik)
    elif mode == "latest":
        all_filings = discover_filings(cik)
        filings = [all_filings[-1]] if all_filings else []
    else:
        raise ValueError(f"unknown mode: {mode}")

    total = 0
    for f in filings:
        try:
            rows = extract_long_rows(ticker, cik, f)
        except Exception as e:
            logger.error(f"{ticker} {f.get('period_end')}: extraction failed: {e!r}")
            continue
        append_rows(rows)
        total += len(rows)
        logger.info(f"{ticker} {f['form_type']} {f['period_end']}: {len(rows)} rows")
    return total


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", help="single ticker to process")
    p.add_argument("--cik", help="CIK for --ticker")
    p.add_argument("--universe", action="store_true", help="run against registry/universe.csv")
    p.add_argument("--status", default="active,active_needs_review",
                   help="comma-separated statuses to include when --universe")
    p.add_argument("--mode", default="growth", choices=["growth", "latest"])
    p.add_argument("--limit", type=int, default=0, help="cap number of issuers (0 = no cap)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.ticker:
        if not args.cik:
            print("--cik required with --ticker", file=sys.stderr)
            return 2
        n = run_for_issuer(args.ticker.upper(), args.cik, mode=args.mode)
        print(f"{args.ticker}: {n} rows written to {LONG_CSV}")
        return 0

    if args.universe:
        import csv
        allowed = set(args.status.split(","))
        with (REPO_ROOT / "registry" / "universe.csv").open(encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            issuers = [r for r in rdr if r["status"] in allowed]
        if args.limit:
            issuers = issuers[: args.limit]
        print(f"Processing {len(issuers)} issuers, mode={args.mode}")
        total = 0
        for i, row in enumerate(issuers, 1):
            print(f"[{i}/{len(issuers)}] {row['ticker']} ({row['cik']})")
            try:
                total += run_for_issuer(row["ticker"], row["cik"], mode=args.mode)
            except Exception as e:
                print(f"  FAILED: {e!r}")
        print(f"\nTotal rows written: {total}")
        print(f"Output: {LONG_CSV}")
        return 0

    print("Specify --ticker --cik, or --universe", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
