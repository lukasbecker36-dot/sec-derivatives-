"""FX derivatives top-10 ranker.

Reads output/_aggregate/derivatives_long.csv (produced by
src.derivatives_table_extract) and computes:
  - per-issuer current FX notional (sum across all FX rows, latest filing)
  - per-issuer baseline FX notional (prior same-quarter 10-Q or prior 10-K)
  - absolute and percent growth
  - top-10 ranking by current notional, with growth columns

Usage:
    python -m scripts.fx_top10
    python -m scripts.fx_top10 --by growth      # rank by growth pct instead
    python -m scripts.fx_top10 --csv out.csv    # write to CSV
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LONG_CSV = REPO_ROOT / "output" / "_aggregate" / "derivatives_long.csv"


def _to_float(x: str | None) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def load_rows():
    if not LONG_CSV.exists():
        print(f"No data at {LONG_CSV}. Run src.derivatives_table_extract first.",
              file=sys.stderr)
        sys.exit(1)
    with LONG_CSV.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def aggregate_fx_by_filing(rows):
    """Return {(ticker, period_end, form_type): total_fx_notional_millions}."""
    totals: dict[tuple[str, str, str], float] = defaultdict(float)
    seen: dict[tuple[str, str, str], bool] = {}
    for r in rows:
        if r.get("asset_class") != "fx":
            continue
        # Skip "total" rows to avoid double-counting when we also have the
        # designated + not_designated breakdown.
        designation = (r.get("designation") or "").lower()
        notional = _to_float(r.get("notional_usd_millions"))
        if notional is None:
            continue
        key = (r["ticker"], r["period_end"], r["form_type"])
        # If we see designated + not_designated rows, sum them and skip totals.
        # Track which keys have non-total rows.
        if designation != "total":
            totals[key] += notional
            seen[key] = True
        elif key not in seen:
            totals[key] = notional
    return totals


def pick_current_and_baseline(totals):
    """For each ticker, pick latest filing + appropriate baseline.

    - Latest 10-Q → baseline is same-quarter prior 10-Q if available, else
      prior 10-K.
    - Latest 10-K → baseline is prior 10-K.
    """
    by_ticker: dict[str, list[tuple[str, str, float]]] = defaultdict(list)
    for (ticker, period_end, form_type), v in totals.items():
        by_ticker[ticker].append((period_end, form_type, v))

    result = []
    for ticker, entries in by_ticker.items():
        entries.sort(key=lambda x: x[0])  # by period_end asc
        latest = entries[-1]
        cur_pe, cur_form, cur_v = latest

        baseline = None
        if cur_form.startswith("10-Q"):
            cur_month = cur_pe[5:7]
            prior_q = [e for e in entries[:-1]
                       if e[1].startswith("10-Q") and e[0][5:7] == cur_month]
            if prior_q:
                baseline = prior_q[-1]
            else:
                prior_k = [e for e in entries[:-1] if e[1].startswith("10-K")]
                if prior_k:
                    baseline = prior_k[-1]
        elif cur_form.startswith("10-K"):
            prior_k = [e for e in entries[:-1] if e[1].startswith("10-K")]
            if prior_k:
                baseline = prior_k[-1]

        base_pe = baseline[0] if baseline else None
        base_form = baseline[1] if baseline else None
        base_v = baseline[2] if baseline else None
        growth_abs = (cur_v - base_v) if base_v is not None else None
        growth_pct = ((cur_v - base_v) / base_v * 100) if base_v else None

        result.append({
            "ticker": ticker,
            "current_period": cur_pe,
            "current_form": cur_form,
            "current_fx_notional_m": round(cur_v, 1),
            "baseline_period": base_pe,
            "baseline_form": base_form,
            "baseline_fx_notional_m": round(base_v, 1) if base_v is not None else None,
            "growth_abs_m": round(growth_abs, 1) if growth_abs is not None else None,
            "growth_pct": round(growth_pct, 1) if growth_pct is not None else None,
        })
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--by", choices=["notional", "growth_pct", "growth_abs"],
                   default="notional")
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--csv", help="write full ranking to this path")
    args = p.parse_args()

    rows = load_rows()
    totals = aggregate_fx_by_filing(rows)
    ranking = pick_current_and_baseline(totals)

    key_fn = {
        "notional": lambda r: r["current_fx_notional_m"] or 0,
        "growth_abs": lambda r: r["growth_abs_m"] or 0,
        "growth_pct": lambda r: r["growth_pct"] or 0,
    }[args.by]
    ranking.sort(key=key_fn, reverse=True)

    headers = ["#", "ticker", "current", "FX notional $m", "baseline",
               "baseline $m", "Δ $m", "Δ %"]
    print(f"\nTop {args.top} by {args.by} (FX notional, $ millions)\n")
    print(" | ".join(f"{h:>14}" for h in headers))
    print("-" * 130)
    for i, r in enumerate(ranking[: args.top], 1):
        print(" | ".join([
            f"{i:>14}",
            f"{r['ticker']:>14}",
            f"{r['current_form']} {r['current_period']:>10}",
            f"{r['current_fx_notional_m']:>14,.0f}",
            f"{(r['baseline_form'] or '-'):>6} {r['baseline_period'] or '-':>10}",
            f"{(r['baseline_fx_notional_m'] or 0):>14,.0f}" if r["baseline_fx_notional_m"] is not None else f"{'-':>14}",
            f"{(r['growth_abs_m'] or 0):>14,.0f}" if r["growth_abs_m"] is not None else f"{'-':>14}",
            f"{(r['growth_pct'] or 0):>13.1f}%" if r["growth_pct"] is not None else f"{'-':>14}",
        ]))

    if args.csv:
        out = Path(args.csv)
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(ranking[0].keys()))
            w.writeheader()
            w.writerows(ranking)
        print(f"\nFull ranking written to {out}")


if __name__ == "__main__":
    main()
