"""Rebuild the DuckDB store from all existing tracking CSVs.

Reads registry/universe.csv for issuer metadata, then loads every
output/{ticker}/tracking.csv into data/derivatives.duckdb.

Usage:
    python scripts/rebuild_db.py
    python scripts/rebuild_db.py --db-path data/derivatives.duckdb
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db import get_connection, load_csv_into_db, load_issuer_text_files, query

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('rebuild_db')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / 'output'
UNIVERSE_CSV = PROJECT_ROOT / 'registry' / 'universe.csv'


def load_universe() -> dict:
    """Load universe.csv into a lookup dict keyed by uppercase ticker."""
    lookup = {}
    if not UNIVERSE_CSV.exists():
        return lookup
    with open(UNIVERSE_CSV, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            ticker = row.get('ticker', '').upper()
            if ticker:
                lookup[ticker] = row
    return lookup


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db-path', default=None, help='Override DB path')
    args = p.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    conn = get_connection(db_path)

    conn.execute('DELETE FROM extractions')

    universe = load_universe()
    logger.info(f'Loaded {len(universe)} issuers from universe.csv')

    total_rows = 0
    issuers_loaded = 0
    total_findings = 0
    total_alerts = 0

    for issuer_dir in sorted(OUTPUT_DIR.iterdir()):
        if not issuer_dir.is_dir():
            continue
        ticker = issuer_dir.name.upper()
        csv_path = issuer_dir / 'tracking.csv'
        if csv_path.exists():
            count = load_csv_into_db(conn, csv_path, ticker, universe)
            if count > 0:
                issuers_loaded += 1
                total_rows += count
        nf, na = load_issuer_text_files(conn, issuer_dir, ticker)
        total_findings += nf
        total_alerts += na

    logger.info(f'Loaded {total_rows} rows from {issuers_loaded} issuers')
    logger.info(f'Loaded {total_findings} qualitative findings, {total_alerts} alerts')

    stats = query(conn, """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT ticker) as issuers,
            MIN(period_end) as earliest,
            MAX(period_end) as latest,
            COUNT(CASE WHEN has_derivatives = 'Yes' THEN 1 END) as has_derivs,
            COUNT(CASE WHEN fx_derivatives_notional IS NOT NULL THEN 1 END) as has_fx_notional,
            COUNT(CASE WHEN ir_swap_notional IS NOT NULL THEN 1 END) as has_ir_notional
        FROM extractions
    """)[0]

    logger.info(f'DB stats:')
    logger.info(f'  Rows: {stats["total_rows"]}')
    logger.info(f'  Issuers: {stats["issuers"]}')
    logger.info(f'  Period range: {stats["earliest"]} → {stats["latest"]}')
    logger.info(f'  Rows with has_derivatives=Yes: {stats["has_derivs"]}')
    logger.info(f'  Rows with FX notional: {stats["has_fx_notional"]}')
    logger.info(f'  Rows with IR notional: {stats["has_ir_notional"]}')

    # Sample query to verify
    top_fx = query(conn, """
        SELECT ticker, period_end, fx_derivatives_notional
        FROM extractions
        WHERE fx_derivatives_notional IS NOT NULL
        ORDER BY fx_derivatives_notional DESC
        LIMIT 5
    """)
    if top_fx:
        logger.info(f'Top 5 FX derivatives notional:')
        for r in top_fx:
            logger.info(f'  {r["ticker"]} ({r["period_end"]}): ${r["fx_derivatives_notional"]:,.0f}M')

    conn.close()


if __name__ == '__main__':
    main()
