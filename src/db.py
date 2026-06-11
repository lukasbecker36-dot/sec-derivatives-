"""Consolidated DuckDB store for all extraction data.

Single source of truth for cross-issuer queries. Both the daily pipeline
and the backfill pipeline write here. Per-issuer CSVs remain as a
human-readable export but are not the query target.

Usage:
    from src.db import get_connection, upsert_extraction, query

    conn = get_connection()
    upsert_extraction(conn, row_dict)
    results = query(conn, "SELECT ticker, fx_derivatives_notional ...")
"""

import csv
import json
import logging
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / 'data' / 'derivatives.duckdb'

# Fields shared by all issuers (from minimal_hedger archetype).
# These get real columns for direct SQL queries.
CORE_FIELDS = [
    # Financial instruments
    'total_cash_equivalents_fv',
    'total_marketable_securities_fv',
    'marketable_equity_securities_fv',
    'corporate_debt_securities_fv',
    'gross_unrealized_gains_debt',
    'fi_fx_derivatives_notional',
    'fi_fx_designated_notional',
    'fi_fx_not_designated_notional',
    'fi_ir_swap_notional',
    'fi_total_derivative_asset',
    'fi_total_derivative_liability',
    # Derivatives note
    'has_derivatives',
    'fx_derivatives_notional',
    'fx_designated_notional',
    'fx_not_designated_notional',
    'ir_swap_notional',
    'commodity_derivatives_notional',
    'equity_derivatives_notional',
    'total_derivative_asset',
    'total_derivative_liability',
    'net_derivative_position',
    'cash_flow_hedge_aoci',
    'expected_12mo_reclass_from_aoci',
    # Market risk
    'fx_forwards_outstanding',
    'fx_transaction_gain_loss',
    'ir_sensitivity_100bp',
    'fx_sensitivity_10pct',
    'fixed_rate_notes_billion',
    'marketable_equity_value',
    'equity_sensitivity_10pct',
]

_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS extractions (
    -- Identity / dedup key
    ticker              VARCHAR NOT NULL,
    accession_number    VARCHAR NOT NULL,
    -- Filing metadata
    issuer_name         VARCHAR,
    cik                 VARCHAR,
    sector              VARCHAR,
    period_end          DATE NOT NULL,
    form_type           VARCHAR,
    filing_date         DATE,
    processed_at        TIMESTAMP,
    extraction_version  INTEGER,
    -- Financial instruments (core)
    total_cash_equivalents_fv       DOUBLE,
    total_marketable_securities_fv  DOUBLE,
    marketable_equity_securities_fv DOUBLE,
    corporate_debt_securities_fv    DOUBLE,
    gross_unrealized_gains_debt     DOUBLE,
    fi_fx_derivatives_notional      DOUBLE,
    fi_fx_designated_notional       DOUBLE,
    fi_fx_not_designated_notional   DOUBLE,
    fi_ir_swap_notional             DOUBLE,
    fi_total_derivative_asset       DOUBLE,
    fi_total_derivative_liability   DOUBLE,
    -- Derivatives note (core)
    has_derivatives                 VARCHAR,
    fx_derivatives_notional         DOUBLE,
    fx_designated_notional          DOUBLE,
    fx_not_designated_notional      DOUBLE,
    ir_swap_notional                DOUBLE,
    commodity_derivatives_notional  DOUBLE,
    equity_derivatives_notional     DOUBLE,
    total_derivative_asset          DOUBLE,
    total_derivative_liability      DOUBLE,
    net_derivative_position         DOUBLE,
    cash_flow_hedge_aoci            DOUBLE,
    expected_12mo_reclass_from_aoci DOUBLE,
    -- Market risk (core)
    fx_forwards_outstanding         DOUBLE,
    fx_transaction_gain_loss        DOUBLE,
    ir_sensitivity_100bp            DOUBLE,
    fx_sensitivity_10pct            DOUBLE,
    fixed_rate_notes_billion        DOUBLE,
    marketable_equity_value         DOUBLE,
    equity_sensitivity_10pct        DOUBLE,
    -- Issuer-specific fields (JSON blob)
    extra_fields        JSON,
    -- Dedup constraint
    PRIMARY KEY (ticker, accession_number)
);
"""

_VIEW_DDL = """
CREATE OR REPLACE VIEW latest_extractions AS
SELECT e.*
FROM extractions e
INNER JOIN (
    SELECT ticker, MAX(period_end) AS max_period
    FROM extractions
    GROUP BY ticker
) latest ON e.ticker = latest.ticker AND e.period_end = latest.max_period;
"""


def get_connection(db_path: Path | None = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path), read_only=read_only)
    if not read_only:
        conn.execute(_SCHEMA_DDL)
        conn.execute(_VIEW_DDL)
    return conn


def _coerce_numeric(val: Any) -> float | None:
    if val is None or val == '' or val == 'null':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _coerce_date(val: Any) -> str | None:
    if val is None or val == '':
        return None
    s = str(val).strip()[:10]
    if len(s) >= 10 and s[4] == '-':
        return s
    return None


def upsert_extraction(conn: duckdb.DuckDBPyConnection, row: dict):
    """Insert or replace a single extraction row."""
    ticker = row.get('ticker', '').upper()
    accession = row.get('accession_number', '')
    if not ticker or not accession:
        logger.warning(f'Skipping row without ticker/accession: {row.get("period_end_date", "?")}')
        return

    core_vals = {}
    extra = {}
    for key, val in row.items():
        if key in ('ticker', 'accession_number', 'issuer_name', 'cik', 'sector',
                   'period_end_date', 'period_end', 'form_type', 'filing_date',
                   'processed_at', 'extraction_version'):
            continue
        if key in CORE_FIELDS:
            if key == 'has_derivatives':
                core_vals[key] = str(val) if val is not None and val != '' else None
            else:
                core_vals[key] = _coerce_numeric(val)
        elif val is not None and val != '':
            extra[key] = val

    period = row.get('period_end') or row.get('period_end_date', '')

    params = {
        'ticker': ticker,
        'accession_number': accession,
        'issuer_name': row.get('issuer_name'),
        'cik': row.get('cik'),
        'sector': row.get('sector'),
        'period_end': _coerce_date(period),
        'form_type': row.get('form_type'),
        'filing_date': _coerce_date(row.get('filing_date')),
        'processed_at': row.get('processed_at'),
        'extraction_version': int(row['extraction_version']) if row.get('extraction_version') else None,
        'extra_fields': json.dumps(extra) if extra else None,
    }
    params.update(core_vals)

    cols = list(params.keys())
    placeholders = ', '.join(f'${col}' for col in cols)
    col_names = ', '.join(cols)

    conn.execute(
        f'INSERT OR REPLACE INTO extractions ({col_names}) VALUES ({placeholders})',
        params,
    )


def upsert_many(conn: duckdb.DuckDBPyConnection, rows: list[dict]):
    """Batch upsert. Wraps in a transaction for speed."""
    conn.execute('BEGIN TRANSACTION')
    try:
        for row in rows:
            upsert_extraction(conn, row)
        conn.execute('COMMIT')
    except Exception:
        conn.execute('ROLLBACK')
        raise


def query(conn: duckdb.DuckDBPyConnection, sql: str, params: dict | None = None) -> list[dict]:
    """Run a SQL query and return results as a list of dicts."""
    result = conn.execute(sql, params or {})
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def load_csv_into_db(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    ticker: str,
    universe_lookup: dict | None = None,
):
    """Load a single issuer's tracking.csv into the DB."""
    if not csv_path.exists():
        return 0
    meta = {}
    if universe_lookup and ticker.upper() in universe_lookup:
        meta = universe_lookup[ticker.upper()]

    count = 0
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for csv_row in reader:
            row = dict(csv_row)
            row['ticker'] = ticker.upper()
            row['issuer_name'] = meta.get('issuer_name', '')
            row['cik'] = meta.get('cik', '')
            row['sector'] = meta.get('sector', '')
            if not row.get('accession_number'):
                row['accession_number'] = f'{ticker.upper()}_{row.get("period_end_date", "unknown")}'
            upsert_extraction(conn, row)
            count += 1
    return count
