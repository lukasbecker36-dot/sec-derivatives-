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

CREATE TABLE IF NOT EXISTS qualitative_findings (
    ticker      VARCHAR NOT NULL,
    period_end  DATE,
    form_type   VARCHAR,
    category    VARCHAR NOT NULL,
    finding     VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    ticker       VARCHAR NOT NULL,
    period_end   DATE,
    form_type    VARCHAR,
    processed_on DATE,
    historical   BOOLEAN DEFAULT FALSE,
    alert_type   VARCHAR,
    message      VARCHAR NOT NULL
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


_NOTES_HEADER_RE = None  # compiled lazily
_ALERT_HEADER_RE = None
_ALERT_TAG_RE = None


def parse_notes_text(text: str) -> list[dict]:
    """Parse a notes.txt file into finding records.

    Format:
        --- 10-Q | Period ending 2026-03-31 ---
          [Category name]
            - finding text
    """
    import re
    global _NOTES_HEADER_RE
    if _NOTES_HEADER_RE is None:
        _NOTES_HEADER_RE = re.compile(
            r'^--- (?P<form>10-[QK](?:/A)?) \| Period ending (?P<period>\d{4}-\d{2}-\d{2}) ---')

    records = []
    form = period = None
    category = None
    current = None  # last finding record, for continuation lines
    for line in text.splitlines():
        m = _NOTES_HEADER_RE.match(line)
        if m:
            form, period = m.group('form'), m.group('period')
            category = None
            current = None
            continue
        stripped = line.strip()
        if stripped.startswith('[') and stripped.endswith(']'):
            category = stripped[1:-1]
            current = None
            continue
        if stripped.startswith('- ') and form and category:
            current = {
                'form_type': form,
                'period_end': period,
                'category': category,
                'finding': stripped[2:].strip(),
            }
            records.append(current)
        elif stripped and current is not None:
            # Continuation of a multi-line finding (e.g. table excerpt)
            current['finding'] += ' ' + stripped
    return records


def parse_alerts_text(text: str) -> list[dict]:
    """Parse an alert_log.txt file into alert records.

    Format:
        === 10-Q | Period ending 2026-03-31 | Processed 2026-06-10 ===
        [HISTORICAL] [NUMERIC] message...
        [LLM_FLAG] message...
    """
    import re
    global _ALERT_HEADER_RE, _ALERT_TAG_RE
    if _ALERT_HEADER_RE is None:
        _ALERT_HEADER_RE = re.compile(
            r'^=== (?P<form>10-[QK](?:/A)?) \| Period ending (?P<period>\d{4}-\d{2}-\d{2})'
            r'(?: \| Processed (?P<processed>\d{4}-\d{2}-\d{2}))? ===')
        _ALERT_TAG_RE = re.compile(r'^\[(?P<tag>[A-Z_]+)\]\s*')

    records = []
    form = period = processed = None
    for line in text.splitlines():
        m = _ALERT_HEADER_RE.match(line)
        if m:
            form, period = m.group('form'), m.group('period')
            processed = m.group('processed')
            continue
        stripped = line.strip()
        if not stripped or form is None:
            continue
        historical = False
        alert_type = None
        rest = stripped
        while True:
            tm = _ALERT_TAG_RE.match(rest)
            if not tm:
                break
            tag = tm.group('tag')
            if tag == 'HISTORICAL':
                historical = True
            elif alert_type is None:
                alert_type = tag
            rest = rest[tm.end():]
        if not rest:
            continue
        records.append({
            'form_type': form,
            'period_end': period,
            'processed_on': processed,
            'historical': historical,
            'alert_type': alert_type or 'OTHER',
            'message': rest,
        })
    return records


def replace_issuer_findings(conn: duckdb.DuckDBPyConnection, ticker: str,
                            notes_text: str = '', alerts_text: str = '') -> tuple[int, int]:
    """Replace all qualitative findings + alerts for one issuer from file text.

    Parses and de-duplicates (notes files accumulate duplicate blocks when a
    period is reprocessed). Returns (findings_inserted, alerts_inserted).
    """
    ticker = ticker.upper()
    conn.execute('DELETE FROM qualitative_findings WHERE ticker = ?', [ticker])
    conn.execute('DELETE FROM alerts WHERE ticker = ?', [ticker])

    seen = set()
    n_findings = 0
    for rec in parse_notes_text(notes_text):
        key = (rec['period_end'], rec['category'], rec['finding'])
        if key in seen:
            continue
        seen.add(key)
        conn.execute(
            'INSERT INTO qualitative_findings VALUES (?, ?, ?, ?, ?)',
            [ticker, _coerce_date(rec['period_end']), rec['form_type'],
             rec['category'], rec['finding']],
        )
        n_findings += 1

    seen = set()
    n_alerts = 0
    for rec in parse_alerts_text(alerts_text):
        key = (rec['period_end'], rec['alert_type'], rec['message'])
        if key in seen:
            continue
        seen.add(key)
        conn.execute(
            'INSERT INTO alerts VALUES (?, ?, ?, ?, ?, ?, ?)',
            [ticker, _coerce_date(rec['period_end']), rec['form_type'],
             _coerce_date(rec['processed_on']), rec['historical'],
             rec['alert_type'], rec['message']],
        )
        n_alerts += 1
    return n_findings, n_alerts


def load_issuer_text_files(conn: duckdb.DuckDBPyConnection, issuer_dir: Path,
                           ticker: str) -> tuple[int, int]:
    """Load notes.txt + alert_log.txt for one issuer directory into the DB."""
    notes_path = issuer_dir / 'notes.txt'
    alerts_path = issuer_dir / 'alert_log.txt'
    notes_text = notes_path.read_text(encoding='utf-8') if notes_path.exists() else ''
    alerts_text = alerts_path.read_text(encoding='utf-8') if alerts_path.exists() else ''
    if not notes_text and not alerts_text:
        return 0, 0
    return replace_issuer_findings(conn, ticker, notes_text, alerts_text)


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
