"""Query interface for the consolidated derivatives store.

Usage:
    python -m src.query sql "SELECT ticker, fx_derivatives_notional FROM latest_extractions ORDER BY fx_derivatives_notional DESC LIMIT 10"
    python -m src.query top-fx [--limit 20]
    python -m src.query top-ir [--limit 20]
    python -m src.query issuer ABBV
    python -m src.query summary
    python -m src.query who-uses "interest rate option"
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from .db import get_connection, query, DB_PATH

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def _print_table(rows: list[dict], max_col_width: int = 40):
    """Simple tabular output."""
    if not rows:
        print('(no results)')
        return
    cols = list(rows[0].keys())
    widths = {c: len(c) for c in cols}
    for row in rows:
        for c in cols:
            val = str(row.get(c, '') or '')
            if len(val) > max_col_width:
                val = val[:max_col_width - 3] + '...'
            widths[c] = max(widths[c], len(val))

    header = '  '.join(c.ljust(widths[c]) for c in cols)
    print(header)
    print('  '.join('-' * widths[c] for c in cols))
    for row in rows:
        vals = []
        for c in cols:
            val = str(row.get(c, '') or '')
            if len(val) > max_col_width:
                val = val[:max_col_width - 3] + '...'
            vals.append(val.ljust(widths[c]))
        print('  '.join(vals))


def cmd_sql(conn, args):
    results = query(conn, args.sql)
    if args.json:
        for r in results:
            for k, v in r.items():
                if hasattr(v, 'isoformat'):
                    r[k] = v.isoformat()
            print(json.dumps(r))
    else:
        _print_table(results)


def cmd_top_fx(conn, args):
    results = query(conn, """
        SELECT e.ticker, e.issuer_name, e.sector, e.period_end,
               e.fx_derivatives_notional,
               e.fx_designated_notional,
               e.fx_not_designated_notional,
               e.has_derivatives
        FROM latest_extractions e
        WHERE e.fx_derivatives_notional IS NOT NULL
        ORDER BY e.fx_derivatives_notional DESC
        LIMIT $limit
    """, {'limit': args.limit})
    _print_table(results)


def cmd_top_ir(conn, args):
    results = query(conn, """
        SELECT e.ticker, e.issuer_name, e.sector, e.period_end,
               e.ir_swap_notional,
               e.total_derivative_asset,
               e.total_derivative_liability
        FROM latest_extractions e
        WHERE e.ir_swap_notional IS NOT NULL
        ORDER BY e.ir_swap_notional DESC
        LIMIT $limit
    """, {'limit': args.limit})
    _print_table(results)


def cmd_issuer(conn, args):
    results = query(conn, """
        SELECT period_end, form_type,
               fx_derivatives_notional, ir_swap_notional,
               commodity_derivatives_notional, equity_derivatives_notional,
               total_derivative_asset, total_derivative_liability,
               net_derivative_position,
               cash_flow_hedge_aoci, expected_12mo_reclass_from_aoci,
               accession_number
        FROM extractions
        WHERE ticker = $ticker
        ORDER BY period_end
    """, {'ticker': args.ticker.upper()})
    if not results:
        print(f'No data for {args.ticker.upper()}')
        return
    print(f'\n{args.ticker.upper()} — derivatives time series\n')
    _print_table(results)


def cmd_summary(conn, args):
    stats = query(conn, """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT ticker) as issuers,
            MIN(period_end) as earliest_period,
            MAX(period_end) as latest_period,
            COUNT(CASE WHEN has_derivatives = 'Yes' THEN 1 END) as rows_with_derivatives,
            COUNT(CASE WHEN fx_derivatives_notional IS NOT NULL THEN 1 END) as rows_with_fx,
            COUNT(CASE WHEN ir_swap_notional IS NOT NULL THEN 1 END) as rows_with_ir,
            COUNT(CASE WHEN commodity_derivatives_notional IS NOT NULL THEN 1 END) as rows_with_commodity
        FROM extractions
    """)
    _print_table(stats)

    print('\nIssuers by sector with derivatives data:')
    sector_stats = query(conn, """
        SELECT sector,
               COUNT(DISTINCT ticker) as issuers,
               COUNT(*) as rows,
               ROUND(AVG(fx_derivatives_notional), 0) as avg_fx_notional_m,
               ROUND(AVG(ir_swap_notional), 0) as avg_ir_notional_m
        FROM latest_extractions
        WHERE has_derivatives = 'Yes'
        GROUP BY sector
        ORDER BY issuers DESC
    """)
    _print_table(sector_stats)


def cmd_who_uses(conn, args):
    term = args.term.lower()
    results = query(conn, """
        SELECT e.ticker, e.issuer_name, e.sector, e.period_end,
               e.extra_fields
        FROM latest_extractions e
        WHERE e.extra_fields IS NOT NULL
    """)
    matches = []
    for r in results:
        extra = json.loads(r['extra_fields']) if r['extra_fields'] else {}
        for k, v in extra.items():
            if term in k.lower() or (isinstance(v, str) and term in v.lower()):
                matches.append({
                    'ticker': r['ticker'],
                    'issuer': r['issuer_name'],
                    'sector': r['sector'],
                    'field': k,
                    'value': v,
                })
                break
    if not matches:
        # Also search core fields for non-null values matching the term
        col_matches = [c for c in [
            'fx_derivatives_notional', 'ir_swap_notional',
            'commodity_derivatives_notional', 'equity_derivatives_notional',
            'fi_ir_swap_notional', 'fi_fx_derivatives_notional',
        ] if term.replace(' ', '_') in c or any(w in c for w in term.split())]
        if col_matches:
            col = col_matches[0]
            results = query(conn, f"""
                SELECT ticker, issuer_name, sector, period_end, {col}
                FROM latest_extractions
                WHERE {col} IS NOT NULL
                ORDER BY {col} DESC
                LIMIT 20
            """)
            _print_table(results)
            return

    if matches:
        _print_table(matches)
    else:
        print(f'No issuers found matching "{args.term}"')


def cmd_findings(conn, args):
    """Search qualitative findings by text and/or category."""
    clauses = []
    params: dict = {}
    if args.search:
        clauses.append('LOWER(finding) LIKE $term')
        params['term'] = f'%{args.search.lower()}%'
    if args.category:
        clauses.append('LOWER(category) LIKE $cat')
        params['cat'] = f'%{args.category.lower()}%'
    if args.ticker:
        clauses.append('ticker = $ticker')
        params['ticker'] = args.ticker.upper()
    if args.since:
        clauses.append('period_end >= $since')
        params['since'] = args.since
    where = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''
    params['limit'] = args.limit
    results = query(conn, f"""
        SELECT ticker, period_end, form_type, category,
               LEFT(finding, 200) AS finding
        FROM qualitative_findings
        {where}
        ORDER BY period_end DESC, ticker
        LIMIT $limit
    """, params)
    _print_table(results, max_col_width=120)
    print(f'\n({len(results)} shown; use --limit to see more)')


def cmd_alerts(conn, args):
    clauses = []
    params: dict = {}
    if args.search:
        clauses.append('LOWER(message) LIKE $term')
        params['term'] = f'%{args.search.lower()}%'
    if args.type:
        clauses.append('alert_type = $atype')
        params['atype'] = args.type.upper()
    if args.ticker:
        clauses.append('ticker = $ticker')
        params['ticker'] = args.ticker.upper()
    if args.since:
        clauses.append('period_end >= $since')
        params['since'] = args.since
    if not args.include_historical:
        clauses.append('NOT historical')
    where = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''
    params['limit'] = args.limit
    results = query(conn, f"""
        SELECT ticker, period_end, alert_type,
               LEFT(message, 160) AS message
        FROM alerts
        {where}
        ORDER BY period_end DESC, ticker
        LIMIT $limit
    """, params)
    _print_table(results, max_col_width=120)
    print(f'\n({len(results)} shown; use --limit to see more)')


def main():
    p = argparse.ArgumentParser(prog='python -m src.query')
    p.add_argument('--db', default=None, help='DB path override')
    p.add_argument('--json', action='store_true', help='JSON output (for sql subcommand)')
    sub = p.add_subparsers(dest='command')

    sql_p = sub.add_parser('sql', help='Run raw SQL')
    sql_p.add_argument('sql', help='SQL query string')

    fx_p = sub.add_parser('top-fx', help='Largest FX derivatives users')
    fx_p.add_argument('--limit', type=int, default=20)

    ir_p = sub.add_parser('top-ir', help='Largest IR swap users')
    ir_p.add_argument('--limit', type=int, default=20)

    iss_p = sub.add_parser('issuer', help='Time series for one issuer')
    iss_p.add_argument('ticker', help='Ticker symbol')

    sub.add_parser('summary', help='Database summary stats')

    who_p = sub.add_parser('who-uses', help='Find issuers using a specific instrument/term')
    who_p.add_argument('term', help='Search term (e.g. "interest rate option", "commodity")')

    f_p = sub.add_parser('findings', help='Search qualitative findings (notes.txt content)')
    f_p.add_argument('search', nargs='?', default='', help='Text to search for in findings')
    f_p.add_argument('--category', default='', help='Filter by category (e.g. "New instruments")')
    f_p.add_argument('--ticker', default='', help='Filter by ticker')
    f_p.add_argument('--since', default='', help='Period cutoff (YYYY-MM-DD)')
    f_p.add_argument('--limit', type=int, default=25)

    a_p = sub.add_parser('alerts', help='Search change-detection alerts')
    a_p.add_argument('search', nargs='?', default='', help='Text to search for in messages')
    a_p.add_argument('--type', default='', help='Alert type (NUMERIC, NEW_FIELD, LLM_FLAG...)')
    a_p.add_argument('--ticker', default='', help='Filter by ticker')
    a_p.add_argument('--since', default='', help='Period cutoff (YYYY-MM-DD)')
    a_p.add_argument('--include-historical', action='store_true',
                     help='Include [HISTORICAL] backfill-regenerated alerts')
    a_p.add_argument('--limit', type=int, default=25)

    args = p.parse_args()
    if not args.command:
        p.print_help()
        sys.exit(1)

    db_path = Path(args.db) if args.db else None
    conn = get_connection(db_path, read_only=True)

    handlers = {
        'sql': cmd_sql,
        'top-fx': cmd_top_fx,
        'top-ir': cmd_top_ir,
        'issuer': cmd_issuer,
        'summary': cmd_summary,
        'who-uses': cmd_who_uses,
        'findings': cmd_findings,
        'alerts': cmd_alerts,
    }
    handlers[args.command](conn, args)
    conn.close()


if __name__ == '__main__':
    main()
