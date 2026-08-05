"""Shared test fixtures.

Several production paths write to the consolidated DuckDB store as a
best-effort side effect — engine._write_to_db and backfill._commit_issuer both
call get_connection() and swallow any failure with logger.debug. That means a
test exercising those paths silently rewrote the committed
data/derivatives.duckdb, and a subsequent `git add -A` would have committed a
test-mutated database as if it were real extracted data.
"""

import pytest

import src.db


@pytest.fixture(autouse=True)
def isolate_duckdb(tmp_path, monkeypatch):
    """Point every DB write at a per-test temp file.

    Autouse so no test can reach the committed database, including tests added
    later that don't know the store exists.
    """
    monkeypatch.setattr(src.db, 'DB_PATH', tmp_path / 'test.duckdb')
    yield
