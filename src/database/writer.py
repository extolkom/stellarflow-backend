from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from collections import OrderedDict

logger = logging.getLogger(__name__)

try:
    import psycopg2
    from psycopg2 import sql as psql
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import sqlite3
    HAS_SQLITE3 = True
except ImportError:
    HAS_SQLITE3 = False


class DatabaseWriter:
    """Reusable prepared‑statement writer for telemetry inserts.

    Pre‑compiles INSERT statements keyed by (table, *columns) so that the
    database engine does not re‑parse the SQL on every batch.  Supports both
    PostgreSQL (psycopg2) and SQLite backends.

    Usage:
        writer = DatabaseWriter(connection)
        writer.insert("telemetry", {"asset_id": "abc", "price": 123.45})
        writer.insert_batch("telemetry", [{"asset_id": "def", "price": 54.32}, ...])
    """

    def __init__(self, connection: Any) -> None:
        self._conn = connection
        self._statements: Dict[Tuple[str, ...], Any] = OrderedDict()

    # ── public API ──────────────────────────────────────────────────────────

    def insert(self, table: str, data: Dict[str, Any]) -> None:
        """Insert a single row."""
        sql, params = self._compile(table, data)
        self._conn.execute(sql, params)
        self._conn.commit()

    def insert_batch(
        self, table: str, rows: List[Dict[str, Any]], commit: bool = True
    ) -> None:
        """Bulk‑insert multiple rows.

        Uses ``executemany`` for SQLite and ``cursor.executemany`` for
        PostgreSQL so the prepared statement is reused across all rows.

        All rows must share the same column set (keys of the first dict).
        """
        if not rows:
            return

        columns = list(rows[0].keys())
        sql = self._cached_statement(table, columns)
        values = [tuple(r[col] for col in columns) for r in rows]

        if _is_psycopg2(self._conn):
            with self._conn.cursor() as cursor:
                cursor.executemany(sql, values)
            if commit:
                self._conn.commit()
        elif _is_sqlite(self._conn):
            self._conn.execute("BEGIN")
            try:
                self._conn.executemany(sql, values)
                if commit:
                    self._conn.commit()
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        else:
            self._conn.executemany(sql, values)
            if commit:
                self._conn.commit()

    # ── internal helpers ────────────────────────────────────────────────────

    def _compile(
        self, table: str, data: Dict[str, Any]
    ) -> Tuple[str, Tuple[Any, ...]]:
        """Return (sql_string, params_tuple) for a single row insert."""
        columns = list(data.keys())
        sql = self._cached_statement(table, columns)
        return sql, tuple(data[col] for col in columns)

    def _cached_statement(self, table: str, columns: List[str]) -> str:
        """Return a pre‑compiled INSERT statement, caching it on first use."""
        key = (table, *columns)
        if key not in self._statements:
            self._statements[key] = self._build_insert_sql(table, columns)
            logger.debug(
                "Compiled new insert statement for %s(%s)", table, ", ".join(columns)
            )
        return self._statements[key]

    def _build_insert_sql(self, table: str, columns: List[str]) -> str:
        """Build a parameterised INSERT statement appropriate for the backend."""
        if HAS_PSYCOPG2 and _is_psycopg2(self._conn):
            col_identifiers = [psql.Identifier(c) for c in columns]
            placeholders = psql.SQL(", ").join(
                psql.Placeholder() for _ in columns
            )
            stmt = psql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                psql.Identifier(table),
                psql.SQL(", ").join(col_identifiers),
                placeholders,
            )
            return stmt.as_string(self._conn)

        placeholders = ", ".join("?" for _ in columns)
        col_clause = ", ".join(columns)
        return f"INSERT INTO {table} ({col_clause}) VALUES ({placeholders})"


# ── backend detection helpers ────────────────────────────────────────────────

def _is_psycopg2(conn: Any) -> bool:
    return HAS_PSYCOPG2 and isinstance(conn, psycopg2.extensions.connection)


def _is_sqlite(conn: Any) -> bool:
    return HAS_SQLITE3 and isinstance(conn, sqlite3.Connection)
