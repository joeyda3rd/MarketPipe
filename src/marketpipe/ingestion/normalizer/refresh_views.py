"""Python wrapper for symbol views creation SQL script.

This module provides a function to create or refresh the symbol views
(v_symbol_history and v_symbol_latest) against a DuckDB database
containing a symbols_master table.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import duckdb


def refresh(
    db_path: Union[str, duckdb.DuckDBPyConnection],
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    """Create or refresh symbol views against a DuckDB database.

    Creates/replaces v_symbol_history and v_symbol_latest views
    that provide read-only access to the SCD-2 symbols_master table.

    Args:
        db_path: Path to DuckDB database file, or ":memory:" for in-memory DB
        connection: Optional existing DuckDB connection to reuse

    Raises:
        RuntimeError: If symbols_master table doesn't exist
        duckdb.Error: If SQL execution fails
        FileNotFoundError: If SQL script file is missing
    """
    sql_file = Path(__file__).with_name("create_symbol_views.sql")
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL script not found: {sql_file}")

    sql_script = sql_file.read_text(encoding="utf-8")

    # Handle different input types for db_path
    if isinstance(db_path, duckdb.DuckDBPyConnection):
        # db_path is actually a connection
        _validate_and_refresh(db_path, sql_script)
    elif connection is not None:
        # Use existing connection
        _validate_and_refresh(connection, sql_script)
    else:
        # Create new connection
        with duckdb.connect(db_path) as conn:
            _validate_and_refresh(conn, sql_script)


def _validate_and_refresh(conn: duckdb.DuckDBPyConnection, sql_script: str) -> None:
    """Validate prerequisites and execute view creation SQL.

    Args:
        conn: Active DuckDB connection
        sql_script: SQL script content to execute

    Raises:
        RuntimeError: If symbols_master table doesn't exist
    """
    # Check if symbols_master table exists
    try:
        result = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'symbols_master'"
        ).fetchone()
        if result is None:
            raise RuntimeError(
                "symbols_master table not found; run symbol normalization (Story B3) first"
            )
    except duckdb.Error as e:
        # If information_schema doesn't work, try a direct query
        try:
            conn.execute("SELECT COUNT(*) FROM symbols_master LIMIT 1").fetchone()
        except duckdb.Error:
            raise RuntimeError(
                "symbols_master table not found; run symbol normalization (Story B3) first"
            ) from e

    # Execute the view creation script
    conn.execute(sql_script)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python refresh_views.py <db_path>")
        print("Example: python refresh_views.py symbols.db")
        print("Example: python refresh_views.py :memory:")
        sys.exit(1)

    refresh(sys.argv[1])
