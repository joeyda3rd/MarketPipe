"""Python wrapper for symbol normalization SQL script.

This module provides a function to execute the symbol normalizer against
a DuckDB database containing a symbols_stage table.
"""

from __future__ import annotations

import duckdb
from pathlib import Path


def normalize_stage(db_path: str | duckdb.DuckDBPyConnection, connection: duckdb.DuckDBPyConnection | None = None, output_table: str = "symbols_master") -> None:
    """Execute symbol normalization SQL against a DuckDB database.
    
    Reads the symbols_stage table and creates/replaces the specified output table
    with deduped rows and assigned surrogate IDs.
    
    Args:
        db_path: Path to DuckDB database file, ":memory:" for in-memory DB, or DuckDB connection
        connection: Optional existing DuckDB connection to reuse
        output_table: Name of the output table to create (default: "symbols_master")
        
    Raises:
        duckdb.Error: If SQL execution fails
        FileNotFoundError: If SQL script file is missing
    """
    sql_file = Path(__file__).with_name("symbol_normalizer.sql")
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL script not found: {sql_file}")
    
    sql_script = sql_file.read_text(encoding="utf-8")
    
    # Replace the output table name in the SQL script
    sql_script = sql_script.replace("symbols_master", output_table)
    
    # Handle different input types for db_path
    if isinstance(db_path, duckdb.DuckDBPyConnection):
        # db_path is actually a connection
        db_path.execute(sql_script)
    elif connection is not None:
        # Use existing connection
        connection.execute(sql_script)
    else:
        # Create new connection
        with duckdb.connect(db_path) as conn:
            conn.execute(sql_script)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python run_symbol_normalizer.py <db_path>")
        print("Example: python run_symbol_normalizer.py symbols.db")
        print("Example: python run_symbol_normalizer.py :memory:")
        sys.exit(1)
    
    normalize_stage(sys.argv[1]) 