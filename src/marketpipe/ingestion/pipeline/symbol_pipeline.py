"""
High-level helpers used by the CLI; keeps symbols.py thin.
"""

from __future__ import annotations
import asyncio, os, datetime as _dt
from pathlib import Path
from typing import Sequence

import duckdb
import pyarrow as pa

from marketpipe.ingestion.symbol_providers import get as get_provider
from marketpipe.ingestion.normalizer.run_symbol_normalizer import normalize_stage
from marketpipe.ingestion.normalizer.scd_writer import run_scd_update
from marketpipe.ingestion.normalizer.refresh_views import refresh
from marketpipe.metrics import SYMBOLS_SNAPSHOT_RECORDS, SYMBOLS_NULL_RATIO

# -- 1.  fetch ---------------------------------------------------------------


async def _fetch_one(name: str,
                     snapshot_as_of: _dt.date) -> list:
    token_env = f"{name.upper()}_API_KEY"
    token = os.getenv(token_env)
    provider = get_provider(name,
                            token=token,
                            as_of=snapshot_as_of)
    records = await provider.fetch_symbols()
    # attach provider column for tie-breaks / auditing
    for r in records:
        r.meta = r.meta or {}
        r.meta["provider"] = name
    return records


async def fetch_providers(names: Sequence[str],
                          snapshot_as_of: _dt.date) -> list:
    coros = [_fetch_one(n, snapshot_as_of) for n in names]
    results = await asyncio.gather(*coros)
    flat: list = [rec for sub in results for rec in sub]
    return flat


# -- 2.  stage  --------------------------------------------------------------

def records_to_stage(conn: duckdb.DuckDBPyConnection,
                     records: list) -> None:
    """
    Convert List[SymbolRecord] to Arrow table and load into
    `symbols_stage` inside the supplied DuckDB connection.
    """
    tbl = pa.Table.from_pylist([r.model_dump() for r in records])
    conn.execute("DROP TABLE IF EXISTS symbols_stage")
    conn.register("records_py", tbl)  # zero-copy Arrow
    conn.execute("CREATE TABLE symbols_stage AS SELECT * FROM records_py")
    conn.unregister("records_py")


# -- 3.  diff snapshot (stub for Story B2) ----------------------------------

def diff_snapshot(conn: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    """
    Create diff tables by comparing symbols_snapshot with symbols_master.
    This is a placeholder implementation - the full diff logic will be
    implemented in Story B2.
    
    Returns:
        tuple[int, int]: (insert_count, update_count)
    """
    # For now, create empty diff tables that the SCD writer expects
    conn.execute("""
        CREATE TABLE IF NOT EXISTS diff_insert AS
        SELECT * FROM symbols_snapshot WHERE 1=0
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS diff_update AS
        SELECT * FROM symbols_snapshot WHERE 1=0
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS diff_unchanged AS
        SELECT * FROM symbols_snapshot WHERE 1=0
    """)
    
    # Simple placeholder: treat all snapshot records as inserts if no master exists
    try:
        master_count = conn.execute("SELECT COUNT(*) FROM symbols_master").fetchone()[0]
        if master_count == 0:
            # No existing master data, treat all as inserts
            conn.execute("""
                INSERT INTO diff_insert
                SELECT * FROM symbols_snapshot
            """)
            insert_count = conn.execute("SELECT COUNT(*) FROM diff_insert").fetchone()[0]
            update_count = 0
        else:
            # Has existing data - for now, treat all as unchanged
            # This prevents duplicate inserts in the simple case
            conn.execute("""
                INSERT INTO diff_unchanged  
                SELECT * FROM symbols_snapshot
            """)
            insert_count = 0
            update_count = 0
    except Exception:
        # symbols_master doesn't exist, treat all as inserts
        conn.execute("""
            INSERT INTO diff_insert
            SELECT * FROM symbols_snapshot
        """)
        insert_count = conn.execute("SELECT COUNT(*) FROM diff_insert").fetchone()[0]
        update_count = 0

    return insert_count, update_count


# -- 4.  metrics helpers -----------------------------------------------------

def _update_null_ratio_metrics(con: duckdb.DuckDBPyConnection) -> None:
    """Calculate and update null ratio metrics for v_symbol_latest."""
    try:
        # Get all columns except system columns
        columns_result = con.sql("DESCRIBE v_symbol_latest").fetchall()
        cols = [c[0] for c in columns_result 
                if c[0] not in ("id", "valid_from", "valid_to")]
        
        # Get total row count
        rowcount_result = con.sql("SELECT COUNT(*) FROM v_symbol_latest").fetchone()
        rowcount = rowcount_result[0] if rowcount_result else 1
        
        # Avoid division by zero
        if rowcount == 0:
            rowcount = 1
        
        # Calculate null ratios for each column
        for col in cols:
            try:
                null_result = con.sql(f"SELECT COUNT(*) FROM v_symbol_latest WHERE {col} IS NULL").fetchone()
                nulls = null_result[0] if null_result else 0
                ratio = nulls / rowcount
                SYMBOLS_NULL_RATIO.labels(column=col).set(ratio)
            except Exception as e:
                # Skip columns that cause SQL errors (e.g., reserved keywords)
                continue
                
    except Exception as e:
        # If v_symbol_latest doesn't exist or has issues, skip metrics update
        pass


# -- 5.  full pipeline ------------------------------------------------------

def run_symbol_pipeline(
    *,
    db_path: Path,
    data_dir: Path,
    provider_names: Sequence[str],
    snapshot_as_of: _dt.date,
    dry_run: bool = False,
    diff_only: bool = False,
) -> tuple[int, int]:
    """
    End-to-end execution used by --execute flag.
    
    Args:
        db_path: Path to DuckDB database file
        data_dir: Directory for Parquet data storage
        provider_names: List of provider names to fetch from
        snapshot_as_of: Date for snapshot
        dry_run: If True, uses in-memory database and no file writes
        diff_only: If True, skips provider fetch and assumes symbols_snapshot exists
        
    Returns:
        tuple[int, int]: (insert_count, update_count)
    """
    # Choose connection type based on dry_run mode
    if dry_run:
        con = duckdb.connect(':memory:')
    else:
        con = duckdb.connect(str(db_path))

    try:
        insert_count = 0
        update_count = 0

        if not diff_only:
            # 3a) fetch + stage
            records = asyncio.run(fetch_providers(provider_names, snapshot_as_of))
            if not records:
                raise RuntimeError("Provider fetch returned zero records.")
            
            # Record snapshot metrics
            SYMBOLS_SNAPSHOT_RECORDS.inc(len(records))
            
            records_to_stage(con, records)

            # 3b) normalise -> symbols_snapshot
            normalize_stage(con, output_table="symbols_snapshot")

        # 3c) diff & SCD update
        insert_count, update_count = diff_snapshot(con)

        if not dry_run:
            # Only write to Parquet and refresh views if not in dry-run mode
            run_scd_update(con, str(data_dir))
            # 3d) refresh read views
            refresh(con)
            
            # 3e) calculate null ratio metrics
            _update_null_ratio_metrics(con)

        return insert_count, update_count

    finally:
        con.close() 