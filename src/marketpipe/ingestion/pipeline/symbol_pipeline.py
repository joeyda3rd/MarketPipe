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

def diff_snapshot(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create diff tables by comparing symbols_snapshot with symbols_master.
    This is a placeholder implementation - the full diff logic will be
    implemented in Story B2.
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
        else:
            # Has existing data - for now, treat all as unchanged
            # This prevents duplicate inserts in the simple case
            conn.execute("""
                INSERT INTO diff_unchanged  
                SELECT * FROM symbols_snapshot
            """)
    except Exception:
        # symbols_master doesn't exist, treat all as inserts
        conn.execute("""
            INSERT INTO diff_insert
            SELECT * FROM symbols_snapshot
        """)


# -- 4.  full pipeline ------------------------------------------------------

def run_symbol_pipeline(db_path: Path,
                        data_dir: Path,
                        provider_names: Sequence[str],
                        snapshot_as_of: _dt.date) -> None:
    """
    End-to-end execution used by --execute flag.
    """
    con = duckdb.connect(str(db_path))

    # 3a) fetch + stage
    records = asyncio.run(fetch_providers(provider_names, snapshot_as_of))
    if not records:
        raise RuntimeError("Provider fetch returned zero records.")
    records_to_stage(con, records)

    # 3b) normalise -> symbols_snapshot
    normalize_stage(con, output_table="symbols_snapshot")

    # 3c) diff & SCD update
    diff_snapshot(con)
    run_scd_update(con, str(data_dir))

    # 3d) refresh read views
    refresh(con)

    con.close() 