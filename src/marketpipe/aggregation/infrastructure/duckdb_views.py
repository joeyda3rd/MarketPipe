# SPDX-License-Identifier: Apache-2.0
"""DuckDB view helpers for fast querying of aggregated Parquet data."""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Union

import duckdb
import pandas as pd

# Default path to aggregated data - can be overridden for testing
AGG_ROOT = Path("data/agg")

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_connection() -> duckdb.DuckDBPyConnection:
    """Get cached DuckDB connection with optimal settings."""
    con = duckdb.connect(":memory:")

    # Optimize for analytical workloads
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='1GB'")
    con.execute("PRAGMA max_memory='1GB'")

    logger.debug("Created DuckDB connection with optimized settings")
    return con


def _attach_partition(frame: str) -> None:
    """Attach a timeframe partition as a view.

    Args:
        frame: Timeframe name (5m, 15m, 1h, 1d)
    """
    path = AGG_ROOT / f"frame={frame}"

    if not path.exists():
        logger.warning(f"Partition path does not exist: {path}")
        # Create empty view to avoid SQL errors
        _get_connection().execute(
            f"CREATE OR REPLACE VIEW bars_{frame} AS "
            f"SELECT NULL::VARCHAR as symbol, NULL::BIGINT as ts_ns, "
            f"NULL::DOUBLE as open, NULL::DOUBLE as high, NULL::DOUBLE as low, "
            f"NULL::DOUBLE as close, NULL::BIGINT as volume, NULL::VARCHAR as date "
            f"WHERE 1=0"
        )
        return

    # Create view using Hive partitioning
    view_sql = (
        f"CREATE OR REPLACE VIEW bars_{frame} AS "
        f"SELECT * FROM parquet_scan('{path}/**/*.parquet', hive_partitioning=1)"
    )

    try:
        _get_connection().execute(view_sql)
        logger.debug(f"Created view bars_{frame} for path {path}")
    except Exception as e:
        logger.error(f"Failed to create view bars_{frame}: {e}")
        # Create empty view as fallback
        _get_connection().execute(
            f"CREATE OR REPLACE VIEW bars_{frame} AS "
            f"SELECT NULL::VARCHAR as symbol, NULL::BIGINT as ts_ns, "
            f"NULL::DOUBLE as open, NULL::DOUBLE as high, NULL::DOUBLE as low, "
            f"NULL::DOUBLE as close, NULL::BIGINT as volume, NULL::VARCHAR as date "
            f"WHERE 1=0"
        )


def ensure_views() -> None:
    """Ensure all timeframe views are created.

    Creates views for all standard timeframes: 5m, 15m, 1h, 4h, 1d
    """
    frames = ["5m", "15m", "1h", "4h", "1d"]

    logger.debug(f"Ensuring views for frames: {frames}")

    for frame in frames:
        _attach_partition(frame)

    logger.info(f"Ensured {len(frames)} timeframe views")


def refresh_views() -> None:
    """Refresh all views to pick up new data.

    This is an alias for ensure_views() since DuckDB views are dynamic.
    """
    ensure_views()


def query(sql: str) -> pd.DataFrame:
    """Execute SQL query against aggregated data views.

    Args:
        sql: SQL query string. Available views: bars_5m, bars_15m, bars_1h, bars_1d

    Returns:
        DataFrame with query results

    Raises:
        ValueError: If SQL query is invalid
        RuntimeError: If query execution fails
    """
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty")

    # Ensure views are available
    ensure_views()

    try:
        logger.debug(f"Executing query: {sql[:100]}...")
        result_df = _get_connection().execute(sql).fetch_df()
        logger.debug(f"Query returned {len(result_df)} rows")
        return result_df

    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise RuntimeError(f"Failed to execute query: {e}") from e


def get_available_data() -> pd.DataFrame:
    """Get summary of available data across all timeframes.

    Returns:
        DataFrame with frame, symbol, date_count, and row_count columns
    """
    ensure_views()

    summary_sql = """
    WITH frame_data AS (
        SELECT '5m' as frame, symbol, date, COUNT(*) as row_count FROM bars_5m GROUP BY symbol, date
        UNION ALL
        SELECT '15m' as frame, symbol, date, COUNT(*) as row_count FROM bars_15m GROUP BY symbol, date
        UNION ALL
        SELECT '1h' as frame, symbol, date, COUNT(*) as row_count FROM bars_1h GROUP BY symbol, date
        UNION ALL
        SELECT '4h' as frame, symbol, date, COUNT(*) as row_count FROM bars_4h GROUP BY symbol, date
        UNION ALL
        SELECT '1d' as frame, symbol, date, COUNT(*) as row_count FROM bars_1d GROUP BY symbol, date
    )
    SELECT
        frame,
        symbol,
        COUNT(DISTINCT date) as date_count,
        SUM(row_count) as total_rows
    FROM frame_data
    WHERE symbol IS NOT NULL
    GROUP BY frame, symbol
    ORDER BY frame, symbol
    """

    try:
        return _get_connection().execute(summary_sql).fetch_df()
    except Exception as e:
        logger.warning(f"Failed to get data summary: {e}")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=["frame", "symbol", "date_count", "total_rows"])


def validate_views() -> dict[str, bool]:
    """Validate that all views are accessible and return status.

    Returns:
        Dictionary mapping view names to availability status
    """
    frames = ["5m", "15m", "1h", "4h", "1d"]
    status = {}

    ensure_views()

    for frame in frames:
        view_name = f"bars_{frame}"
        try:
            # Try to execute a simple query
            _get_connection().execute(f"SELECT COUNT(*) FROM {view_name} LIMIT 1").fetch_df()
            status[view_name] = True
            logger.debug(f"View {view_name} is accessible")
        except Exception as e:
            status[view_name] = False
            logger.warning(f"View {view_name} is not accessible: {e}")

    return status


def set_agg_root(path: Union[str, Path]) -> None:
    """Set the aggregation root path for testing or custom configurations.

    Args:
        path: Path to aggregated data directory
    """
    global AGG_ROOT
    AGG_ROOT = Path(path)

    # Clear cached connection to force recreation with new path
    _get_connection.cache_clear()

    logger.info(f"Set aggregation root to: {AGG_ROOT}")
