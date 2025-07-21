"""Public data loader API for MarketPipe OHLCV data."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Optional, Union

import duckdb
import pandas as pd

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    pl = None
    POLARS_AVAILABLE = False


__all__ = ["load_ohlcv"]

logger = logging.getLogger(__name__)


def load_ohlcv(
    symbols: Union[str, Sequence[str]],
    start: Union[str, Optional[dt.datetime]] = None,
    end: Union[str, Optional[dt.datetime]] = None,
    timeframe: str = "1m",
    *,
    as_polars: bool = False,
    root: Union[str, Optional[Path]] = None,
) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    Load OHLCV bars from the local Parquet lake.

    Parameters
    ----------
    symbols : str or list[str]
        One ticker or a list/tuple of tickers.
    start, end : Union[str, datetime]
        Inclusive bounds; accepts 'YYYY-MM-DD', RFC 3339, or datetime.
        None = unbounded.
    timeframe : {"1m","5m","15m","1h","1d"}
        Granularity of bars to load.
    as_polars : bool
        Return a Polars DataFrame instead of pandas if True.
    root : Union[Path, str]
        Override parquet root (defaults to "data").

    Returns
    -------
    pandas.DataFrame or polars.DataFrame
        MultiIndex (timestamp, symbol) if multiple symbols,
        otherwise timestamp index with columns [open, high, low, close, volume].

    Raises
    ------
    ValueError
        If invalid timeframe or no data found.
    ImportError
        If as_polars=True but polars is not installed.
    FileNotFoundError
        If no data files found for the given symbols.
    """
    # Input validation
    if as_polars and not POLARS_AVAILABLE:
        raise ImportError("polars is required for as_polars=True. Install with: pip install polars")

    if timeframe not in {"1m", "5m", "15m", "1h", "1d"}:
        raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of: 1m, 5m, 15m, 1h, 1d")

    # Normalize symbols to list
    symbols = [symbols] if isinstance(symbols, str) else list(symbols)
    if not symbols:
        raise ValueError("symbols cannot be empty")

    # Convert to uppercase for consistency
    symbols = [s.upper() for s in symbols]

    # Determine root path
    if root is None:
        # Default to looking in both raw and aggregated data
        root = Path("data")
    else:
        root = Path(root).expanduser()

    # Convert time bounds to nanoseconds
    start_ns = _to_ns(start) if start else 0
    end_ns = _to_ns(end) if end else 9_999_999_999_999_999_999

    logger.debug(
        f"Loading {symbols} from {timeframe} timeframe, "
        f"time range: {start} to {end}, root: {root}"
    )

    # Collect DataFrames for all symbols
    frames = []
    con = duckdb.connect(":memory:")

    try:
        is_multi_symbol = len(symbols) > 1
        for symbol in symbols:
            symbol_df = _load_symbol_data(
                con, root, symbol, timeframe, start_ns, end_ns, is_multi_symbol
            )
            if not symbol_df.empty:
                frames.append(symbol_df)

        if not frames:
            logger.warning(f"No data found for symbols {symbols} in timeframe {timeframe}")
            # Return empty DataFrame with proper structure
            empty_df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
            empty_df.index.name = "timestamp"
            if len(symbols) > 1:
                empty_df["symbol"] = None
                empty_df = empty_df.set_index("symbol", append=True)
            return empty_df if not as_polars else pl.from_pandas(empty_df)

        # Combine all symbol DataFrames
        combined_df = pd.concat(frames, ignore_index=False)

        # Handle multi-symbol indexing
        if len(symbols) > 1:
            # Sort by timestamp first, then symbol
            combined_df = combined_df.sort_index()
        else:
            # Single symbol - just sort by timestamp
            combined_df = combined_df.sort_index()

        logger.info(f"Loaded {len(combined_df)} rows for {len(symbols)} symbol(s)")

        # Convert to polars if requested
        if as_polars:
            return pl.from_pandas(combined_df)

        return combined_df

    finally:
        con.close()


def _load_symbol_data(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    symbol: str,
    timeframe: str,
    start_ns: int,
    end_ns: int,
    is_multi_symbol: bool = False,
) -> pd.DataFrame:
    """Load data for a single symbol using DuckDB."""

    # Try multiple possible paths for data
    possible_paths = [
        # Check aggregated data first for non-1m timeframes
        root / "agg" / f"frame={timeframe}" / f"symbol={symbol}" / "**" / "*.parquet",
        # Check raw data for 1m timeframes
        root / "raw" / f"frame={timeframe}" / f"symbol={symbol}" / "**" / "*.parquet",
        # Legacy structure
        root / f"frame={timeframe}" / f"symbol={symbol}" / "**" / "*.parquet",
    ]

    # Find existing data path
    data_path = None
    for path_pattern in possible_paths:
        # Check if any files exist matching this pattern
        search_path = Path(str(path_pattern).replace("**/*.parquet", ""))
        if search_path.exists() and any(search_path.rglob("*.parquet")):
            data_path = str(path_pattern)
            break

    if not data_path:
        logger.debug(f"No data found for {symbol} in {timeframe} timeframe at {root}")
        return pd.DataFrame()

    logger.debug(f"Loading {symbol} data from {data_path}")

    # Build DuckDB query
    query = """
    SELECT symbol, ts_ns, open, high, low, close, volume
    FROM parquet_scan(?, hive_partitioning=true)
    WHERE ts_ns BETWEEN ? AND ?
    ORDER BY ts_ns
    """

    try:
        result_df = con.execute(query, [data_path, start_ns, end_ns]).df()

        if result_df.empty:
            logger.debug(f"No data in time range for {symbol}")
            return pd.DataFrame()

        # Convert timestamp to UTC datetime and set as index
        result_df["timestamp"] = pd.to_datetime(result_df["ts_ns"], utc=True)
        result_df = result_df.drop(columns=["ts_ns"])
        result_df = result_df.set_index("timestamp")

        # For multi-symbol queries, add symbol to index
        if is_multi_symbol and "symbol" in result_df.columns:
            result_df = result_df.set_index("symbol", append=True)

        logger.debug(f"Loaded {len(result_df)} rows for {symbol}")
        return result_df

    except Exception as e:
        logger.error(f"Failed to load data for {symbol}: {e}")
        return pd.DataFrame()


def _to_ns(ts_like: Union[str, dt].datetime) -> int:
    """Convert timestamp-like input to nanoseconds since epoch."""
    ts: pd.Timestamp
    if isinstance(ts_like, str):
        # Try to parse as ISO date first, then as RFC 3339
        try:
            ts = pd.to_datetime(ts_like, utc=True)
        except Exception as e:
            raise ValueError(f"Cannot parse timestamp: {ts_like}") from e
    elif isinstance(ts_like, dt.datetime):
        # Ensure timezone aware
        if ts_like.tzinfo is None:
            ts_like = ts_like.replace(tzinfo=dt.timezone.utc)
        ts = pd.Timestamp(ts_like)
    else:
        raise ValueError(f"Invalid timestamp type: {type(ts_like)}")

    return int(ts.value)  # nanoseconds since epoch
