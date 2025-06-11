# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pandas as pd
import duckdb
import pyarrow as pa

from marketpipe.aggregation.domain.value_objects import FrameSpec
from marketpipe.aggregation.domain.services import AggregationDomainService


def test_duckdb_sql():
    """Test SQL generation for different timeframes."""
    # Test 5-minute frame
    sql = AggregationDomainService.duckdb_sql(FrameSpec("5m", 300))
    assert "GROUP BY" in sql
    assert "first(open" in sql
    assert "max(high)" in sql
    assert "min(low)" in sql
    assert "last(close" in sql
    assert "sum(volume)" in sql

    # Test that the SQL includes the correct time window calculation
    assert "300000000000" in sql  # 300 seconds in nanoseconds

    # Test 1-hour frame
    sql_1h = AggregationDomainService.duckdb_sql(FrameSpec("1h", 3600))
    assert "3600000000000" in sql_1h  # 3600 seconds in nanoseconds


def test_duckdb_sql_execution():
    """Test that generated SQL actually works with DuckDB."""
    # Create sample data - 10 minutes of 1-minute bars
    sample_data = pd.DataFrame(
        {
            "symbol": ["AAPL"] * 10,
            "ts_ns": [
                1640995800000000000 + i * 60000000000  # 1-minute intervals
                for i in range(10)
            ],
            "open": [100.0 + i for i in range(10)],
            "high": [101.0 + i for i in range(10)],
            "low": [99.0 + i for i in range(10)],
            "close": [100.5 + i for i in range(10)],
            "volume": [1000 + i * 100 for i in range(10)],
        }
    )

    # Test 5-minute aggregation
    spec = FrameSpec("5m", 300)
    sql = AggregationDomainService.duckdb_sql(spec)

    # Execute with DuckDB
    con = duckdb.connect(":memory:")
    con.register("bars", pa.Table.from_pandas(sample_data))
    result = con.execute(sql).fetch_df()

    # Verify results
    assert not result.empty
    assert len(result) == 2  # 10 minutes of data should create 2 five-minute bars
    assert "symbol" in result.columns
    assert "ts_ns" in result.columns
    assert "open" in result.columns
    assert "high" in result.columns
    assert "low" in result.columns
    assert "close" in result.columns
    assert "volume" in result.columns

    # Test aggregation logic for first 5-minute bar (bars 0-4)
    first_bar = result.iloc[0]
    assert first_bar["symbol"] == "AAPL"
    assert first_bar["open"] == 100.0  # First open (bar 0)
    assert (
        first_bar["high"] == 105.0
    )  # Max high from bars 0-4 (101, 102, 103, 104, 105)
    assert first_bar["low"] == 99.0  # Min low from bars 0-4 (99, 100, 101, 102, 103)
    assert (
        first_bar["close"] == 104.5
    )  # Last close from bars 0-4 (100.5, 101.5, 102.5, 103.5, 104.5)
    assert (
        first_bar["volume"] == 6000.0
    )  # Sum of volumes from bars 0-4 (1000+1100+1200+1300+1400)

    # Test aggregation logic for second 5-minute bar (bars 5-9)
    second_bar = result.iloc[1]
    assert second_bar["symbol"] == "AAPL"
    assert second_bar["open"] == 105.0  # First open (bar 5)
    assert (
        second_bar["high"] == 110.0
    )  # Max high from bars 5-9 (106, 107, 108, 109, 110)
    assert second_bar["low"] == 104.0  # Min low from bars 5-9 (104, 105, 106, 107, 108)
    assert (
        second_bar["close"] == 109.5
    )  # Last close from bars 5-9 (105.5, 106.5, 107.5, 108.5, 109.5)
    assert (
        second_bar["volume"] == 8500.0
    )  # Sum of volumes from bars 5-9 (1500+1600+1700+1800+1900)

    con.close()
