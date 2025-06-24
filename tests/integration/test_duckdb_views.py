# SPDX-License-Identifier: Apache-2.0
"""Integration tests for DuckDB views functionality."""

from __future__ import annotations

import pandas as pd
import pytest

from marketpipe.aggregation.infrastructure import duckdb_views


@pytest.fixture
def temp_agg_data(tmp_path):
    """Create temporary aggregated data for testing."""
    # Setup test aggregation data structure
    agg_root = tmp_path / "agg"

    # Create test data for different timeframes
    test_data = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "ts_ns": [
                1704067800000000000,
                1704067800000000000 + 300000000000,
                1704067800000000000,
                1704067800000000000 + 300000000000,
            ],  # 5 minutes apart
            "date": ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"],
            "open": [100.0, 101.0, 200.0, 201.0],
            "high": [102.0, 103.0, 202.0, 203.0],
            "low": [99.0, 100.0, 199.0, 200.0],
            "close": [101.5, 102.5, 201.5, 202.5],
            "volume": [1000, 1500, 2000, 2500],
        }
    )

    # Create directories and write test files
    for frame in ["5m", "15m", "1h", "1d"]:
        frame_dir = agg_root / f"frame={frame}" / "symbol=AAPL" / "date=2024-01-01"
        frame_dir.mkdir(parents=True)

        # Write test parquet file
        frame_data = (
            test_data[test_data["symbol"] == "AAPL"] if frame == "5m" else test_data.head(1)
        )
        frame_data.to_parquet(frame_dir / "test_job.parquet", index=False)

        # Also create MSFT data
        msft_dir = agg_root / f"frame={frame}" / "symbol=MSFT" / "date=2024-01-01"
        msft_dir.mkdir(parents=True)
        msft_data = (
            test_data[test_data["symbol"] == "MSFT"]
            if frame == "5m"
            else test_data[test_data["symbol"] == "MSFT"].head(1)
        )
        msft_data.to_parquet(msft_dir / "test_job.parquet", index=False)

    return agg_root


@pytest.fixture
def mock_agg_root(temp_agg_data, monkeypatch):
    """Patch the AGG_ROOT to use test data."""
    monkeypatch.setattr(duckdb_views, "AGG_ROOT", temp_agg_data)
    # Clear the connection cache to pick up new path
    duckdb_views._get_connection.cache_clear()
    return temp_agg_data


def test_ensure_views_creates_all_timeframes(mock_agg_root):
    """Test that ensure_views() creates views for all timeframes."""
    duckdb_views.ensure_views()

    # Test that we can query each view
    frames = ["5m", "15m", "1h", "1d"]
    for frame in frames:
        result = duckdb_views.query(f"SELECT COUNT(*) as cnt FROM bars_{frame}")
        assert len(result) == 1
        assert result.iloc[0]["cnt"] >= 0  # Should have some data or empty view


def test_query_basic_functionality(mock_agg_root):
    """Test basic query functionality."""
    # Simple count query
    result = duckdb_views.query("SELECT COUNT(*) as cnt FROM bars_5m")
    assert len(result) == 1
    assert result.iloc[0]["cnt"] >= 2  # Should have AAPL and MSFT data

    # Symbol filtering
    result = duckdb_views.query("SELECT COUNT(*) as cnt FROM bars_5m WHERE symbol='AAPL'")
    assert len(result) == 1
    assert result.iloc[0]["cnt"] >= 1


def test_query_with_symbol_filter(mock_agg_root):
    """Test querying with symbol filtering."""
    result = duckdb_views.query(
        "SELECT symbol, COUNT(*) as cnt FROM bars_5m WHERE symbol='AAPL' GROUP BY symbol"
    )

    assert len(result) == 1
    assert result.iloc[0]["symbol"] == "AAPL"
    assert result.iloc[0]["cnt"] >= 1


def test_query_aggregation(mock_agg_root):
    """Test aggregation queries across symbols."""
    result = duckdb_views.query(
        """
        SELECT symbol, COUNT(*) as cnt, AVG(close) as avg_close 
        FROM bars_5m 
        GROUP BY symbol 
        ORDER BY symbol
    """
    )

    assert len(result) >= 2  # Should have AAPL and MSFT
    assert "AAPL" in result["symbol"].values
    assert "MSFT" in result["symbol"].values


def test_query_empty_result(mock_agg_root):
    """Test query that returns empty result."""
    result = duckdb_views.query("SELECT * FROM bars_5m WHERE symbol='NONEXISTENT'")
    assert len(result) == 0


def test_query_invalid_sql(mock_agg_root):
    """Test error handling for invalid SQL."""
    with pytest.raises(RuntimeError, match="Failed to execute query"):
        duckdb_views.query("SELECT * FROM nonexistent_table")


def test_query_empty_sql():
    """Test error handling for empty SQL."""
    with pytest.raises(ValueError, match="SQL query cannot be empty"):
        duckdb_views.query("")

    with pytest.raises(ValueError, match="SQL query cannot be empty"):
        duckdb_views.query("   ")


def test_get_available_data(mock_agg_root):
    """Test get_available_data() functionality."""
    summary = duckdb_views.get_available_data()

    # Should have data for all frames and both symbols
    assert len(summary) >= 4  # At least 2 symbols × 2 frames with data
    assert "frame" in summary.columns
    assert "symbol" in summary.columns
    assert "date_count" in summary.columns
    assert "total_rows" in summary.columns


def test_validate_views(mock_agg_root):
    """Test view validation functionality."""
    status = duckdb_views.validate_views()

    expected_views = ["bars_5m", "bars_15m", "bars_1h", "bars_1d"]

    assert len(status) == len(expected_views)
    for view_name in expected_views:
        assert view_name in status
        assert isinstance(status[view_name], bool)


def test_views_with_nonexistent_path(tmp_path, monkeypatch):
    """Test view creation when aggregation path doesn't exist."""
    # Point to non-existent directory
    empty_path = tmp_path / "nonexistent"
    monkeypatch.setattr(duckdb_views, "AGG_ROOT", empty_path)
    duckdb_views._get_connection.cache_clear()

    # Should create empty views without error
    duckdb_views.ensure_views()

    # Empty views should return no results
    result = duckdb_views.query("SELECT COUNT(*) as cnt FROM bars_5m")
    assert len(result) == 1
    assert result.iloc[0]["cnt"] == 0


def test_refresh_views(mock_agg_root):
    """Test refresh_views() functionality."""
    # Initial setup
    duckdb_views.ensure_views()
    initial_result = duckdb_views.query("SELECT COUNT(*) as cnt FROM bars_5m")

    # Refresh views
    duckdb_views.refresh_views()

    # Should still work
    refreshed_result = duckdb_views.query("SELECT COUNT(*) as cnt FROM bars_5m")
    assert refreshed_result.iloc[0]["cnt"] == initial_result.iloc[0]["cnt"]


def test_set_agg_root_functionality(tmp_path):
    """Test set_agg_root() changes the data path."""
    # Create test data in new location
    new_root = tmp_path / "new_agg"
    test_dir = new_root / "frame=5m" / "symbol=TEST" / "date=2024-01-01"
    test_dir.mkdir(parents=True)

    test_data = pd.DataFrame(
        {
            "symbol": ["TEST"],
            "ts_ns": [1704067800000000000],
            "date": ["2024-01-01"],
            "open": [50.0],
            "high": [51.0],
            "low": [49.0],
            "close": [50.5],
            "volume": [500],
        }
    )
    test_data.to_parquet(test_dir / "test.parquet", index=False)

    # Set new root
    duckdb_views.set_agg_root(new_root)

    # Should see the new data
    result = duckdb_views.query("SELECT symbol FROM bars_5m WHERE symbol='TEST'")
    assert len(result) == 1
    assert result.iloc[0]["symbol"] == "TEST"


def test_complex_query_with_joins(mock_agg_root):
    """Test complex query with multiple timeframes."""
    # Query that joins different timeframes (if data allows)
    result = duckdb_views.query(
        """
        SELECT 
            d.symbol,
            d.date,
            d.close as daily_close,
            COUNT(h.close) as hourly_bars
        FROM bars_1d d
        LEFT JOIN bars_1h h ON d.symbol = h.symbol AND d.date = h.date
        GROUP BY d.symbol, d.date, d.close
        ORDER BY d.symbol
    """
    )

    # Should execute without error and return some results
    assert len(result) >= 0  # May be empty if joins don't match


def test_query_with_time_filters(mock_agg_root):
    """Test queries with timestamp filtering."""
    # Query with timestamp range
    result = duckdb_views.query(
        """
        SELECT symbol, ts_ns, close 
        FROM bars_5m 
        WHERE ts_ns >= 1704067800000000000 
        ORDER BY ts_ns
    """
    )

    assert len(result) >= 0
    if len(result) > 0:
        assert "ts_ns" in result.columns
        assert all(result["ts_ns"] >= 1704067800000000000)
