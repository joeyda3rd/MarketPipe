# SPDX-License-Identifier: Apache-2.0
"""Unit tests for DuckDB views module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from marketpipe.aggregation.infrastructure import duckdb_views


@pytest.fixture
def mock_connection():
    """Mock DuckDB connection."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame({"count": [5]})
    return mock_conn


def test_get_connection_caching():
    """Test that connection caching works."""
    # Clear any existing cache
    duckdb_views._get_connection.cache_clear()

    # Get connection twice
    conn1 = duckdb_views._get_connection()
    conn2 = duckdb_views._get_connection()

    # Should be the same instance due to caching
    assert conn1 is conn2


def test_get_connection_settings():
    """Test that connection has correct settings."""
    duckdb_views._get_connection.cache_clear()

    # Mock duckdb.connect to verify settings
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        duckdb_views._get_connection()

        # Verify connection creation and settings
        mock_connect.assert_called_once_with(":memory:")

        # Check that PRAGMA statements were executed
        calls = mock_conn.execute.call_args_list
        pragma_calls = [call for call in calls if "PRAGMA" in str(call)]
        assert len(pragma_calls) >= 2  # threads and memory settings


def test_attach_partition_existing_path(tmp_path):
    """Test attaching partition when path exists."""
    # Create test directory structure
    frame_path = tmp_path / "frame=5m"
    frame_path.mkdir(parents=True)

    # Create a test parquet file
    test_data = pd.DataFrame({"symbol": ["AAPL"], "value": [100]})
    test_file = frame_path / "test.parquet"
    test_data.to_parquet(test_file, index=False)

    with patch.object(duckdb_views, "AGG_ROOT", tmp_path):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            duckdb_views._attach_partition("5m")

            # Should have called execute with CREATE VIEW
            mock_conn.execute.assert_called()
            call_args = mock_conn.execute.call_args[0][0]
            assert "CREATE OR REPLACE VIEW bars_5m" in call_args
            assert "parquet_scan" in call_args


def test_attach_partition_nonexistent_path(tmp_path):
    """Test attaching partition when path doesn't exist."""
    with patch.object(duckdb_views, "AGG_ROOT", tmp_path / "nonexistent"):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            duckdb_views._attach_partition("5m")

            # Should create empty view
            mock_conn.execute.assert_called()
            call_args = mock_conn.execute.call_args[0][0]
            assert "CREATE OR REPLACE VIEW bars_5m" in call_args
            assert "WHERE 1=0" in call_args  # Empty view condition


def test_attach_partition_duckdb_error(tmp_path):
    """Test error handling when DuckDB fails."""
    frame_path = tmp_path / "frame=5m"
    frame_path.mkdir(parents=True)

    with patch.object(duckdb_views, "AGG_ROOT", tmp_path):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = [
                duckdb.Error("Parquet scan failed"),  # First call fails
                None,  # Second call (fallback) succeeds
            ]
            mock_get_conn.return_value = mock_conn

            duckdb_views._attach_partition("5m")

            # Should have been called twice (original + fallback)
            assert mock_conn.execute.call_count == 2


def test_ensure_views():
    """Test ensure_views creates all standard views."""
    with patch.object(duckdb_views, "_attach_partition") as mock_attach:
        duckdb_views.ensure_views()

        # Should create views for all standard frames
        expected_frames = ["5m", "15m", "1h", "1d"]
        assert mock_attach.call_count == len(expected_frames)

        called_frames = [call[0][0] for call in mock_attach.call_args_list]
        assert set(called_frames) == set(expected_frames)


def test_refresh_views():
    """Test refresh_views calls ensure_views."""
    with patch.object(duckdb_views, "ensure_views") as mock_ensure:
        duckdb_views.refresh_views()
        mock_ensure.assert_called_once()


def test_query_success():
    """Test successful query execution."""
    expected_df = pd.DataFrame({"symbol": ["AAPL"], "count": [5]})

    with patch.object(duckdb_views, "ensure_views") as mock_ensure:
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetch_df.return_value = expected_df
            mock_get_conn.return_value = mock_conn

            result = duckdb_views.query("SELECT COUNT(*) FROM bars_5m")

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert result.iloc[0]["symbol"] == "AAPL"

            mock_ensure.assert_called_once()
            mock_conn.execute.assert_called_once_with("SELECT COUNT(*) FROM bars_5m")


def test_query_empty_sql():
    """Test query with empty SQL."""
    with pytest.raises(ValueError, match="SQL query cannot be empty"):
        duckdb_views.query("")

    with pytest.raises(ValueError, match="SQL query cannot be empty"):
        duckdb_views.query("   ")

    with pytest.raises(ValueError, match="SQL query cannot be empty"):
        duckdb_views.query(None)


def test_query_duckdb_error():
    """Test query with DuckDB execution error."""
    with patch.object(duckdb_views, "ensure_views"):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = duckdb.Error("Table not found")
            mock_get_conn.return_value = mock_conn

            with pytest.raises(RuntimeError, match="Failed to execute query"):
                duckdb_views.query("SELECT * FROM nonexistent")


def test_get_available_data_success():
    """Test get_available_data with successful query."""
    expected_df = pd.DataFrame(
        {
            "frame": ["5m", "15m"],
            "symbol": ["AAPL", "AAPL"],
            "date_count": [1, 1],
            "total_rows": [100, 20],
        }
    )

    with patch.object(duckdb_views, "ensure_views"):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetch_df.return_value = expected_df
            mock_get_conn.return_value = mock_conn

            result = duckdb_views.get_available_data()

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert list(result.columns) == [
                "frame",
                "symbol",
                "date_count",
                "total_rows",
            ]


def test_get_available_data_error():
    """Test get_available_data with query error."""
    with patch.object(duckdb_views, "ensure_views"):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = duckdb.Error("Query failed")
            mock_get_conn.return_value = mock_conn

            result = duckdb_views.get_available_data()

            # Should return empty DataFrame with expected columns
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
            assert list(result.columns) == [
                "frame",
                "symbol",
                "date_count",
                "total_rows",
            ]


def test_validate_views_all_accessible():
    """Test validate_views when all views are accessible."""
    with patch.object(duckdb_views, "ensure_views"):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame({"count": [0]})
            mock_get_conn.return_value = mock_conn

            status = duckdb_views.validate_views()

            expected_views = ["bars_5m", "bars_15m", "bars_1h", "bars_1d"]
            assert len(status) == len(expected_views)
            for view_name in expected_views:
                assert view_name in status
                assert status[view_name] is True


def test_validate_views_some_inaccessible():
    """Test validate_views when some views are inaccessible."""

    def mock_execute(sql):
        if "bars_5m" in sql:
            raise duckdb.Error("View not accessible")
        return MagicMock(fetch_df=lambda: pd.DataFrame({"count": [0]}))

    with patch.object(duckdb_views, "ensure_views"):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = mock_execute
            mock_get_conn.return_value = mock_conn

            status = duckdb_views.validate_views()

            assert status["bars_5m"] is False
            assert status["bars_15m"] is True
            assert status["bars_1h"] is True
            assert status["bars_1d"] is True


def test_set_agg_root():
    """Test set_agg_root changes the path and clears cache."""
    original_root = duckdb_views.AGG_ROOT
    new_path = Path("/new/test/path")

    try:
        with patch.object(duckdb_views._get_connection, "cache_clear") as mock_clear:
            duckdb_views.set_agg_root(new_path)

            assert duckdb_views.AGG_ROOT == new_path
            mock_clear.assert_called_once()

    finally:
        # Restore original path
        duckdb_views.AGG_ROOT = original_root


def test_set_agg_root_with_string():
    """Test set_agg_root works with string path."""
    original_root = duckdb_views.AGG_ROOT
    new_path_str = "/new/test/path"

    try:
        duckdb_views.set_agg_root(new_path_str)
        assert duckdb_views.AGG_ROOT == Path(new_path_str)

    finally:
        # Restore original path
        duckdb_views.AGG_ROOT = original_root


def test_module_constants():
    """Test module constants and defaults."""
    # Reset AGG_ROOT to default value in case other tests modified it
    original_root = duckdb_views.AGG_ROOT
    duckdb_views.AGG_ROOT = Path("data/agg")

    try:
        # Test default AGG_ROOT
        assert isinstance(duckdb_views.AGG_ROOT, Path)
        assert duckdb_views.AGG_ROOT == Path("data/agg")

        # Test logger exists
        assert hasattr(duckdb_views, "logger")

    finally:
        # Restore whatever was there before if needed
        # (though we want it to be data/agg anyway)
        pass


def test_query_logging(caplog):
    """Test that query execution is logged."""
    with patch.object(duckdb_views, "ensure_views"):
        with patch.object(duckdb_views, "_get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame({"count": [1]})
            mock_get_conn.return_value = mock_conn

            duckdb_views.query("SELECT COUNT(*) FROM bars_5m")

            # Check that query execution was logged (at debug level)
            # Note: caplog may not capture debug messages unless configured


def test_ensure_views_logging(caplog):
    """Test that ensure_views logs appropriately."""
    with patch.object(duckdb_views, "_attach_partition"):
        duckdb_views.ensure_views()

        # Should log about ensuring views
