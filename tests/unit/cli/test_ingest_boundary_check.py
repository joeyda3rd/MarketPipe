"""Unit tests for ingest boundary check functionality."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marketpipe.cli.ohlcv_ingest import _check_boundaries


class TestBoundaryCheck:
    """Test the boundary check functionality."""

    def test_boundary_check_success(self, tmp_path):
        """Test boundary check passes when data matches requested range."""
        # Create test data within requested range
        test_data = [
            {
                "symbol": "AAPL",
                "date": "2024-01-01",
                "ts_ns": 1704067800000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
            {
                "symbol": "AAPL",
                "date": "2024-01-02",
                "ts_ns": 1704154200000000000,
                "open": 100.5,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1500,
            },
        ]

        # Create parquet file in correct structure: frame=1m/symbol=AAPL/date=2024-01-01/
        parquet_dir = tmp_path / "frame=1m" / "symbol=AAPL" / "date=2024-01-01"
        parquet_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(test_data)
        pq.write_table(table, parquet_dir / "data.parquet")

        # Mock print to capture output
        with patch("builtins.print") as mock_print:
            _check_boundaries(
                path=str(tmp_path),
                symbol="AAPL",
                start="2024-01-01",
                end="2024-01-02",
                provider="test",
            )

        # Verify success message was printed
        mock_print.assert_called_once()
        call_args = mock_print.call_args[0][0]
        assert "Ingest OK:" in call_args
        assert "2 bars" in call_args
        assert "2024-01-01..2024-01-02" in call_args
        assert "symbol AAPL" in call_args
        assert "provider test" in call_args

    def test_boundary_check_data_outside_range(self, tmp_path):
        """Test boundary check fails when data is outside requested range."""
        # Create test data outside requested range (older data)
        test_data = [
            {
                "symbol": "AAPL",
                "date": "2020-07-27",
                "ts_ns": 1595836200000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
            {
                "symbol": "AAPL",
                "date": "2020-07-28",
                "ts_ns": 1595922600000000000,
                "open": 100.5,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1500,
            },
        ]

        # Create parquet file in correct structure: frame=1m/symbol=AAPL/date=YYYY-MM-DD/
        parquet_dir = tmp_path / "frame=1m" / "symbol=AAPL" / "date=2024-01-01"
        parquet_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(test_data)
        pq.write_table(table, parquet_dir / "data.parquet")

        # Mock print and sys.exit - sys.exit will prevent further execution
        with patch("builtins.print") as mock_print, patch("sys.exit") as mock_exit:
            # Mock sys.exit to raise SystemExit instead of actually exiting
            mock_exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                _check_boundaries(
                    path=str(tmp_path),
                    symbol="AAPL",
                    start="2024-01-01",
                    end="2024-01-02",
                    provider="alpaca",
                )

        # Verify error message was printed to stderr
        mock_print.assert_called_once()
        call_args = mock_print.call_args
        assert call_args[1]["file"] == sys.stderr  # Printed to stderr

        error_msg = call_args[0][0]
        assert "ERROR:" in error_msg
        assert "2020-07-27" in error_msg
        assert "2020-07-28" in error_msg
        assert "2024-01-01" in error_msg
        assert "2024-01-02" in error_msg
        assert "Try a different provider" in error_msg

        # Verify sys.exit(1) was called
        mock_exit.assert_called_once_with(1)

    def test_boundary_check_data_incomplete_range(self, tmp_path):
        """Test boundary check fails when data doesn't cover full requested range."""
        # Create test data that only covers part of requested range (only 2024-01-01, missing 2024-01-02)
        test_data = [
            {
                "symbol": "AAPL",
                "date": "2024-01-01",
                "ts_ns": 1704110400000000000,  # 2024-01-01
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ]

        # Create parquet file in correct structure: frame=1m/symbol=AAPL/date=YYYY-MM-DD/
        parquet_dir = tmp_path / "frame=1m" / "symbol=AAPL" / "date=2024-01-01"
        parquet_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(test_data)
        pq.write_table(table, parquet_dir / "data.parquet")

        # Mock print and sys.exit
        with patch("builtins.print") as mock_print, patch("sys.exit") as mock_exit:
            mock_exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                _check_boundaries(
                    path=str(tmp_path),
                    symbol="AAPL",
                    start="2024-01-01",
                    end="2024-01-03",
                    provider="alpaca",
                )

        # Verify error message was printed
        mock_print.assert_called_once()
        error_msg = mock_print.call_args[0][0]
        assert "ERROR:" in error_msg
        assert "2024-01-01" in error_msg  # Data covers 2024-01-01
        assert "2024-01-03" in error_msg  # But requested until 2024-01-03

        # Verify sys.exit(1) was called
        mock_exit.assert_called_once_with(1)

    def test_boundary_check_no_data_found(self, tmp_path):
        """Test boundary check fails when no data is found for symbol."""
        # Create empty directory structure
        parquet_dir = tmp_path / "symbol=AAPL"
        parquet_dir.mkdir(parents=True)

        # Mock print and sys.exit
        with patch("builtins.print") as mock_print, patch("sys.exit") as mock_exit:
            mock_exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                _check_boundaries(
                    path=str(tmp_path),
                    symbol="AAPL",
                    start="2024-01-01",
                    end="2024-01-02",
                    provider="test",
                )

        # Verify error message was printed
        mock_print.assert_called_once()
        error_msg = mock_print.call_args[0][0]
        # The error message might be different due to DuckDB not finding files
        assert "ERROR:" in error_msg
        assert "AAPL" in error_msg

        # Verify sys.exit(1) was called
        mock_exit.assert_called_once_with(1)

    def test_boundary_check_duckdb_error(self, tmp_path):
        """Test boundary check handles DuckDB errors gracefully."""
        # Mock duckdb.connect to raise an exception
        with (
            patch("duckdb.connect") as mock_connect,
            patch("builtins.print") as mock_print,
            patch("sys.exit") as mock_exit,
        ):
            mock_connect.side_effect = Exception("DuckDB connection failed")
            mock_exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                _check_boundaries(
                    path=str(tmp_path),
                    symbol="AAPL",
                    start="2024-01-01",
                    end="2024-01-02",
                    provider="test",
                )

        # Verify error message was printed
        mock_print.assert_called_once()
        error_msg = mock_print.call_args[0][0]
        assert "ERROR: Boundary check failed for AAPL" in error_msg
        assert "DuckDB connection failed" in error_msg

        # Verify sys.exit(1) was called
        mock_exit.assert_called_once_with(1)

    def test_boundary_check_date_string_conversion(self, tmp_path):
        """Test boundary check handles date string conversion correctly."""
        # Create test data with string dates (as might come from DuckDB)
        test_data = [
            {
                "symbol": "AAPL",
                "date": "2024-01-01",
                "ts_ns": 1704067800000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ]

        # Create parquet file in correct structure: frame=1m/symbol=AAPL/date=YYYY-MM-DD/
        parquet_dir = tmp_path / "frame=1m" / "symbol=AAPL" / "date=2024-01-01"
        parquet_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(test_data)
        pq.write_table(table, parquet_dir / "data.parquet")

        # Mock DuckDB to return string dates
        mock_db = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = ("2024-01-01", "2024-01-01", 1)
        mock_db.execute.return_value = mock_result

        with (
            patch("duckdb.connect", return_value=mock_db),
            patch("builtins.print") as mock_print,
        ):
            _check_boundaries(
                path=str(tmp_path),
                symbol="AAPL",
                start="2024-01-01",
                end="2024-01-01",
                provider="test",
            )

        # Verify success message was printed
        mock_print.assert_called_once()
        call_args = mock_print.call_args[0][0]
        assert "Ingest OK:" in call_args

    def test_boundary_check_future_data(self, tmp_path):
        """Test boundary check fails when data is in the future."""
        # Create test data in the future
        test_data = [
            {
                "symbol": "AAPL",
                "date": "2025-12-31",
                "ts_ns": 1767182400000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            },
        ]

        # Create parquet file in correct structure matching the data date: frame=1m/symbol=AAPL/date=2025-12-31/
        parquet_dir = tmp_path / "frame=1m" / "symbol=AAPL" / "date=2025-12-31"
        parquet_dir.mkdir(parents=True)

        table = pa.Table.from_pylist(test_data)
        pq.write_table(table, parquet_dir / "data.parquet")

        # Mock print and sys.exit
        with patch("builtins.print") as mock_print, patch("sys.exit") as mock_exit:
            mock_exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                _check_boundaries(
                    path=str(tmp_path),
                    symbol="AAPL",
                    start="2024-01-01",
                    end="2024-01-02",
                    provider="test",
                )

        # Verify error message was printed
        mock_print.assert_called_once()
        error_msg = mock_print.call_args[0][0]
        assert "ERROR:" in error_msg
        assert "2025-12-31" in error_msg
        assert "2024-01-01" in error_msg
        assert "2024-01-02" in error_msg

        # Verify sys.exit(1) was called
        mock_exit.assert_called_once_with(1)
