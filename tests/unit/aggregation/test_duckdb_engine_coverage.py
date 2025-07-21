"""Additional tests for DuckDB aggregation engine to improve coverage."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from marketpipe.aggregation.domain.value_objects import FrameSpec
from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine


def test_duckdb_aggregation_engine_initialization():
    """Test DuckDBAggregationEngine can be initialized."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_root = Path(temp_dir) / "raw"
        agg_root = Path(temp_dir) / "agg"
        raw_root.mkdir()
        agg_root.mkdir()

        engine = DuckDBAggregationEngine(raw_root, agg_root)
        assert engine._raw_storage is not None
        assert engine._agg_storage is not None
        assert engine.log is not None


def test_duckdb_aggregation_engine_aggregate_job():
    """Test aggregating a job with multiple timeframes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_root = Path(temp_dir) / "raw"
        agg_root = Path(temp_dir) / "agg"
        raw_root.mkdir()
        agg_root.mkdir()

        engine = DuckDBAggregationEngine(raw_root, agg_root)

        # Mock the storage engines
        with patch.object(engine._raw_storage, "load_job_bars") as mock_load:
            with patch.object(engine, "_write_aggregated_data") as mock_write:
                with patch("duckdb.connect") as mock_connect:
                    # Setup mock data
                    sample_df = pd.DataFrame(
                        {
                            "symbol": ["AAPL"] * 3,
                            "timestamp_ns": [
                                1704105000000000000,
                                1704105060000000000,
                                1704105120000000000,
                            ],
                            "open": [150.0, 151.0, 152.0],
                            "high": [150.5, 151.5, 152.5],
                            "low": [149.5, 150.5, 151.5],
                            "close": [150.2, 151.2, 152.2],
                            "volume": [1000, 1100, 1200],
                        }
                    )

                    mock_load.return_value = {"AAPL": sample_df}

                    # Mock DuckDB connection
                    mock_conn = Mock()
                    mock_connect.return_value = mock_conn

                    # Mock SQL execution result
                    result_df = pd.DataFrame(
                        {
                            "symbol": ["AAPL"],
                            "ts_ns": [1704105000000000000],
                            "open": [150.0],
                            "high": [152.5],
                            "low": [149.5],
                            "close": [152.2],
                            "volume": [3300],
                        }
                    )
                    mock_conn.execute.return_value.fetch_df.return_value = result_df

                    # Test aggregation
                    frame_spec = FrameSpec(name="5m", seconds=300)
                    sql = "SELECT symbol, open, high, low, close, volume FROM bars"

                    engine.aggregate_job("test_job", [(frame_spec, sql)])

                    # Verify interactions
                    mock_load.assert_called_once_with("test_job")
                    mock_connect.assert_called_once()
                    mock_conn.register.assert_called()
                    mock_conn.execute.assert_called()
                    mock_write.assert_called()


def test_duckdb_aggregation_engine_no_data():
    """Test handling when no data is found for a job."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_root = Path(temp_dir) / "raw"
        agg_root = Path(temp_dir) / "agg"
        raw_root.mkdir()
        agg_root.mkdir()

        engine = DuckDBAggregationEngine(raw_root, agg_root)

        with patch.object(engine._raw_storage, "load_job_bars") as mock_load:
            mock_load.return_value = {}

            # Should handle empty data gracefully
            engine.aggregate_job("empty_job", [])

            mock_load.assert_called_once_with("empty_job")


def test_duckdb_aggregation_engine_write_aggregated_data():
    """Test writing aggregated data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_root = Path(temp_dir) / "raw"
        agg_root = Path(temp_dir) / "agg"
        raw_root.mkdir()
        agg_root.mkdir()

        engine = DuckDBAggregationEngine(raw_root, agg_root)

        # Mock the agg storage write method
        with patch.object(engine._agg_storage, "write") as mock_write:
            mock_write.return_value = agg_root / "test.parquet"

            # Test data with ts_ns column
            df = pd.DataFrame(
                {
                    "symbol": ["AAPL"],
                    "ts_ns": [1704105000000000000],
                    "open": [150.0],
                    "high": [150.5],
                    "low": [149.5],
                    "close": [150.2],
                    "volume": [1000],
                }
            )

            frame_spec = FrameSpec(name="5m", seconds=300)

            engine._write_aggregated_data(df, "AAPL", frame_spec, "test_job")

            # Verify write was called
            mock_write.assert_called_once()
            call_args = mock_write.call_args[1]
            assert call_args["frame"] == "5m"
            assert call_args["symbol"] == "AAPL"
            assert call_args["job_id"] == "test_job"


def test_duckdb_aggregation_engine_get_aggregated_data():
    """Test getting aggregated data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_root = Path(temp_dir) / "raw"
        agg_root = Path(temp_dir) / "agg"
        raw_root.mkdir()
        agg_root.mkdir()

        engine = DuckDBAggregationEngine(raw_root, agg_root)

        # Mock the agg storage load method
        with patch.object(engine._agg_storage, "load_symbol_data") as mock_load:
            sample_df = pd.DataFrame(
                {
                    "symbol": ["AAPL"] * 3,
                    "ts_ns": [
                        1704105000000000000,
                        1704105300000000000,
                        1704105600000000000,
                    ],
                    "open": [150.0, 151.0, 152.0],
                    "high": [150.5, 151.5, 152.5],
                    "low": [149.5, 150.5, 151.5],
                    "close": [150.2, 151.2, 152.2],
                    "volume": [1000, 1100, 1200],
                }
            )

            mock_load.return_value = sample_df

            frame_spec = FrameSpec(name="5m", seconds=300)

            # Test without time filtering
            result = engine.get_aggregated_data("AAPL", frame_spec)
            assert len(result) == 3

            # Test with time filtering
            start_ts = 1704105100000000000
            end_ts = 1704105500000000000

            result_filtered = engine.get_aggregated_data("AAPL", frame_spec, start_ts, end_ts)
            assert len(result_filtered) == 1  # Only middle record should match

            mock_load.assert_called()


def test_duckdb_aggregation_engine_error_handling():
    """Test error handling in aggregation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_root = Path(temp_dir) / "raw"
        agg_root = Path(temp_dir) / "agg"
        raw_root.mkdir()
        agg_root.mkdir()

        engine = DuckDBAggregationEngine(raw_root, agg_root)

        # Mock storage to raise an exception
        with patch.object(engine._raw_storage, "load_job_bars") as mock_load:
            mock_load.side_effect = Exception("Storage error")

            # Should raise the exception
            try:
                engine.aggregate_job("error_job", [])
                raise AssertionError("Should have raised exception")
            except Exception as e:
                assert "Storage error" in str(e)


def test_duckdb_aggregation_engine_sql_error_handling():
    """Test handling SQL execution errors."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_root = Path(temp_dir) / "raw"
        agg_root = Path(temp_dir) / "agg"
        raw_root.mkdir()
        agg_root.mkdir()

        engine = DuckDBAggregationEngine(raw_root, agg_root)

        with patch.object(engine._raw_storage, "load_job_bars") as mock_load:
            with patch("duckdb.connect") as mock_connect:
                # Setup mock data
                sample_df = pd.DataFrame(
                    {
                        "symbol": ["AAPL"],
                        "timestamp_ns": [1704105000000000000],
                        "open": [150.0],
                        "high": [150.5],
                        "low": [149.5],
                        "close": [150.2],
                        "volume": [1000],
                    }
                )

                mock_load.return_value = {"AAPL": sample_df}

                # Mock DuckDB connection with SQL error
                mock_conn = Mock()
                mock_connect.return_value = mock_conn
                mock_conn.execute.side_effect = Exception("SQL error")

                # Should handle SQL errors gracefully
                frame_spec = FrameSpec(name="5m", seconds=300)
                sql = "INVALID SQL"

                # Should not raise but log error
                engine.aggregate_job("test_job", [(frame_spec, sql)])

                mock_load.assert_called_once()
                mock_connect.assert_called_once()
