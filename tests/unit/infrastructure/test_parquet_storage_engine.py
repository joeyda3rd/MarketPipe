# SPDX-License-Identifier: Apache-2.0
"""Tests for ParquetStorageEngine with comprehensive coverage."""

from __future__ import annotations

import pytest
from datetime import date
from pathlib import Path
from unittest.mock import patch
import pandas as pd

from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "ts_ns": [
                1640995800000000000,
                1640995860000000000,
            ],  # 2022-01-01 09:30:00, 09:31:00
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.5],
            "close": [100.5, 101.5],
            "volume": [1000, 1500],
            "symbol": ["AAPL", "AAPL"],
        }
    )


@pytest.fixture
def engine(tmp_path: Path):
    """Create a storage engine instance with temporary directory."""
    return ParquetStorageEngine(tmp_path)


class TestParquetStorageEngineInitialization:
    """Test engine initialization and configuration."""

    def test_init_creates_root_directory(self, tmp_path: Path):
        """Test that initialization creates the root directory."""
        engine_path = tmp_path / "storage"
        engine = ParquetStorageEngine(engine_path)

        assert engine_path.exists()
        assert engine_path.is_dir()

    def test_init_with_compression(self, tmp_path: Path):
        """Test initialization with different compression algorithms."""
        engine = ParquetStorageEngine(tmp_path, compression="snappy")
        assert engine._compression == "snappy"

        engine = ParquetStorageEngine(tmp_path, compression="zstd")
        assert engine._compression == "zstd"

    def test_init_invalid_compression_raises_error(self, tmp_path: Path):
        """Test that invalid compression raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported compression"):
            ParquetStorageEngine(tmp_path, compression="invalid")

    def test_init_with_string_path(self, tmp_path: Path):
        """Test initialization with string path."""
        engine = ParquetStorageEngine(str(tmp_path))
        assert engine._root == tmp_path


class TestParquetStorageEngineWrite:
    """Test write operations."""

    def test_write_basic(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame, tmp_path: Path
    ):
        """Test basic write operation."""
        output_path = engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        expected_path = (
            tmp_path / "frame=1m" / "symbol=AAPL" / "date=2022-01-01" / "job1.parquet"
        )
        assert output_path == expected_path
        assert expected_path.exists()

        # Verify content
        df_read = pd.read_parquet(expected_path)
        assert len(df_read) == 2
        assert list(df_read.columns) == list(sample_df.columns)

    def test_write_empty_dataframe_raises_error(self, engine: ParquetStorageEngine):
        """Test that writing empty DataFrame raises ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot write empty DataFrame"):
            engine.write(
                empty_df,
                frame="1m",
                symbol="AAPL",
                trading_day=date(2022, 1, 1),
                job_id="job1",
            )

    def test_write_missing_required_columns_raises_error(
        self, engine: ParquetStorageEngine
    ):
        """Test that missing required columns raises ValueError."""
        invalid_df = pd.DataFrame(
            {"ts_ns": [1, 2], "open": [1, 2]}
        )  # Missing required columns

        with pytest.raises(ValueError, match="DataFrame missing required columns"):
            engine.write(
                invalid_df,
                frame="1m",
                symbol="AAPL",
                trading_day=date(2022, 1, 1),
                job_id="job1",
            )

    def test_write_file_exists_without_overwrite_raises_error(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test that existing file without overwrite raises FileExistsError."""
        # Write first time
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Try to write again without overwrite
        with pytest.raises(FileExistsError, match="File already exists"):
            engine.write(
                sample_df,
                frame="1m",
                symbol="AAPL",
                trading_day=date(2022, 1, 1),
                job_id="job1",
                overwrite=False,
            )

    def test_write_with_overwrite(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test overwriting existing file."""
        # Write first time
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Modify data and overwrite
        modified_df = sample_df.copy()
        modified_df["volume"] = [2000, 2500]

        output_path = engine.write(
            modified_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
            overwrite=True,
        )

        # Verify overwritten content
        df_read = pd.read_parquet(output_path)
        assert df_read["volume"].tolist() == [2000, 2500]

    @patch("pyarrow.parquet.write_table")
    def test_write_cleanup_on_failure(
        self,
        mock_write_table,
        engine: ParquetStorageEngine,
        sample_df: pd.DataFrame,
        tmp_path: Path,
    ):
        """Test that partial files are cleaned up on write failure."""
        mock_write_table.side_effect = Exception("Write failed")

        with pytest.raises(Exception, match="Write failed"):
            engine.write(
                sample_df,
                frame="1m",
                symbol="AAPL",
                trading_day=date(2022, 1, 1),
                job_id="job1",
            )

        # File should not exist after cleanup
        expected_path = (
            tmp_path / "frame=1m" / "symbol=AAPL" / "date=2022-01-01" / "job1.parquet"
        )
        assert not expected_path.exists()

    def test_append_to_job_new_file(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test appending to non-existent job file creates new file."""
        output_path = engine.append_to_job(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        assert output_path.exists()
        df_read = pd.read_parquet(output_path)
        assert len(df_read) == 2

    def test_append_to_job_existing_file(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test appending to existing job file."""
        # Write initial data
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Create additional data
        additional_df = pd.DataFrame(
            {
                "ts_ns": [1640995920000000000],  # 09:32:00
                "open": [102.0],
                "high": [103.0],
                "low": [101.5],
                "close": [102.5],
                "volume": [2000],
                "symbol": ["AAPL"],
            }
        )

        # Append data
        output_path = engine.append_to_job(
            additional_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Verify combined data
        df_read = pd.read_parquet(output_path)
        assert len(df_read) == 3
        assert df_read["ts_ns"].is_monotonic_increasing


class TestParquetStorageEngineRead:
    """Test read operations."""

    def test_load_partition_existing(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test loading existing partition."""
        # Write data
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Load partition
        df_loaded = engine.load_partition("1m", "AAPL", date(2022, 1, 1))

        assert len(df_loaded) == 2
        assert df_loaded["symbol"].tolist() == ["AAPL", "AAPL"]

    def test_load_partition_nonexistent(self, engine: ParquetStorageEngine):
        """Test loading non-existent partition returns empty DataFrame."""
        df_loaded = engine.load_partition("1m", "AAPL", date(2022, 1, 1))

        assert df_loaded.empty

    def test_load_partition_multiple_files(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test loading partition with multiple job files."""
        # Write first job
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Write second job
        sample_df2 = sample_df.copy()
        sample_df2["ts_ns"] = [
            1640995980000000000,
            1641000000000000000,
        ]  # Different times
        engine.write(
            sample_df2,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job2",
        )

        # Load partition - should combine both files
        df_loaded = engine.load_partition("1m", "AAPL", date(2022, 1, 1))

        assert len(df_loaded) == 4

    def test_load_job_bars_single_symbol(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test loading bars for a specific job."""
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        result = engine.load_job_bars("job1")

        assert "AAPL" in result
        assert len(result["AAPL"]) == 2

    def test_load_job_bars_multiple_symbols(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test loading bars for job with multiple symbols."""
        # Write AAPL data
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Write GOOGL data
        googl_df = sample_df.copy()
        googl_df["symbol"] = "GOOGL"
        engine.write(
            googl_df,
            frame="1m",
            symbol="GOOGL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        result = engine.load_job_bars("job1")

        assert "AAPL" in result
        assert "GOOGL" in result
        assert len(result["AAPL"]) == 2
        assert len(result["GOOGL"]) == 2

    def test_load_job_bars_nonexistent_job(self, engine: ParquetStorageEngine):
        """Test loading non-existent job returns empty dict."""
        result = engine.load_job_bars("nonexistent")

        assert result == {}

    def test_load_symbol_data_basic(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test loading symbol data across dates."""
        # Write data for multiple dates
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 2),
            job_id="job2",
        )

        # Load all symbol data
        df_loaded = engine.load_symbol_data("AAPL", "1m")

        assert len(df_loaded) == 4  # 2 rows × 2 dates

    def test_load_symbol_data_with_date_filter(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test loading symbol data with date filtering."""
        # Write data for multiple dates
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 2),
            job_id="job2",
        )

        # Load with date filter
        df_loaded = engine.load_symbol_data(
            "AAPL", "1m", start_date=date(2022, 1, 2), end_date=date(2022, 1, 2)
        )

        assert len(df_loaded) == 2  # Only data from 2022-01-02

    def test_load_symbol_data_nonexistent_symbol(self, engine: ParquetStorageEngine):
        """Test loading non-existent symbol returns empty DataFrame."""
        df_loaded = engine.load_symbol_data("NONEXISTENT", "1m")

        assert df_loaded.empty


class TestParquetStorageEngineUtilities:
    """Test utility operations."""

    def test_delete_job_existing(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test deleting existing job files."""
        # Write data for multiple symbols
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        engine.write(
            sample_df,
            frame="1m",
            symbol="GOOGL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Delete job
        deleted_count = engine.delete_job("job1")

        assert deleted_count == 2

        # Verify files are gone
        result = engine.load_job_bars("job1")
        assert result == {}

    def test_delete_job_nonexistent(self, engine: ParquetStorageEngine):
        """Test deleting non-existent job returns 0."""
        deleted_count = engine.delete_job("nonexistent")

        assert deleted_count == 0

    def test_list_jobs(self, engine: ParquetStorageEngine, sample_df: pd.DataFrame):
        """Test listing jobs for a symbol/frame."""
        # Write multiple jobs
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 2),
            job_id="job2",
        )

        job_ids = engine.list_jobs("1m", "AAPL")

        assert sorted(job_ids) == ["job1", "job2"]

    def test_list_jobs_nonexistent_symbol(self, engine: ParquetStorageEngine):
        """Test listing jobs for non-existent symbol returns empty list."""
        job_ids = engine.list_jobs("1m", "NONEXISTENT")

        assert job_ids == []

    def test_get_storage_stats(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test getting storage statistics."""
        # Write some data
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        engine.write(
            sample_df,
            frame="5m",
            symbol="GOOGL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        stats = engine.get_storage_stats()

        assert stats["total_files"] == 2
        assert stats["total_size_bytes"] > 0
        assert stats["unique_frames"] == 2
        assert stats["unique_symbols"] == 2
        assert "1m" in stats["frames"]
        assert "5m" in stats["frames"]
        assert "AAPL" in stats["symbols"]
        assert "GOOGL" in stats["symbols"]

    def test_validate_integrity_healthy(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame
    ):
        """Test integrity validation with healthy files."""
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        result = engine.validate_integrity()

        assert result["valid_files"] == 1
        assert result["corrupted_files"] == 0
        assert result["total_rows"] == 2
        assert result["is_healthy"] is True

    def test_validate_integrity_with_corrupted_file(
        self, engine: ParquetStorageEngine, sample_df: pd.DataFrame, tmp_path: Path
    ):
        """Test integrity validation with corrupted file."""
        # Write valid file
        engine.write(
            sample_df,
            frame="1m",
            symbol="AAPL",
            trading_day=date(2022, 1, 1),
            job_id="job1",
        )

        # Create corrupted file
        corrupted_path = (
            tmp_path
            / "frame=1m"
            / "symbol=AAPL"
            / "date=2022-01-02"
            / "corrupted.parquet"
        )
        corrupted_path.parent.mkdir(parents=True, exist_ok=True)
        corrupted_path.write_text("invalid parquet data")

        result = engine.validate_integrity()

        assert result["valid_files"] == 1
        assert result["corrupted_files"] == 1
        assert result["is_healthy"] is False
        assert len(result["corruption_details"]) == 1


class TestParquetStorageEngineErrorHandling:
    """Test error handling scenarios."""

    @patch("pyarrow.parquet.read_table")
    def test_load_partition_handles_read_errors(
        self,
        mock_read_table,
        engine: ParquetStorageEngine,
        sample_df: pd.DataFrame,
        tmp_path: Path,
    ):
        """Test that read errors are handled gracefully."""
        # Create a file that will cause read error
        file_path = (
            tmp_path / "frame=1m" / "symbol=AAPL" / "date=2022-01-01" / "job1.parquet"
        )
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        mock_read_table.side_effect = Exception("Read failed")

        # Should return empty DataFrame instead of crashing
        df_loaded = engine.load_partition("1m", "AAPL", date(2022, 1, 1))

        assert df_loaded.empty

    @patch("pandas.read_parquet")
    def test_load_job_bars_handles_read_errors(
        self, mock_read_parquet, engine: ParquetStorageEngine, tmp_path: Path
    ):
        """Test that job bar loading handles read errors gracefully."""
        # Create a file that will cause read error
        file_path = (
            tmp_path / "frame=1m" / "symbol=AAPL" / "date=2022-01-01" / "job1.parquet"
        )
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        mock_read_parquet.side_effect = Exception("Read failed")

        # Should return empty dict instead of crashing
        result = engine.load_job_bars("job1")

        assert result == {}


# Integration test helper
def test_write_and_load_roundtrip(tmp_path: Path):
    """Integration test: write data and read it back."""
    engine = ParquetStorageEngine(tmp_path)

    # Create test data
    df = pd.DataFrame(
        {
            "ts_ns": [1640995800000000000, 1640995860000000000],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.5],
            "close": [100.5, 101.5],
            "volume": [1000, 1500],
            "symbol": ["AAPL", "AAPL"],
        }
    )

    # Write data
    output_path = engine.write(
        df, frame="1m", symbol="AAPL", trading_day=date(2022, 1, 1), job_id="job1"
    )

    assert output_path.exists()

    # Test different read methods
    df_partition = engine.load_partition("1m", "AAPL", date(2022, 1, 1))
    assert len(df_partition) == 2

    job_bars = engine.load_job_bars("job1")
    assert "AAPL" in job_bars
    assert len(job_bars["AAPL"]) == 2

    symbol_data = engine.load_symbol_data("AAPL", "1m")
    assert len(symbol_data) == 2

    # Clean up
    deleted_count = engine.delete_job("job1")
    assert deleted_count == 1


def test_job_roundtrip(tmp_path: Path):
    """Test job-based operations: write, load, delete."""
    engine = ParquetStorageEngine(tmp_path)

    df = pd.DataFrame(
        {
            "ts_ns": [1, 2],
            "open": [1, 1],
            "high": [1, 1],
            "low": [1, 1],
            "close": [1, 1],
            "volume": [1, 1],
        }
    )

    # Write data
    engine.write(
        df, frame="1m", symbol="AAPL", trading_day=date(2025, 6, 1), job_id="jobX"
    )

    # Load by job
    dfs = engine.load_job_bars("jobX")
    assert "AAPL" in dfs
    assert dfs["AAPL"].shape[0] == 2

    # Delete job
    deleted_count = engine.delete_job("jobX")
    assert deleted_count == 1
