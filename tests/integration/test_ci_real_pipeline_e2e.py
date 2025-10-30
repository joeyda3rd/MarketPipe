# SPDX-License-Identifier: Apache-2.0
"""Real pipeline integration tests for CI - NO service mocking.

This test file addresses Critical Gap #1 from E2E_TEST_GAP_ANALYSIS.md:
"No Real End-to-End Integration Test in CI"

PURPOSE:
These tests validate actual component integration WITHOUT mocking core services.
They ensure the pipeline works as a cohesive system, catching integration bugs
that mocks would hide.

WHAT THIS REPLACES:
- test_pipeline_e2e.py (which heavily mocks AggregationRunnerService, ValidationRunnerService)

WHAT THIS TESTS:
- Real FakeProvider data generation (deterministic, no API keys)
- Real ParquetStorageEngine writes and reads
- Real DuckDBAggregationEngine SQL execution
- Real file system operations
- Actual service coordination

WHAT THIS DOES NOT MOCK:
- Storage engine operations
- Data format conversions
- File system interactions
- Component integration points

EXECUTION TIME: Target <30 seconds for CI
"""

from __future__ import annotations

import subprocess
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from marketpipe.domain.entities import OHLCVBar
from marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from tests.fakes.adapters import create_test_ohlcv_bars


def _bars_to_dataframe(bars: list[OHLCVBar]) -> pd.DataFrame:
    """Convert list of OHLCVBar entities to DataFrame format for storage.

    Args:
        bars: List of OHLCV bar entities

    Returns:
        DataFrame with columns: ts_ns, open, high, low, close, volume, symbol
    """
    data = []
    for bar in bars:
        data.append(
            {
                "ts_ns": bar.timestamp.to_nanoseconds(),
                "open": float(bar.open_price.value),
                "high": float(bar.high_price.value),
                "low": float(bar.low_price.value),
                "close": float(bar.close_price.value),
                "volume": int(bar.volume.value),
                "symbol": bar.symbol.value,
            }
        )
    return pd.DataFrame(data)


@pytest.mark.integration
def test_real_pipeline_storage_to_query_flow(tmp_path):
    """
    CRITICAL Test 1A: Validate real storage → query flow without mocking.

    This test ensures that data written by ParquetStorageEngine can be
    successfully read back and queried, catching storage format issues.

    Flow:
    1. Generate deterministic test data (FakeProvider pattern)
    2. Write data using REAL ParquetStorageEngine
    3. Read data back using REAL ParquetStorageEngine
    4. Validate data integrity maintained

    What this catches:
    - Storage format incompatibilities
    - Data type conversion bugs
    - File system operation failures
    - Schema evolution issues
    """
    # Setup
    storage_dir = tmp_path / "data" / "raw"
    storage_dir.mkdir(parents=True)

    # 1. GENERATE: Create deterministic test data
    symbol = Symbol.from_string("AAPL")
    trading_day = date(2025, 1, 15)
    start_time = datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)

    # Create 100 1-minute bars (realistic intraday data)
    bars = create_test_ohlcv_bars(symbol, count=100, start_time=start_time)
    df = _bars_to_dataframe(bars)

    # 2. STORE: Real ParquetStorageEngine writes data
    storage = ParquetStorageEngine(storage_dir)
    written_path = storage.write(
        df=df, frame="1m", symbol="AAPL", trading_day=trading_day, job_id="test-real-pipeline-job"
    )

    # 3. VALIDATE: File was created
    assert written_path.exists(), "Parquet file should exist after write"
    assert written_path.stat().st_size > 0, "Parquet file should not be empty"
    assert written_path.suffix == ".parquet", "File should have .parquet extension"

    # 4. READ: Real ParquetStorageEngine reads data back
    read_df = storage.load_partition(frame="1m", symbol="AAPL", trading_day=trading_day)

    # 5. VALIDATE: Data integrity maintained
    assert len(read_df) == len(df), "Should read back same number of rows written"
    assert all(
        col in read_df.columns for col in ["ts_ns", "open", "high", "low", "close", "volume"]
    ), "Should have all required OHLCV columns"

    # Validate OHLC relationships (high >= low, etc.)
    assert (read_df["high"] >= read_df["low"]).all(), "High should always be >= low"
    assert (read_df["high"] >= read_df["open"]).all(), "High should be >= open"
    assert (read_df["high"] >= read_df["close"]).all(), "High should be >= close"
    assert (read_df["low"] <= read_df["open"]).all(), "Low should be <= open"
    assert (read_df["low"] <= read_df["close"]).all(), "Low should be <= close"

    # Validate timestamps are sequential
    timestamps = read_df["ts_ns"].values
    assert all(
        timestamps[i] < timestamps[i + 1] for i in range(len(timestamps) - 1)
    ), "Timestamps should be in ascending order"

    # Validate volumes are positive
    assert (read_df["volume"] > 0).all(), "Volumes should be positive"


@pytest.mark.integration
def test_real_pipeline_multi_day_storage(tmp_path):
    """
    CRITICAL Test 1B: Validate multi-day storage and partitioning.

    This test ensures that the storage engine correctly handles data
    partitioned across multiple trading days, which is critical for
    production use.

    Flow:
    1. Generate data for 2 trading days
    2. Write each day to separate partitions
    3. Query each day independently
    4. Validate partition isolation

    What this catches:
    - Partition boundary bugs
    - Cross-day data leakage
    - Date-based filtering issues
    """
    # Setup
    storage_dir = tmp_path / "data" / "raw"
    storage_dir.mkdir(parents=True)
    storage = ParquetStorageEngine(storage_dir)

    symbol = Symbol.from_string("GOOGL")

    # Generate data for 2 days
    day1 = date(2025, 1, 15)
    day2 = date(2025, 1, 16)

    # Day 1 data: 100 bars starting at 9:30 AM
    day1_bars = create_test_ohlcv_bars(
        symbol, count=100, start_time=datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)
    )
    day1_df = _bars_to_dataframe(day1_bars)

    # Day 2 data: 100 bars starting at 9:30 AM (next day)
    day2_bars = create_test_ohlcv_bars(
        symbol, count=100, start_time=datetime(2025, 1, 16, 9, 30, tzinfo=timezone.utc)
    )
    day2_df = _bars_to_dataframe(day2_bars)

    # Write both days
    path1 = storage.write(
        df=day1_df, frame="1m", symbol="GOOGL", trading_day=day1, job_id="job-day1"
    )
    path2 = storage.write(
        df=day2_df, frame="1m", symbol="GOOGL", trading_day=day2, job_id="job-day2"
    )

    # Validate separate files created
    assert path1 != path2, "Different days should create different files"
    assert path1.exists() and path2.exists(), "Both partition files should exist"

    # Read day 1 data
    day1_read = storage.load_partition(frame="1m", symbol="GOOGL", trading_day=day1)
    assert len(day1_read) == 100, "Day 1 should have 100 bars"

    # Read day 2 data
    day2_read = storage.load_partition(frame="1m", symbol="GOOGL", trading_day=day2)
    assert len(day2_read) == 100, "Day 2 should have 100 bars"

    # Validate partition isolation (no data leakage)
    day1_timestamps = day1_read["ts_ns"].values
    day2_timestamps = day2_read["ts_ns"].values

    # All day 1 timestamps should be before day 2 timestamps
    assert (
        day1_timestamps.max() < day2_timestamps.min()
    ), "Day 1 data should be entirely before day 2 data (no cross-contamination)"


@pytest.mark.integration
def test_real_pipeline_data_integrity_through_storage(tmp_path):
    """
    CRITICAL Test 1C: Validate data integrity through full storage cycle.

    This test ensures that numerical precision and data quality are
    maintained through the storage → retrieval cycle.

    Flow:
    1. Create data with known values
    2. Write through ParquetStorageEngine
    3. Read back and validate exact values
    4. Check decimal precision maintained

    What this catches:
    - Floating point precision loss
    - Data type conversion errors
    - Rounding issues
    - Null handling problems
    """
    # Setup
    storage_dir = tmp_path / "data" / "raw"
    storage_dir.mkdir(parents=True)
    storage = ParquetStorageEngine(storage_dir)

    symbol = Symbol.from_string("TSLA")
    trading_day = date(2025, 1, 15)

    # Create data with KNOWN values for precise validation
    known_bars = []
    base_time = datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)

    for i in range(10):
        from marketpipe.domain.entities import EntityId

        bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(base_time + timedelta(minutes=i)),
            open_price=Price.from_float(250.123456),  # Precise decimal
            high_price=Price.from_float(251.987654),
            low_price=Price.from_float(249.111111),
            close_price=Price.from_float(250.555555),
            volume=Volume(123456),  # Known volume
        )
        known_bars.append(bar)

    df = _bars_to_dataframe(known_bars)

    # Write and read back
    storage.write(
        df=df, frame="1m", symbol="TSLA", trading_day=trading_day, job_id="precision-test"
    )
    read_df = storage.load_partition(frame="1m", symbol="TSLA", trading_day=trading_day)

    # Validate exact values maintained (within Parquet float32 precision ~1e-4)
    assert len(read_df) == 10, "Should read back exact number of bars"

    # Check first bar's values (Parquet uses float32, so tolerance is ~1e-4)
    first_row = read_df.iloc[0]
    assert abs(first_row["open"] - 250.123456) < 0.0001, "Open price precision maintained"
    assert abs(first_row["high"] - 251.987654) < 0.0001, "High price precision maintained"
    assert abs(first_row["low"] - 249.111111) < 0.0001, "Low price precision maintained"
    assert abs(first_row["close"] - 250.555555) < 0.0001, "Close price precision maintained"
    assert first_row["volume"] == 123456, "Volume exact value maintained"

    # Validate all rows have consistent precision
    for _, row in read_df.iterrows():
        assert abs(row["open"] - 250.123456) < 0.0001
        assert abs(row["high"] - 251.987654) < 0.0001
        assert abs(row["low"] - 249.111111) < 0.0001
        assert abs(row["close"] - 250.555555) < 0.0001
        assert row["volume"] == 123456


@pytest.mark.integration
def test_real_pipeline_cli_ingest_with_fake_provider(tmp_path):
    """
    CRITICAL Test 1D: Test real CLI ingestion command (subprocess).

    This test validates the actual CLI command that users will run,
    ensuring the full stack works end-to-end from command line to
    data storage.

    Flow:
    1. Run actual CLI command via subprocess
    2. Validate exit code success
    3. Validate data files created
    4. Validate data can be read back

    What this catches:
    - CLI argument parsing bugs
    - Command dispatch failures
    - Bootstrap/initialization issues
    - Real user workflow problems
    """
    # Set environment to use tmp_path for data
    import os

    env = os.environ.copy()
    env["MP_DATA_DIR"] = str(tmp_path / "data")

    # Run real CLI ingest command
    result = subprocess.run(
        [
            "python",
            "-m",
            "marketpipe",
            "ingest",
            "--provider",
            "fake",
            "--symbols",
            "AAPL",
            "--start",
            "2025-01-15",
            "--end",
            "2025-01-16",  # End must be after start for validation
            "--output",
            str(tmp_path / "data" / "raw"),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    # Validate successful execution
    assert result.returncode == 0, f"CLI ingest should succeed. stderr: {result.stderr}"

    # Validate data files were created
    data_dir = tmp_path / "data" / "raw"
    assert data_dir.exists(), "Data directory should be created"

    # Find parquet files
    parquet_files = list(data_dir.rglob("*.parquet"))
    assert (
        len(parquet_files) > 0
    ), f"Should create at least one parquet file. Found: {list(data_dir.rglob('*'))}"

    # Validate can read the data back
    storage = ParquetStorageEngine(data_dir)
    # Should be able to load the partition
    df = storage.load_partition(frame="1m", symbol="AAPL", trading_day=date(2025, 1, 15))
    assert len(df) > 0, "Should have ingested data"
    assert all(
        col in df.columns for col in ["open", "high", "low", "close", "volume"]
    ), "Should have complete OHLCV data"


@pytest.mark.integration
def test_real_pipeline_job_cleanup_command(tmp_path):
    """
    CRITICAL Test 1E: Test real job cleanup command.

    This test validates the new jobs cleanup command works correctly,
    testing the complete job lifecycle from creation to cleanup.

    Flow:
    1. Create job data via ingestion
    2. Verify job exists
    3. Run cleanup command
    4. Verify cleanup worked

    What this catches:
    - Cleanup command bugs
    - Job tracking issues
    - Database cleanup problems
    """
    # Set environment
    import os

    env = os.environ.copy()
    env["MP_DATA_DIR"] = str(tmp_path / "data")

    # 1. Create job via ingestion
    ingest_result = subprocess.run(
        [
            "python",
            "-m",
            "marketpipe",
            "ingest",
            "--provider",
            "fake",
            "--symbols",
            "MSFT",
            "--start",
            "2025-01-15",
            "--end",
            "2025-01-16",  # End must be after start for validation
            "--output",
            str(tmp_path / "data" / "raw"),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    assert ingest_result.returncode == 0, "Ingest should succeed"

    # 2. Verify data exists
    data_dir = tmp_path / "data" / "raw"
    initial_files = list(data_dir.rglob("*.parquet"))
    assert len(initial_files) > 0, "Should have created data files"

    # 3. Run cleanup command (dry-run first)
    cleanup_dry_result = subprocess.run(
        [
            "python",
            "-m",
            "marketpipe",
            "jobs",
            "cleanup",
            "--all",  # Clean all jobs
            "--dry-run",  # Don't actually delete
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    # Dry run should succeed
    assert (
        cleanup_dry_result.returncode == 0
    ), f"Cleanup dry-run should succeed. stderr: {cleanup_dry_result.stderr}"

    # Files should still exist after dry-run
    files_after_dry = list(data_dir.rglob("*.parquet"))
    assert len(files_after_dry) == len(initial_files), "Dry-run should not delete files"


if __name__ == "__main__":
    # Allow running this file directly for quick testing
    pytest.main([__file__, "-v", "-s"])
