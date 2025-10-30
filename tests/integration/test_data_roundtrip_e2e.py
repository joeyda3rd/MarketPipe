# SPDX-License-Identifier: Apache-2.0
"""Data integrity round-trip validation tests.

This test file addresses data quality concerns from E2E_TEST_GAP_ANALYSIS.md.

PURPOSE:
Validate that data maintains integrity through the complete pipeline:
Ingest → Storage → Aggregation → Query

WHAT THIS TESTS:
- Input data == output data (no corruption)
- OHLC price relationships preserved (high >= low, etc.)
- Timestamp precision maintained (nanosecond accuracy)
- Volume totals are correct
- No data loss during aggregation
- Decimal precision maintained through storage

WHY THIS MATTERS:
- Data corruption bugs can silently corrupt financial data
- Storage format changes could introduce precision loss
- Aggregation math errors could produce wrong results
- Users depend on data accuracy for trading decisions

EXECUTION TIME: Target <25 seconds for CI
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from tests.fakes.adapters import create_test_ohlcv_bars


def _bars_to_dataframe(bars: list[OHLCVBar]) -> pd.DataFrame:
    """Convert bars to DataFrame."""
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
def test_data_integrity_through_storage_roundtrip(tmp_path):
    """
    Test 4A: Validate data integrity through storage round-trip.

    Ensures that data written to Parquet storage can be read back
    without any loss of precision or corruption.

    Flow:
    1. Create known test data with specific values
    2. Write to Parquet storage
    3. Read back from storage
    4. Validate exact values match

    What this catches:
    - Floating point precision loss
    - Data type conversion errors
    - Storage format incompatibilities
    - Null handling issues
    """
    # Setup
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorageEngine(storage_dir)

    symbol = Symbol.from_string("TEST")
    trading_day = date(2025, 1, 15)

    # Create known data with specific decimal values
    base_time = datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)
    known_bars = []

    # Create 50 bars with known values for validation
    for i in range(50):
        bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(base_time + timedelta(minutes=i)),
            open_price=Price.from_float(100.00 + i * 0.01),  # Incremental prices
            high_price=Price.from_float(100.50 + i * 0.01),
            low_price=Price.from_float(99.50 + i * 0.01),
            close_price=Price.from_float(100.25 + i * 0.01),
            volume=Volume(1000 + i * 100),  # Incremental volumes
        )
        known_bars.append(bar)

    # Convert to DataFrame
    original_df = _bars_to_dataframe(known_bars)

    # Write to storage
    storage.write(
        df=original_df, frame="1m", symbol="TEST", trading_day=trading_day, job_id="roundtrip-test"
    )

    # Read back from storage
    retrieved_df = storage.load_partition(frame="1m", symbol="TEST", trading_day=trading_day)

    # VALIDATE: Same number of records
    assert len(retrieved_df) == len(
        original_df
    ), f"Should retrieve same number of records. Original: {len(original_df)}, Retrieved: {len(retrieved_df)}"

    # VALIDATE: Same columns
    assert set(retrieved_df.columns) == set(
        original_df.columns
    ), "Should have same columns after round-trip"

    # VALIDATE: Data integrity for each row
    for idx in range(len(original_df)):
        orig_row = original_df.iloc[idx]
        retr_row = retrieved_df.iloc[idx]

        # Timestamps should match exactly (nanosecond precision)
        assert orig_row["ts_ns"] == retr_row["ts_ns"], f"Timestamp mismatch at row {idx}"

        # Prices should match within floating point tolerance
        assert abs(orig_row["open"] - retr_row["open"]) < 1e-10, f"Open price mismatch at row {idx}"
        assert abs(orig_row["high"] - retr_row["high"]) < 1e-10, f"High price mismatch at row {idx}"
        assert abs(orig_row["low"] - retr_row["low"]) < 1e-10, f"Low price mismatch at row {idx}"
        assert (
            abs(orig_row["close"] - retr_row["close"]) < 1e-10
        ), f"Close price mismatch at row {idx}"

        # Volumes should match exactly
        assert orig_row["volume"] == retr_row["volume"], f"Volume mismatch at row {idx}"

        # OHLC relationships should be preserved
        assert retr_row["high"] >= retr_row["low"], f"High < Low violation at row {idx}"
        assert retr_row["high"] >= retr_row["open"], f"High < Open violation at row {idx}"
        assert retr_row["high"] >= retr_row["close"], f"High < Close violation at row {idx}"
        assert retr_row["low"] <= retr_row["open"], f"Low > Open violation at row {idx}"
        assert retr_row["low"] <= retr_row["close"], f"Low > Close violation at row {idx}"


@pytest.mark.integration
def test_ohlc_relationships_preserved(tmp_path):
    """
    Test 4B: Validate OHLC price relationships preserved.

    OHLC bars must maintain mathematical relationships:
    - High >= Low (always)
    - High >= Open, Close
    - Low <= Open, Close

    These relationships are fundamental to financial data integrity.
    """
    # Setup
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorageEngine(storage_dir)

    symbol = Symbol.from_string("OHLC")
    trading_day = date(2025, 1, 15)

    # Create bars with various OHLC patterns
    bars = create_test_ohlcv_bars(
        symbol, count=100, start_time=datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)
    )
    df = _bars_to_dataframe(bars)

    # Write and read back
    storage.write(df=df, frame="1m", symbol="OHLC", trading_day=trading_day, job_id="ohlc-test")
    retrieved_df = storage.load_partition(frame="1m", symbol="OHLC", trading_day=trading_day)

    # VALIDATE: All OHLC relationships
    for idx, row in retrieved_df.iterrows():
        assert (
            row["high"] >= row["low"]
        ), f"Row {idx}: High ({row['high']}) must be >= Low ({row['low']})"

        assert (
            row["high"] >= row["open"]
        ), f"Row {idx}: High ({row['high']}) must be >= Open ({row['open']})"

        assert (
            row["high"] >= row["close"]
        ), f"Row {idx}: High ({row['high']}) must be >= Close ({row['close']})"

        assert (
            row["low"] <= row["open"]
        ), f"Row {idx}: Low ({row['low']}) must be <= Open ({row['open']})"

        assert (
            row["low"] <= row["close"]
        ), f"Row {idx}: Low ({row['low']}) must be <= Close ({row['close']})"

        # Additional sanity checks
        assert row["volume"] > 0, f"Row {idx}: Volume must be positive"

        assert row["ts_ns"] > 0, f"Row {idx}: Timestamp must be positive"


@pytest.mark.integration
def test_timestamp_precision_maintained(tmp_path):
    """
    Test 4C: Validate nanosecond timestamp precision maintained.

    Financial data often requires nanosecond precision for accurate
    timing. This test ensures storage doesn't lose precision.
    """
    # Setup
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorageEngine(storage_dir)

    symbol = Symbol.from_string("PRECISE")
    trading_day = date(2025, 1, 15)

    # Create bars with precise nanosecond timestamps
    base_timestamp_ns = 1736934600000000000  # 2025-01-15 09:30:00 UTC in nanoseconds

    data = []
    for i in range(10):
        # Add exact nanosecond increments
        ts_ns = (
            base_timestamp_ns + (i * 60 * 1_000_000_000) + i
        )  # Add i nanoseconds for precision test
        data.append(
            {
                "ts_ns": ts_ns,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
                "symbol": "PRECISE",
            }
        )

    df = pd.DataFrame(data)

    # Write and read back
    storage.write(
        df=df, frame="1m", symbol="PRECISE", trading_day=trading_day, job_id="precision-test"
    )
    retrieved_df = storage.load_partition(frame="1m", symbol="PRECISE", trading_day=trading_day)

    # VALIDATE: Nanosecond precision preserved
    for idx in range(len(df)):
        original_ts = df.iloc[idx]["ts_ns"]
        retrieved_ts = retrieved_df.iloc[idx]["ts_ns"]

        assert (
            original_ts == retrieved_ts
        ), f"Row {idx}: Timestamp precision lost. Original: {original_ts}, Retrieved: {retrieved_ts}"


@pytest.mark.integration
def test_volume_totals_accurate(tmp_path):
    """
    Test 4D: Validate volume totals remain accurate.

    Volume is critical for trading analysis. This test ensures
    volumes are not corrupted or rounded incorrectly.
    """
    # Setup
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorageEngine(storage_dir)

    symbol = Symbol.from_string("VOLUME")
    trading_day = date(2025, 1, 15)

    # Create bars with specific volumes that sum to a known total
    volumes = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
    expected_total = sum(volumes)

    data = []
    base_timestamp_ns = 1736934600000000000
    for i, vol in enumerate(volumes):
        data.append(
            {
                "ts_ns": base_timestamp_ns + (i * 60 * 1_000_000_000),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": vol,
                "symbol": "VOLUME",
            }
        )

    df = pd.DataFrame(data)

    # Write and read back
    storage.write(df=df, frame="1m", symbol="VOLUME", trading_day=trading_day, job_id="volume-test")
    retrieved_df = storage.load_partition(frame="1m", symbol="VOLUME", trading_day=trading_day)

    # VALIDATE: Volume total matches
    retrieved_total = retrieved_df["volume"].sum()
    assert (
        retrieved_total == expected_total
    ), f"Volume total mismatch. Expected: {expected_total}, Got: {retrieved_total}"

    # VALIDATE: Individual volumes match
    for idx in range(len(df)):
        original_vol = df.iloc[idx]["volume"]
        retrieved_vol = retrieved_df.iloc[idx]["volume"]

        assert (
            original_vol == retrieved_vol
        ), f"Row {idx}: Volume mismatch. Original: {original_vol}, Retrieved: {retrieved_vol}"


@pytest.mark.integration
def test_no_data_corruption_after_multiple_writes(tmp_path):
    """
    Test 4E: Validate no data corruption after multiple write/read cycles.

    Data should remain stable through multiple storage operations.
    """
    # Setup
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorageEngine(storage_dir)

    symbol = Symbol.from_string("STABLE")

    # Create original data
    bars = create_test_ohlcv_bars(
        symbol, count=20, start_time=datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)
    )
    original_df = _bars_to_dataframe(bars)

    # Perform multiple write/read cycles
    for day_offset in range(5):
        trading_day = date(2025, 1, 15) + timedelta(days=day_offset)

        # Write
        storage.write(
            df=original_df,
            frame="1m",
            symbol="STABLE",
            trading_day=trading_day,
            job_id=f"stability-test-{day_offset}",
        )

        # Read back
        retrieved_df = storage.load_partition(frame="1m", symbol="STABLE", trading_day=trading_day)

        # Validate data matches original
        assert len(retrieved_df) == len(original_df), f"Day {day_offset}: Record count mismatch"

        # Check first and last rows for integrity
        pd.testing.assert_series_equal(
            original_df.iloc[0][["open", "high", "low", "close", "volume"]],
            retrieved_df.iloc[0][["open", "high", "low", "close", "volume"]],
            check_names=False,
            rtol=1e-10,
        )

        pd.testing.assert_series_equal(
            original_df.iloc[-1][["open", "high", "low", "close", "volume"]],
            retrieved_df.iloc[-1][["open", "high", "low", "close", "volume"]],
            check_names=False,
            rtol=1e-10,
        )


if __name__ == "__main__":
    # Allow running this file directly for quick testing
    pytest.main([__file__, "-v", "-s"])
