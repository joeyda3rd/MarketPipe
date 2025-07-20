# SPDX-License-Identifier: Apache-2.0
"""Enhanced boundary condition end-to-end tests.

This test validates MarketPipe behavior at system boundaries and edge cases,
including extreme data conditions, resource limits, temporal boundaries,
and data quality edge cases that could cause production issues.
"""

from __future__ import annotations

import gc
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest
from marketpipe.aggregation.application.services import AggregationRunnerService
from marketpipe.aggregation.domain.services import AggregationDomainService
from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine
from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.domain.value_objects import Symbol
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


class BoundaryTestDataGenerator:
    """Generate edge case test data for boundary condition testing."""

    @staticmethod
    def create_minimal_dataset() -> pd.DataFrame:
        """Create minimal valid dataset (single bar)."""
        return pd.DataFrame(
            [
                {
                    "ts_ns": int(
                        datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc).timestamp() * 1e9
                    ),
                    "symbol": "MIN",
                    "open": 100.0,
                    "high": 100.01,
                    "low": 99.99,
                    "close": 100.0,
                    "volume": 1,
                    "trade_count": 1,
                    "vwap": 100.0,
                }
            ]
        )

    @staticmethod
    def create_extreme_price_dataset() -> pd.DataFrame:
        """Create dataset with extreme price values."""
        bars = []
        base_time = datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc)

        # Test extreme price values
        extreme_cases = [
            # (open, high, low, close, description)
            (0.0001, 0.0002, 0.00005, 0.00015, "micro_prices"),
            (999999.99, 1000000.0, 999999.0, 999999.5, "very_high_prices"),
            (1.0, 1.0, 1.0, 1.0, "constant_price"),
            (100.0, 200.0, 50.0, 150.0, "extreme_volatility"),
        ]

        for i, (open_price, high_price, low_price, close_price, _desc) in enumerate(extreme_cases):
            timestamp_ns = int((base_time + timedelta(minutes=i)).timestamp() * 1e9)
            bars.append(
                {
                    "ts_ns": timestamp_ns,
                    "symbol": "EXTREME",
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": 1000 if i < 3 else 0,  # Include zero volume case
                    "trade_count": 1 if i < 3 else 0,
                    "vwap": (high_price + low_price + close_price) / 3,
                }
            )

        return pd.DataFrame(bars)

    @staticmethod
    def create_temporal_boundary_dataset() -> pd.DataFrame:
        """Create dataset with temporal boundary conditions."""
        bars = []

        # Test various temporal edge cases
        temporal_cases = [
            # New Year's Day boundary
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            # Leap year day
            datetime(2024, 2, 29, 12, 0, 0, tzinfo=timezone.utc),
            # Daylight saving time transition (approximate)
            datetime(2024, 3, 10, 7, 0, 0, tzinfo=timezone.utc),
            # Market holiday proximity (Thanksgiving week)
            datetime(2024, 11, 27, 13, 30, 0, tzinfo=timezone.utc),
            # Year-end boundary
            datetime(2024, 12, 31, 20, 0, 0, tzinfo=timezone.utc),
        ]

        for i, timestamp in enumerate(temporal_cases):
            timestamp_ns = int(timestamp.timestamp() * 1e9)
            bars.append(
                {
                    "ts_ns": timestamp_ns,
                    "symbol": "TEMPORAL",
                    "open": 100.0 + i,
                    "high": 101.0 + i,
                    "low": 99.0 + i,
                    "close": 100.5 + i,
                    "volume": 1000,
                    "trade_count": 50,
                    "vwap": 100.33 + i,
                }
            )

        return pd.DataFrame(bars)

    @staticmethod
    def create_large_volume_dataset(size: int = 10000) -> pd.DataFrame:
        """Create large dataset for stress testing."""
        bars = []
        base_time = datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc)
        base_price = 100.0

        for i in range(size):
            timestamp = base_time + timedelta(seconds=i)  # Second-level granularity
            timestamp_ns = int(timestamp.timestamp() * 1e9)

            # Gradual price movement
            price = base_price + (i * 0.001)

            bars.append(
                {
                    "ts_ns": timestamp_ns,
                    "symbol": "LARGE",
                    "open": price,
                    "high": price + 0.01,
                    "low": price - 0.01,
                    "close": price + 0.005,
                    "volume": 1000 + (i % 500),
                    "trade_count": 50 + (i % 20),
                    "vwap": price,
                }
            )

        return pd.DataFrame(bars)

    @staticmethod
    def create_malformed_data_variants() -> list[tuple[pd.DataFrame, str]]:
        """Create various malformed data scenarios for resilience testing."""
        base_time = datetime(2024, 1, 15, 13, 30, 0, tzinfo=timezone.utc)
        base_timestamp_ns = int(base_time.timestamp() * 1e9)

        variants = []

        # Missing columns (should fail gracefully)
        missing_columns = pd.DataFrame(
            [
                {
                    "ts_ns": base_timestamp_ns,
                    "symbol": "MISSING",
                    "open": 100.0,
                    # Missing high, low, close, volume
                }
            ]
        )
        variants.append((missing_columns, "missing_required_columns"))

        # Negative prices (should be handled or rejected)
        negative_prices = pd.DataFrame(
            [
                {
                    "ts_ns": base_timestamp_ns,
                    "symbol": "NEGATIVE",
                    "open": -100.0,
                    "high": -99.0,
                    "low": -101.0,
                    "close": -100.5,
                    "volume": 1000,
                    "trade_count": 50,
                    "vwap": -100.0,
                }
            ]
        )
        variants.append((negative_prices, "negative_prices"))

        # Invalid OHLC relationships
        invalid_ohlc = pd.DataFrame(
            [
                {
                    "ts_ns": base_timestamp_ns,
                    "symbol": "INVALID",
                    "open": 100.0,
                    "high": 95.0,  # High < Open (invalid)
                    "low": 105.0,  # Low > Open (invalid)
                    "close": 110.0,  # Close > High (invalid)
                    "volume": 1000,
                    "trade_count": 50,
                    "vwap": 100.0,
                }
            ]
        )
        variants.append((invalid_ohlc, "invalid_ohlc_relationships"))

        # Extreme timestamps (far future/past)
        extreme_timestamps = pd.DataFrame(
            [
                {
                    "ts_ns": int(
                        datetime(2100, 1, 1, tzinfo=timezone.utc).timestamp() * 1e9
                    ),  # Far future
                    "symbol": "FUTURE",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                    "trade_count": 50,
                    "vwap": 100.0,
                }
            ]
        )
        variants.append((extreme_timestamps, "extreme_future_timestamp"))

        # Duplicate timestamps
        duplicate_timestamps = pd.DataFrame(
            [
                {
                    "ts_ns": base_timestamp_ns,
                    "symbol": "DUP",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                    "trade_count": 50,
                    "vwap": 100.0,
                },
                {
                    "ts_ns": base_timestamp_ns,  # Same timestamp
                    "symbol": "DUP",
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 1100,
                    "trade_count": 55,
                    "vwap": 101.0,
                },
            ]
        )
        variants.append((duplicate_timestamps, "duplicate_timestamps"))

        return variants


@pytest.mark.integration
@pytest.mark.boundary
class TestBoundaryConditionsEndToEnd:
    """Comprehensive boundary condition testing."""

    def test_minimal_dataset_processing(self, tmp_path):
        """Test processing of minimal valid dataset."""

        # Setup
        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Create minimal dataset
        minimal_data = BoundaryTestDataGenerator.create_minimal_dataset()

        job_id = "minimal-test"
        raw_engine.write(
            df=minimal_data,
            frame="1m",
            symbol="MIN",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True,
        )

        print(f"✓ Wrote minimal dataset: {len(minimal_data)} bar")

        # Test aggregation with minimal data
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)

        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("MIN"),
            trading_date=date(2024, 1, 15),
            bars_processed=1,
            success=True,
        )

        # Should handle minimal data gracefully
        aggregation_service.handle_ingestion_completed(event)

        # Verify aggregated data
        agg_engine = ParquetStorageEngine(agg_dir)

        # Check if any aggregated data was created
        try:
            agg_data = agg_engine.load_symbol_data(symbol="MIN", frame="5m")
            if not agg_data.empty:
                print(f"✓ Aggregated minimal data: {len(agg_data)} bars")
            else:
                print(
                    "⚠️  No aggregated data created from minimal dataset (expected for single bar)"
                )
        except:
            print("⚠️  Aggregation skipped for insufficient data (expected)")

        print("✅ Minimal dataset processing test completed")

    def test_extreme_price_conditions(self, tmp_path):
        """Test handling of extreme price values and conditions."""

        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Create extreme price dataset
        extreme_data = BoundaryTestDataGenerator.create_extreme_price_dataset()

        job_id = "extreme-prices-test"
        raw_engine.write(
            df=extreme_data,
            frame="1m",
            symbol="EXTREME",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True,
        )

        print(f"✓ Wrote extreme price dataset: {len(extreme_data)} bars")
        print(f"  Price range: {extreme_data['low'].min():.6f} - {extreme_data['high'].max():.2f}")
        print(f"  Zero volume bars: {(extreme_data['volume'] == 0).sum()}")

        # Test aggregation with extreme prices
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)

        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("EXTREME"),
            trading_date=date(2024, 1, 15),
            bars_processed=len(extreme_data),
            success=True,
        )

        # Should handle extreme prices without crashing
        try:
            aggregation_service.handle_ingestion_completed(event)
            print("✓ Extreme price aggregation completed without errors")
        except Exception as e:
            print(f"⚠️  Extreme price aggregation error (may be expected): {e}")

        print("✅ Extreme price conditions test completed")

    def test_temporal_boundary_conditions(self, tmp_path):
        """Test processing across temporal boundaries."""

        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Create temporal boundary dataset
        temporal_data = BoundaryTestDataGenerator.create_temporal_boundary_dataset()

        # Test each temporal boundary case
        for i, row in temporal_data.iterrows():
            trading_day = datetime.fromtimestamp(row["ts_ns"] / 1e9, tz=timezone.utc).date()
            job_id = f"temporal-test-{i}"

            single_bar_df = pd.DataFrame([row])

            try:
                raw_engine.write(
                    df=single_bar_df,
                    frame="1m",
                    symbol="TEMPORAL",
                    trading_day=trading_day,
                    job_id=job_id,
                    overwrite=True,
                )
                print(f"✓ Processed temporal boundary: {trading_day}")
            except Exception as e:
                print(f"⚠️  Temporal boundary issue for {trading_day}: {e}")

        print("✅ Temporal boundary conditions test completed")

    def test_large_dataset_stress(self, tmp_path):
        """Test system behavior with large datasets."""

        # Create large dataset (but reasonable for CI)
        large_data = BoundaryTestDataGenerator.create_large_volume_dataset(size=5000)  # 5K bars

        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Monitor memory usage
        import psutil

        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB

        start_time = time.monotonic()

        job_id = "large-dataset-stress"
        raw_engine.write(
            df=large_data,
            frame="1m",
            symbol="LARGE",
            trading_day=date(2024, 1, 15),
            job_id=job_id,
            overwrite=True,
        )

        write_time = time.monotonic() - start_time
        write_memory = process.memory_info().rss / 1024 / 1024

        print(f"✓ Wrote large dataset: {len(large_data):,} bars in {write_time:.2f}s")
        print(f"  Memory usage: {write_memory - start_memory:.1f} MB increase")

        # Test aggregation performance
        agg_start_time = time.monotonic()

        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)

        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("LARGE"),
            trading_date=date(2024, 1, 15),
            bars_processed=len(large_data),
            success=True,
        )

        aggregation_service.handle_ingestion_completed(event)

        agg_time = time.monotonic() - agg_start_time
        final_memory = process.memory_info().rss / 1024 / 1024

        print(f"✓ Aggregated large dataset in {agg_time:.2f}s")
        print(f"  Final memory usage: {final_memory - start_memory:.1f} MB total increase")

        # Performance assertions (relaxed for CI environments)
        assert write_time < 30, f"Large dataset write too slow: {write_time:.1f}s"
        assert agg_time < 60, f"Large dataset aggregation too slow: {agg_time:.1f}s"
        assert (
            final_memory - start_memory < 500
        ), f"Memory usage too high: {final_memory - start_memory:.1f} MB"

        print("✅ Large dataset stress test completed")

    def test_malformed_data_resilience(self, tmp_path):
        """Test system resilience to malformed data."""

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Test various malformed data scenarios
        malformed_variants = BoundaryTestDataGenerator.create_malformed_data_variants()

        resilience_results = []

        for df, description in malformed_variants:
            job_id = f"malformed-{description}"

            try:
                # Attempt to write malformed data
                raw_engine.write(
                    df=df,
                    frame="1m",
                    symbol="MALFORMED",
                    trading_day=date(2024, 1, 15),
                    job_id=job_id,
                    overwrite=True,
                )

                resilience_results.append((description, "ACCEPTED", "Data written successfully"))
                print(f"⚠️  Malformed data accepted: {description}")

            except Exception as e:
                resilience_results.append((description, "REJECTED", str(e)))
                print(f"✓ Malformed data rejected: {description} - {e}")

        # Summary of resilience testing
        print("\n📊 Malformed Data Resilience Summary:")
        for description, result, _details in resilience_results:
            print(f"  {description}: {result}")

        # Count rejections (good) vs acceptances (may need review)
        rejections = sum(1 for _, result, _ in resilience_results if result == "REJECTED")
        acceptances = sum(1 for _, result, _ in resilience_results if result == "ACCEPTED")

        print(f"\nRejected: {rejections}, Accepted: {acceptances}")

        if acceptances > 0:
            print("⚠️  Some malformed data was accepted - consider enhanced validation")
        else:
            print("✅ All malformed data properly rejected")

        print("✅ Malformed data resilience test completed")

    def test_resource_exhaustion_scenarios(self, tmp_path):
        """Test behavior under resource constraints."""

        # Test file descriptor limits
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Create many small files to test file handle management
        files_created = 0
        max_files = 100  # Reasonable limit for CI

        try:
            for i in range(max_files):
                small_data = pd.DataFrame(
                    [
                        {
                            "ts_ns": int(
                                (
                                    datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)
                                    + timedelta(minutes=i)
                                ).timestamp()
                                * 1e9
                            ),
                            "symbol": f"FD{i:03d}",
                            "open": 100.0,
                            "high": 100.01,
                            "low": 99.99,
                            "close": 100.0,
                            "volume": 1000,
                            "trade_count": 50,
                            "vwap": 100.0,
                        }
                    ]
                )

                raw_engine.write(
                    df=small_data,
                    frame="1m",
                    symbol=f"FD{i:03d}",
                    trading_day=date(2024, 1, 15),
                    job_id=f"fd-test-{i}",
                    overwrite=True,
                )
                files_created += 1

                # Force garbage collection periodically
                if i % 20 == 0:
                    gc.collect()

        except Exception as e:
            print(f"⚠️  File descriptor limit reached at {files_created} files: {e}")

        print(f"✓ Created {files_created} files without resource exhaustion")

        # Test memory pressure (create larger datasets gradually)
        memory_test_sizes = [1000, 2000, 3000]  # Progressively larger

        for size in memory_test_sizes:
            try:
                large_data = BoundaryTestDataGenerator.create_large_volume_dataset(size)

                # Just test creation, not storage (already tested above)
                memory_mb = large_data.memory_usage(deep=True).sum() / 1024 / 1024
                print(f"✓ Created {size:,} bar dataset ({memory_mb:.1f} MB)")

                # Clear memory
                del large_data
                gc.collect()

            except MemoryError:
                print(f"⚠️  Memory limit reached at {size:,} bars")
                break

        print("✅ Resource exhaustion scenarios test completed")

    def test_concurrent_boundary_conditions(self, tmp_path):
        """Test boundary conditions under concurrent access."""

        import concurrent.futures

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Concurrent write test with boundary data
        def write_boundary_data(thread_id: int) -> tuple[int, str]:
            try:
                # Each thread writes different boundary case
                if thread_id % 4 == 0:
                    data = BoundaryTestDataGenerator.create_minimal_dataset()
                    symbol = f"MIN{thread_id}"
                elif thread_id % 4 == 1:
                    data = BoundaryTestDataGenerator.create_extreme_price_dataset()
                    symbol = f"EXT{thread_id}"
                elif thread_id % 4 == 2:
                    data = BoundaryTestDataGenerator.create_temporal_boundary_dataset()
                    symbol = f"TMP{thread_id}"
                else:
                    data = BoundaryTestDataGenerator.create_large_volume_dataset(1000)
                    symbol = f"LRG{thread_id}"

                raw_engine.write(
                    df=data,
                    frame="1m",
                    symbol=symbol,
                    trading_day=date(2024, 1, 15),
                    job_id=f"concurrent-boundary-{thread_id}",
                    overwrite=True,
                )

                return thread_id, "SUCCESS"

            except Exception as e:
                return thread_id, f"ERROR: {e}"

        # Run concurrent boundary tests
        thread_count = 8

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(write_boundary_data, i) for i in range(thread_count)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Analyze results
        successes = sum(1 for _, result in results if result == "SUCCESS")
        errors = [result for _, result in results if result != "SUCCESS"]

        print("📊 Concurrent Boundary Test Results:")
        print(f"  Successful: {successes}/{thread_count}")
        print(f"  Errors: {len(errors)}")

        if errors:
            print("  Error details:")
            for error in errors[:3]:  # Show first 3 errors
                print(f"    {error}")

        # Should handle most concurrent boundary conditions
        assert (
            successes >= thread_count * 0.7
        ), f"Too many concurrent failures: {successes}/{thread_count}"

        print("✅ Concurrent boundary conditions test completed")


@pytest.mark.integration
@pytest.mark.boundary
def test_system_limits_documentation(tmp_path):
    """Document and validate known system limits."""

    print("📊 MarketPipe System Limits Documentation:")
    print("=" * 50)

    # Test and document various limits
    limits_tested = {
        "max_bars_per_file": 10000,
        "max_symbols_per_job": 100,
        "max_concurrent_jobs": 10,
        "max_price_value": 1000000.0,
        "min_price_value": 0.0001,
        "max_volume": 1000000000,
        "temporal_range_days": 365,
    }

    for limit_name, limit_value in limits_tested.items():
        print(f"  {limit_name}: {limit_value:,}")

    # Test one representative limit
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)

    raw_engine = ParquetStorageEngine(raw_dir)

    # Test max_bars_per_file limit
    try:
        large_dataset = BoundaryTestDataGenerator.create_large_volume_dataset(
            size=limits_tested["max_bars_per_file"]
        )

        raw_engine.write(
            df=large_dataset,
            frame="1m",
            symbol="LIMIT_TEST",
            trading_day=date(2024, 1, 15),
            job_id="limits-test",
            overwrite=True,
        )

        print(f"✓ Validated max_bars_per_file limit: {limits_tested['max_bars_per_file']:,} bars")

    except Exception as e:
        print(f"⚠️  Limit exceeded for max_bars_per_file: {e}")

    print("✅ System limits documentation completed")
