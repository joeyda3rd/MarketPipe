# SPDX-License-Identifier: Apache-2.0
"""End-to-end performance integration tests with realistic workload scenarios.

This test validates performance characteristics of the complete MarketPipe
system under realistic data volumes and concurrent operations, establishing
baseline performance metrics and detecting regressions.
"""

from __future__ import annotations

import gc
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import psutil
import pytest
from marketpipe.aggregation.application.services import AggregationRunnerService
from marketpipe.aggregation.domain.services import AggregationDomainService
from marketpipe.aggregation.infrastructure.duckdb_engine import DuckDBAggregationEngine
from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.domain.value_objects import Symbol
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


class PerformanceMonitor:
    """Monitor system performance during test execution."""

    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.end_time = None
        self.peak_memory_mb = 0
        self.start_memory_mb = 0
        self.monitoring = False
        self._monitor_thread = None

    def start(self):
        """Start performance monitoring."""
        gc.collect()  # Clean up before monitoring
        self.start_time = time.monotonic()
        self.start_memory_mb = self.process.memory_info().rss / 1024 / 1024
        self.peak_memory_mb = self.start_memory_mb
        self.monitoring = True

        # Start background monitoring thread
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop(self) -> dict[str, float]:
        """Stop monitoring and return performance metrics."""
        self.monitoring = False
        self.end_time = time.monotonic()

        if self._monitor_thread:
            self._monitor_thread.join(timeout=1.0)

        current_memory_mb = self.process.memory_info().rss / 1024 / 1024

        return {
            "duration_seconds": self.end_time - self.start_time,
            "peak_memory_mb": self.peak_memory_mb,
            "memory_delta_mb": current_memory_mb - self.start_memory_mb,
            "final_memory_mb": current_memory_mb,
        }

    def _monitor_loop(self):
        """Background monitoring loop."""
        while self.monitoring:
            try:
                current_memory_mb = self.process.memory_info().rss / 1024 / 1024
                self.peak_memory_mb = max(self.peak_memory_mb, current_memory_mb)
                time.sleep(0.1)  # Monitor every 100ms
            except:
                break


def generate_trading_calendar(start_date: date, end_date: date) -> list[date]:
    """Generate list of trading days (weekdays only for simplicity)."""
    current = start_date
    trading_days = []

    while current <= end_date:
        # Skip weekends (simplified trading calendar)
        if current.weekday() < 5:  # Monday = 0, Friday = 4
            trading_days.append(current)
        current += timedelta(days=1)

    return trading_days


def generate_realistic_trading_data(
    symbols: list[str], trading_days: list[date], bars_per_day: int = 390
) -> dict[str, pd.DataFrame]:
    """Generate realistic trading data for performance testing."""

    import random

    random.seed(42)  # Reproducible test data

    symbol_data = {}

    for symbol in symbols:
        all_bars = []
        base_price = 100.0 + random.uniform(-50, 100)  # Random starting price

        for trading_day in trading_days:
            # Generate a full trading day of minute bars
            market_open = datetime.combine(trading_day, datetime.min.time()) + timedelta(
                hours=13, minutes=30
            )
            market_open = market_open.replace(tzinfo=timezone.utc)

            current_price = base_price
            daily_volatility = random.uniform(0.01, 0.03)  # 1-3% daily volatility

            for minute in range(bars_per_day):
                # Realistic price movement
                price_change = random.gauss(0, daily_volatility / (bars_per_day**0.5))
                current_price *= 1 + price_change

                # Generate OHLC
                open_price = current_price
                close_price = current_price * (
                    1 + random.gauss(0, daily_volatility / (bars_per_day**0.5))
                )
                high_price = max(open_price, close_price) * (
                    1 + abs(random.gauss(0, daily_volatility / (bars_per_day * 2)))
                )
                low_price = min(open_price, close_price) * (
                    1 - abs(random.gauss(0, daily_volatility / (bars_per_day * 2)))
                )

                # Volume with intraday patterns
                volume_base = 1000
                if minute < 30 or minute > 360:  # Higher volume at open/close
                    volume_base *= 2
                volume = int(volume_base * (1 + random.gauss(0, 0.5)))
                volume = max(volume, 100)

                timestamp = market_open + timedelta(minutes=minute)
                timestamp_ns = int(timestamp.timestamp() * 1_000_000_000)

                all_bars.append(
                    {
                        "ts_ns": timestamp_ns,
                        "symbol": symbol,
                        "open": round(open_price, 2),
                        "high": round(high_price, 2),
                        "low": round(low_price, 2),
                        "close": round(close_price, 2),
                        "volume": volume,
                        "trade_count": random.randint(50, 200),
                        "vwap": round((high_price + low_price + close_price) / 3, 2),
                    }
                )

                current_price = close_price

            # Slight drift for next day
            base_price = current_price * (1 + random.gauss(0, 0.001))

        symbol_data[symbol] = pd.DataFrame(all_bars)

    return symbol_data


@pytest.mark.integration
@pytest.mark.slow
class TestPerformanceIntegration:
    """Performance integration tests with realistic workloads."""

    def test_single_symbol_full_year_performance(self, tmp_path):
        """Test performance with single symbol, full trading year of data."""

        # Performance expectations (adjust based on system capabilities)
        MAX_EXECUTION_TIME = 60  # seconds
        MAX_MEMORY_USAGE = 500  # MB

        monitor = PerformanceMonitor()
        monitor.start()

        # Setup test scenario
        symbols = ["AAPL"]
        trading_days = generate_trading_calendar(date(2024, 1, 1), date(2024, 12, 31))
        bars_per_day = 390  # Full trading day

        print(
            f"🔄 Testing single symbol with {len(trading_days)} trading days ({len(trading_days) * bars_per_day:,} total bars)"
        )

        # Generate realistic data
        symbol_data = generate_realistic_trading_data(symbols, trading_days, bars_per_day)

        # Setup storage and pipeline
        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Write all data (simulate full year ingestion)
        job_id = "full-year-perf-test"
        total_bars_written = 0

        write_start = time.monotonic()
        for symbol in symbols:
            df = symbol_data[symbol]

            # Split by trading day for realistic partitioning
            for trading_day in trading_days:
                day_start = datetime.combine(trading_day, datetime.min.time()).replace(
                    tzinfo=timezone.utc
                )
                day_end = day_start + timedelta(days=1)

                day_start_ns = int(day_start.timestamp() * 1e9)
                day_end_ns = int(day_end.timestamp() * 1e9)

                day_df = df[(df["ts_ns"] >= day_start_ns) & (df["ts_ns"] < day_end_ns)]

                if not day_df.empty:
                    raw_engine.write(
                        df=day_df,
                        frame="1m",
                        symbol=symbol,
                        trading_day=trading_day,
                        job_id=job_id,
                        overwrite=True,
                    )
                    total_bars_written += len(day_df)

        write_duration = time.monotonic() - write_start
        print(
            f"✓ Wrote {total_bars_written:,} bars in {write_duration:.1f}s ({total_bars_written/write_duration:.0f} bars/sec)"
        )

        # Test aggregation performance
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)

        agg_start = time.monotonic()
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol(symbols[0]),
            trading_date=trading_days[0],
            bars_processed=total_bars_written,
            success=True,
        )

        aggregation_service.handle_ingestion_completed(event)
        agg_duration = time.monotonic() - agg_start

        print(f"✓ Aggregated {total_bars_written:,} bars in {agg_duration:.1f}s")

        # Stop monitoring and check performance
        metrics = monitor.stop()

        print("📊 Performance Metrics:")
        print(f"   Total Duration: {metrics['duration_seconds']:.1f}s")
        print(f"   Peak Memory: {metrics['peak_memory_mb']:.1f} MB")
        print(f"   Memory Delta: {metrics['memory_delta_mb']:.1f} MB")
        print(f"   Throughput: {total_bars_written / metrics['duration_seconds']:.0f} bars/sec")

        # Performance assertions
        assert (
            metrics["duration_seconds"] < MAX_EXECUTION_TIME
        ), f"Performance regression: {metrics['duration_seconds']:.1f}s > {MAX_EXECUTION_TIME}s"

        assert (
            metrics["peak_memory_mb"] < MAX_MEMORY_USAGE
        ), f"Memory regression: {metrics['peak_memory_mb']:.1f}MB > {MAX_MEMORY_USAGE}MB"

        print("✅ Single symbol full year performance test passed")

    def test_multi_symbol_single_day_performance(self, tmp_path):
        """Test performance with many symbols, single trading day."""

        # Test with first 50 symbols (representative of medium workload)
        symbols = [f"SYM{i:03d}" for i in range(50)]
        trading_days = [date(2024, 1, 15)]  # Single day

        MAX_EXECUTION_TIME = 30  # seconds
        MAX_MEMORY_USAGE = 1000  # MB

        monitor = PerformanceMonitor()
        monitor.start()

        print(f"🔄 Testing {len(symbols)} symbols with single trading day")

        # Generate data
        symbol_data = generate_realistic_trading_data(symbols, trading_days)

        # Setup pipeline
        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Test concurrent writes (simulate parallel ingestion)
        job_id = "multi-symbol-perf-test"
        total_bars = 0

        def write_symbol_data(symbol: str) -> int:
            """Write data for a single symbol."""
            df = symbol_data[symbol]
            raw_engine.write(
                df=df,
                frame="1m",
                symbol=symbol,
                trading_day=trading_days[0],
                job_id=job_id,
                overwrite=True,
            )
            return len(df)

        # Concurrent writes
        with ThreadPoolExecutor(max_workers=4) as executor:
            write_futures = {
                executor.submit(write_symbol_data, symbol): symbol for symbol in symbols
            }

            for future in as_completed(write_futures):
                bars_written = future.result()
                total_bars += bars_written

        print(f"✓ Wrote {total_bars:,} bars across {len(symbols)} symbols")

        # Test aggregation
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)

        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol(symbols[0]),
            trading_date=trading_days[0],
            bars_processed=total_bars,
            success=True,
        )

        aggregation_service.handle_ingestion_completed(event)

        # Stop monitoring
        metrics = monitor.stop()

        print("📊 Multi-Symbol Performance Metrics:")
        print(f"   Total Duration: {metrics['duration_seconds']:.1f}s")
        print(f"   Peak Memory: {metrics['peak_memory_mb']:.1f} MB")
        print(f"   Symbols/Second: {len(symbols) / metrics['duration_seconds']:.1f}")

        # Performance assertions
        assert (
            metrics["duration_seconds"] < MAX_EXECUTION_TIME
        ), f"Multi-symbol performance regression: {metrics['duration_seconds']:.1f}s > {MAX_EXECUTION_TIME}s"

        assert (
            metrics["peak_memory_mb"] < MAX_MEMORY_USAGE
        ), f"Multi-symbol memory regression: {metrics['peak_memory_mb']:.1f}MB > {MAX_MEMORY_USAGE}MB"

        print("✅ Multi-symbol single day performance test passed")

    def test_concurrent_read_write_performance(self, tmp_path):
        """Test performance under concurrent read/write operations."""

        MAX_EXECUTION_TIME = 20  # seconds

        monitor = PerformanceMonitor()
        monitor.start()

        # Setup
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]
        trading_days = generate_trading_calendar(date(2024, 1, 15), date(2024, 1, 19))  # 5 days

        symbol_data = generate_realistic_trading_data(symbols, trading_days)

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True)
        raw_engine = ParquetStorageEngine(raw_dir)

        # Phase 1: Write initial data
        job_id = "concurrent-perf-test"
        for symbol in symbols[:2]:  # Write first 2 symbols
            for trading_day in trading_days:
                day_data = symbol_data[symbol]
                day_start_ns = int(
                    datetime.combine(trading_day, datetime.min.time())
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                    * 1e9
                )
                day_end_ns = day_start_ns + int(24 * 60 * 60 * 1e9)

                day_df = day_data[
                    (day_data["ts_ns"] >= day_start_ns) & (day_data["ts_ns"] < day_end_ns)
                ]
                if not day_df.empty:
                    raw_engine.write(
                        df=day_df,
                        frame="1m",
                        symbol=symbol,
                        trading_day=trading_day,
                        job_id=job_id,
                        overwrite=True,
                    )

        # Phase 2: Concurrent reads and writes
        read_results = []
        write_results = []
        errors = []

        def concurrent_reader():
            """Continuously read data while writes are happening."""
            try:
                for _ in range(10):
                    job_data = raw_engine.load_job_bars(job_id)
                    read_results.append(len(job_data))
                    time.sleep(0.1)
            except Exception as e:
                errors.append(f"Read error: {e}")

        def concurrent_writer():
            """Write remaining symbols while reads are happening."""
            try:
                for symbol in symbols[2:]:  # Write remaining symbols
                    for trading_day in trading_days:
                        day_data = symbol_data[symbol]
                        day_start_ns = int(
                            datetime.combine(trading_day, datetime.min.time())
                            .replace(tzinfo=timezone.utc)
                            .timestamp()
                            * 1e9
                        )
                        day_end_ns = day_start_ns + int(24 * 60 * 60 * 1e9)

                        day_df = day_data[
                            (day_data["ts_ns"] >= day_start_ns) & (day_data["ts_ns"] < day_end_ns)
                        ]
                        if not day_df.empty:
                            raw_engine.write(
                                df=day_df,
                                frame="1m",
                                symbol=symbol,
                                trading_day=trading_day,
                                job_id=job_id,
                                overwrite=True,
                            )
                            write_results.append(len(day_df))
                        time.sleep(0.05)
            except Exception as e:
                errors.append(f"Write error: {e}")

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=3) as executor:
            read_future = executor.submit(concurrent_reader)
            write_future = executor.submit(concurrent_writer)

            # Wait for completion
            read_future.result()
            write_future.result()

        metrics = monitor.stop()

        print("📊 Concurrent Read/Write Performance:")
        print(f"   Duration: {metrics['duration_seconds']:.1f}s")
        print(f"   Successful Reads: {len(read_results)}")
        print(f"   Successful Writes: {len(write_results)}")
        print(f"   Errors: {len(errors)}")

        # Performance assertions
        assert (
            metrics["duration_seconds"] < MAX_EXECUTION_TIME
        ), f"Concurrent operation timeout: {metrics['duration_seconds']:.1f}s > {MAX_EXECUTION_TIME}s"

        assert len(errors) == 0, f"Concurrent operation errors: {errors}"
        assert len(read_results) > 5, "Too few successful reads during concurrent operations"
        assert len(write_results) > 5, "Too few successful writes during concurrent operations"

        print("✅ Concurrent read/write performance test passed")

    def test_memory_leak_detection(self, tmp_path):
        """Test for memory leaks during repeated operations."""

        monitor = PerformanceMonitor()
        monitor.start()

        # Setup small dataset for repeated operations
        symbols = ["LEAK_TEST"]
        trading_days = [date(2024, 1, 15)]

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True)
        raw_engine = ParquetStorageEngine(raw_dir)

        # Track memory usage across iterations
        memory_readings = []

        # Perform repeated write/read cycles
        for iteration in range(20):
            # Generate new data each iteration
            symbol_data = generate_realistic_trading_data(symbols, trading_days, bars_per_day=100)

            # Write data
            job_id = f"leak-test-{iteration}"
            raw_engine.write(
                df=symbol_data[symbols[0]],
                frame="1m",
                symbol=symbols[0],
                trading_day=trading_days[0],
                job_id=job_id,
                overwrite=True,
            )

            # Read data back
            job_data = raw_engine.load_job_bars(job_id)
            assert symbols[0] in job_data

            # Force garbage collection and measure memory
            gc.collect()
            current_memory = monitor.process.memory_info().rss / 1024 / 1024
            memory_readings.append(current_memory)

            # Small delay to allow any background cleanup
            time.sleep(0.1)

        monitor.stop()

        # Analyze memory trend
        initial_memory = memory_readings[0]
        final_memory = memory_readings[-1]
        peak_memory = max(memory_readings)

        # Check for significant memory growth (potential leak)
        memory_growth = final_memory - initial_memory
        memory_growth_percent = (memory_growth / initial_memory) * 100

        print("📊 Memory Leak Detection Results:")
        print(f"   Initial Memory: {initial_memory:.1f} MB")
        print(f"   Final Memory: {final_memory:.1f} MB")
        print(f"   Peak Memory: {peak_memory:.1f} MB")
        print(f"   Memory Growth: {memory_growth:.1f} MB ({memory_growth_percent:.1f}%)")
        print(f"   Iterations: {len(memory_readings)}")

        # Memory leak assertions (allow some growth but not excessive)
        MAX_MEMORY_GROWTH_PERCENT = 50  # 50% growth allowance

        assert (
            memory_growth_percent < MAX_MEMORY_GROWTH_PERCENT
        ), f"Potential memory leak detected: {memory_growth_percent:.1f}% growth > {MAX_MEMORY_GROWTH_PERCENT}%"

        print("✅ No significant memory leaks detected")

    def test_large_file_handling_performance(self, tmp_path):
        """Test performance with large individual files."""

        MAX_EXECUTION_TIME = 30  # seconds
        MAX_MEMORY_USAGE = 800  # MB

        monitor = PerformanceMonitor()
        monitor.start()

        # Create large dataset (simulate very active trading day)
        symbols = ["FAKE1"]
        trading_days = [date(2024, 1, 15)]
        bars_per_day = 2000  # Very active day with extended hours

        print(f"🔄 Testing large file performance with {bars_per_day} bars per symbol")

        symbol_data = generate_realistic_trading_data(symbols, trading_days, bars_per_day)

        raw_dir = tmp_path / "raw"
        agg_dir = tmp_path / "agg"
        raw_dir.mkdir(parents=True)
        agg_dir.mkdir(parents=True)

        raw_engine = ParquetStorageEngine(raw_dir)

        # Write large file
        job_id = "large-file-perf-test"
        large_df = symbol_data[symbols[0]]

        write_start = time.monotonic()
        output_path = raw_engine.write(
            df=large_df,
            frame="1m",
            symbol=symbols[0],
            trading_day=trading_days[0],
            job_id=job_id,
            overwrite=True,
        )
        write_duration = time.monotonic() - write_start

        file_size_mb = output_path.stat().st_size / 1024 / 1024
        print(f"✓ Wrote {len(large_df):,} bars ({file_size_mb:.1f} MB) in {write_duration:.1f}s")

        # Read large file
        read_start = time.monotonic()
        loaded_data = raw_engine.load_job_bars(job_id)
        read_duration = time.monotonic() - read_start

        print(f"✓ Read {len(loaded_data[symbols[0]]):,} bars in {read_duration:.1f}s")

        # Test aggregation on large dataset
        duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
        domain_service = AggregationDomainService()
        aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)

        agg_start = time.monotonic()
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol(symbols[0]),
            trading_date=trading_days[0],
            bars_processed=len(large_df),
            success=True,
        )

        aggregation_service.handle_ingestion_completed(event)
        agg_duration = time.monotonic() - agg_start

        print(f"✓ Aggregated {len(large_df):,} bars in {agg_duration:.1f}s")

        metrics = monitor.stop()

        print("📊 Large File Performance Metrics:")
        print(f"   Total Duration: {metrics['duration_seconds']:.1f}s")
        print(f"   Peak Memory: {metrics['peak_memory_mb']:.1f} MB")
        print(f"   File Size: {file_size_mb:.1f} MB")
        print(f"   Write Throughput: {len(large_df) / write_duration:.0f} bars/sec")
        print(f"   Read Throughput: {len(large_df) / read_duration:.0f} bars/sec")

        # Performance assertions
        assert (
            metrics["duration_seconds"] < MAX_EXECUTION_TIME
        ), f"Large file performance regression: {metrics['duration_seconds']:.1f}s > {MAX_EXECUTION_TIME}s"

        assert (
            metrics["peak_memory_mb"] < MAX_MEMORY_USAGE
        ), f"Large file memory regression: {metrics['peak_memory_mb']:.1f}MB > {MAX_MEMORY_USAGE}MB"

        print("✅ Large file handling performance test passed")


@pytest.mark.integration
def test_performance_baseline_establishment(tmp_path):
    """Establish performance baselines for future regression testing."""

    # This test documents expected performance characteristics
    # and can be used to detect regressions in CI/CD

    monitor = PerformanceMonitor()
    monitor.start()

    # Standard test scenario
    symbols = ["BASELINE"]
    trading_days = generate_trading_calendar(date(2024, 1, 15), date(2024, 1, 19))  # 5 days

    symbol_data = generate_realistic_trading_data(symbols, trading_days)

    raw_dir = tmp_path / "raw"
    agg_dir = tmp_path / "agg"
    raw_dir.mkdir(parents=True)
    agg_dir.mkdir(parents=True)

    raw_engine = ParquetStorageEngine(raw_dir)

    # Execute standard workflow
    job_id = "baseline-test"
    total_bars = 0

    for trading_day in trading_days:
        day_data = symbol_data[symbols[0]]
        day_start_ns = int(
            datetime.combine(trading_day, datetime.min.time())
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1e9
        )
        day_end_ns = day_start_ns + int(24 * 60 * 60 * 1e9)

        day_df = day_data[(day_data["ts_ns"] >= day_start_ns) & (day_data["ts_ns"] < day_end_ns)]
        if not day_df.empty:
            raw_engine.write(
                df=day_df,
                frame="1m",
                symbol=symbols[0],
                trading_day=trading_day,
                job_id=job_id,
                overwrite=True,
            )
            total_bars += len(day_df)

    # Aggregation
    duckdb_engine = DuckDBAggregationEngine(raw_root=raw_dir, agg_root=agg_dir)
    domain_service = AggregationDomainService()
    aggregation_service = AggregationRunnerService(engine=duckdb_engine, domain=domain_service)

    event = IngestionJobCompleted(
        job_id=job_id,
        symbol=Symbol(symbols[0]),
        trading_date=trading_days[0],
        bars_processed=total_bars,
        success=True,
    )

    aggregation_service.handle_ingestion_completed(event)

    metrics = monitor.stop()

    # Document baseline performance
    baseline_metrics = {
        "bars_processed": total_bars,
        "duration_seconds": metrics["duration_seconds"],
        "peak_memory_mb": metrics["peak_memory_mb"],
        "throughput_bars_per_sec": total_bars / metrics["duration_seconds"],
        "memory_per_1k_bars_mb": metrics["peak_memory_mb"] / (total_bars / 1000),
    }

    print("📊 Performance Baseline Metrics:")
    for key, value in baseline_metrics.items():
        print(f"   {key}: {value:.2f}")

    # Basic sanity checks (not strict performance requirements)
    assert baseline_metrics["throughput_bars_per_sec"] > 100, "Baseline throughput too low"
    assert (
        baseline_metrics["memory_per_1k_bars_mb"] < 200
    ), "Baseline memory usage too high"  # Relaxed for test environment

    print("✅ Performance baseline established")
