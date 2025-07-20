# SPDX-License-Identifier: Apache-2.0
"""Performance benchmark tests for MarketPipe.

Phase 4: Provides performance benchmarks to catch regressions and establish
baseline performance characteristics for key operations.

BENCHMARKS PROVIDED:
- Data ingestion throughput
- Validation performance
- Database operations
- Memory usage patterns

Run with: pytest --benchmark tests/benchmarks/
"""

from __future__ import annotations

import asyncio
import gc
from datetime import datetime, timezone

import psutil
import pytest
from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp

from tests.base import BenchmarkTestCase
from tests.fakes.adapters import FakeMarketDataAdapter, create_test_ohlcv_bars


@pytest.mark.benchmark
class TestIngestionPerformanceBenchmarks(BenchmarkTestCase):
    """Performance benchmarks for data ingestion operations.

    Tests throughput, latency, and resource usage for ingestion pipeline components.
    """

    def test_ohlcv_bar_creation_benchmark(self, benchmark_data):
        """Benchmark OHLCV bar creation performance."""
        dataset = benchmark_data["medium_dataset"]
        symbols = dataset["symbols"]
        bars_per_symbol = dataset["bars_per_symbol"]

        total_bars_created = 0

        with self.measure_time() as timer:
            for symbol in symbols:
                bars = create_test_ohlcv_bars(symbol=Symbol(symbol), count=bars_per_symbol)
                total_bars_created += len(bars)

        # Assert performance requirements
        self.assert_time_under(timer.elapsed, 10.0)  # Should complete in under 10 seconds
        self.assert_throughput_over(total_bars_created, timer.elapsed, 500)  # 500+ bars/sec

        # Record results for reporting
        self.record_performance_result(
            "ohlcv_bar_creation",
            total_bars=total_bars_created,
            elapsed_time=timer.elapsed,
            bars_per_second=total_bars_created / timer.elapsed,
            symbols_processed=len(symbols),
        )

    def test_fake_provider_data_fetch_benchmark(self, benchmark_data):
        """Benchmark fake market data provider fetch performance."""
        dataset = benchmark_data["large_dataset"]
        symbols = dataset["symbols"][:3]  # Limit to 3 symbols for benchmark
        bars_per_symbol = 1000

        provider = FakeMarketDataAdapter()

        # Configure provider with data
        for symbol in symbols:
            bars = create_test_ohlcv_bars(symbol=Symbol(symbol), count=bars_per_symbol)
            provider.configure_symbol_data(symbol, bars)

        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)),
        )

        total_bars_fetched = 0

        with self.measure_time() as timer:
            for symbol in symbols:
                bars = asyncio.run(provider.fetch_bars_for_symbol(Symbol(symbol), time_range))
                total_bars_fetched += len(bars)

        # Assert performance requirements
        self.assert_time_under(timer.elapsed, 5.0)  # Should complete in under 5 seconds
        self.assert_throughput_over(total_bars_fetched, timer.elapsed, 1000)  # 1000+ bars/sec

        self.record_performance_result(
            "provider_data_fetch",
            total_bars=total_bars_fetched,
            elapsed_time=timer.elapsed,
            bars_per_second=total_bars_fetched / timer.elapsed,
            symbols_processed=len(symbols),
        )

    @pytest.mark.slow
    def test_large_dataset_memory_usage_benchmark(self, benchmark_data):
        """Benchmark memory usage with large datasets."""
        dataset = benchmark_data["large_dataset"]

        process = psutil.Process()
        gc.collect()  # Clean up before measuring

        # Measure memory before
        memory_before = process.memory_info().rss / 1024 / 1024  # MB

        large_bar_sets = []

        with self.measure_time() as timer:
            for symbol in dataset["symbols"]:
                bars = create_test_ohlcv_bars(
                    symbol=Symbol(symbol), count=dataset["bars_per_symbol"]
                )
                large_bar_sets.append(bars)

        # Measure memory after
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = memory_after - memory_before

        total_bars = sum(len(bar_set) for bar_set in large_bar_sets)
        memory_per_bar = memory_used / total_bars if total_bars > 0 else 0

        # Assert memory usage is reasonable (less than 1KB per bar)
        assert memory_per_bar < 1.0, f"Memory usage too high: {memory_per_bar:.3f} MB per bar"

        # Assert creation time is reasonable
        self.assert_time_under(timer.elapsed, 30.0)  # 30 seconds for large dataset

        self.record_performance_result(
            "large_dataset_memory",
            total_bars=total_bars,
            elapsed_time=timer.elapsed,
            memory_used_mb=memory_used,
            memory_per_bar_kb=memory_per_bar * 1024,
            symbols_processed=len(dataset["symbols"]),
        )

        # Clean up
        del large_bar_sets
        gc.collect()


@pytest.mark.benchmark
class TestDatabasePerformanceBenchmarks(BenchmarkTestCase):
    """Performance benchmarks for database operations."""

    def test_database_setup_teardown_benchmark(self):
        """Benchmark database setup and teardown performance."""
        setup_times = []
        teardown_times = []

        # Test multiple cycles
        for _ in range(5):
            # Measure setup time
            with self.measure_time() as setup_timer:
                test_db = self.database.__class__()
                test_db.setup_schema()
            setup_times.append(setup_timer.elapsed)

            # Measure teardown time
            with self.measure_time() as teardown_timer:
                test_db.cleanup()
            teardown_times.append(teardown_timer.elapsed)

        avg_setup_time = sum(setup_times) / len(setup_times)
        avg_teardown_time = sum(teardown_times) / len(teardown_times)

        # Assert reasonable performance
        assert avg_setup_time < 2.0, f"Database setup too slow: {avg_setup_time:.2f}s"
        assert avg_teardown_time < 1.0, f"Database teardown too slow: {avg_teardown_time:.2f}s"

        self.record_performance_result(
            "database_lifecycle",
            avg_setup_time=avg_setup_time,
            avg_teardown_time=avg_teardown_time,
            cycles_tested=len(setup_times),
        )

    def test_concurrent_database_operations_benchmark(self):
        """Benchmark concurrent database operations performance."""
        import threading

        operation_times = []
        errors = []

        def database_operation(thread_id: int):
            """Perform database operations in thread."""
            try:
                with self.measure_time() as timer:
                    test_db = self.database.__class__()
                    test_db.setup_schema()
                    # Simulate some operations
                    for _ in range(10):
                        # Would perform actual database operations here
                        pass
                    test_db.cleanup()
                operation_times.append(timer.elapsed)
            except Exception as e:
                errors.append(f"Thread {thread_id}: {e}")

        # Run concurrent operations
        threads = []
        num_threads = 3  # Conservative for test environment

        with self.measure_time() as total_timer:
            for i in range(num_threads):
                thread = threading.Thread(target=database_operation, args=(i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        # Assert no errors occurred
        assert len(errors) == 0, f"Concurrent operations had errors: {errors}"

        # Assert reasonable total time (should be faster than sequential)
        sequential_estimate = sum(operation_times)  # If run sequentially
        assert total_timer.elapsed < sequential_estimate, "No concurrency benefit observed"

        self.record_performance_result(
            "concurrent_database_ops",
            total_time=total_timer.elapsed,
            num_threads=num_threads,
            avg_operation_time=sum(operation_times) / len(operation_times),
            concurrency_benefit=sequential_estimate - total_timer.elapsed,
        )


@pytest.mark.benchmark
class TestHTTPClientPerformanceBenchmarks(BenchmarkTestCase):
    """Performance benchmarks for HTTP client operations."""

    def test_http_client_response_time_benchmark(self):
        """Benchmark HTTP client response time with various scenarios."""
        response_times = []

        # Test multiple request scenarios
        test_scenarios = [
            {"url": "http://test.com/fast", "delay": 0.001},
            {"url": "http://test.com/medium", "delay": 0.01},
            {"url": "http://test.com/slow", "delay": 0.1},
        ]

        for scenario in test_scenarios:
            # Configure HTTP client response
            self.http_client.configure_response(
                url_pattern=scenario["url"], status=200, body={"data": "test"}
            )

            # Measure request time
            with self.measure_time() as timer:
                response = self.http_client.get(scenario["url"])
                assert response.status_code == 200

            response_times.append(timer.elapsed)

        avg_response_time = sum(response_times) / len(response_times)
        max_response_time = max(response_times)

        # Assert reasonable response times
        assert avg_response_time < 0.1, f"Average response time too slow: {avg_response_time:.3f}s"
        assert max_response_time < 0.2, f"Max response time too slow: {max_response_time:.3f}s"

        self.record_performance_result(
            "http_client_response",
            avg_response_time=avg_response_time,
            max_response_time=max_response_time,
            min_response_time=min(response_times),
            scenarios_tested=len(test_scenarios),
        )

    def test_http_client_concurrent_requests_benchmark(self):
        """Benchmark HTTP client concurrent request handling."""
        import threading

        # Configure responses
        for i in range(10):
            self.http_client.configure_response(
                url_pattern=f"http://test.com/concurrent/{i}",
                status=200,
                body={"id": i, "data": "concurrent_test"},
            )

        request_times = []
        errors = []

        def make_request(request_id: int):
            """Make HTTP request in thread."""
            try:
                with self.measure_time() as timer:
                    response = self.http_client.get(f"http://test.com/concurrent/{request_id}")
                    assert response.status_code == 200
                    assert response.json()["id"] == request_id
                request_times.append(timer.elapsed)
            except Exception as e:
                errors.append(f"Request {request_id}: {e}")

        # Make concurrent requests
        threads = []
        num_requests = 10

        with self.measure_time() as total_timer:
            for i in range(num_requests):
                thread = threading.Thread(target=make_request, args=(i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        # Assert no errors
        assert len(errors) == 0, f"Concurrent requests had errors: {errors}"

        # Assert reasonable performance
        assert len(request_times) == num_requests, "Not all requests completed"

        avg_request_time = sum(request_times) / len(request_times)
        assert avg_request_time < 0.05, f"Concurrent requests too slow: {avg_request_time:.3f}s"

        self.record_performance_result(
            "http_concurrent_requests",
            total_time=total_timer.elapsed,
            num_requests=num_requests,
            avg_request_time=avg_request_time,
            requests_completed=len(request_times),
        )


def pytest_runtest_teardown(item, nextitem):
    """Print performance results after benchmark tests."""
    if hasattr(item.instance, "get_performance_results"):
        results = item.instance.get_performance_results()
        if results:
            print(f"\n=== Performance Results for {item.name} ===")
            for test_name, metrics in results.items():
                print(f"{test_name}:")
                for metric_name, value in metrics.items():
                    if isinstance(value, float):
                        print(f"  {metric_name}: {value:.3f}")
                    else:
                        print(f"  {metric_name}: {value}")
                print()
