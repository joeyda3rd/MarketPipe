# SPDX-License-Identifier: Apache-2.0
"""Example tests demonstrating Phase 4 shared test infrastructure.

This file shows how to use the new shared fixtures, base classes, and utilities
to create clean, maintainable tests with minimal boilerplate.

DEMONSTRATES:
- Using integration_environment fixture
- Using domain_objects factory
- Using base classes (IntegrationTestCase, PipelineTestCase)
- Performance benchmarking patterns
- Best practices for test organization
"""

from __future__ import annotations

import pytest
from marketpipe.domain.value_objects import Symbol

from tests.base import BenchmarkTestCase, IntegrationTestCase, PipelineTestCase


class TestUsingIntegrationEnvironmentFixture:
    """Examples of using the integration_environment fixture."""

    def test_basic_fixture_usage(self, integration_environment):
        """Basic example of using integration environment fixture."""
        env = integration_environment

        # Database operations
        assert env.database is not None

        # HTTP client operations
        env.http_client.configure_response(
            url_pattern="http://example.com/test", status=200, body={"message": "success"}
        )

        response = env.http_client.get("http://example.com/test")
        assert response.status_code == 200
        assert response.json()["message"] == "success"

        # Metrics verification
        env.metrics.increment_counter("test_operations")
        assert env.metrics.get_counter_value("test_operations") == 1

        # Temporary directory
        temp_dir = env.get_temp_dir()
        assert temp_dir.exists()

        # Test file creation
        test_file = temp_dir / "test.txt"
        test_file.write_text("hello world")
        assert test_file.read_text() == "hello world"

    def test_backward_compatibility_dict_access(self, integration_environment):
        """Show backward compatibility with dict-style access."""
        env_dict = integration_environment.as_dict()

        assert "database" in env_dict
        assert "http_client" in env_dict
        assert "metrics" in env_dict
        assert "temp_dir" in env_dict

        # Can use like the old style
        database = env_dict["database"]
        http_client = env_dict["http_client"]

        assert database is not None
        assert http_client is not None


class TestUsingDomainObjectsFactory:
    """Examples of using the domain_objects factory."""

    def test_create_single_ohlcv_bar(self, domain_objects):
        """Example of creating a single OHLCV bar with defaults."""
        bar = domain_objects.create_ohlcv_bar()

        assert str(bar.symbol) == "AAPL"  # Default symbol
        assert bar.open_price.value > 0
        assert bar.high_price.value >= bar.open_price.value
        assert bar.low_price.value <= bar.open_price.value
        assert bar.volume.value > 0

    def test_create_ohlcv_bar_with_overrides(self, domain_objects):
        """Example of creating OHLCV bar with custom values."""
        bar = domain_objects.create_ohlcv_bar(
            symbol="GOOGL",
            open_price=150.0,
            high_price=151.0,  # Must be >= open_price for OHLC consistency
            volume=2500,
        )

        assert str(bar.symbol) == "GOOGL"
        assert bar.open_price.value == 150.0
        assert bar.high_price.value == 151.0
        assert bar.volume.value == 2500

    def test_create_multiple_bars(self, domain_objects):
        """Example of creating multiple bars with sequential timestamps."""
        bars = domain_objects.create_ohlcv_bars("MSFT", count=5)

        assert len(bars) == 5
        assert all(str(bar.symbol) == "MSFT" for bar in bars)

        # Timestamps should be sequential
        for i in range(1, len(bars)):
            current_time = bars[i].timestamp.value
            previous_time = bars[i - 1].timestamp.value
            assert current_time > previous_time

    def test_create_time_range(self, domain_objects):
        """Example of creating TimeRange for tests."""
        time_range = domain_objects.create_time_range(duration_minutes=60)

        duration = time_range.end.value - time_range.start.value
        assert duration.total_seconds() == 3600  # 60 minutes

    def test_create_ingestion_job(self, domain_objects):
        """Example of creating ingestion job (placeholder)."""
        job = domain_objects.create_ingestion_job(
            symbols=["AAPL", "GOOGL"], trading_date=None  # Uses default
        )

        assert job["symbols"] == ["AAPL", "GOOGL"]
        assert "trading_date" in job
        assert "created_at" in job


class TestUsingCommonFixtures:
    """Examples of using common test data fixtures."""

    def test_using_test_symbols(self, test_symbols):
        """Example of using common test symbols."""
        assert "AAPL" in test_symbols
        assert "GOOGL" in test_symbols
        assert len(test_symbols) >= 5  # Should have several symbols

        # Can modify without affecting other tests
        test_symbols.append("CUSTOM")
        assert "CUSTOM" in test_symbols

    def test_using_test_trading_dates(self, test_trading_dates):
        """Example of using common trading dates."""
        assert len(test_trading_dates) >= 3

        # All should be valid dates
        from datetime import date

        assert all(isinstance(d, date) for d in test_trading_dates)

    def test_using_benchmark_data(self, benchmark_data):
        """Example of using benchmark data configurations."""
        small = benchmark_data["small_dataset"]
        medium = benchmark_data["medium_dataset"]
        large = benchmark_data["large_dataset"]

        assert small["total_bars"] < medium["total_bars"] < large["total_bars"]
        assert len(small["symbols"]) <= len(medium["symbols"]) <= len(large["symbols"])


class TestUsingIntegrationTestCase(IntegrationTestCase):
    """Examples of using IntegrationTestCase base class."""

    def test_automatic_setup_and_cleanup(self):
        """Base class automatically sets up database, http client, metrics."""
        # These are available automatically
        assert self.database is not None
        assert self.http_client is not None
        assert self.metrics is not None

        # Database schema should be set up
        assert hasattr(self.database, "_is_setup")

    def test_service_factory_creation(self):
        """Base class can create service factory for dependency injection."""
        services = self.create_service_factory()

        assert "database" in services
        assert "http_client" in services
        assert "metrics_collector" in services
        assert "temp_dir" in services

        # Services should be the same instances
        assert services["database"] is self.database
        assert services["http_client"] is self.http_client

    def test_metrics_assertions(self):
        """Base class provides metrics assertion helpers."""
        # Record some metrics
        self.metrics.increment_counter("operations")
        self.metrics.increment_counter("operations")
        self.metrics.increment_counter("errors")

        # Use helper to verify
        self.assert_metrics_recorded({"operations": 2, "errors": 1})

    def test_temp_directory_usage(self):
        """Base class provides temp directory management."""
        temp_dir = self.get_temp_dir()
        assert temp_dir.exists()

        # Create test files
        test_file = temp_dir / "integration_test.txt"
        test_file.write_text("test data")

        assert test_file.exists()
        assert test_file.read_text() == "test data"

        # Cleanup happens automatically in teardown


class TestUsingPipelineTestCase(PipelineTestCase):
    """Examples of using PipelineTestCase for pipeline integration tests."""

    def test_market_data_provider_creation(self):
        """Pipeline test case can create and configure providers."""
        provider = self.create_market_data_provider()
        assert provider is not None

        # Configure with test data
        self.configure_provider_data("AAPL", bar_count=5)

        # Provider should now have data configured
        import asyncio
        from datetime import datetime, timezone

        from marketpipe.domain.value_objects import TimeRange, Timestamp

        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)),
        )

        bars = asyncio.run(provider.fetch_bars_for_symbol(Symbol("AAPL"), time_range))
        assert len(bars) == 5

    def test_pipeline_execution_helper(self):
        """Pipeline test case provides pipeline execution helpers."""
        # Configure provider data
        self.configure_provider_data("AAPL", bar_count=10)
        self.configure_provider_data("GOOGL", bar_count=8)

        # Run pipeline
        result = self.run_ingestion_pipeline(["AAPL", "GOOGL"])

        # Verify results
        self.assert_pipeline_success(result)
        assert result["total_bars"] == 18  # 10 + 8
        assert "AAPL" in result["symbols_processed"]
        assert "GOOGL" in result["symbols_processed"]

    def test_pipeline_partial_success(self):
        """Pipeline test case can handle partial success scenarios."""
        # Configure some symbols to succeed, others to fail
        provider = self.create_market_data_provider()
        self.configure_provider_data("AAPL", bar_count=5)
        provider.configure_error("BADSTOCK", ValueError("Unknown symbol"))

        # Run pipeline
        result = self.run_ingestion_pipeline(["AAPL", "BADSTOCK"], provider)

        # Verify partial success
        self.assert_pipeline_partial_success(result, expected_successful_symbols=["AAPL"])
        assert not result["success"]  # Overall failed due to one error
        assert len(result["errors"]) == 1


@pytest.mark.benchmark
class TestUsingBenchmarkTestCase(BenchmarkTestCase):
    """Examples of using BenchmarkTestCase for performance testing."""

    def test_timing_measurement(self):
        """Benchmark test case provides timing utilities."""
        import time

        with self.measure_time() as timer:
            time.sleep(0.1)  # Simulate work

        # Assert timing
        self.assert_time_under(timer.elapsed, 0.2)  # Should be under 200ms
        assert timer.elapsed >= 0.1  # Should be at least 100ms

    def test_throughput_measurement(self):
        """Benchmark test case can measure throughput."""
        items_processed = 1000

        with self.measure_time() as timer:
            # Simulate processing items
            for _ in range(items_processed):
                pass  # Simulate work

        # Assert throughput (very lenient for this simple example)
        self.assert_throughput_over(items_processed, timer.elapsed, 10000)  # 10k+ items/sec

    def test_performance_result_recording(self):
        """Benchmark test case can record performance metrics."""
        with self.measure_time() as timer:
            # Simulate work
            items = list(range(100))

        # Record performance
        self.record_performance_result(
            "list_creation",
            items_created=len(items),
            elapsed_time=timer.elapsed,
            items_per_second=len(items) / timer.elapsed,
        )

        # Verify recording
        results = self.get_performance_results()
        assert "list_creation" in results
        assert results["list_creation"]["items_created"] == 100


class TestMigrationFromOldPatterns:
    """Examples showing how to migrate from old test patterns to new ones."""

    def test_old_pattern_with_manual_setup(self):
        """OLD PATTERN: Manual setup of test dependencies."""
        # OLD WAY: Lots of manual setup
        from tests.fakes.adapters import FakeHttpClient
        from tests.fakes.database import FakeDatabase
        from tests.fakes.metrics import FakeMetricsCollector

        database = FakeDatabase()
        database.setup_schema()

        http_client = FakeHttpClient()
        metrics = FakeMetricsCollector()

        try:
            # Test logic here
            http_client.configure_response("http://test.com", 200, {"ok": True})
            response = http_client.get("http://test.com")
            assert response.status_code == 200
            
            # Use metrics to avoid unused variable warning
            metrics.increment_counter("test_operations")

        finally:
            database.cleanup()

    def test_new_pattern_with_fixture(self, integration_environment):
        """NEW PATTERN: Use integration_environment fixture."""
        # NEW WAY: Automatic setup and cleanup
        env = integration_environment

        # Same test logic, much cleaner
        env.http_client.configure_response("http://test.com", 200, {"ok": True})
        response = env.http_client.get("http://test.com")
        assert response.status_code == 200

        # Cleanup happens automatically


# Example of combining old and new patterns during migration
class TestMixedApproach(IntegrationTestCase):
    """Example showing mixed old/new patterns during migration."""

    def test_can_use_both_approaches(self, domain_objects):
        """Shows you can use base class AND fixtures together."""
        # From base class
        assert self.database is not None

        # From fixture
        bar = domain_objects.create_ohlcv_bar("TSLA")
        assert str(bar.symbol) == "TSLA"

        # Combined usage
        self.metrics.increment_counter("bars_created")
        self.assert_metrics_recorded({"bars_created": 1})
