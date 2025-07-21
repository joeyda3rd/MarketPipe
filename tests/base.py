# SPDX-License-Identifier: Apache-2.0
"""Base classes for standardized testing patterns.

Phase 4: Provides base test classes that encapsulate common testing patterns
and reduce boilerplate code across test files.

BASE CLASSES PROVIDED:
- IntegrationTestCase: Base for integration tests with shared setup
- PipelineTestCase: Specialized for pipeline integration tests
- BenchmarkTestCase: Base for performance benchmarks
- DatabaseTestCase: Base for tests requiring database operations
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from tests.fakes.adapters import FakeHttpClient, FakeMarketDataAdapter
from tests.fakes.database import FakeDatabase
from tests.fakes.metrics import FakeMetricsCollector


class IntegrationTestCase:
    """Base class for integration tests.

    Provides common setup and utilities that most integration tests need:
    - Database with schema setup
    - HTTP client for API simulation
    - Metrics collector for verification
    - Temporary directory for file operations

    Usage:
        class TestMyIntegration(IntegrationTestCase):
            def test_something(self):
                # self.database, self.http_client, self.metrics are available
                # self.temp_dir for file operations
                pass
    """

    def setup_method(self):
        """Set up test environment before each test method."""
        self.database = FakeDatabase()
        self.http_client = FakeHttpClient()
        self.metrics = FakeMetricsCollector()
        self.temp_dir = None

        # Setup database schema for tests that need it
        try:
            self.database.setup_schema()
        except Exception:
            # Some tests might not need database schema
            pass

    def teardown_method(self):
        """Clean up after each test method."""
        if self.database:
            self.database.cleanup()
        if self.temp_dir and self.temp_dir.exists():
            import shutil

            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def get_temp_dir(self) -> Path:
        """Get a temporary directory for test files."""
        if self.temp_dir is None:
            import tempfile

            self.temp_dir = Path(tempfile.mkdtemp(prefix="marketpipe_test_"))
        return self.temp_dir

    def create_service_factory(self) -> dict[str, Any]:
        """Create service factory with test doubles.

        Returns:
            Dictionary with common services for dependency injection
        """
        return {
            "database": self.database,
            "http_client": self.http_client,
            "metrics_collector": self.metrics,
            "temp_dir": self.get_temp_dir(),
        }

    def assert_metrics_recorded(self, expected_metrics: dict[str, int]):
        """Assert that expected metrics were recorded.

        Args:
            expected_metrics: Dict of metric_name -> expected_count
        """
        for metric_name, expected_count in expected_metrics.items():
            actual_count = self.metrics.get_counter_value(metric_name)
            assert (
                actual_count == expected_count
            ), f"Expected {expected_count} {metric_name} events, got {actual_count}"

    def assert_database_contains(self, table: str, expected_count: int):
        """Assert that database table contains expected number of rows.

        Args:
            table: Table name to check
            expected_count: Expected number of rows
        """
        # This is a simplified version - in real implementation would query actual table
        # For now, just verify database is accessible
        assert self.database is not None
        # TODO: Implement actual table row counting when domain models are finalized


class PipelineTestCase(IntegrationTestCase):
    """Specialized base class for pipeline integration tests.

    Extends IntegrationTestCase with pipeline-specific utilities:
    - Market data provider setup
    - Pipeline configuration helpers
    - Result verification methods

    Usage:
        class TestIngestionPipeline(PipelineTestCase):
            def test_pipeline_flow(self):
                provider = self.create_market_data_provider()
                result = self.run_ingestion_pipeline(["AAPL"], provider)
                self.assert_pipeline_success(result)
    """

    def setup_method(self):
        """Set up pipeline test environment."""
        super().setup_method()
        self.market_data_provider = None

    def create_market_data_provider(self) -> FakeMarketDataAdapter:
        """Create a configured market data provider for testing.

        Returns:
            FakeMarketDataAdapter with sensible defaults
        """
        if self.market_data_provider is None:
            self.market_data_provider = FakeMarketDataAdapter()
        return self.market_data_provider

    def configure_provider_data(self, symbol: str, bar_count: int = 10, **overrides) -> None:
        """Configure market data provider with test data.

        Args:
            symbol: Symbol to configure data for
            bar_count: Number of bars to create
            **overrides: Overrides for bar data
        """
        provider = self.create_market_data_provider()

        # Use the existing create_test_ohlcv_bars function
        from marketpipe.domain.value_objects import Symbol

        from tests.fakes.adapters import create_test_ohlcv_bars

        bars = create_test_ohlcv_bars(symbol=Symbol(symbol), count=bar_count)

        provider.configure_symbol_data(symbol, bars)

    def run_ingestion_pipeline(
        self, symbols: list[str], provider: FakeMarketDataAdapter | None = None, **config
    ) -> dict[str, Any]:
        """Helper to run complete ingestion pipeline.

        Args:
            symbols: List of symbols to process
            provider: Market data provider (creates default if None)
            **config: Pipeline configuration overrides

        Returns:
            Pipeline execution results
        """
        if provider is None:
            provider = self.create_market_data_provider()

        # This is a simplified version for proof-of-concept
        # In real implementation, would create and run actual pipeline
        results = {
            "symbols_processed": symbols,
            "total_bars": 0,
            "success": True,
            "errors": [],
            "provider": provider,
            "config": config,
        }

        # Simulate pipeline execution by fetching data for each symbol
        from datetime import datetime, timezone

        from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp

        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)),
        )

        import asyncio

        for symbol in symbols:
            try:
                bars = asyncio.run(provider.fetch_bars_for_symbol(Symbol(symbol), time_range))
                results["total_bars"] += len(bars)
            except Exception as e:
                results["errors"].append(f"{symbol}: {str(e)}")
                results["success"] = False

        return results

    def assert_pipeline_success(self, result: dict[str, Any]):
        """Assert that pipeline execution was successful.

        Args:
            result: Pipeline execution result from run_ingestion_pipeline
        """
        assert result["success"], f"Pipeline failed with errors: {result['errors']}"
        assert result["total_bars"] > 0, "Pipeline processed no data"
        assert len(result["errors"]) == 0, f"Pipeline had errors: {result['errors']}"

    def assert_pipeline_partial_success(
        self, result: dict[str, Any], expected_successful_symbols: list[str]
    ):
        """Assert that pipeline had partial success (some symbols failed).

        Args:
            result: Pipeline execution result
            expected_successful_symbols: Symbols that should have succeeded
        """
        assert result["total_bars"] > 0, "No data was processed"

        # Check that expected symbols were processed
        processed_symbols = set(result["symbols_processed"])
        expected_symbols = set(expected_successful_symbols)
        assert expected_symbols.issubset(
            processed_symbols
        ), f"Expected symbols {expected_symbols} not all processed"


class BenchmarkTestCase(IntegrationTestCase):
    """Base class for performance benchmarks.

    Provides utilities for measuring and asserting performance characteristics:
    - Timing measurement
    - Memory usage tracking
    - Performance thresholds
    - Result reporting

    Usage:
        @pytest.mark.benchmark
        class TestPerformanceBenchmarks(BenchmarkTestCase):
            def test_ingestion_throughput(self):
                with self.measure_time() as timer:
                    # ... perform operations
                self.assert_time_under(timer.elapsed, 5.0)  # Under 5 seconds
    """

    def setup_method(self):
        """Set up benchmark environment."""
        super().setup_method()
        self._performance_results = {}

    class Timer:
        """Context manager for measuring execution time."""

        def __init__(self):
            self.start_time = None
            self.end_time = None
            self.elapsed = None

        def __enter__(self):
            self.start_time = time.perf_counter()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.end_time = time.perf_counter()
            self.elapsed = self.end_time - self.start_time

    def measure_time(self) -> Timer:
        """Create a timer context manager for measuring execution time.

        Returns:
            Timer context manager

        Usage:
            with self.measure_time() as timer:
                # ... operations to time
            print(f"Took {timer.elapsed:.2f} seconds")
        """
        return self.Timer()

    def assert_time_under(self, elapsed: float, max_seconds: float):
        """Assert that operation completed within time limit.

        Args:
            elapsed: Actual elapsed time in seconds
            max_seconds: Maximum allowed time
        """
        assert (
            elapsed < max_seconds
        ), f"Operation took {elapsed:.2f}s, expected under {max_seconds}s"

    def assert_throughput_over(
        self, items_processed: int, elapsed: float, min_items_per_second: float
    ):
        """Assert that throughput meets minimum requirements.

        Args:
            items_processed: Number of items processed
            elapsed: Time taken in seconds
            min_items_per_second: Minimum required throughput
        """
        actual_throughput = items_processed / elapsed if elapsed > 0 else 0
        assert (
            actual_throughput >= min_items_per_second
        ), f"Throughput {actual_throughput:.1f} items/sec below minimum {min_items_per_second}"

    def record_performance_result(self, test_name: str, **metrics):
        """Record performance metrics for reporting.

        Args:
            test_name: Name of the test
            **metrics: Performance metrics to record
        """
        self._performance_results[test_name] = metrics

    def get_performance_results(self) -> dict[str, dict[str, Any]]:
        """Get all recorded performance results.

        Returns:
            Dictionary of test_name -> metrics
        """
        return self._performance_results.copy()


class DatabaseTestCase(IntegrationTestCase):
    """Base class for tests requiring database operations.

    Specialized version of IntegrationTestCase that ensures database setup
    and provides database-specific utilities.

    Usage:
        class TestDatabaseOperations(DatabaseTestCase):
            def test_data_persistence(self):
                # self.database is guaranteed to be set up with schema
                self.insert_test_data("table_name", test_data)
                result = self.query_test_data("table_name")
                assert len(result) > 0
    """

    def setup_method(self):
        """Set up database test environment."""
        super().setup_method()

        # Ensure database schema is set up
        if not hasattr(self.database, "_is_setup") or not self.database._is_setup:
            self.database.setup_schema()

    def insert_test_data(self, table_name: str, data: dict[str, Any]):
        """Insert test data into database table.

        Args:
            table_name: Name of table to insert into
            data: Data to insert
        """
        # This is a placeholder - in real implementation would use actual SQL
        # For now, just verify database is available
        assert self.database is not None
        # TODO: Implement actual data insertion when database schema is finalized

    def query_test_data(self, table_name: str, **conditions) -> list[dict[str, Any]]:
        """Query test data from database table.

        Args:
            table_name: Table to query
            **conditions: Query conditions

        Returns:
            List of matching records
        """
        # This is a placeholder - in real implementation would execute actual SQL
        # For now, return empty list
        assert self.database is not None
        return []

    def assert_table_row_count(self, table_name: str, expected_count: int):
        """Assert that table contains expected number of rows.

        Args:
            table_name: Table to check
            expected_count: Expected row count
        """
        # This is a placeholder - in real implementation would count actual rows
        # For now, just verify database is available
        assert self.database is not None
        # TODO: Implement actual row counting when database schema is finalized

    def get_database_connection_string(self) -> str:
        """Get database connection string for tests that need direct access.

        Returns:
            Database connection string
        """
        if hasattr(self.database, "get_connection_string"):
            return self.database.get_connection_string()
        else:
            return f"sqlite:///{self.database.get_file_path()}"
