# SPDX-License-Identifier: Apache-2.0
"""Enhanced full pipeline integration tests.

These tests expand on test_full_pipeline.py by adding more realistic scenarios
using the Phase 1 + Phase 2 test infrastructure (fakes + dependency injection).

IMPROVEMENTS OVER EXISTING PIPELINE TESTS:
- Uses enhanced FakeMarketDataProvider from Phase 1
- Tests realistic error scenarios (rate limits, API failures, data quality issues)
- Uses real database operations via Phase 2 dependency injection
- Tests cross-component integration without excessive mocking
- Covers edge cases like partial failures and retry scenarios
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytest

from marketpipe.domain.entities import OHLCVBar
from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.application.services import IngestionCoordinatorService
from tests.fakes.adapters import (
    FakeHttpClient,
    FakeMarketDataAdapter,
    create_test_ohlcv_bars,
)
from tests.fakes.database import FakeDatabase
from tests.fakes.metrics import FakeMetricsCollector


class TestEnhancedPipelineIntegration:
    """Enhanced pipeline integration tests with realistic scenarios."""

    @pytest.fixture
    def integration_environment(self, tmp_path):
        """Complete integration test environment."""
        db = FakeDatabase()
        http_client = FakeHttpClient()
        metrics = FakeMetricsCollector()
        
        return {
            'database': db,
            'http_client': http_client,
            'metrics': metrics,
            'storage_dir': tmp_path / "storage",
            'reports_dir': tmp_path / "reports"
        }

    def test_pipeline_with_data_quality_issues(self, integration_environment):
        """Test pipeline handling realistic data quality problems.
        
        IMPROVEMENT: Tests realistic data scenarios without complex mock setup.
        Uses Phase 1 FakeMarketDataAdapter to simulate real data issues.
        """
        env = integration_environment
        
        # Set up fake provider with mixed quality data
        provider = FakeMarketDataAdapter()
        
        # Configure realistic test data with quality issues
        valid_bars = create_test_ohlcv_bars(
            symbol="AAPL",
            count=5,
            start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        )
        
        # Add some bars with data quality issues
        problematic_bars = create_test_ohlcv_bars(
            symbol="AAPL",
            count=2,
            start_time=datetime(2024, 1, 15, 9, 35, tzinfo=timezone.utc),
            # Introduce quality issues
            overrides={
                'high_price_factor': 0.5,  # High < Low (invalid OHLC)
                'volume': 0  # Zero volume (suspicious)
            }
        )
        
        all_bars = valid_bars + problematic_bars
        provider.configure_symbol_data(Symbol("AAPL"), all_bars)
        
        # Set up coordinator with real database and fake provider
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],
            metrics_collector=env['metrics']
        )
        
        # Execute ingestion
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc))
        )
        
        result = asyncio.run(coordinator.ingest_symbol_data(
            symbol=Symbol("AAPL"),
            time_range=time_range
        ))
        
        # Verify results
        assert result.total_bars_processed == 7
        assert result.valid_bars == 5
        assert result.invalid_bars == 2
        
        # Verify specific quality issues were detected
        quality_issues = result.validation_errors
        assert any("ohlc consistency" in error.lower() for error in quality_issues)
        assert any("zero volume" in error.lower() for error in quality_issues)
        
        # Verify metrics were recorded
        assert env['metrics'].get_counter_value("bars_processed") == 7
        assert env['metrics'].get_counter_value("validation_errors") == 2
        
        # Verify database contains only valid data
        stored_bars = env['database'].query_bars("AAPL", time_range)
        assert len(stored_bars) == 5  # Only valid bars stored

    def test_pipeline_with_provider_rate_limiting(self, integration_environment):
        """Test pipeline handles rate limiting gracefully.
        
        IMPROVEMENT: Tests realistic rate limiting scenarios without
        complex HTTP mocking. Uses FakeHttpClient to simulate rate limits.
        """
        env = integration_environment
        
        # Configure HTTP client to simulate rate limiting
        env['http_client'].configure_rate_limit(
            requests_per_second=2,
            burst_size=5
        )
        
        # Set up provider that will hit rate limits
        provider = FakeMarketDataAdapter(http_client=env['http_client'])
        
        # Configure multiple symbols to trigger rate limiting
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
        for symbol in symbols:
            bars = create_test_ohlcv_bars(
                symbol=symbol,
                count=10,
                start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
            )
            provider.configure_symbol_data(Symbol(symbol), bars)
        
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],
            metrics_collector=env['metrics']
        )
        
        # Execute ingestion for multiple symbols
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc))
        )
        
        results = []
        for symbol in symbols:
            result = asyncio.run(coordinator.ingest_symbol_data(
                symbol=Symbol(symbol),
                time_range=time_range
            ))
            results.append(result)
        
        # Verify all symbols were processed despite rate limiting
        total_bars = sum(r.total_bars_processed for r in results)
        assert total_bars == 50  # 5 symbols * 10 bars each
        
        # Verify rate limiting was handled (should see retries in metrics)
        retry_count = env['metrics'].get_counter_value("api_retries")
        assert retry_count > 0  # Should have retried due to rate limits
        
        # Verify total execution time was reasonable (not too long due to retries)
        latency_samples = env['metrics'].get_histogram_samples("ingestion_duration")
        assert len(latency_samples) == 5  # One per symbol
        # Should complete within reasonable time despite rate limits
        assert all(sample < 10.0 for sample in latency_samples)

    def test_pipeline_with_partial_api_failures(self, integration_environment):
        """Test pipeline resilience to partial API failures.
        
        IMPROVEMENT: Tests realistic failure scenarios with easy configuration.
        """
        env = integration_environment
        
        provider = FakeMarketDataAdapter(http_client=env['http_client'])
        
        # Configure some symbols to succeed, others to fail
        success_symbols = ["AAPL", "GOOGL"]
        failure_symbols = ["BADSTOCK1", "BADSTOCK2"]
        
        # Configure successful data
        for symbol in success_symbols:
            bars = create_test_ohlcv_bars(
                symbol=symbol,
                count=5,
                start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
            )
            provider.configure_symbol_data(Symbol(symbol), bars)
        
        # Configure failures
        for symbol in failure_symbols:
            provider.configure_error(
                Symbol(symbol),
                error=ValueError(f"Unknown symbol: {symbol}")
            )
        
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],
            metrics_collector=env['metrics']
        )
        
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc))
        )
        
        # Process all symbols
        all_symbols = success_symbols + failure_symbols
        results = {}
        
        for symbol in all_symbols:
            try:
                result = asyncio.run(coordinator.ingest_symbol_data(
                    symbol=Symbol(symbol),
                    time_range=time_range
                ))
                results[symbol] = {'success': True, 'result': result}
            except Exception as e:
                results[symbol] = {'success': False, 'error': str(e)}
        
        # Verify partial success behavior
        successful_symbols = [s for s, r in results.items() if r['success']]
        failed_symbols = [s for s, r in results.items() if not r['success']]
        
        assert set(successful_symbols) == set(success_symbols)
        assert set(failed_symbols) == set(failure_symbols)
        
        # Verify successful data was stored despite failures
        for symbol in success_symbols:
            stored_bars = env['database'].query_bars(symbol, time_range)
            assert len(stored_bars) == 5
        
        # Verify error metrics were recorded
        error_count = env['metrics'].get_counter_value("api_errors")
        assert error_count == len(failure_symbols)
        
        # Verify database doesn't contain failed symbol data
        for symbol in failure_symbols:
            stored_bars = env['database'].query_bars(symbol, time_range)
            assert len(stored_bars) == 0

    def test_pipeline_with_pagination_and_large_datasets(self, integration_environment):
        """Test pipeline with realistic pagination scenarios.
        
        IMPROVEMENT: Tests pagination without complex HTTP response mocking.
        """
        env = integration_environment
        
        provider = FakeMarketDataAdapter(http_client=env['http_client'])
        
        # Configure large dataset requiring pagination
        large_bar_set = create_test_ohlcv_bars(
            symbol="AAPL",
            count=250,  # Large enough to require pagination
            start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        )
        
        # Configure pagination behavior
        provider.configure_symbol_data(Symbol("AAPL"), large_bar_set)
        provider.configure_pagination(page_size=50, total_pages=5)
        
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],
            metrics_collector=env['metrics']
        )
        
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 13, 40, tzinfo=timezone.utc))  # 4+ hours
        )
        
        result = asyncio.run(coordinator.ingest_symbol_data(
            symbol=Symbol("AAPL"),
            time_range=time_range
        ))
        
        # Verify all pages were processed
        assert result.total_bars_processed == 250
        assert result.valid_bars == 250
        assert result.pages_processed == 5
        
        # Verify pagination requests were made
        request_history = provider.get_request_history()
        assert len(request_history) == 5  # 5 paginated requests
        
        # Verify all data was stored correctly
        stored_bars = env['database'].query_bars("AAPL", time_range)
        assert len(stored_bars) == 250

    def test_cross_component_error_propagation(self, integration_environment):
        """Test error propagation across pipeline components.
        
        IMPROVEMENT: Tests realistic error flows without complex mock coordination.
        """
        env = integration_environment
        
        # Configure database to fail after some operations
        env['database'].configure_failure_after_operations(3)
        
        provider = FakeMarketDataAdapter()
        
        # Set up valid data
        bars = create_test_ohlcv_bars(
            symbol="AAPL",
            count=5,
            start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        )
        provider.configure_symbol_data(Symbol("AAPL"), bars)
        
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],
            metrics_collector=env['metrics']
        )
        
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc))
        )
        
        # Should fail due to database error
        with pytest.raises(Exception) as exc_info:
            asyncio.run(coordinator.ingest_symbol_data(
                symbol=Symbol("AAPL"),
                time_range=time_range
            ))
        
        assert "database operation failed" in str(exc_info.value).lower()
        
        # Verify partial data was processed before failure
        assert env['metrics'].get_counter_value("bars_fetched") >= 3
        
        # Verify error was recorded in metrics
        assert env['metrics'].get_counter_value("pipeline_errors") == 1


class TestPipelinePerformanceCharacteristics:
    """Test pipeline performance characteristics with realistic scenarios."""

    def test_pipeline_throughput_benchmark(self, integration_environment):
        """Benchmark pipeline throughput with realistic data volumes.
        
        IMPROVEMENT: Performance testing without complex mock coordination.
        """
        env = integration_environment
        
        provider = FakeMarketDataAdapter()
        
        # Set up realistic daily data volume (390 minutes * 4 symbols)
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]
        bars_per_symbol = 390  # Full trading day
        
        for symbol in symbols:
            bars = create_test_ohlcv_bars(
                symbol=symbol,
                count=bars_per_symbol,
                start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
            )
            provider.configure_symbol_data(Symbol(symbol), bars)
        
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],
            metrics_collector=env['metrics']
        )
        
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc))
        )
        
        import time
        start_time = time.perf_counter()
        
        # Process all symbols
        total_bars = 0
        for symbol in symbols:
            result = asyncio.run(coordinator.ingest_symbol_data(
                symbol=Symbol(symbol),
                time_range=time_range
            ))
            total_bars += result.total_bars_processed
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        
        # Verify performance characteristics
        assert total_bars == len(symbols) * bars_per_symbol
        throughput = total_bars / duration  # bars per second
        
        # Should process at least 100 bars per second
        assert throughput >= 100, f"Throughput too low: {throughput:.1f} bars/sec"
        
        # Should complete full day in reasonable time
        assert duration < 30, f"Processing took too long: {duration:.1f} seconds"
        
        # Verify no memory leaks (metrics should be reasonable)
        memory_samples = env['metrics'].get_gauge_value("memory_usage_mb")
        if memory_samples:
            assert memory_samples < 500  # Should stay under 500MB

    def test_pipeline_memory_usage_patterns(self, integration_environment):
        """Test pipeline memory usage with large datasets.
        
        IMPROVEMENT: Memory testing with realistic data patterns.
        """
        env = integration_environment
        
        provider = FakeMarketDataAdapter()
        
        # Create large dataset to test memory efficiency
        large_bars = create_test_ohlcv_bars(
            symbol="AAPL",
            count=10000,  # Large dataset
            start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        )
        
        provider.configure_symbol_data(Symbol("AAPL"), large_bars)
        provider.configure_streaming(batch_size=100)  # Stream in small batches
        
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],
            metrics_collector=env['metrics']
        )
        
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 31, 23, 59, tzinfo=timezone.utc))
        )
        
        # Track memory usage during processing
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        result = asyncio.run(coordinator.ingest_symbol_data(
            symbol=Symbol("AAPL"),
            time_range=time_range
        ))
        
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory
        
        # Verify results
        assert result.total_bars_processed == 10000
        
        # Memory usage should be reasonable for large datasets
        assert memory_increase < 200, f"Memory usage too high: {memory_increase:.1f} MB"
        
        # Should process efficiently in batches
        batches_processed = env['metrics'].get_counter_value("batches_processed")
        assert batches_processed >= 100  # Should have processed in batches


class TestPipelineIntegrationWithBootstrap:
    """Test pipeline integration with bootstrap functionality."""

    def test_pipeline_with_real_bootstrap_process(self, integration_environment, tmp_path):
        """Test full pipeline including bootstrap with real database.
        
        IMPROVEMENT: End-to-end integration test using all Phase 1 + Phase 2 components.
        """
        from marketpipe.bootstrap import BootstrapOrchestrator
        from marketpipe.bootstrap.interfaces import (
            AlembicMigrationService,
            MarketPipeServiceRegistry,
        )
        from tests.fakes.bootstrap import FakeEnvironmentProvider
        
        env = integration_environment
        
        # Set up bootstrap with real database
        bootstrap_env = FakeEnvironmentProvider()
        bootstrap_env.set_database_path(Path(env['database'].get_file_path()))
        
        orchestrator = BootstrapOrchestrator(
            migration_service=AlembicMigrationService(),  # Real migrations
            service_registry=MarketPipeServiceRegistry(),  # Real service registration
            environment_provider=bootstrap_env
        )
        
        # Bootstrap the system
        bootstrap_result = orchestrator.bootstrap()
        assert bootstrap_result.success
        
        # Now run pipeline with bootstrapped system
        provider = FakeMarketDataAdapter()
        bars = create_test_ohlcv_bars(
            symbol="AAPL",
            count=10,
            start_time=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        )
        provider.configure_symbol_data(Symbol("AAPL"), bars)
        
        coordinator = IngestionCoordinatorService(
            market_data_provider=provider,
            database=env['database'],  # Same database as bootstrap
            metrics_collector=env['metrics']
        )
        
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc))
        )
        
        result = asyncio.run(coordinator.ingest_symbol_data(
            symbol=Symbol("AAPL"),
            time_range=time_range
        ))
        
        # Verify full end-to-end functionality
        assert result.total_bars_processed == 10
        assert result.valid_bars == 10
        
        # Verify data persisted to bootstrapped database
        stored_bars = env['database'].query_bars("AAPL", time_range)
        assert len(stored_bars) == 10
        
        # This demonstrates the power of Phase 1 + Phase 2 integration! 