# SPDX-License-Identifier: Apache-2.0
"""Shared test fixtures and utilities for MarketPipe test suite.

Phase 4: Introduces shared test infrastructure to standardize testing patterns
and reduce duplication across test files.

FIXTURES PROVIDED:
- integration_environment: Full test environment with real database and fake services
- domain_objects: Factory for creating valid domain objects with reasonable defaults
- benchmark_data: Large datasets for performance testing
- test_symbols: Common test symbols for consistency across tests

BASE CLASSES PROVIDED:
- IntegrationTestCase: Base for integration tests
- PipelineTestCase: Specialized for pipeline integration tests
- BenchmarkTestCase: Base for performance benchmarks
"""

from __future__ import annotations

import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.adapters import FakeHttpClient
from tests.fakes.database import FakeDatabase
from tests.fakes.metrics import FakeMetricsCollector


class IntegrationEnvironment:
    """Complete test environment with all necessary fakes and real components.

    Provides a unified interface to all test infrastructure, making it easy
    for tests to access exactly what they need without complex setup.
    """

    def __init__(self):
        self.database = FakeDatabase()
        self.http_client = FakeHttpClient()
        self.metrics = FakeMetricsCollector()
        self._temp_dir = None

    def get_temp_dir(self) -> Path:
        """Get a temporary directory for test files."""
        if self._temp_dir is None:
            self._temp_dir = Path(tempfile.mkdtemp(prefix="marketpipe_test_"))
        return self._temp_dir

    def cleanup(self):
        """Clean up test resources."""
        if self.database:
            self.database.cleanup()
        if self._temp_dir and self._temp_dir.exists():
            import shutil

            shutil.rmtree(self._temp_dir, ignore_errors=True)

    def as_dict(self) -> dict[str, Any]:
        """Return as dictionary for backward compatibility."""
        return {
            "database": self.database,
            "http_client": self.http_client,
            "metrics": self.metrics,
            "temp_dir": self.get_temp_dir(),
        }


@pytest.fixture
def integration_environment():
    """Full test environment with real database and fake services.

    Provides everything needed for integration tests:
    - FakeDatabase with real SQLite and schema setup
    - FakeHttpClient for controllable HTTP behavior
    - FakeMetricsCollector for metrics verification
    - Temporary directory for file operations

    Example:
        def test_pipeline_integration(integration_environment):
            env = integration_environment
            # env.database has real SQLite operations
            # env.http_client can simulate API responses
            # env.metrics tracks operations
    """
    env = IntegrationEnvironment()

    # Setup database with schema
    env.database.setup_schema()

    yield env

    # Cleanup
    env.cleanup()


class DomainObjectFactory:
    """Factory for creating valid domain objects with reasonable defaults.

    Reduces test setup boilerplate by providing domain objects with sensible
    defaults that can be overridden for specific test scenarios.
    """

    def __init__(self):
        self.default_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
        self.default_start_time = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)

    def create_ohlcv_bar(self, symbol: str = "AAPL", **overrides):
        """Create valid OHLCV bar with overrides.

        Args:
            symbol: Stock symbol (default: "AAPL")
            **overrides: Any field to override (timestamp, open_price, etc.)

        Returns:
            Valid OHLCVBar entity

        Example:
            bar = factory.create_ohlcv_bar(
                symbol="GOOGL",
                open_price=150.0,
                volume=2000
            )
        """
        from marketpipe.domain.entities import EntityId, OHLCVBar
        from marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume

        defaults = {
            "id": EntityId.generate(),
            "symbol": Symbol(symbol),
            "timestamp": Timestamp(overrides.get("timestamp", self.default_start_time)),
            "open_price": Price.from_float(overrides.get("open_price", 100.0)),
            "high_price": Price.from_float(overrides.get("high_price", 101.0)),
            "low_price": Price.from_float(overrides.get("low_price", 99.0)),
            "close_price": Price.from_float(overrides.get("close_price", 100.5)),
            "volume": Volume(overrides.get("volume", 1000)),
        }

        # Apply overrides, converting to proper value objects where needed
        for key, value in overrides.items():
            if key in defaults:
                if key in ["open_price", "high_price", "low_price", "close_price"] and isinstance(
                    value, (int, float)
                ):
                    defaults[key] = Price.from_float(value)
                elif key == "volume" and isinstance(value, (int, float)):
                    defaults[key] = Volume(int(value))
                elif key == "symbol" and isinstance(value, str):
                    defaults[key] = Symbol(value)
                elif key == "timestamp" and isinstance(value, datetime):
                    defaults[key] = Timestamp(value)
                else:
                    defaults[key] = value

        return OHLCVBar(**defaults)

    def create_ohlcv_bars(
        self,
        symbol: str = "AAPL",
        count: int = 10,
        start_time: datetime | None = None,
        **overrides,
    ):
        """Create multiple OHLCV bars with sequential timestamps.

        Args:
            symbol: Stock symbol
            count: Number of bars to create
            start_time: Starting timestamp (defaults to market hours)
            **overrides: Overrides to apply to each bar

        Returns:
            List of OHLCVBar entities with sequential timestamps
        """
        if start_time is None:
            start_time = self.default_start_time

        bars = []
        for i in range(count):
            timestamp = start_time + timedelta(minutes=i)
            # Create bar with incremental pricing
            bar_overrides = {
                "timestamp": timestamp,
                "open_price": overrides.get("open_price", 100.0 + i * 0.1),
                "high_price": overrides.get("high_price", 101.0 + i * 0.1),
                "low_price": overrides.get("low_price", 99.0 + i * 0.1),
                "close_price": overrides.get("close_price", 100.5 + i * 0.1),
                "volume": overrides.get("volume", 1000 + i * 10),
            }
            # Add any other overrides that aren't prices/volume/timestamp
            for k, v in overrides.items():
                if k not in [
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "timestamp",
                ]:
                    bar_overrides[k] = v

            bar = self.create_ohlcv_bar(symbol=symbol, **bar_overrides)
            bars.append(bar)

        return bars

    def create_ingestion_job(
        self, symbols: list[str] | None = None, trading_date: date | None = None, **overrides
    ):
        """Create valid ingestion job with overrides.

        Args:
            symbols: List of symbols (defaults to common test symbols)
            trading_date: Trading date (defaults to recent business day)
            **overrides: Other fields to override

        Returns:
            Valid IngestionJob entity
        """
        # Note: This is a placeholder since IngestionJob doesn't exist yet
        # In real implementation, would create actual IngestionJob
        if symbols is None:
            symbols = self.default_symbols[:3]  # First 3 symbols
        if trading_date is None:
            trading_date = date(2024, 1, 15)

        return {
            "symbols": symbols,
            "trading_date": trading_date,
            "created_at": datetime.now(timezone.utc),
            **overrides,
        }

    def create_time_range(self, start: datetime | None = None, duration_minutes: int = 30):
        """Create TimeRange for testing.

        Args:
            start: Start time (defaults to market hours)
            duration_minutes: Duration in minutes

        Returns:
            TimeRange value object
        """
        from marketpipe.domain.value_objects import TimeRange, Timestamp

        if start is None:
            start = self.default_start_time
        end = start + timedelta(minutes=duration_minutes)

        return TimeRange(start=Timestamp(start), end=Timestamp(end))


@pytest.fixture
def domain_objects():
    """Factory for creating valid domain objects with reasonable defaults.

    Provides a DomainObjectFactory instance for easy creation of test data.

    Example:
        def test_ohlcv_processing(domain_objects):
            bars = domain_objects.create_ohlcv_bars("AAPL", count=5)
            assert len(bars) == 5
            assert all(bar.symbol.value == "AAPL" for bar in bars)
    """
    return DomainObjectFactory()


# Common test data that many tests can reuse
TEST_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NFLX"]
TEST_TRADING_DATES = [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17)]


@pytest.fixture
def test_symbols():
    """Common test symbols for consistency across tests."""
    return TEST_SYMBOLS.copy()


@pytest.fixture
def test_trading_dates():
    """Common trading dates for consistency across tests."""
    return TEST_TRADING_DATES.copy()


@pytest.fixture
def benchmark_data(domain_objects):
    """Large datasets for performance testing.

    Provides realistic data volumes for benchmarking and performance tests.
    """
    return {
        "small_dataset": {"symbols": TEST_SYMBOLS[:2], "bars_per_symbol": 100, "total_bars": 200},
        "medium_dataset": {
            "symbols": TEST_SYMBOLS[:4],
            "bars_per_symbol": 1000,
            "total_bars": 4000,
        },
        "large_dataset": {"symbols": TEST_SYMBOLS, "bars_per_symbol": 10000, "total_bars": 70000},
    }


# Performance testing marker
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "benchmark: mark test as a performance benchmark")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "integration: mark test as an integration test")


# Optional: Skip slow tests by default unless explicitly requested
def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle markers."""
    if config.getoption("--run-slow"):
        # Don't skip anything if --run-slow is specified
        return

    skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
    skip_benchmark = pytest.mark.skip(reason="benchmarks run only when explicitly requested")

    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
        if "benchmark" in item.keywords and not config.getoption("--benchmark"):
            item.add_marker(skip_benchmark)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption("--run-slow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--benchmark", action="store_true", default=False, help="run benchmark tests")
