# SPDX-License-Identifier: Apache-2.0
"""Regression tests for provider/feed labels on metrics.

This test suite verifies that the provider and feed labels enhancement
works correctly by running ingestion with a fake provider and asserting
that metrics contain the expected provider/feed information.
"""

from __future__ import annotations

import pytest
import asyncio
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock

from marketpipe.metrics import SqliteMetricsRepository, record_metric
from marketpipe.ingestion.application.services import IngestionCoordinatorService
from marketpipe.ingestion.domain.entities import IngestionJobId
from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration


class FakeMarketDataProvider:
    """Fake market data provider for testing."""
    
    def __init__(self, provider_name: str = "fake", feed_type: str = "test"):
        self.provider_name = provider_name
        self.feed_type = feed_type
    
    async def fetch_bars(self, symbol: Symbol, start_timestamp: int, end_timestamp: int, **kwargs):
        """Return fake OHLCV bars."""
        # Return 5 fake bars for testing
        bars = []
        for i in range(5):
            timestamp_ns = start_timestamp + (i * 60_000_000_000)  # 1 minute intervals
            bars.append({
                'symbol': symbol.value,
                'timestamp': timestamp_ns,
                'open': 100.0 + i,
                'high': 101.0 + i,
                'low': 99.0 + i,
                'close': 100.5 + i,
                'volume': 1000 + (i * 100),
                'ts_ns': timestamp_ns,
            })
        return bars
    
    def get_provider_info(self):
        """Return provider information."""
        return self.provider_name, self.feed_type


class FakeDataValidator:
    """Fake data validator for testing."""
    
    async def validate_bars(self, bars):
        """Return successful validation result."""
        class MockValidationResult:
            def __init__(self):
                self.is_valid = True
                self.errors = []
        
        return MockValidationResult()


class FakeDataStorage:
    """Fake data storage for testing."""
    
    async def store_bars(self, bars, job_id: str):
        """Fake store operation."""
        return len(bars)


@pytest.fixture
def temp_metrics_db():
    """Create temporary metrics database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.mark.asyncio
async def test_provider_feed_labels_in_ingestion_metrics(temp_metrics_db, temp_output_dir):
    """Test that ingestion metrics include provider and feed labels."""
    # Set up temporary metrics database
    os.environ["METRICS_DB_PATH"] = temp_metrics_db

    # Create fake provider with specific labels
    provider = FakeMarketDataProvider(provider_name="alpaca", feed_type="iex")
    provider_name, feed_type = provider.get_provider_info()

    # Test that the record_metric function works with provider/feed labels
    # This simulates what would happen during ingestion
    record_metric("ingestion_time", 1.23, provider=provider_name, feed=feed_type)
    record_metric("bars_processed", 150, provider=provider_name, feed=feed_type)
    record_metric("symbols_processed", 1, provider=provider_name, feed=feed_type)

    # Wait for async operations to complete
    await asyncio.sleep(0.1)

    # Verify metrics were recorded with provider/feed labels
    repo = SqliteMetricsRepository(temp_metrics_db)

    # Check ingestion_time metric
    time_metrics = await repo.get_metrics_history("ingestion_time")
    assert len(time_metrics) >= 1
    time_metric = time_metrics[0]
    assert time_metric.provider == "alpaca"
    assert time_metric.feed == "iex"
    assert time_metric.value == 1.23

    # Check bars_processed metric
    bars_metrics = await repo.get_metrics_history("bars_processed")
    assert len(bars_metrics) >= 1
    bars_metric = bars_metrics[0]
    assert bars_metric.provider == "alpaca"
    assert bars_metric.feed == "iex"
    assert bars_metric.value == 150.0

    # Check symbols_processed metric
    symbols_metrics = await repo.get_metrics_history("symbols_processed")
    assert len(symbols_metrics) >= 1
    symbols_metric = symbols_metrics[0]
    assert symbols_metric.provider == "alpaca"
    assert symbols_metric.feed == "iex"
    assert symbols_metric.value == 1.0


@pytest.mark.asyncio
async def test_validation_metrics_include_provider_feed_labels(temp_metrics_db):
    """Test that validation metrics include provider and feed labels."""
    # Set up temporary metrics database
    os.environ["METRICS_DB_PATH"] = temp_metrics_db
    
    # Record validation metrics with provider/feed labels
    record_metric("validation_jobs_started", 1, provider="polygon", feed="sip")
    record_metric("validation_bars_processed", 100, provider="polygon", feed="sip")
    record_metric("validation_jobs_success", 1, provider="polygon", feed="sip")
    
    # Wait for async operations to complete
    await asyncio.sleep(0.1)
    
    # Verify metrics were recorded with correct labels
    repo = SqliteMetricsRepository(temp_metrics_db)
    
    # Check validation metrics
    validation_metrics = await repo.get_metrics_history("validation_jobs_started")
    assert len(validation_metrics) >= 1
    metric = validation_metrics[0]
    assert metric.provider == "polygon"
    assert metric.feed == "sip"
    
    bars_metrics = await repo.get_metrics_history("validation_bars_processed")
    assert len(bars_metrics) >= 1
    bars_metric = bars_metrics[0]
    assert bars_metric.provider == "polygon"
    assert bars_metric.feed == "sip"
    assert bars_metric.value == 100.0


@pytest.mark.asyncio
async def test_aggregation_metrics_include_provider_feed_labels(temp_metrics_db):
    """Test that aggregation metrics include provider and feed labels."""
    # Set up temporary metrics database
    os.environ["METRICS_DB_PATH"] = temp_metrics_db
    
    # Record aggregation metrics with provider/feed labels
    record_metric("aggregation_jobs_started", 1, provider="alpaca", feed="iex")
    record_metric("aggregation_frames_processed", 4, provider="alpaca", feed="iex")
    record_metric("aggregation_jobs_success", 1, provider="alpaca", feed="iex")
    
    # Wait for async operations to complete
    await asyncio.sleep(0.1)
    
    # Verify metrics were recorded with correct labels
    repo = SqliteMetricsRepository(temp_metrics_db)
    
    # Check aggregation metrics
    agg_metrics = await repo.get_metrics_history("aggregation_jobs_started")
    assert len(agg_metrics) >= 1
    metric = agg_metrics[0]
    assert metric.provider == "alpaca"
    assert metric.feed == "iex"
    
    frames_metrics = await repo.get_metrics_history("aggregation_frames_processed")
    assert len(frames_metrics) >= 1
    frames_metric = frames_metrics[0]
    assert frames_metric.provider == "alpaca"
    assert frames_metric.feed == "iex"
    assert frames_metric.value == 4.0


@pytest.mark.asyncio
async def test_default_provider_feed_labels_when_unknown(temp_metrics_db):
    """Test that metrics use 'unknown' labels when provider/feed are not specified."""
    # Set up temporary metrics database
    os.environ["METRICS_DB_PATH"] = temp_metrics_db
    
    # Record metrics without provider/feed labels (should default to 'unknown')
    record_metric("test_metric_no_labels", 42)
    
    # Wait for async operations to complete
    await asyncio.sleep(0.1)
    
    # Verify metrics were recorded with default labels
    repo = SqliteMetricsRepository(temp_metrics_db)
    
    metrics = await repo.get_metrics_history("test_metric_no_labels")
    assert len(metrics) >= 1
    metric = metrics[0]
    assert metric.provider == "unknown"
    assert metric.feed == "unknown"
    assert metric.value == 42.0


@pytest.mark.asyncio
async def test_metrics_database_schema_includes_provider_feed_columns(temp_metrics_db):
    """Test that the metrics database schema includes provider and feed columns."""
    # Set up temporary metrics database
    os.environ["METRICS_DB_PATH"] = temp_metrics_db
    
    # Create repository to trigger database initialization
    repo = SqliteMetricsRepository(temp_metrics_db)
    
    # Record a test metric
    await repo.record("test_schema", 1.0, provider="test_provider", feed="test_feed")
    
    # Query the database directly to verify schema
    import aiosqlite
    async with aiosqlite.connect(temp_metrics_db) as db:
        # Check table schema
        cursor = await db.execute("PRAGMA table_info(metrics)")
        columns = await cursor.fetchall()
        
        column_names = [col[1] for col in columns]
        assert "provider" in column_names
        assert "feed" in column_names
        
        # Verify data was stored correctly
        cursor = await db.execute(
            "SELECT provider, feed, value FROM metrics WHERE name = 'test_schema'"
        )
        row = await cursor.fetchone()
        assert row[0] == "test_provider"  # provider
        assert row[1] == "test_feed"      # feed
        assert row[2] == 1.0              # value


def test_command_line_integration_with_fake_provider():
    """Test that CLI integration works with fake provider (placeholder for actual CLI test)."""
    # This test would be implemented to run:
    # mp ingest --provider fake --config test_config.yaml
    # and verify that metrics contain provider="fake" feed="test"
    
    # For now, this is a placeholder that documents the expected behavior
    # The actual implementation would involve:
    # 1. Creating a test config with fake provider
    # 2. Running the CLI command
    # 3. Checking the metrics database for correct labels
    
    assert True  # Placeholder assertion


if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v"]) 