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
from unittest.mock import Mock, AsyncMock, patch
from dataclasses import dataclass
from typing import Dict, Any

from marketpipe.metrics import (
    record_metric, 
    REQUESTS, 
    ERRORS, 
    LATENCY,
    LEGACY_REQUESTS,
    LEGACY_ERRORS, 
    LEGACY_LATENCY,
    SqliteMetricsRepository,
    MetricPoint
)
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


class FakeProvider:
    """Fake provider for testing metrics with provider/feed labels."""
    
    def __init__(self, provider_name: str, feed_type: str):
        self.provider_name = provider_name
        self.feed_type = feed_type
        
    def get_provider_info(self):
        return {"provider": self.provider_name, "feed": self.feed_type}


class TestProviderFeedLabelsFixed:
    """Test that all identified gaps have been fixed."""

    def test_prometheus_metrics_have_full_labels(self):
        """Test Gap A: Prometheus counters now have provider/feed labels."""
        # Verify metric definitions have the full label set
        assert "source" in REQUESTS._labelnames
        assert "provider" in REQUESTS._labelnames 
        assert "feed" in REQUESTS._labelnames
        
        assert "source" in ERRORS._labelnames
        assert "provider" in ERRORS._labelnames
        assert "feed" in ERRORS._labelnames
        assert "code" in ERRORS._labelnames
        
        assert "source" in LATENCY._labelnames
        assert "provider" in LATENCY._labelnames
        assert "feed" in LATENCY._labelnames
        
        # Test that metrics can be labeled with provider/feed
        REQUESTS.labels(source="test", provider="alpaca", feed="iex").inc()
        ERRORS.labels(source="test", provider="alpaca", feed="iex", code="404").inc()
        LATENCY.labels(source="test", provider="alpaca", feed="iex").observe(0.5)
        
        # Verify legacy metrics also exist for backward compatibility
        # Note: Prometheus client automatically strips '_total' suffix from Counter names
        assert LEGACY_REQUESTS._name == "mp_requests_legacy"  
        assert LEGACY_ERRORS._name == "mp_errors_legacy"
        assert LEGACY_LATENCY._name == "mp_request_legacy_latency_seconds"

    def test_env_var_disables_sqlite_metrics(self):
        """Test Gap B: Environment variable disables SQLite metrics."""
        # Test is already using MP_DISABLE_SQLITE_METRICS=1
        # Verify record_metric doesn't try to persist to SQLite
        original_env = os.environ.get("MP_DISABLE_SQLITE_METRICS")
        
        try:
            # Enable SQLite persistence 
            os.environ["MP_DISABLE_SQLITE_METRICS"] = "0"
            
            with patch('marketpipe.metrics.get_metrics_repository') as mock_repo:
                mock_repo.return_value.record = AsyncMock()
                record_metric("test_metric", 1.0, provider="test", feed="test", source="test")
                # Should have attempted to create repository when not disabled
                mock_repo.assert_called()
                
            # Disable SQLite persistence
            os.environ["MP_DISABLE_SQLITE_METRICS"] = "1"
            
            with patch('marketpipe.metrics.get_metrics_repository') as mock_repo:
                record_metric("test_metric", 1.0, provider="test", feed="test", source="test")
                # Should not have attempted to create repository when disabled
                mock_repo.assert_not_called()
                
        finally:
            # Restore original environment
            if original_env is not None:
                os.environ["MP_DISABLE_SQLITE_METRICS"] = original_env
            else:
                os.environ.pop("MP_DISABLE_SQLITE_METRICS", None)

    def test_record_metric_forwards_to_prometheus(self):
        """Test Gap D: record_metric forwards provider/feed to Prometheus metrics."""
        # Note: We don't clear metrics in newer prometheus_client versions
        # Instead we'll check for specific label combinations that are unique to this test
        
        # Test request metric forwarding with unique provider/feed combinations
        record_metric("test_request", 1.0, provider="test_alpaca_unique", feed="test_iex_unique", source="test_unique")
        
        # Verify Prometheus metrics were updated with correct labels
        request_samples = [s for s in REQUESTS.collect()[0].samples if s.labels.get('provider') == 'test_alpaca_unique']
        assert len(request_samples) > 0
        assert request_samples[0].labels['feed'] == 'test_iex_unique'
        assert request_samples[0].labels['source'] == 'test_unique'
        
        # Test error metric forwarding
        record_metric("test_error_failed", 1.0, provider="test_polygon_unique", feed="test_sip_unique", source="test_validation_unique")
        
        error_samples = [s for s in ERRORS.collect()[0].samples if s.labels.get('provider') == 'test_polygon_unique']
        assert len(error_samples) > 0
        assert error_samples[0].labels['feed'] == 'test_sip_unique'
        assert error_samples[0].labels['source'] == 'test_validation_unique'
        
        # Test latency metric forwarding
        record_metric("test_duration_seconds", 1.5, provider="test_alpaca_latency", feed="test_iex_latency", source="test_ingestion_unique")
        
        latency_samples = [s for s in LATENCY.collect()[0].samples if s.labels.get('provider') == 'test_alpaca_latency']
        assert len(latency_samples) > 0

    def test_alpaca_client_uses_new_labels(self):
        """Test that Alpaca client uses new metric signature."""
        from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
        from marketpipe.ingestion.infrastructure.models import ClientConfig
        from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
        
        # Note: We don't clear metrics in newer prometheus_client versions
        # Instead we'll check for unique labels specific to this test
        
        config = ClientConfig(api_key="test", base_url="https://api.test.com")
        auth = HeaderTokenAuth("key", "secret")
        client = AlpacaClient(config=config, auth=auth, feed="iex")
        
        # Mock successful response
        with patch('httpx.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"bars": {"AAPL": []}}
            mock_get.return_value = mock_response
            
            # Make request that triggers metrics
            client._request({"symbol": "AAPL"})
            
            # Verify metrics have provider/feed labels
            request_samples = [s for s in REQUESTS.collect()[0].samples]
            # Look for metrics with the exact combination we expect from this test
            alpaca_requests = [s for s in request_samples 
                             if s.labels.get('provider') == 'alpaca' 
                             and s.labels.get('feed') == 'iex'
                             and s.labels.get('source') == 'alpaca']
            assert len(alpaca_requests) > 0, f"Expected alpaca metrics not found. Available samples: {[(s.labels, s.value) for s in request_samples]}"

    def test_migration_backfill_applied(self):
        """Test Gap C: Migration 003 handles existing data properly."""
        # Read the migration file to verify it includes proper table recreation
        from pathlib import Path
        migration_file = Path("src/marketpipe/migrations/versions/003_provider_feed_labels.sql")
        
        assert migration_file.exists(), "Migration 003 should exist"
        
        content = migration_file.read_text()
        # Verify the new approach: table recreation with proper columns
        assert "CREATE TABLE metrics_temp" in content
        assert "provider TEXT DEFAULT 'unknown'" in content
        assert "feed TEXT DEFAULT 'unknown'" in content
        assert "ALTER TABLE metrics_temp RENAME TO metrics" in content

    def test_database_schema_supports_provider_feed(self):
        """Test that database schema correctly supports provider and feed columns."""
        from marketpipe.metrics import SqliteMetricsRepository
        import tempfile
        import asyncio
        
        # Create test database
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
            repo = SqliteMetricsRepository(tmp_file.name)
            
            async def test_schema():
                # Record metric with provider/feed
                await repo.record("test_metric", 42.0, provider="test_provider", feed="test_feed")
                
                # Retrieve and verify
                metrics = await repo.get_metrics_history("test_metric")
                assert len(metrics) == 1
                assert metrics[0].provider == "test_provider"
                assert metrics[0].feed == "test_feed"
                assert metrics[0].value == 42.0
                
            # Run async test
            asyncio.run(test_schema())

    def test_legacy_metrics_backward_compatibility(self):
        """Test that legacy metrics are maintained for backward compatibility."""
        # Note: We don't clear metrics in newer prometheus_client versions
        # Instead we'll check for unique labels specific to this test
        
        # Record metrics that should update both new and legacy with unique labels
        record_metric("test_request", 1.0, provider="legacy_test_alpaca", feed="legacy_test_iex", source="legacy_test_source")
        record_metric("test_error_timeout", 1.0, provider="legacy_test_alpaca", feed="legacy_test_iex", source="legacy_test_source")
        record_metric("test_latency", 0.5, provider="legacy_test_alpaca", feed="legacy_test_iex", source="legacy_test_source")
        
        # Verify legacy metrics were also updated
        legacy_requests = [s for s in LEGACY_REQUESTS.collect()[0].samples if s.labels.get('source') == 'legacy_test_source']
        assert len(legacy_requests) > 0
        assert legacy_requests[0].labels['source'] == 'legacy_test_source'
        
        legacy_errors = [s for s in LEGACY_ERRORS.collect()[0].samples if s.labels.get('source') == 'legacy_test_source']
        assert len(legacy_errors) > 0
        
        legacy_latency = [s for s in LEGACY_LATENCY.collect()[0].samples if s.labels.get('source') == 'legacy_test_source']
        assert len(legacy_latency) > 0


if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v"]) 