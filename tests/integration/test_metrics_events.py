# SPDX-License-Identifier: Apache-2.0
"""Integration tests for event-driven metrics collection."""

from __future__ import annotations

import pytest
import tempfile
import os
import asyncio
from datetime import date, datetime, timezone
from unittest.mock import patch

from marketpipe.domain.events import IngestionJobCompleted, ValidationFailed
from marketpipe.bootstrap import get_event_bus
from marketpipe.domain.value_objects import Symbol, Timestamp
from marketpipe.metrics import SqliteMetricsRepository
from marketpipe.aggregation.domain.events import AggregationCompleted, AggregationFailed
from marketpipe.validation.domain.events import ValidationCompleted
from marketpipe.validation.domain.value_objects import ValidationResult


@pytest.fixture
def temp_metrics_db():
    """Create temporary metrics database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Set environment variable for metrics repository
    old_env = os.environ.get("METRICS_DB_PATH")
    os.environ["METRICS_DB_PATH"] = db_path

    yield db_path

    # Cleanup
    if old_env is not None:
        os.environ["METRICS_DB_PATH"] = old_env
    elif "METRICS_DB_PATH" in os.environ:
        del os.environ["METRICS_DB_PATH"]

    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def clear_event_bus():
    """Clear the event bus before and after tests."""
    # Reset the event bus singleton
    import marketpipe.bootstrap
    marketpipe.bootstrap._EVENT_BUS = None
    yield
    marketpipe.bootstrap._EVENT_BUS = None


@pytest.mark.asyncio
async def test_ingestion_completed_event_records_metrics(
    temp_metrics_db, clear_event_bus
):
    """Test that IngestionJobCompleted events trigger metric recording."""
    from marketpipe.infrastructure.monitoring.event_handlers import register
    
    register()

    # Create test event with correct constructor
    symbol = Symbol.from_string("AAPL")
    event = IngestionJobCompleted(
        job_id="ingest-job-123",
        symbol=symbol,
        trading_date=date(2024, 1, 15),
        bars_processed=1500,
        success=True,
    )

    # Publish the event
    event_bus = get_event_bus()
    event_bus.publish(event)

    # Wait for processing
    await asyncio.sleep(0.1)

    # Check metrics were recorded
    repo = SqliteMetricsRepository(temp_metrics_db)

    ingest_points = await repo.get_metrics_history("ingest_jobs")
    assert len(ingest_points) >= 1
    assert ingest_points[0].value == 1.0

    # Check symbol-specific ingestion metrics
    symbol_points = await repo.get_metrics_history(f"ingest_bars_{symbol}")
    assert len(symbol_points) >= 1
    assert symbol_points[0].value == 1500.0


@pytest.mark.asyncio
async def test_validation_failed_event_records_metrics(
    temp_metrics_db, clear_event_bus
):
    """Test that ValidationFailed events trigger metric recording."""
    from marketpipe.infrastructure.monitoring.event_handlers import register
    
    register()

    # Create test event
    symbol = Symbol.from_string("GOOGL")
    timestamp = Timestamp(datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc))

    event = ValidationFailed(
        symbol=symbol,
        timestamp=timestamp,
        error_message="OHLC consistency check failed",
        rule_id="ohlc_consistency",
        severity="error",
    )

    # Publish the event
    event_bus = get_event_bus()
    event_bus.publish(event)

    # Wait for processing
    await asyncio.sleep(0.1)

    # Check metrics were recorded
    repo = SqliteMetricsRepository(temp_metrics_db)

    validation_error_points = await repo.get_metrics_history("validation_errors")
    assert len(validation_error_points) >= 1

    # Check symbol-specific error metrics
    symbol_error_points = await repo.get_metrics_history(f"validation_errors_{symbol}")
    assert len(symbol_error_points) >= 1


@pytest.mark.asyncio
async def test_validation_completed_event_records_metrics(
    temp_metrics_db, clear_event_bus
):
    """Test that ValidationCompleted events trigger metric recording."""
    from marketpipe.infrastructure.monitoring.event_handlers import register
    
    register()

    # Create validation result using the actual domain structure
    result = ValidationResult(
        symbol="MSFT", total=2000, errors=[]  # No errors - successful validation
    )

    event = ValidationCompleted(result)

    # Publish the event
    event_bus = get_event_bus()
    event_bus.publish(event)

    # Wait for processing
    await asyncio.sleep(0.1)

    # Check metrics
    repo = SqliteMetricsRepository(temp_metrics_db)

    validation_points = await repo.get_metrics_history("validation_jobs")
    assert len(validation_points) >= 1


@pytest.mark.asyncio
async def test_aggregation_completed_event_records_metrics(
    temp_metrics_db, clear_event_bus
):
    """Test that AggregationCompleted events trigger metric recording."""
    from marketpipe.infrastructure.monitoring.event_handlers import register
    
    register()

    # Create test event with correct constructor
    event = AggregationCompleted(job_id="agg-job-789", frames_processed=4)

    # Publish the event
    event_bus = get_event_bus()
    event_bus.publish(event)

    # Wait for processing
    await asyncio.sleep(0.1)

    # Check metrics
    repo = SqliteMetricsRepository(temp_metrics_db)

    agg_points = await repo.get_metrics_history("aggregation_jobs")
    assert len(agg_points) >= 1
    assert agg_points[0].value == 1.0


@pytest.mark.asyncio
async def test_aggregation_failed_event_records_metrics(
    temp_metrics_db, clear_event_bus
):
    """Test that AggregationFailed events trigger metric recording."""
    from marketpipe.infrastructure.monitoring.event_handlers import register
    
    register()

    # Create test event with correct constructor (no frame parameter)
    event = AggregationFailed(
        job_id="agg-job-fail-999", error_message="DuckDB connection failed"
    )

    # Publish the event
    event_bus = get_event_bus()
    event_bus.publish(event)

    # Wait for processing
    await asyncio.sleep(0.1)

    # Check metrics
    repo = SqliteMetricsRepository(temp_metrics_db)

    fail_points = await repo.get_metrics_history("aggregation_failures")
    assert len(fail_points) >= 1
    assert fail_points[0].value == 1.0


@pytest.mark.asyncio
async def test_multiple_events_record_separate_metrics(
    temp_metrics_db, clear_event_bus
):
    """Test that multiple different events record separate metrics."""
    from marketpipe.infrastructure.monitoring.event_handlers import register
    
    register()

    # Create multiple events
    symbol = Symbol.from_string("TSLA")

    ingest_event = IngestionJobCompleted(
        job_id="multi-job-1",
        symbol=symbol,
        trading_date=date(2024, 1, 15),
        bars_processed=3000,
        success=True,
    )

    validation_event = ValidationFailed(
        symbol=symbol,
        timestamp=Timestamp(datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)),
        error_message="Price spike detected",
        rule_id="price_spike",
        severity="warning",
    )

    agg_event = AggregationCompleted(job_id="multi-job-1", frames_processed=3)

    # Publish all events
    event_bus = get_event_bus()
    event_bus.publish(ingest_event)
    event_bus.publish(validation_event)
    event_bus.publish(agg_event)

    # Wait for processing
    await asyncio.sleep(0.2)

    # Check all metrics were recorded
    repo = SqliteMetricsRepository(temp_metrics_db)

    ingest_points = await repo.get_metrics_history("ingest_jobs")
    assert len(ingest_points) >= 1

    validation_error_points = await repo.get_metrics_history("validation_errors")
    assert len(validation_error_points) >= 1

    agg_points = await repo.get_metrics_history("aggregation_jobs")
    assert len(agg_points) >= 1


@pytest.mark.asyncio
async def test_event_handlers_gracefully_handle_errors(
    temp_metrics_db, clear_event_bus
):
    """Test that event handlers gracefully handle errors without crashing."""
    from marketpipe.infrastructure.monitoring.event_handlers import register
    
    register()

    # Create a normal event that should work
    symbol = Symbol.from_string("NVDA")
    normal_event = IngestionJobCompleted(
        job_id="test-job-error-handling",
        symbol=symbol,
        trading_date=date(2024, 1, 15),
        bars_processed=500,
        success=True,
    )

    # Mock the metrics repository to raise an error
    with patch(
        "marketpipe.infrastructure.monitoring.event_handlers.record_metric"
    ) as mock_record:
        mock_record.side_effect = Exception("Database connection failed")

        # Publishing should not raise even if metric recording fails
        event_bus = get_event_bus()
        event_bus.publish(normal_event)  # Should not raise

        # Wait for processing
        await asyncio.sleep(0.1)

        # The event should have been published, even though metrics failed
        assert mock_record.called


if __name__ == "__main__":
    pytest.main([__file__])
