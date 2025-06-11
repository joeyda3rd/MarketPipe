# SPDX-License-Identifier: Apache-2.0
"""Integration tests for event-driven metrics collection."""

from __future__ import annotations

import pytest
import tempfile
import os
import asyncio
from datetime import date, datetime, timezone
from unittest.mock import patch

from marketpipe.events import EventBus, IngestionJobCompleted, ValidationFailed
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
    EventBus._subs.clear()
    yield
    EventBus._subs.clear()


@pytest.mark.asyncio
async def test_ingestion_completed_event_records_metrics(
    temp_metrics_db, clear_event_bus
):
    """Test that IngestionJobCompleted events trigger metric recording."""
    from marketpipe.metrics_event_handlers import setup_metrics_event_handlers

    setup_metrics_event_handlers()

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
    EventBus.publish(event)

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
    from marketpipe.metrics_event_handlers import setup_metrics_event_handlers

    setup_metrics_event_handlers()

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
    EventBus.publish(event)

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
    from marketpipe.metrics_event_handlers import setup_metrics_event_handlers

    setup_metrics_event_handlers()

    # Create validation result using the actual domain structure
    result = ValidationResult(
        symbol="MSFT", total=2000, errors=[]  # No errors - successful validation
    )

    event = ValidationCompleted(result)

    # Publish the event
    EventBus.publish(event)

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
    from marketpipe.metrics_event_handlers import setup_metrics_event_handlers

    setup_metrics_event_handlers()

    # Create test event with correct constructor
    event = AggregationCompleted(job_id="agg-job-789", frames_processed=4)

    # Publish the event
    EventBus.publish(event)

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
    from marketpipe.metrics_event_handlers import setup_metrics_event_handlers

    setup_metrics_event_handlers()

    # Create test event with correct constructor (no frame parameter)
    event = AggregationFailed(
        job_id="agg-job-fail-999", error_message="DuckDB connection failed"
    )

    # Publish the event
    EventBus.publish(event)

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
    from marketpipe.metrics_event_handlers import setup_metrics_event_handlers

    setup_metrics_event_handlers()

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
    EventBus.publish(ingest_event)
    EventBus.publish(validation_event)
    EventBus.publish(agg_event)

    # Wait for processing
    await asyncio.sleep(0.2)

    # Check all metrics were recorded
    repo = SqliteMetricsRepository(temp_metrics_db)

    # Should have ingestion metrics
    ingest_points = await repo.get_metrics_history("ingest_jobs")
    assert len(ingest_points) >= 1

    # Should have validation error metrics
    validation_points = await repo.get_metrics_history("validation_errors")
    assert len(validation_points) >= 1

    # Should have aggregation metrics
    agg_points = await repo.get_metrics_history("aggregation_jobs")
    assert len(agg_points) >= 1

    # Should have symbol-specific metrics
    symbol_ingest_points = await repo.get_metrics_history(f"ingest_bars_{symbol}")
    assert len(symbol_ingest_points) >= 1


@pytest.mark.asyncio
async def test_event_handlers_gracefully_handle_errors(
    temp_metrics_db, clear_event_bus
):
    """Test that event handlers handle errors gracefully without breaking the event bus."""
    from marketpipe.metrics_event_handlers import setup_metrics_event_handlers

    setup_metrics_event_handlers()

    # Mock the record_metric function to raise an error
    with patch(
        "marketpipe.metrics_event_handlers.record_metric",
        side_effect=Exception("Database error"),
    ):

        # Create and publish an event
        symbol = Symbol.from_string("ERROR")
        event = IngestionJobCompleted(
            job_id="error-job",
            symbol=symbol,
            trading_date=date(2024, 1, 15),
            bars_processed=100,
            success=True,
        )

        # This should not raise an exception
        EventBus.publish(event)

        # Wait for processing
        await asyncio.sleep(0.1)

        # Event bus should still work for other events
        normal_event = AggregationCompleted(job_id="normal-job", frames_processed=1)
        EventBus.publish(normal_event)  # Should not raise


if __name__ == "__main__":
    pytest.main([__file__])
