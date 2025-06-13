# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date
from unittest.mock import patch, MagicMock

from marketpipe.domain.value_objects import Symbol
from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.bootstrap import get_event_bus
from marketpipe.aggregation import AggregationRunnerService


def test_subscribe_and_publish(monkeypatch):
    """Test that aggregation service subscribes to events and processes them."""
    called = []

    # Mock the DuckDB engine so we don't hit disk
    def mock_aggregate_job(self, job_id, sql_pairs):
        called.append(job_id)

    monkeypatch.setattr(
        "marketpipe.aggregation.infrastructure.duckdb_engine.DuckDBAggregationEngine.aggregate_job",
        mock_aggregate_job,
    )

    # Reset the event bus completely
    import marketpipe.bootstrap
    from marketpipe.infrastructure.messaging.in_memory_bus import InMemoryEventBus
    
    marketpipe.bootstrap._EVENT_BUS = None
    InMemoryEventBus.clear_subscriptions()

    # Register aggregation service  
    AggregationRunnerService.register()

    # Mock the record_metric function to avoid metrics issues
    with patch("marketpipe.metrics.record_metric"):
        # Mock DuckDB views refresh to avoid filesystem dependencies
        with patch("marketpipe.aggregation.infrastructure.duckdb_views.refresh_views"):
            # Publish event with required arguments
            event_bus = get_event_bus()
            event_bus.publish(
                IngestionJobCompleted(
                    job_id="job-x",
                    symbol=Symbol("AAPL"),
                    trading_date=date(2024, 1, 15),
                    bars_processed=1000,
                    success=True,
                )
            )

    # Verify the job was processed
    assert called == ["job-x"]


def test_aggregation_service_handles_multiple_events(monkeypatch):
    """Test that the aggregation service can handle multiple events."""
    called_jobs = []

    def mock_aggregate_job(self, job_id, sql_pairs):
        called_jobs.append(job_id)

    monkeypatch.setattr(
        "marketpipe.aggregation.infrastructure.duckdb_engine.DuckDBAggregationEngine.aggregate_job",
        mock_aggregate_job,
    )

    # Reset the event bus completely
    import marketpipe.bootstrap
    from marketpipe.infrastructure.messaging.in_memory_bus import InMemoryEventBus
    
    marketpipe.bootstrap._EVENT_BUS = None
    InMemoryEventBus.clear_subscriptions()

    # Register service
    AggregationRunnerService.register()

    # Mock the record_metric function to avoid metrics issues
    with patch("marketpipe.metrics.record_metric"):
        # Mock DuckDB views refresh to avoid filesystem dependencies
        with patch("marketpipe.aggregation.infrastructure.duckdb_views.refresh_views"):
            # Publish multiple events with required arguments
            event_bus = get_event_bus()
            event_bus.publish(
                IngestionJobCompleted(
                    job_id="job-1",
                    symbol=Symbol("AAPL"),
                    trading_date=date(2024, 1, 15),
                    bars_processed=500,
                    success=True,
                )
            )
            event_bus.publish(
                IngestionJobCompleted(
                    job_id="job-2",
                    symbol=Symbol("GOOGL"),
                    trading_date=date(2024, 1, 15),
                    bars_processed=750,
                    success=True,
                )
            )
            event_bus.publish(
                IngestionJobCompleted(
                    job_id="job-3",
                    symbol=Symbol("MSFT"),
                    trading_date=date(2024, 1, 15),
                    bars_processed=600,
                    success=True,
                )
            )

    # Verify all jobs were processed
    assert called_jobs == ["job-1", "job-2", "job-3"]


def test_aggregation_service_error_handling(monkeypatch):
    """Test that aggregation service handles errors gracefully."""
    error_count = 0

    def mock_aggregate_job_with_error(self, job_id, sql_pairs):
        nonlocal error_count
        error_count += 1
        raise Exception(f"Test error for {job_id}")

    monkeypatch.setattr(
        "marketpipe.aggregation.infrastructure.duckdb_engine.DuckDBAggregationEngine.aggregate_job",
        mock_aggregate_job_with_error,
    )

    # Reset the event bus completely
    import marketpipe.bootstrap
    from marketpipe.infrastructure.messaging.in_memory_bus import InMemoryEventBus
    
    marketpipe.bootstrap._EVENT_BUS = None
    InMemoryEventBus.clear_subscriptions()

    # Register service
    AggregationRunnerService.register()

    # Mock the record_metric function to avoid metrics issues
    with patch("marketpipe.metrics.record_metric"):
        # Mock logging to capture error messages
        with patch("logging.getLogger") as mock_logger:
            mock_log = MagicMock()
            mock_logger.return_value = mock_log

            # Publish event that will cause error with required arguments
            # The service re-raises exceptions, so we need to catch it
            try:
                event_bus = get_event_bus()
                event_bus.publish(
                    IngestionJobCompleted(
                        job_id="failing-job",
                        symbol=Symbol("FAIL"),
                        trading_date=date(2024, 1, 15),
                        bars_processed=0,
                        success=False,
                        error_message="Test failure",
                    )
                )
                # If we get here, the exception wasn't raised as expected
                assert False, "Expected exception was not raised"
            except Exception:
                # This is expected due to re-raising in the service
                pass

    # Verify error was processed (the mock was called)
    assert error_count == 1


def test_manual_aggregation():
    """Test manual aggregation through the service."""
    called_jobs = []

    def mock_aggregate_job(job_id, sql_pairs):
        called_jobs.append(job_id)

    # Create service and mock its engine
    service = AggregationRunnerService.build_default()

    # Mock the record_metric function to avoid metrics issues
    with patch("marketpipe.metrics.record_metric"):
        # Mock DuckDB views refresh to avoid filesystem dependencies
        with patch("marketpipe.aggregation.infrastructure.duckdb_views.refresh_views"):
            # Mock the aggregate_job method on the engine instance
            with patch.object(service._engine, "aggregate_job", side_effect=mock_aggregate_job):
                # Run manual aggregation
                service.run_manual_aggregation("manual-job")

    # Verify job was processed manually
    assert called_jobs == ["manual-job"]
