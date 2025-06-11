# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date
from unittest.mock import patch, MagicMock

from marketpipe.domain.value_objects import Symbol
from marketpipe.events import EventBus, IngestionJobCompleted
from marketpipe.aggregation import AggregationRunnerService


def test_subscribe_and_publish(monkeypatch):
    """Test that aggregation service subscribes to events and processes them."""
    called = []
    
    # Mock the DuckDB engine so we don't hit disk
    def mock_aggregate_job(self, job_id, sql_pairs):
        called.append(job_id)
    
    monkeypatch.setattr(
        "marketpipe.aggregation.infrastructure.duckdb_engine.DuckDBAggregationEngine.aggregate_job",
        mock_aggregate_job
    )
    
    # Clear any existing subscribers to avoid interference
    EventBus._subs.clear()
    
    # Register aggregation service
    AggregationRunnerService.register()
    
    # Publish event with required arguments
    EventBus.publish(IngestionJobCompleted(
        job_id="job-x",
        symbol=Symbol("AAPL"),
        trading_date=date(2024, 1, 15),
        bars_processed=1000,
        success=True
    ))
    
    # Verify the job was processed
    assert called == ["job-x"]


def test_aggregation_service_handles_multiple_events(monkeypatch):
    """Test that the aggregation service can handle multiple events."""
    called_jobs = []
    
    def mock_aggregate_job(self, job_id, sql_pairs):
        called_jobs.append(job_id)
    
    monkeypatch.setattr(
        "marketpipe.aggregation.infrastructure.duckdb_engine.DuckDBAggregationEngine.aggregate_job",
        mock_aggregate_job
    )
    
    # Clear any existing subscribers
    EventBus._subs.clear()
    
    # Register service
    AggregationRunnerService.register()
    
    # Publish multiple events with required arguments
    EventBus.publish(IngestionJobCompleted(
        job_id="job-1",
        symbol=Symbol("AAPL"),
        trading_date=date(2024, 1, 15),
        bars_processed=500,
        success=True
    ))
    EventBus.publish(IngestionJobCompleted(
        job_id="job-2",
        symbol=Symbol("GOOGL"),
        trading_date=date(2024, 1, 15),
        bars_processed=750,
        success=True
    ))
    EventBus.publish(IngestionJobCompleted(
        job_id="job-3",
        symbol=Symbol("MSFT"),
        trading_date=date(2024, 1, 15),
        bars_processed=600,
        success=True
    ))
    
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
        mock_aggregate_job_with_error
    )
    
    # Clear any existing subscribers
    EventBus._subs.clear()
    
    # Register service
    AggregationRunnerService.register()
    
    # Mock logging to capture error messages
    with patch('logging.getLogger') as mock_logger:
        mock_log = MagicMock()
        mock_logger.return_value = mock_log
        
        try:
            # Publish event that will cause error with required arguments
            EventBus.publish(IngestionJobCompleted(
                job_id="failing-job",
                symbol=Symbol("FAIL"),
                trading_date=date(2024, 1, 15),
                bars_processed=0,
                success=False,
                error_message="Test failure"
            ))
        except Exception:
            pass  # Expected to raise due to error handling
    
    # Verify error was processed
    assert error_count == 1


def test_manual_aggregation():
    """Test manual aggregation through the service."""
    called_jobs = []
    
    def mock_aggregate_job(job_id, sql_pairs):
        called_jobs.append(job_id)
    
    # Create service and mock its engine
    service = AggregationRunnerService.build_default()
    
    # Mock the aggregate_job method on the engine instance
    with patch.object(service._engine, 'aggregate_job', side_effect=mock_aggregate_job):
        # Run manual aggregation
        service.run_manual_aggregation("manual-job")
    
    # Verify job was processed manually
    assert called_jobs == ["manual-job"] 