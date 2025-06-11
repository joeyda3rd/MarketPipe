# SPDX-License-Identifier: Apache-2.0
"""Unit tests for IngestionJob domain entity."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from typing import List

from marketpipe.domain.entities import EntityId
from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.domain.entities import (
    IngestionJob, 
    IngestionJobId, 
    ProcessingState
)
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration, IngestionPartition
from marketpipe.ingestion.domain.events import (
    IngestionJobStarted,
    IngestionJobCompleted,
    IngestionJobFailed,
    IngestionBatchProcessed
)
from tests.fakes.adapters import create_test_ohlcv_bars


def create_test_ingestion_configuration() -> IngestionConfiguration:
    """Create a test ingestion configuration."""
    from pathlib import Path
    return IngestionConfiguration(
        output_path=Path("/tmp/test"),
        compression="snappy",
        max_workers=4,
        batch_size=1000,
        rate_limit_per_minute=200,
        feed_type="iex"
    )


def create_test_ingestion_job() -> IngestionJob:
    """Create a test ingestion job."""
    job_id = IngestionJobId.generate()
    symbols = [Symbol("AAPL"), Symbol("GOOGL")]
    start_time = datetime(2023, 1, 2, 9, 30, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 2, 16, 0, tzinfo=timezone.utc)
    time_range = TimeRange(
        start=Timestamp(start_time),
        end=Timestamp(end_time)
    )
    configuration = create_test_ingestion_configuration()
    
    return IngestionJob(job_id, configuration, symbols, time_range)


class TestIngestionJobCreation:
    """Test ingestion job creation and validation."""
    
    def test_creates_job_with_valid_parameters(self):
        """Test that an ingestion job can be created with valid parameters."""
        job = create_test_ingestion_job()
        
        assert job.state == ProcessingState.PENDING
        assert len(job.symbols) == 2
        assert Symbol("AAPL") in job.symbols
        assert Symbol("GOOGL") in job.symbols
        assert job.progress_percentage == 0.0
        assert job.is_complete is False
        assert job.can_start is True
    
    def test_validates_symbols_not_empty(self):
        """Test that job creation fails with empty symbols list."""
        job_id = IngestionJobId.generate()
        symbols: List[Symbol] = []  # Empty list
        start_time = datetime(2023, 1, 2, 9, 30, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 16, 0, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        configuration = create_test_ingestion_configuration()
        
        with pytest.raises(ValueError, match="must have at least one symbol"):
            IngestionJob(job_id, configuration, symbols, time_range)
    
    def test_validates_no_duplicate_symbols(self):
        """Test that job creation fails with duplicate symbols."""
        job_id = IngestionJobId.generate()
        symbols = [Symbol("AAPL"), Symbol("AAPL")]  # Duplicate
        start_time = datetime(2023, 1, 2, 9, 30, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 16, 0, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        configuration = create_test_ingestion_configuration()
        
        with pytest.raises(ValueError, match="cannot have duplicate symbols"):
            IngestionJob(job_id, configuration, symbols, time_range)
    
    def test_validates_time_range_order(self):
        """Test that time range creation fails when start time is after end time."""
        job_id = IngestionJobId.generate()
        symbols = [Symbol("AAPL")]
        start_time = datetime(2023, 1, 2, 16, 0, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 9, 30, tzinfo=timezone.utc)  # Before start
        configuration = create_test_ingestion_configuration()
        
        with pytest.raises(ValueError, match="Start time .* must be before end time"):
            TimeRange(
                start=Timestamp(start_time),
                end=Timestamp(end_time)
            )
    
    def test_validates_no_future_dates(self):
        """Test that job creation fails for future dates."""
        job_id = IngestionJobId.generate()
        symbols = [Symbol("AAPL")]
        future_time = datetime(2030, 1, 1, tzinfo=timezone.utc)  # Future date
        time_range = TimeRange(
            start=Timestamp(future_time),
            end=Timestamp(future_time.replace(hour=16))
        )
        configuration = create_test_ingestion_configuration()
        
        with pytest.raises(ValueError, match="Cannot create ingestion job for future dates"):
            IngestionJob(job_id, configuration, symbols, time_range)


class TestIngestionJobLifecycle:
    """Test ingestion job lifecycle state transitions."""
    
    def test_job_can_be_started_when_pending(self):
        """Test that a pending job can be started."""
        job = create_test_ingestion_job()
        
        assert job.can_start is True
        job.start()
        
        assert job.state == ProcessingState.IN_PROGRESS
        assert job.started_at is not None
        assert job.can_start is False
    
    def test_job_cannot_be_started_twice(self):
        """Test that a job cannot be started twice."""
        job = create_test_ingestion_job()
        job.start()
        
        with pytest.raises(ValueError, match="Cannot start job in state"):
            job.start()
    
    def test_job_can_be_completed_when_all_symbols_processed(self):
        """Test that a job can be completed when all symbols are processed."""
        job = create_test_ingestion_job()
        job.start()
        
        # Process all symbols
        for symbol in job.symbols:
            partition = IngestionPartition(
                symbol=symbol,
                file_path=job.configuration.output_path / f"{symbol.value}.parquet",
                record_count=100,
                file_size_bytes=1024,
                created_at=datetime.now(timezone.utc)
            )
            job.mark_symbol_processed(symbol, 100, partition)
        
        assert job.state == ProcessingState.COMPLETED
        assert job.completed_at is not None
        assert job.is_complete is True
        assert job.progress_percentage == 100.0
    
    def test_job_can_be_failed_with_error_message(self):
        """Test that a job can be marked as failed with an error message."""
        job = create_test_ingestion_job()
        job.start()
        
        error_message = "Market data provider connection failed"
        job.fail(error_message)
        
        assert job.state == ProcessingState.FAILED
        assert job.failed_at is not None
        assert job.error_message == error_message
        assert job.can_fail is False
    
    def test_job_can_be_cancelled(self):
        """Test that a job can be cancelled."""
        job = create_test_ingestion_job()
        
        job.cancel()
        
        assert job.state == ProcessingState.CANCELLED
        assert job.can_cancel is False


class TestIngestionJobSymbolProcessing:
    """Test symbol processing within ingestion jobs."""
    
    def test_marks_symbol_as_processed_correctly(self):
        """Test that symbols can be marked as processed."""
        job = create_test_ingestion_job()
        job.start()
        
        symbol = job.symbols[0]
        partition = IngestionPartition(
            symbol=symbol,
            file_path=job.configuration.output_path / f"{symbol.value}.parquet",
            record_count=150,
            file_size_bytes=2048,
            created_at=datetime.now(timezone.utc)
        )
        
        job.mark_symbol_processed(symbol, 150, partition)
        
        assert symbol in job.processed_symbols
        assert job.total_bars_processed == 150
        assert len(job.completed_partitions) == 1
        assert job.progress_percentage == 50.0  # 1 of 2 symbols
    
    def test_cannot_process_symbol_not_in_job(self):
        """Test that symbols not in the job cannot be processed."""
        job = create_test_ingestion_job()
        job.start()
        
        invalid_symbol = Symbol("MSFT")  # Not in job
        partition = IngestionPartition(
            symbol=invalid_symbol,
            file_path=job.configuration.output_path / f"{invalid_symbol.value}.parquet",
            record_count=100,
            file_size_bytes=1024,
            created_at=datetime.now(timezone.utc)
        )
        
        with pytest.raises(ValueError, match="is not part of this job"):
            job.mark_symbol_processed(invalid_symbol, 100, partition)
    
    def test_cannot_process_same_symbol_twice(self):
        """Test that the same symbol cannot be processed twice."""
        job = create_test_ingestion_job()
        job.start()
        
        symbol = job.symbols[0]
        partition = IngestionPartition(
            symbol=symbol,
            file_path=job.configuration.output_path / f"{symbol.value}.parquet",
            record_count=100,
            file_size_bytes=1024,
            created_at=datetime.now(timezone.utc)
        )
        
        job.mark_symbol_processed(symbol, 100, partition)
        
        with pytest.raises(ValueError, match="already processed"):
            job.mark_symbol_processed(symbol, 50, partition)
    
    def test_calculates_progress_percentage_correctly(self):
        """Test that progress percentage is calculated correctly."""
        job = create_test_ingestion_job()
        job.start()
        
        assert job.progress_percentage == 0.0
        
        # Process first symbol
        symbol1 = job.symbols[0]
        partition1 = IngestionPartition(
            symbol=symbol1,
            file_path=job.configuration.output_path / f"{symbol1.value}.parquet",
            record_count=100,
            file_size_bytes=1024,
            created_at=datetime.now(timezone.utc)
        )
        job.mark_symbol_processed(symbol1, 100, partition1)
        
        assert job.progress_percentage == 50.0  # 1 of 2 symbols
        
        # Process second symbol
        symbol2 = job.symbols[1]
        partition2 = IngestionPartition(
            symbol=symbol2,
            file_path=job.configuration.output_path / f"{symbol2.value}.parquet",
            record_count=200,
            file_size_bytes=2048,
            created_at=datetime.now(timezone.utc)
        )
        job.mark_symbol_processed(symbol2, 200, partition2)
        
        assert job.progress_percentage == 100.0  # All symbols processed
        assert job.state == ProcessingState.COMPLETED


class TestIngestionJobDomainEvents:
    """Test that ingestion jobs emit proper domain events."""
    
    def test_emits_job_started_event_when_started(self):
        """Test that starting a job emits IngestionJobStarted event."""
        job = create_test_ingestion_job()
        
        job.start()
        
        events = job.domain_events
        assert len(events) == 1
        assert isinstance(events[0], IngestionJobStarted)
        
        event = events[0]
        assert event.job_id == job.job_id
        assert event.symbols == job.symbols
        assert event.time_range == job.time_range
    
    def test_emits_batch_processed_event_when_symbol_completed(self):
        """Test that processing a symbol emits IngestionBatchProcessed event."""
        job = create_test_ingestion_job()
        job.start()
        job.clear_domain_events()  # Clear start event
        
        symbol = job.symbols[0]
        partition = IngestionPartition(
            symbol=symbol,
            file_path=job.configuration.output_path / f"{symbol.value}.parquet",
            record_count=100,
            file_size_bytes=1024,
            created_at=datetime.now(timezone.utc)
        )
        
        job.mark_symbol_processed(symbol, 100, partition)
        
        events = job.domain_events
        assert len(events) == 1
        assert isinstance(events[0], IngestionBatchProcessed)
        
        event = events[0]
        assert event.job_id == job.job_id
        assert event.symbol == symbol
        assert event.bars_processed == 100
        assert event.partition == partition
    
    def test_emits_job_completed_event_when_job_finishes(self):
        """Test that completing a job emits IngestionJobCompleted event."""
        job = create_test_ingestion_job()
        job.start()
        
        # Process all symbols to trigger completion
        for symbol in job.symbols:
            partition = IngestionPartition(
                symbol=symbol,
                file_path=job.configuration.output_path / f"{symbol.value}.parquet",
                record_count=100,
                file_size_bytes=1024,
                created_at=datetime.now(timezone.utc)
            )
            job.mark_symbol_processed(symbol, 100, partition)
        
        # Find the completion event
        completion_events = [
            event for event in job.domain_events
            if isinstance(event, IngestionJobCompleted)
        ]
        
        assert len(completion_events) == 1
        event = completion_events[0]
        assert event.job_id == job.job_id
        assert event.symbols_processed == 2
        assert event.total_bars_processed == 200
        assert event.partitions_created == 2
    
    def test_emits_job_failed_event_when_job_fails(self):
        """Test that failing a job emits IngestionJobFailed event."""
        job = create_test_ingestion_job()
        job.start()
        job.clear_domain_events()  # Clear start event
        
        error_message = "Connection timeout"
        job.fail(error_message)
        
        events = job.domain_events
        assert len(events) == 1
        assert isinstance(events[0], IngestionJobFailed)
        
        event = events[0]
        assert event.job_id == job.job_id
        assert event.error_message == error_message
        assert event.symbols_processed == 0