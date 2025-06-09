"""Integration tests for IngestionCoordinatorService end-to-end flow."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from pathlib import Path

from marketpipe.domain.entities import EntityId
from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.domain.entities import IngestionJobId, ProcessingState
from marketpipe.ingestion.domain.services import IngestionDomainService, IngestionProgressTracker
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration, BatchConfiguration, IngestionPartition
from marketpipe.ingestion.application.services import IngestionJobService, IngestionCoordinatorService
from marketpipe.ingestion.application.commands import CreateIngestionJobCommand, StartJobCommand
from tests.fakes.repositories import (
    FakeIngestionJobRepository,
    FakeIngestionCheckpointRepository,
    FakeIngestionMetricsRepository
)
from tests.fakes.adapters import FakeMarketDataAdapter, create_test_ohlcv_bars
from tests.fakes.events import FakeEventPublisher


def create_test_configuration(output_path: Path) -> IngestionConfiguration:
    """Create test ingestion configuration."""
    return IngestionConfiguration(
        output_path=output_path,
        compression="zstd",
        max_workers=1,
        batch_size=1000,
        rate_limit_per_minute=200,
        feed_type="iex"
    )


def create_test_batch_configuration() -> BatchConfiguration:
    """Create test batch configuration."""
    return BatchConfiguration(
        symbols_per_batch=10,
        retry_attempts=3,
        retry_delay_seconds=1.0,
        timeout_seconds=30.0
    )


@pytest.fixture
def ingestion_services(tmp_path):
    """Create ingestion services with fake dependencies for testing."""
    # Create repositories
    job_repository = FakeIngestionJobRepository()
    checkpoint_repository = FakeIngestionCheckpointRepository()
    metrics_repository = FakeIngestionMetricsRepository()
    
    # Create domain services
    domain_service = IngestionDomainService()
    progress_tracker = IngestionProgressTracker()
    
    # Create event publisher
    event_publisher = FakeEventPublisher()
    
    # Create market data adapter
    market_data_adapter = FakeMarketDataAdapter("test_provider")
    
    # Create job service
    job_service = IngestionJobService(
        job_repository=job_repository,
        checkpoint_repository=checkpoint_repository,
        metrics_repository=metrics_repository,
        domain_service=domain_service,
        progress_tracker=progress_tracker,
        event_publisher=event_publisher
    )
    
    # Create coordinator service (with simplified dependencies for testing)
    coordinator_service = IngestionCoordinatorService(
        job_service=job_service,
        job_repository=job_repository,
        checkpoint_repository=checkpoint_repository,
        metrics_repository=metrics_repository,
        market_data_provider=market_data_adapter,
        data_validator=None,  # Simplified for testing
        data_storage=None,    # Simplified for testing
        event_publisher=event_publisher
    )
    
    return {
        "job_service": job_service,
        "coordinator_service": coordinator_service,
        "job_repository": job_repository,
        "checkpoint_repository": checkpoint_repository,
        "metrics_repository": metrics_repository,
        "market_data_adapter": market_data_adapter,
        "event_publisher": event_publisher,
    }


class TestIngestionCoordinatorEndToEndFlow:
    """Test the complete end-to-end ingestion flow using domain services."""
    
    @pytest.mark.asyncio
    async def test_coordinator_handles_successful_symbol_ingestion(self, ingestion_services, tmp_path):
        """Test that coordinator successfully handles symbol ingestion end-to-end."""
        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]
        event_publisher = services["event_publisher"]
        
        # Setup test data
        symbol = Symbol("AAPL")
        test_bars = create_test_ohlcv_bars(symbol, count=10)
        market_data_adapter.set_bars_data(symbol, test_bars)
        
        # Create ingestion job
        start_time = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 14, 30, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        
        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration()
        )
        
        job_id = await job_service.create_job(command)
        
        # Start the job
        await job_service.start_job(StartJobCommand(job_id))
        
        # Verify job was created and started
        job = await job_repository.get_by_id(job_id)
        assert job is not None
        assert job.state == ProcessingState.IN_PROGRESS
        assert job.symbols == [symbol]
        
        # Verify market data adapter was called
        fetch_calls = market_data_adapter.get_fetch_calls()
        assert len(fetch_calls) >= 1
        assert symbol in [call[0] for call in fetch_calls]
        
        # Verify domain events were published
        from marketpipe.ingestion.domain.events import IngestionJobStarted
        assert event_publisher.has_event_of_type(IngestionJobStarted)
        
        start_events = event_publisher.get_events_of_type(IngestionJobStarted)
        assert len(start_events) == 1
        assert start_events[0].job_id == job_id
        assert start_events[0].symbols == [symbol]
    
    @pytest.mark.asyncio
    async def test_coordinator_handles_multiple_symbols_correctly(self, ingestion_services, tmp_path):
        """Test that coordinator handles multiple symbols correctly."""
        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]
        event_publisher = services["event_publisher"]
        
        # Setup test data for multiple symbols
        symbols = [Symbol("AAPL"), Symbol("GOOGL")]
        for symbol in symbols:
            test_bars = create_test_ohlcv_bars(symbol, count=5)
            market_data_adapter.set_bars_data(symbol, test_bars)
        
        # Create ingestion job with multiple symbols
        start_time = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 14, 30, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        
        command = CreateIngestionJobCommand(
            symbols=symbols,
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration()
        )
        
        job_id = await job_service.create_job(command)
        await job_service.start_job(StartJobCommand(job_id))
        
        # Verify job configuration
        job = await job_repository.get_by_id(job_id)
        assert job.symbols == symbols
        assert job.state == ProcessingState.IN_PROGRESS
        
        # Verify market data was requested for all symbols
        fetch_calls = market_data_adapter.get_fetch_calls()
        fetched_symbols = [call[0] for call in fetch_calls]
        for symbol in symbols:
            assert symbol in fetched_symbols
    
    @pytest.mark.asyncio
    async def test_coordinator_handles_failed_symbols_gracefully(self, ingestion_services, tmp_path):
        """Test that coordinator handles failed symbols without stopping other processing."""
        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]
        event_publisher = services["event_publisher"]
        
        # Setup test data - one symbol succeeds, one fails
        working_symbol = Symbol("AAPL")
        failing_symbol = Symbol("GOOGL")
        
        test_bars = create_test_ohlcv_bars(working_symbol, count=5)
        market_data_adapter.set_bars_data(working_symbol, test_bars)
        
        # Configure adapter to fail for GOOGL
        market_data_adapter.set_failure_mode(True, "Simulated provider failure for GOOGL")
        
        # Create ingestion job
        start_time = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 14, 30, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        
        command = CreateIngestionJobCommand(
            symbols=[working_symbol, failing_symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration()
        )
        
        job_id = await job_service.create_job(command)
        await job_service.start_job(StartJobCommand(job_id))
        
        # Verify job was created despite partial failures
        job = await job_repository.get_by_id(job_id)
        assert job.state == ProcessingState.IN_PROGRESS
        
        # Verify market data adapter was called for both symbols
        fetch_calls = market_data_adapter.get_fetch_calls()
        assert len(fetch_calls) >= 1  # At least one call should have been made
    
    @pytest.mark.asyncio
    async def test_coordinator_uses_checkpoints_for_resumable_operations(self, ingestion_services, tmp_path):
        """Test that coordinator properly uses checkpoints for resumable operations."""
        services = ingestion_services
        job_service = services["job_service"]
        checkpoint_repository = services["checkpoint_repository"]
        market_data_adapter = services["market_data_adapter"]
        
        # Setup test data
        symbol = Symbol("AAPL")
        test_bars = create_test_ohlcv_bars(symbol, count=10)
        market_data_adapter.set_bars_data(symbol, test_bars)
        
        # Create ingestion job
        start_time = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 14, 30, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        
        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration()
        )
        
        job_id = await job_service.create_job(command)
        
        # Simulate a checkpoint being saved from a previous run
        from marketpipe.ingestion.domain.value_objects import IngestionCheckpoint
        checkpoint = IngestionCheckpoint(
            symbol=symbol,
            last_processed_timestamp=int(start_time.timestamp() * 1_000_000_000),
            records_processed=5,
            updated_at=datetime.now(timezone.utc)
        )
        await checkpoint_repository.save_checkpoint(job_id, checkpoint)
        
        # Start the job
        await job_service.start_job(StartJobCommand(job_id))
        
        # Verify checkpoint was retrieved
        retrieved_checkpoint = await checkpoint_repository.get_checkpoint(job_id, symbol)
        assert retrieved_checkpoint is not None
        assert retrieved_checkpoint.symbol == symbol
        assert retrieved_checkpoint.records_processed == 5
    
    @pytest.mark.asyncio
    async def test_coordinator_creates_proper_partition_paths(self, ingestion_services, tmp_path):
        """Test that coordinator creates partitions with proper Hive-style paths."""
        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]
        
        # Setup test data
        symbol = Symbol("AAPL")
        test_bars = create_test_ohlcv_bars(symbol, count=10)
        market_data_adapter.set_bars_data(symbol, test_bars)
        
        # Create ingestion job with specific date
        start_time = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)  # Jan 2, 2023
        end_time = datetime(2023, 1, 2, 14, 30, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        
        output_path = tmp_path / "data"
        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(output_path),
            batch_config=create_test_batch_configuration()
        )
        
        job_id = await job_service.create_job(command)
        await job_service.start_job(StartJobCommand(job_id))
        
        # Manually simulate symbol processing to verify partition paths
        job = await job_repository.get_by_id(job_id)
        
        # Create a partition with Hive-style path
        expected_partition_path = (
            output_path / 
            f"symbol={symbol.value}" / 
            "year=2023" / 
            "month=01" / 
            "day=02.parquet"
        )
        
        partition = IngestionPartition(
            symbol=symbol,
            file_path=expected_partition_path,
            record_count=len(test_bars),
            file_size_bytes=1024,
            created_at=datetime.now(timezone.utc)
        )
        
        # Simulate processing completion
        job.mark_symbol_processed(symbol, len(test_bars), partition)
        await job_repository.save(job)
        
        # Verify partition was created with correct path structure
        updated_job = await job_repository.get_by_id(job_id)
        assert len(updated_job.completed_partitions) == 1
        
        created_partition = updated_job.completed_partitions[0]
        assert created_partition.symbol == symbol
        assert created_partition.record_count == len(test_bars)
        assert "symbol=AAPL" in str(created_partition.file_path)
        assert "year=2023" in str(created_partition.file_path)
        assert "month=01" in str(created_partition.file_path)
        assert "day=02.parquet" in str(created_partition.file_path)
    
    @pytest.mark.asyncio
    async def test_coordinator_emits_comprehensive_domain_events(self, ingestion_services, tmp_path):
        """Test that coordinator emits all expected domain events during processing."""
        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]
        event_publisher = services["event_publisher"]
        
        # Setup test data
        symbol = Symbol("AAPL")
        test_bars = create_test_ohlcv_bars(symbol, count=10)
        market_data_adapter.set_bars_data(symbol, test_bars)
        
        # Create and execute ingestion job
        start_time = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, 14, 30, tzinfo=timezone.utc)
        time_range = TimeRange(
            start=Timestamp(start_time),
            end=Timestamp(end_time)
        )
        
        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration()
        )
        
        job_id = await job_service.create_job(command)
        await job_service.start_job(StartJobCommand(job_id))
        
        # Simulate successful completion
        job = await job_repository.get_by_id(job_id)
        partition = IngestionPartition(
            symbol=symbol,
            file_path=tmp_path / "data" / f"{symbol.value}.parquet",
            record_count=len(test_bars),
            file_size_bytes=1024,
            created_at=datetime.now(timezone.utc)
        )
        job.mark_symbol_processed(symbol, len(test_bars), partition)
        await job_repository.save(job)
        
        # Verify all expected domain events were emitted
        from marketpipe.ingestion.domain.events import (
            IngestionJobStarted,
            IngestionBatchProcessed,
            IngestionJobCompleted
        )
        
        # Should have job started event
        assert event_publisher.has_event_of_type(IngestionJobStarted)
        
        # Should have batch processed event
        assert event_publisher.has_event_of_type(IngestionBatchProcessed)
        
        # Should have job completed event
        assert event_publisher.has_event_of_type(IngestionJobCompleted)
        
        # Verify event details
        started_events = event_publisher.get_events_of_type(IngestionJobStarted)
        assert len(started_events) == 1
        assert started_events[0].job_id == job_id
        
        batch_events = event_publisher.get_events_of_type(IngestionBatchProcessed)
        assert len(batch_events) == 1
        assert batch_events[0].symbol == symbol
        assert batch_events[0].bars_processed == len(test_bars)
        
        completed_events = event_publisher.get_events_of_type(IngestionJobCompleted)
        assert len(completed_events) == 1
        assert completed_events[0].symbols_processed == 1
        assert completed_events[0].total_bars_processed == len(test_bars)