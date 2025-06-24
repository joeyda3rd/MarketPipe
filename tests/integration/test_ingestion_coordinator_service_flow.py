# SPDX-License-Identifier: Apache-2.0
"""Integration tests for IngestionCoordinatorService end-to-end flow."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.application.commands import (
    CreateIngestionJobCommand,
    StartJobCommand,
)
from marketpipe.ingestion.application.services import (
    IngestionCoordinatorService,
    IngestionJobService,
)
from marketpipe.ingestion.domain.services import (
    IngestionDomainService,
    IngestionProgressTracker,
)
from marketpipe.ingestion.domain.value_objects import (
    BatchConfiguration,
    IngestionConfiguration,
)
from marketpipe.ingestion.infrastructure.parquet_storage import ParquetDataStorage
from tests.fakes.adapters import FakeMarketDataAdapter, create_test_ohlcv_bars
from tests.fakes.events import FakeEventPublisher
from tests.fakes.repositories import (
    FakeIngestionCheckpointRepository,
    FakeIngestionJobRepository,
    FakeIngestionMetricsRepository,
)
from tests.fakes.validators import FakeDataValidator


def create_test_configuration(output_path: Path) -> IngestionConfiguration:
    """Create test ingestion configuration."""
    return IngestionConfiguration(
        output_path=output_path,
        compression="zstd",
        max_workers=1,
        batch_size=1000,
        rate_limit_per_minute=200,
        feed_type="iex",
    )


def create_test_batch_configuration() -> BatchConfiguration:
    """Create test batch configuration."""
    return BatchConfiguration(
        symbols_per_batch=10,
        retry_attempts=3,
        retry_delay_seconds=1.0,
        timeout_seconds=30.0,
    )


def create_recent_time_range() -> TimeRange:
    """Create a time range using recent dates to avoid 730-day validation limit."""
    # Use a date from 10 days ago to avoid the 730-day limit
    base_date = datetime.now(timezone.utc) - timedelta(days=10)
    start_time = base_date.replace(hour=13, minute=30, second=0, microsecond=0)
    end_time = base_date.replace(hour=14, minute=30, second=0, microsecond=0)

    return TimeRange(start=Timestamp(start_time), end=Timestamp(end_time))


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
        event_publisher=event_publisher,
    )

    data_validator = FakeDataValidator()
    data_storage = ParquetDataStorage(root=tmp_path / "storage")

    # Create coordinator service (with simplified dependencies for testing)
    coordinator_service = IngestionCoordinatorService(
        job_service=job_service,
        job_repository=job_repository,
        checkpoint_repository=checkpoint_repository,
        metrics_repository=metrics_repository,
        market_data_provider=market_data_adapter,
        data_validator=data_validator,
        data_storage=data_storage,
        event_publisher=event_publisher,
    )

    return {
        "job_service": job_service,
        "coordinator_service": coordinator_service,
        "job_repository": job_repository,
        "checkpoint_repository": checkpoint_repository,
        "metrics_repository": metrics_repository,
        "market_data_adapter": market_data_adapter,
        "event_publisher": event_publisher,
        "data_storage": data_storage,
    }


class TestIngestionCoordinatorEndToEndFlow:
    """Test the complete end-to-end ingestion flow using domain services."""

    @pytest.mark.asyncio
    async def test_coordinator_handles_successful_symbol_ingestion(
        self, ingestion_services, tmp_path
    ):
        """Test that coordinator successfully handles symbol ingestion end-to-end."""
        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]
        event_publisher = services["event_publisher"]

        # Setup test data
        symbol = Symbol("AAPL")
        time_range = create_recent_time_range()
        test_bars = create_test_ohlcv_bars(symbol, count=10, start_time=time_range.start.value)
        market_data_adapter.set_bars_data(symbol, test_bars)

        # Create ingestion job

        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await job_service.create_job(command)

        # Execute the job through the coordinator
        coordinator_service = services["coordinator_service"]
        result = await coordinator_service.execute_job(job_id)

        # Verify job was executed successfully
        job = await job_repository.get_by_id(job_id)
        assert job is not None
        assert job.symbols == [symbol]

        # Verify market data adapter was called
        fetch_calls = market_data_adapter.get_fetch_calls()
        assert len(fetch_calls) >= 1
        assert symbol in [call[0] for call in fetch_calls]

        # Verify execution result
        assert result["status"] == "completed"
        assert result["symbols_processed"] >= 1

        # Verify domain events were published
        from marketpipe.ingestion.domain.events import IngestionJobStarted

        assert event_publisher.has_event_of_type(IngestionJobStarted)

        start_events = event_publisher.get_events_of_type(IngestionJobStarted)
        assert len(start_events) == 1
        assert start_events[0].job_id == job_id
        assert start_events[0].symbols == [symbol]

        await asyncio.sleep(0.1)  # Allow time for any fire-and-forget tasks to complete

    @pytest.mark.asyncio
    async def test_coordinator_handles_multiple_symbols_correctly(
        self, ingestion_services, tmp_path
    ):
        """Coordinator should ingest multiple symbols concurrently without errors."""

        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]

        # Setup test data for multiple symbols
        symbols = [Symbol("AAPL"), Symbol("GOOGL")]
        time_range = create_recent_time_range()
        for symbol in symbols:
            test_bars = create_test_ohlcv_bars(symbol, count=5, start_time=time_range.start.value)
            market_data_adapter.set_bars_data(symbol, test_bars)

        # Create ingestion job with multiple symbols

        command = CreateIngestionJobCommand(
            symbols=symbols,
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await job_service.create_job(command)

        # Execute the job through the coordinator
        coordinator_service = services["coordinator_service"]
        result = await coordinator_service.execute_job(job_id)

        # Verify job configuration
        job = await job_repository.get_by_id(job_id)
        assert job.symbols == symbols

        # Verify market data was requested for all symbols
        fetch_calls = market_data_adapter.get_fetch_calls()
        fetched_symbols = [call[0] for call in fetch_calls]
        for symbol in symbols:
            assert symbol in fetched_symbols

        # Verify execution result
        assert result["status"] == "completed"
        assert result["symbols_processed"] >= len(symbols)

        # Wait for any pending metrics tasks to complete before test ends
        await asyncio.sleep(0.1)  # Allow time for any fire-and-forget tasks to complete

    @pytest.mark.asyncio
    async def test_coordinator_handles_failed_symbols_gracefully(
        self, ingestion_services, tmp_path
    ):
        """Test that coordinator handles failed symbols without stopping other processing."""
        services = ingestion_services
        job_service = services["job_service"]
        market_data_adapter = services["market_data_adapter"]

        # Setup test data - one symbol succeeds, one fails
        working_symbol = Symbol("AAPL")
        failing_symbol = Symbol("GOOGL")

        time_range = create_recent_time_range()
        test_bars = create_test_ohlcv_bars(working_symbol, count=5, start_time=time_range.start.value)
        market_data_adapter.set_bars_data(working_symbol, test_bars)

        # Configure adapter to fail for GOOGL only
        market_data_adapter.set_symbol_failure(failing_symbol)

        # Create ingestion job

        command = CreateIngestionJobCommand(
            symbols=[working_symbol, failing_symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await job_service.create_job(command)

        # Execute the job through the coordinator (it should handle failures gracefully)
        coordinator_service = services["coordinator_service"]
        result = await coordinator_service.execute_job(job_id)

        # Verify market data adapter was called for both symbols
        fetch_calls = market_data_adapter.get_fetch_calls()
        assert len(fetch_calls) >= 1  # At least one call should have been made

        # Verify that at least the working symbol was processed
        assert result["symbols_processed"] >= 1
        assert result["symbols_failed"] >= 1  # GOOGL should have failed

        await asyncio.sleep(0.1)  # Allow time for any fire-and-forget tasks to complete

    @pytest.mark.asyncio
    async def test_coordinator_uses_checkpoints_for_resumable_operations(
        self, ingestion_services, tmp_path
    ):
        """Test that coordinator properly uses checkpoints for resumable operations."""
        services = ingestion_services
        job_service = services["job_service"]
        checkpoint_repository = services["checkpoint_repository"]
        market_data_adapter = services["market_data_adapter"]

        # Setup test data
        symbol = Symbol("AAPL")
        time_range = create_recent_time_range()
        test_bars = create_test_ohlcv_bars(symbol, count=10, start_time=time_range.start.value)
        market_data_adapter.set_bars_data(symbol, test_bars)

        # Create ingestion job
        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await job_service.create_job(command)

        # Simulate a checkpoint being saved from a previous run
        from marketpipe.ingestion.domain.value_objects import IngestionCheckpoint

        checkpoint = IngestionCheckpoint(
            symbol=symbol,
            last_processed_timestamp=int(time_range.start.value.timestamp() * 1_000_000_000),
            records_processed=5,
            updated_at=datetime.now(timezone.utc),
        )
        await checkpoint_repository.save_checkpoint(job_id, checkpoint)

        # Execute the job through the coordinator
        coordinator_service = services["coordinator_service"]
        result = await coordinator_service.execute_job(job_id)

        # Verify checkpoint was retrieved and updated after job execution
        retrieved_checkpoint = await checkpoint_repository.get_checkpoint(job_id, symbol)
        assert retrieved_checkpoint is not None
        assert retrieved_checkpoint.symbol == symbol
        assert retrieved_checkpoint.records_processed == 10  # Updated after processing all test bars

        # Verify job execution was successful
        assert result["status"] == "completed"
        assert result["symbols_processed"] >= 1

        await asyncio.sleep(0.1)  # Allow time for any fire-and-forget tasks to complete

    @pytest.mark.asyncio
    async def test_coordinator_creates_proper_partition_paths(self, ingestion_services, tmp_path):
        """Test that coordinator creates partitions with proper Hive-style paths."""
        services = ingestion_services
        job_service = services["job_service"]
        job_repository = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]

        # Setup test data
        symbol = Symbol("AAPL")
        time_range = create_recent_time_range()
        start_time = time_range.start.value
        test_bars = create_test_ohlcv_bars(symbol, count=10, start_time=start_time)
        market_data_adapter.set_bars_data(symbol, test_bars)

        # Create ingestion job with recent date

        output_path = tmp_path / "data"
        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(output_path),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await job_service.create_job(command)

        # Execute the job through the coordinator
        coordinator_service = services["coordinator_service"]
        result = await coordinator_service.execute_job(job_id)

        # Get the updated job after execution
        job = await job_repository.get_by_id(job_id)

        # Verify execution was successful
        assert result["status"] == "completed"
        assert result["symbols_processed"] >= 1

        # Verify partition was created with correct path structure
        assert len(job.completed_partitions) >= 1

        created_partition = job.completed_partitions[0]
        assert created_partition.symbol == symbol
        assert created_partition.record_count == len(test_bars)

        # Verify the partition path contains the expected structure
        partition_path_str = str(created_partition.file_path)
        assert f"symbol={symbol.value}" in partition_path_str
        # The exact year/month/day format might vary based on the ParquetStorageEngine implementation

        await asyncio.sleep(0.1)  # Allow time for any fire-and-forget tasks to complete

    @pytest.mark.asyncio
    async def test_coordinator_emits_comprehensive_domain_events(
        self, ingestion_services, tmp_path
    ):
        """Test that coordinator emits all expected domain events during processing."""
        services = ingestion_services
        job_service = services["job_service"]
        market_data_adapter = services["market_data_adapter"]
        event_publisher = services["event_publisher"]

        # Setup test data
        symbol = Symbol("AAPL")
        time_range = create_recent_time_range()
        test_bars = create_test_ohlcv_bars(symbol, count=10, start_time=time_range.start.value)
        market_data_adapter.set_bars_data(symbol, test_bars)

        # Create and execute ingestion job

        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await job_service.create_job(command)

        # Execute the job through the coordinator to generate all events
        coordinator_service = services["coordinator_service"]
        result = await coordinator_service.execute_job(job_id)

        # Verify execution was successful
        assert result["status"] == "completed"
        assert result["symbols_processed"] >= 1

        # Verify all expected domain events were emitted
        from marketpipe.ingestion.domain.events import (
            IngestionBatchProcessed,
            IngestionJobCompleted,
            IngestionJobStarted,
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

        await asyncio.sleep(0.1)  # Allow time for any fire-and-forget tasks to complete

    def test_process_symbol_writes_parquet_partition(self, ingestion_services, tmp_path):
        """Coordinator should write Parquet partition via storage service."""
        services = ingestion_services
        job_service = services["job_service"]
        coordinator = services["coordinator_service"]
        job_repo = services["job_repository"]
        market_data_adapter = services["market_data_adapter"]

        symbol = Symbol("AAPL")

        now = datetime.now(timezone.utc)
        start_time = now.replace(hour=13, minute=30, second=0, microsecond=0) - timedelta(days=1)
        end_time = start_time + timedelta(hours=1)
        time_range = TimeRange(start=Timestamp(start_time), end=Timestamp(end_time))

        # Create test bars with timestamps within the job time range
        bars = create_test_ohlcv_bars(symbol, count=4, start_time=start_time)
        market_data_adapter.set_bars_data(symbol, bars)

        command = CreateIngestionJobCommand(
            symbols=[symbol],
            time_range=time_range,
            configuration=create_test_configuration(tmp_path / "data"),
            batch_config=create_test_batch_configuration(),
        )

        job_id = asyncio.run(job_service.create_job(command))
        asyncio.run(job_service.start_job(StartJobCommand(job_id)))
        job = asyncio.run(job_repo.get_by_id(job_id))

        count, partition = asyncio.run(coordinator._process_symbol(job, symbol))

        assert count == len(bars)
        assert partition.file_path.exists()
        table = pq.ParquetFile(partition.file_path).read()
        assert table.num_rows == len(bars)
