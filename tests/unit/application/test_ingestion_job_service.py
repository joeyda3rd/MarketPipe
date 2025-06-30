# SPDX-License-Identifier: Apache-2.0
"""Unit tests for IngestionJobService application service."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.application.commands import (
    CancelJobCommand,
    CreateIngestionJobCommand,
    StartJobCommand,
)
from marketpipe.ingestion.application.queries import (
    GetActiveJobsQuery,
    GetJobStatusQuery,
)
from marketpipe.ingestion.application.services import IngestionJobService
from marketpipe.ingestion.domain.entities import (
    IngestionJob,
    IngestionJobId,
    ProcessingState,
)
from marketpipe.ingestion.domain.services import (
    IngestionDomainService,
    IngestionProgressTracker,
)
from marketpipe.ingestion.domain.value_objects import (
    BatchConfiguration,
    IngestionConfiguration,
)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from fakes.events import FakeEventPublisher
from fakes.repositories import (
    FakeIngestionCheckpointRepository,
    FakeIngestionJobRepository,
    FakeIngestionMetricsRepository,
)


def create_test_configuration() -> IngestionConfiguration:
    """Create test ingestion configuration."""
    from pathlib import Path

    return IngestionConfiguration(
        output_path=Path("/tmp/test"),
        compression="snappy",
        max_workers=4,
        batch_size=1000,
        rate_limit_per_minute=200,
        feed_type="iex",
    )


def create_test_batch_configuration() -> BatchConfiguration:
    """Create test batch configuration."""
    return BatchConfiguration(
        symbols_per_batch=1000,
        retry_attempts=3,
        retry_delay_seconds=1.0,
        timeout_seconds=30.0,
    )


def create_recent_time_range() -> TimeRange:
    """Create a time range using recent dates to avoid 730-day validation error."""
    # Use a date from 10 days ago to ensure it's within the 730-day limit
    base_date = datetime.now(timezone.utc) - timedelta(days=10)
    start_time = base_date.replace(hour=9, minute=30, second=0, microsecond=0)
    end_time = base_date.replace(hour=16, minute=0, second=0, microsecond=0)

    return TimeRange(start=Timestamp(start_time), end=Timestamp(end_time))


@pytest.fixture
def ingestion_job_service():
    """Create ingestion job service with fake dependencies for testing."""
    job_repo = FakeIngestionJobRepository()
    checkpoint_repo = FakeIngestionCheckpointRepository()
    metrics_repo = FakeIngestionMetricsRepository()
    domain_service = IngestionDomainService()
    progress_tracker = IngestionProgressTracker()
    event_publisher = FakeEventPublisher()

    service = IngestionJobService(
        job_repository=job_repo,
        checkpoint_repository=checkpoint_repo,
        metrics_repository=metrics_repo,
        domain_service=domain_service,
        progress_tracker=progress_tracker,
        event_publisher=event_publisher,
    )

    return service, job_repo, event_publisher


class TestCreateIngestionJob:
    """Test job creation scenarios."""

    @pytest.mark.asyncio
    async def test_creates_job_successfully_with_valid_command(self, ingestion_job_service):
        """Test that a valid job creation command succeeds."""
        service, job_repo, event_publisher = ingestion_job_service

        symbols = [Symbol("AAPL"), Symbol("GOOGL")]
        time_range = create_recent_time_range()

        command = CreateIngestionJobCommand(
            symbols=symbols,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await service.create_job(command)

        # Verify job was created and saved
        assert job_id is not None
        saved_jobs = job_repo.get_saved_jobs()
        assert len(saved_jobs) == 1
        assert saved_jobs[0].job_id == job_id
        assert saved_jobs[0].symbols == symbols
        assert saved_jobs[0].state == ProcessingState.PENDING

        # Note: Job creation doesn't emit events by design - only starting/completing/cancelling do

    @pytest.mark.asyncio
    async def test_validates_job_schedule_against_active_jobs(self, ingestion_job_service):
        """Test that job creation validates against existing active jobs."""
        service, job_repo, event_publisher = ingestion_job_service

        # Create and save an active job with AAPL
        existing_job_id = IngestionJobId.generate()
        existing_symbols = [Symbol("AAPL")]
        time_range = create_recent_time_range()

        existing_job = IngestionJob(
            existing_job_id, create_test_configuration(), existing_symbols, time_range
        )
        existing_job.start()  # Make it active
        await job_repo.save(existing_job)

        # Try to create overlapping job
        overlapping_symbols = [Symbol("AAPL"), Symbol("GOOGL")]  # Overlaps with AAPL
        command = CreateIngestionJobCommand(
            symbols=overlapping_symbols,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )

        with pytest.raises(ValueError, match="scheduling conflicts"):
            await service.create_job(command)

    @pytest.mark.asyncio
    async def test_allows_non_overlapping_jobs(self, ingestion_job_service):
        """Test that non-overlapping jobs can be created simultaneously."""
        service, job_repo, event_publisher = ingestion_job_service

        # Create first job with AAPL
        symbols1 = [Symbol("AAPL")]
        time_range = create_recent_time_range()

        command1 = CreateIngestionJobCommand(
            symbols=symbols1,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )

        job_id1 = await service.create_job(command1)

        # Start the first job to make it active
        await service.start_job(StartJobCommand(job_id1))

        # Create second job with different symbols (GOOGL)
        symbols2 = [Symbol("GOOGL")]
        command2 = CreateIngestionJobCommand(
            symbols=symbols2,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )

        job_id2 = await service.create_job(command2)

        # Both jobs should exist
        saved_jobs = job_repo.get_saved_jobs()
        assert len(saved_jobs) == 2
        assert job_id1 != job_id2


class TestStartIngestionJob:
    """Test job starting scenarios."""

    @pytest.mark.asyncio
    async def test_starts_pending_job_successfully(self, ingestion_job_service):
        """Test that a pending job can be started successfully."""
        service, job_repo, event_publisher = ingestion_job_service

        # Create a job first
        symbols = [Symbol("AAPL")]
        time_range = create_recent_time_range()

        command = CreateIngestionJobCommand(
            symbols=symbols,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await service.create_job(command)
        event_publisher.clear_events()  # Clear creation events

        # Start the job
        await service.start_job(StartJobCommand(job_id))

        # Verify job state changed
        job = await job_repo.get_by_id(job_id)
        assert job.state == ProcessingState.IN_PROGRESS
        assert job.started_at is not None

        # Verify start event was published
        from marketpipe.ingestion.domain.events import IngestionJobStarted

        assert event_publisher.has_event_of_type(IngestionJobStarted)

    @pytest.mark.asyncio
    async def test_fails_to_start_nonexistent_job(self, ingestion_job_service):
        """Test that starting a non-existent job fails."""
        service, job_repo, event_publisher = ingestion_job_service

        non_existent_job_id = IngestionJobId.generate()

        from marketpipe.ingestion.domain.repositories import IngestionJobNotFoundError

        with pytest.raises(IngestionJobNotFoundError):
            await service.start_job(StartJobCommand(non_existent_job_id))


class TestCancelIngestionJob:
    """Test job cancellation scenarios."""

    @pytest.mark.asyncio
    async def test_cancels_pending_job_successfully(self, ingestion_job_service):
        """Test that a pending job can be cancelled."""
        service, job_repo, event_publisher = ingestion_job_service

        # Create a job
        symbols = [Symbol("AAPL")]
        time_range = create_recent_time_range()

        command = CreateIngestionJobCommand(
            symbols=symbols,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await service.create_job(command)
        event_publisher.clear_events()  # Clear creation events

        # Cancel the job
        await service.cancel_job(CancelJobCommand(job_id))

        # Verify job state changed
        job = await job_repo.get_by_id(job_id)
        assert job.state == ProcessingState.CANCELLED

        # Verify cancellation event was published
        from marketpipe.ingestion.domain.events import IngestionJobCancelled

        assert event_publisher.has_event_of_type(IngestionJobCancelled)


class TestGetJobStatus:
    """Test job status query scenarios."""

    @pytest.mark.asyncio
    async def test_returns_job_status_for_existing_job(self, ingestion_job_service):
        """Test that job status is returned for existing jobs."""
        service, job_repo, event_publisher = ingestion_job_service

        # Create and start a job
        symbols = [Symbol("AAPL")]
        time_range = create_recent_time_range()

        command = CreateIngestionJobCommand(
            symbols=symbols,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )

        job_id = await service.create_job(command)
        await service.start_job(StartJobCommand(job_id))

        # Get job status
        query = GetJobStatusQuery(job_id)
        status = await service.get_job_status(query)

        assert status is not None
        assert status["job_id"] == str(job_id)
        assert status["state"] == "in_progress"
        assert status["symbols_total"] == 1
        assert status["symbols_processed"] == 0
        assert status["progress_percentage"] == 0.0

    @pytest.mark.asyncio
    async def test_returns_none_for_nonexistent_job(self, ingestion_job_service):
        """Test that None is returned for non-existent jobs."""
        service, job_repo, event_publisher = ingestion_job_service

        non_existent_job_id = IngestionJobId.generate()
        query = GetJobStatusQuery(non_existent_job_id)
        status = await service.get_job_status(query)

        assert status is None


class TestGetActiveJobs:
    """Test active jobs query scenarios."""

    @pytest.mark.asyncio
    async def test_returns_only_active_jobs(self, ingestion_job_service):
        """Test that only active jobs are returned."""
        service, job_repo, event_publisher = ingestion_job_service

        symbols = [Symbol("AAPL")]
        time_range = create_recent_time_range()

        # Create pending job
        command = CreateIngestionJobCommand(
            symbols=symbols,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )
        pending_job_id = await service.create_job(command)

        # Create and start active job
        symbols2 = [Symbol("GOOGL")]
        command2 = CreateIngestionJobCommand(
            symbols=symbols2,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )
        active_job_id = await service.create_job(command2)
        await service.start_job(StartJobCommand(active_job_id))

        # Create and complete a job
        symbols3 = [Symbol("MSFT")]
        command3 = CreateIngestionJobCommand(
            symbols=symbols3,
            time_range=time_range,
            configuration=create_test_configuration(),
            batch_config=create_test_batch_configuration(),
        )
        completed_job_id = await service.create_job(command3)
        completed_job = await job_repo.get_by_id(completed_job_id)
        completed_job.start()

        # Mark the symbol as processed so the job can be completed
        from pathlib import Path

        from marketpipe.ingestion.domain.value_objects import IngestionPartition

        test_partition = IngestionPartition(
            symbol=symbols3[0],
            file_path=Path("/tmp/test.parquet"),
            record_count=100,
            file_size_bytes=1024,
            created_at=datetime.now(timezone.utc),
        )
        completed_job.mark_symbol_processed(symbols3[0], 100, test_partition)
        await job_repo.save(completed_job)

        # Get active jobs
        query = GetActiveJobsQuery()
        active_jobs = await service.get_active_jobs(query)

        # Should only return pending and active jobs, not completed
        assert len(active_jobs) == 2
        active_job_ids = [job["job_id"] for job in active_jobs]
        assert str(pending_job_id) in active_job_ids
        assert str(active_job_id) in active_job_ids
        assert str(completed_job_id) not in active_job_ids
