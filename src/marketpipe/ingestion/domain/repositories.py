# SPDX-License-Identifier: Apache-2.0
"""Ingestion domain repository interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime

from marketpipe.domain.value_objects import Symbol
from .entities import IngestionJob, IngestionJobId, ProcessingState
from .value_objects import IngestionCheckpoint, ProcessingMetrics


class IIngestionJobRepository(ABC):
    """Repository interface for ingestion jobs."""

    @abstractmethod
    async def save(self, job: IngestionJob) -> None:
        """Save an ingestion job."""
        pass

    @abstractmethod
    async def get_by_id(self, job_id: IngestionJobId) -> Optional[IngestionJob]:
        """Retrieve an ingestion job by its ID."""
        pass

    @abstractmethod
    async def get_by_state(self, state: ProcessingState) -> List[IngestionJob]:
        """Get all jobs in a specific state."""
        pass

    @abstractmethod
    async def get_active_jobs(self) -> List[IngestionJob]:
        """Get all jobs that are currently active (pending or in progress)."""
        pass

    @abstractmethod
    async def get_jobs_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[IngestionJob]:
        """Get jobs created within a date range."""
        pass

    @abstractmethod
    async def delete(self, job_id: IngestionJobId) -> bool:
        """Delete an ingestion job. Returns True if job was found and deleted."""
        pass

    @abstractmethod
    async def get_job_history(self, limit: int = 100) -> List[IngestionJob]:
        """Get recent job history, ordered by creation date."""
        pass

    @abstractmethod
    async def count_jobs_by_state(self) -> dict[ProcessingState, int]:
        """Count jobs grouped by their processing state."""
        pass


class IIngestionCheckpointRepository(ABC):
    """Repository interface for ingestion checkpoints."""

    @abstractmethod
    async def save_checkpoint(
        self, job_id: IngestionJobId, checkpoint: IngestionCheckpoint
    ) -> None:
        """Save a checkpoint for a specific job and symbol."""
        pass

    @abstractmethod
    async def get_checkpoint(
        self, job_id: IngestionJobId, symbol: Symbol
    ) -> Optional[IngestionCheckpoint]:
        """Get the latest checkpoint for a job and symbol."""
        pass

    @abstractmethod
    async def get_all_checkpoints(
        self, job_id: IngestionJobId
    ) -> List[IngestionCheckpoint]:
        """Get all checkpoints for a specific job."""
        pass

    @abstractmethod
    async def delete_checkpoints(self, job_id: IngestionJobId) -> None:
        """Delete all checkpoints for a specific job."""
        pass

    @abstractmethod
    async def get_global_checkpoint(
        self, symbol: Symbol
    ) -> Optional[IngestionCheckpoint]:
        """Get the most recent checkpoint for a symbol across all jobs."""
        pass

    @abstractmethod
    async def cleanup_old_checkpoints(self, older_than: datetime) -> int:
        """Remove checkpoints older than the specified date. Returns count of deleted checkpoints."""
        pass


class IIngestionMetricsRepository(ABC):
    """Repository interface for ingestion metrics and performance data."""

    @abstractmethod
    async def save_metrics(
        self, job_id: IngestionJobId, metrics: ProcessingMetrics
    ) -> None:
        """Save processing metrics for a job."""
        pass

    @abstractmethod
    async def get_metrics(self, job_id: IngestionJobId) -> Optional[ProcessingMetrics]:
        """Get metrics for a specific job."""
        pass

    @abstractmethod
    async def get_metrics_history(
        self, start_date: datetime, end_date: datetime
    ) -> List[tuple[IngestionJobId, ProcessingMetrics]]:
        """Get metrics for jobs within a date range."""
        pass

    @abstractmethod
    async def get_average_metrics(
        self, start_date: datetime, end_date: datetime
    ) -> Optional[ProcessingMetrics]:
        """Calculate average metrics across jobs in a date range."""
        pass

    @abstractmethod
    async def get_performance_trends(
        self, days: int = 30
    ) -> List[tuple[datetime, float]]:
        """Get daily average processing performance (bars per second) over time."""
        pass


class IngestionRepositoryError(Exception):
    """Base exception for ingestion repository errors."""

    pass


class IngestionJobNotFoundError(IngestionRepositoryError):
    """Raised when an ingestion job is not found."""

    def __init__(self, job_id: IngestionJobId):
        super().__init__(f"Ingestion job not found: {job_id}")
        self.job_id = job_id


class IngestionCheckpointError(IngestionRepositoryError):
    """Raised when there's an error with checkpoint operations."""

    pass


class IngestionConcurrencyError(IngestionRepositoryError):
    """Raised when there's a concurrency conflict in ingestion operations."""

    pass
