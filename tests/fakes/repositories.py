# SPDX-License-Identifier: Apache-2.0
"""Fake repository implementations for testing."""

from __future__ import annotations

from typing import Dict, List, Optional
from datetime import datetime

from marketpipe.domain.value_objects import Symbol
from marketpipe.ingestion.domain.entities import (
    IngestionJob,
    IngestionJobId,
    ProcessingState,
)
from marketpipe.ingestion.domain.repositories import (
    IIngestionJobRepository,
    IIngestionCheckpointRepository,
    IIngestionMetricsRepository,
)
from marketpipe.ingestion.domain.value_objects import (
    IngestionCheckpoint,
    ProcessingMetrics,
)


class FakeIngestionJobRepository(IIngestionJobRepository):
    """In-memory fake repository for ingestion jobs."""

    def __init__(self):
        self._jobs: Dict[IngestionJobId, IngestionJob] = {}
        self._save_calls: List[IngestionJobId] = []

    async def save(self, job: IngestionJob) -> None:
        """Save an ingestion job."""
        self._jobs[job.job_id] = job
        self._save_calls.append(job.job_id)

    async def get_by_id(self, job_id: IngestionJobId) -> Optional[IngestionJob]:
        """Retrieve an ingestion job by its ID."""
        return self._jobs.get(job_id)

    async def get_by_state(self, state: ProcessingState) -> List[IngestionJob]:
        """Get all jobs in a specific state."""
        return [job for job in self._jobs.values() if job.state == state]

    async def get_active_jobs(self) -> List[IngestionJob]:
        """Get all jobs that are currently active."""
        active_states = {ProcessingState.PENDING, ProcessingState.IN_PROGRESS}
        return [job for job in self._jobs.values() if job.state in active_states]

    async def get_jobs_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[IngestionJob]:
        """Get jobs created within a date range."""
        return [
            job
            for job in self._jobs.values()
            if start_date <= job.created_at <= end_date
        ]

    async def delete(self, job_id: IngestionJobId) -> bool:
        """Delete an ingestion job."""
        if job_id in self._jobs:
            del self._jobs[job_id]
            return True
        return False

    async def get_job_history(self, limit: int = 100) -> List[IngestionJob]:
        """Get recent job history."""
        jobs = list(self._jobs.values())
        jobs.sort(key=lambda job: job.created_at, reverse=True)
        return jobs[:limit]

    async def count_jobs_by_state(self) -> Dict[ProcessingState, int]:
        """Count jobs grouped by their processing state."""
        counts = {}
        for state in ProcessingState:
            counts[state] = len(
                [job for job in self._jobs.values() if job.state == state]
            )
        return counts

    # Test helpers
    def get_saved_jobs(self) -> List[IngestionJob]:
        """Get all saved jobs (for testing)."""
        return list(self._jobs.values())

    def get_save_calls(self) -> List[IngestionJobId]:
        """Get list of job IDs that were saved (for testing)."""
        return self._save_calls.copy()

    def clear(self) -> None:
        """Clear all data (for testing)."""
        self._jobs.clear()
        self._save_calls.clear()


class FakeIngestionCheckpointRepository(IIngestionCheckpointRepository):
    """In-memory fake repository for ingestion checkpoints."""

    def __init__(self):
        self._checkpoints: Dict[tuple[IngestionJobId, Symbol], IngestionCheckpoint] = {}
        self._save_calls: List[tuple[IngestionJobId, IngestionCheckpoint]] = []

    async def save_checkpoint(
        self, job_id: IngestionJobId, checkpoint: IngestionCheckpoint
    ) -> None:
        """Save a checkpoint for a specific job and symbol."""
        key = (job_id, checkpoint.symbol)
        self._checkpoints[key] = checkpoint
        self._save_calls.append((job_id, checkpoint))

    async def get_checkpoint(
        self, job_id: IngestionJobId, symbol: Symbol
    ) -> Optional[IngestionCheckpoint]:
        """Get the latest checkpoint for a job and symbol."""
        key = (job_id, symbol)
        return self._checkpoints.get(key)

    async def get_all_checkpoints(
        self, job_id: IngestionJobId
    ) -> List[IngestionCheckpoint]:
        """Get all checkpoints for a specific job."""
        return [
            checkpoint
            for (jid, symbol), checkpoint in self._checkpoints.items()
            if jid == job_id
        ]

    async def delete_checkpoints(self, job_id: IngestionJobId) -> None:
        """Delete all checkpoints for a specific job."""
        keys_to_delete = [key for key in self._checkpoints.keys() if key[0] == job_id]
        for key in keys_to_delete:
            del self._checkpoints[key]

    async def get_global_checkpoint(
        self, symbol: Symbol
    ) -> Optional[IngestionCheckpoint]:
        """Get the most recent checkpoint for a symbol across all jobs."""
        symbol_checkpoints = [
            checkpoint
            for (job_id, sym), checkpoint in self._checkpoints.items()
            if sym == symbol
        ]
        if not symbol_checkpoints:
            return None

        # Return the most recent one
        return max(symbol_checkpoints, key=lambda cp: cp.updated_at)

    async def cleanup_old_checkpoints(self, older_than: datetime) -> int:
        """Remove checkpoints older than the specified date."""
        keys_to_delete = [
            key
            for key, checkpoint in self._checkpoints.items()
            if checkpoint.updated_at < older_than
        ]
        for key in keys_to_delete:
            del self._checkpoints[key]
        return len(keys_to_delete)

    # Test helpers
    def get_saved_checkpoints(self) -> List[IngestionCheckpoint]:
        """Get all saved checkpoints (for testing)."""
        return list(self._checkpoints.values())

    def get_save_calls(self) -> List[tuple[IngestionJobId, IngestionCheckpoint]]:
        """Get list of checkpoints that were saved (for testing)."""
        return self._save_calls.copy()

    def clear(self) -> None:
        """Clear all data (for testing)."""
        self._checkpoints.clear()
        self._save_calls.clear()


class FakeIngestionMetricsRepository(IIngestionMetricsRepository):
    """In-memory fake repository for ingestion metrics."""

    def __init__(self):
        self._metrics: Dict[IngestionJobId, ProcessingMetrics] = {}
        self._save_calls: List[tuple[IngestionJobId, ProcessingMetrics]] = []

    async def save_metrics(
        self, job_id: IngestionJobId, metrics: ProcessingMetrics
    ) -> None:
        """Save processing metrics for a job."""
        self._metrics[job_id] = metrics
        self._save_calls.append((job_id, metrics))

    async def get_metrics(self, job_id: IngestionJobId) -> Optional[ProcessingMetrics]:
        """Get metrics for a specific job."""
        return self._metrics.get(job_id)

    async def get_metrics_history(
        self, start_date: datetime, end_date: datetime
    ) -> List[tuple[IngestionJobId, ProcessingMetrics]]:
        """Get metrics for jobs within a date range."""
        # Simplified implementation for testing
        return list(self._metrics.items())

    async def get_average_metrics(
        self, start_date: datetime, end_date: datetime
    ) -> Optional[ProcessingMetrics]:
        """Calculate average metrics across jobs in a date range."""
        if not self._metrics:
            return None

        # Simplified average calculation for testing
        metrics_list = list(self._metrics.values())
        total_symbols = sum(m.symbols_processed for m in metrics_list)
        total_failed = sum(m.symbols_failed for m in metrics_list)
        total_bars = sum(m.total_bars_ingested for m in metrics_list)
        total_time = sum(m.total_processing_time_seconds for m in metrics_list)

        avg_time_per_symbol = total_time / len(metrics_list) if metrics_list else 0

        return ProcessingMetrics(
            symbols_processed=total_symbols // len(metrics_list),
            symbols_failed=total_failed // len(metrics_list),
            total_bars_ingested=total_bars // len(metrics_list),
            total_processing_time_seconds=total_time / len(metrics_list),
            average_processing_time_per_symbol=avg_time_per_symbol,
        )

    async def get_performance_trends(
        self, days: int = 30
    ) -> List[tuple[datetime, float]]:
        """Get daily average processing performance over time."""
        # Simplified implementation for testing
        if not self._metrics:
            return []

        avg_performance = sum(
            m.total_bars_ingested / max(m.total_processing_time_seconds, 1)
            for m in self._metrics.values()
        ) / len(self._metrics)

        return [(datetime.now(), avg_performance)]

    # Test helpers
    def get_saved_metrics(self) -> Dict[IngestionJobId, ProcessingMetrics]:
        """Get all saved metrics (for testing)."""
        return self._metrics.copy()

    def get_save_calls(self) -> List[tuple[IngestionJobId, ProcessingMetrics]]:
        """Get list of metrics that were saved (for testing)."""
        return self._save_calls.copy()

    def clear(self) -> None:
        """Clear all data (for testing)."""
        self._metrics.clear()
        self._save_calls.clear()
