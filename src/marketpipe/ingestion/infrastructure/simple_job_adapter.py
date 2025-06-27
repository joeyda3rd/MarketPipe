# SPDX-License-Identifier: Apache-2.0
"""Simple API adapter for ingestion jobs."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.domain.entities import (
    IngestionJob,
    IngestionJobId,
    ProcessingState,
)
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration

from .repository_factory import create_ingestion_job_repository

logger = logging.getLogger(__name__)


# Mapping between simple string statuses and rich domain states
_SIMPLE_TO_STATE = {
    "pending": ProcessingState.PENDING,
    "running": ProcessingState.IN_PROGRESS,
    "done": ProcessingState.COMPLETED,
    "error": ProcessingState.FAILED,
    "cancelled": ProcessingState.CANCELLED,
}

_STATE_TO_SIMPLE = {v: k for k, v in _SIMPLE_TO_STATE.items()}


class SimpleJobRepository:
    """
    Facade for CLI helpers needing a simple (symbol, day, status) API.

    This adapter provides a simplified interface over the rich DDD repository
    while maintaining full compatibility with the domain model.
    """

    def __init__(self):
        """Initialize with auto-selected repository backend."""
        self._repo = create_ingestion_job_repository()
        logger.info(f"SimpleJobRepository initialized with {type(self._repo).__name__}")

    async def upsert(self, symbol: str, day: str, status: str) -> None:
        """
        Create or update a job with simple parameters.

        Args:
            symbol: Stock symbol (e.g., "AAPL")
            day: Trading date in YYYY-MM-DD format
            status: Simple status string ("pending", "running", "done", "error")
        """
        # Normalize status to lowercase to handle "Pending", "RUNNING", etc.
        status = status.lower()

        if status not in _SIMPLE_TO_STATE:
            raise ValueError(
                f"Invalid status '{status}'. Must be one of: {list(_SIMPLE_TO_STATE.keys())}"
            )

        try:
            # Parse inputs
            symbol_obj = Symbol(symbol.upper())
            day_obj = date.fromisoformat(day)
            state = _SIMPLE_TO_STATE[status]

            # Try to get existing job
            existing_job = await self._find_job_by_symbol_day(symbol_obj, day_obj)

            if existing_job:
                # Update existing job state
                self._update_job_state(existing_job, state)
                await self._repo.save(existing_job)
                logger.info(f"Updated job {symbol} {day} to status {status}")
            else:
                # Create new job with minimal configuration
                job = self._create_minimal_job(symbol_obj, day_obj, state)
                await self._repo.save(job)
                logger.info(f"Created new job {symbol} {day} with status {status}")

        except Exception as e:
            logger.error(f"Failed to upsert job {symbol} {day}: {e}")
            raise

    async def claim_pending(self, limit: int = 1) -> List[Tuple[str, str]]:
        """
        Claim pending jobs for processing.

        Args:
            limit: Maximum number of jobs to claim

        Returns:
            List of (symbol, day) tuples for claimed jobs
        """
        try:
            # Use fetch_and_lock if available (PostgreSQL), otherwise get_by_state
            if hasattr(self._repo, "fetch_and_lock"):
                jobs = await self._repo.fetch_and_lock(ProcessingState.PENDING, limit)
            else:
                # Fallback for repositories without locking
                pending_jobs = await self._repo.get_by_state(ProcessingState.PENDING)
                jobs = pending_jobs[:limit]

                # Manually start jobs
                for job in jobs:
                    if job.can_start:
                        job.start()
                        await self._repo.save(job)

            # Convert to simple format
            result = []
            for job in jobs:
                if job.symbols:
                    symbol_str = str(job.symbols[0])
                    day_str = self._extract_day_string(job)
                    result.append((symbol_str, day_str))

            logger.info(f"Claimed {len(result)} pending jobs")
            return result

        except Exception as e:
            logger.error(f"Failed to claim pending jobs: {e}")
            raise

    async def mark_done(self, symbol: str, day: str, status: str = "done") -> None:
        """
        Mark a job as completed with final status.

        Args:
            symbol: Stock symbol
            day: Trading date in YYYY-MM-DD format
            status: Final status ("done", "error", "cancelled")
        """
        # Normalize status to lowercase
        status = status.lower()

        if status not in ["done", "error", "cancelled"]:
            raise ValueError(
                f"Invalid final status '{status}'. Must be one of: done, error, cancelled"
            )

        try:
            symbol_obj = Symbol(symbol.upper())
            day_obj = date.fromisoformat(day)

            job = await self._find_job_by_symbol_day(symbol_obj, day_obj)
            if not job:
                logger.warning(f"Job {symbol} {day} not found for completion")
                return

            # Apply appropriate state transition
            if status == "done" and job.can_complete:
                job.complete()
            elif status == "error" and job.can_fail:
                job.fail("Job marked as failed via simple API")
            elif status == "cancelled" and job.can_cancel:
                job.cancel()
            else:
                # Force state update if transition not allowed
                target_state = _SIMPLE_TO_STATE[status]
                self._update_job_state(job, target_state)

            await self._repo.save(job)
            logger.info(f"Marked job {symbol} {day} as {status}")

        except Exception as e:
            logger.error(f"Failed to mark job {symbol} {day} as {status}: {e}")
            raise

    async def get_status(self, symbol: str, day: str) -> Optional[str]:
        """
        Get simple status string for a job.

        Args:
            symbol: Stock symbol
            day: Trading date in YYYY-MM-DD format

        Returns:
            Simple status string or None if job doesn't exist
        """
        try:
            symbol_obj = Symbol(symbol.upper())
            day_obj = date.fromisoformat(day)

            job = await self._find_job_by_symbol_day(symbol_obj, day_obj)
            if not job:
                return None

            return _STATE_TO_SIMPLE.get(job.state)

        except Exception as e:
            logger.error(f"Failed to get status for {symbol} {day}: {e}")
            raise

    async def list_jobs(self, status: Optional[str] = None) -> List[Tuple[str, str, str]]:
        """
        List jobs with optional status filter.

        Args:
            status: Optional status filter

        Returns:
            List of (symbol, day, status) tuples
        """
        try:
            if status:
                # Normalize status to lowercase
                status = status.lower()
                if status not in _SIMPLE_TO_STATE:
                    raise ValueError(f"Invalid status filter '{status}'")
                state = _SIMPLE_TO_STATE[status]
                jobs = await self._repo.get_by_state(state)
            else:
                jobs = await self._repo.get_job_history(1000)  # Get recent jobs

            result = []
            for job in jobs:
                if job.symbols:
                    symbol_str = str(job.symbols[0])
                    day_str = self._extract_day_string(job)
                    status_str = _STATE_TO_SIMPLE.get(job.state, "unknown")
                    result.append((symbol_str, day_str, status_str))

            return result

        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            raise

    async def close(self) -> None:
        """Close repository resources."""
        if hasattr(self._repo, "close"):
            await self._repo.close()

    async def _find_job_by_symbol_day(self, symbol: Symbol, day: date) -> Optional[IngestionJob]:
        """Find job by symbol and day across all jobs."""
        # Since we don't have a direct symbol+day lookup, we need to search
        # This is not optimal but works for the simple API
        jobs = await self._repo.get_job_history(1000)

        for job in jobs:
            if job.symbols and symbol in job.symbols and self._extract_day_from_job(job) == day:
                return job

        return None

    def _create_minimal_job(
        self, symbol: Symbol, day: date, state: ProcessingState
    ) -> IngestionJob:
        """Create a minimal job for simple API usage."""
        from pathlib import Path

        # Create minimal configuration
        config = IngestionConfiguration(
            output_path=Path("./data"),
            compression="snappy",
            max_workers=1,
            batch_size=1000,
            rate_limit_per_minute=200,
            feed_type="iex",
        )

        # Create time range for the day (market hours)
        day_start = datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc)
        day_end = day_start.replace(hour=23, minute=59, second=59)

        time_range = TimeRange(start=Timestamp(day_start), end=Timestamp(day_end))

        # Create job
        job_id = IngestionJobId.generate()
        job = IngestionJob(
            job_id=job_id,
            configuration=config,
            symbols=[symbol],
            time_range=time_range,
        )

        # Set initial state
        self._update_job_state(job, state)

        return job

    def _update_job_state(self, job: IngestionJob, new_state: ProcessingState) -> None:
        """Update job state directly (for simple API compatibility)."""
        # Direct state update for simple API - bypasses domain validation
        job._state = new_state

        # Update timestamps based on state
        now = datetime.now(timezone.utc)
        if new_state == ProcessingState.IN_PROGRESS and not job.started_at:
            job._started_at = now
        elif new_state in [
            ProcessingState.COMPLETED,
            ProcessingState.FAILED,
            ProcessingState.CANCELLED,
        ]:
            if not job.completed_at and not job.failed_at:
                if new_state == ProcessingState.FAILED:
                    job._failed_at = now
                else:
                    job._completed_at = now

    def _extract_day_from_job(self, job: IngestionJob) -> date:
        """Extract trading day from job."""
        return job.time_range.start.value.date()

    def _extract_day_string(self, job: IngestionJob) -> str:
        """Extract day string from job."""
        return self._extract_day_from_job(job).isoformat()
