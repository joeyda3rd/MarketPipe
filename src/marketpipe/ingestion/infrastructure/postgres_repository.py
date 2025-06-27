# SPDX-License-Identifier: Apache-2.0
"""PostgreSQL repository implementation for ingestion jobs."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import asyncpg

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
from marketpipe.ingestion.domain.entities import (
    IngestionJob,
    IngestionJobId,
    ProcessingState,
)
from marketpipe.ingestion.domain.repositories import (
    IIngestionJobRepository,
    IngestionRepositoryError,
)
from marketpipe.ingestion.domain.value_objects import (
    IngestionConfiguration,
    IngestionPartition,
)

# Import shared metrics from repositories module
from .repositories import REPO_LATENCY, REPO_QUERIES

logger = logging.getLogger(__name__)


class PostgresIngestionJobRepository(IIngestionJobRepository):
    """PostgreSQL implementation of ingestion job repository with rich DDD model support."""

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10):
        """
        Initialize the repository with connection pool.

        Args:
            dsn: PostgreSQL connection string
            min_size: Minimum pool connections
            max_size: Maximum pool connections
        """
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None
        self._min_size = min_size
        self._max_size = max_size
        self._pool_lock = asyncio.Lock()  # Prevent race conditions on pool creation
        logger.info(f"PostgresIngestionJobRepository initialized with pool {min_size}-{max_size}")

    async def _get_pool(self) -> asyncpg.Pool:
        """Get or create connection pool with race condition protection."""
        if self._pool is None:
            async with self._pool_lock:
                # Double-check pattern to avoid creating multiple pools
                if self._pool is None:
                    self._pool = await asyncpg.create_pool(
                        self._dsn,
                        min_size=self._min_size,
                        max_size=self._max_size,
                        command_timeout=60,
                    )
                    logger.info("PostgreSQL connection pool created")
        return self._pool

    async def save(self, job: IngestionJob) -> None:
        """Save an ingestion job to PostgreSQL."""
        with REPO_LATENCY.labels("save", "postgres").time():
            REPO_QUERIES.labels("save", "postgres").inc()

            try:
                pool = await self._get_pool()

                # Serialize job to JSONB payload
                payload = self._serialize_job_to_dict(job)

                # Extract simple fields for fast queries
                symbol_str = str(job.symbols[0]) if job.symbols else "UNKNOWN"
                day_value = self._extract_day_from_job(job)

                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO ingestion_jobs (symbol, day, state, payload, updated_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (symbol, day)
                        DO UPDATE SET 
                            state = EXCLUDED.state,
                            payload = EXCLUDED.payload,
                            updated_at = EXCLUDED.updated_at
                        """,
                        symbol_str,
                        day_value,
                        job.state.value,
                        json.dumps(payload),
                        datetime.now(),
                    )

                logger.debug(f"Saved job {job.job_id} for {symbol_str} on {day_value}")

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to save job {job.job_id}: {e}")
                raise IngestionRepositoryError(f"Failed to save job: {e}") from e

    async def get_by_id(self, job_id: IngestionJobId) -> Optional[IngestionJob]:
        """Retrieve an ingestion job by its ID."""
        with REPO_LATENCY.labels("get_by_id", "postgres").time():
            REPO_QUERIES.labels("get_by_id", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT payload FROM ingestion_jobs WHERE payload->>'job_id' = $1",
                        str(job_id),
                    )

                if row is None:
                    return None

                return self._deserialize_job_from_payload(row["payload"])

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to get job by ID {job_id}: {e}")
                raise IngestionRepositoryError(f"Failed to get job by ID: {e}") from e

    async def get_by_state(self, state: ProcessingState) -> List[IngestionJob]:
        """Get all jobs in a specific state."""
        with REPO_LATENCY.labels("get_by_state", "postgres").time():
            REPO_QUERIES.labels("get_by_state", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT payload FROM ingestion_jobs WHERE state = $1 ORDER BY updated_at DESC",
                        state.value,
                    )

                return [self._deserialize_job_from_payload(row["payload"]) for row in rows]

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to get jobs by state {state}: {e}")
                raise IngestionRepositoryError(f"Failed to get jobs by state: {e}") from e

    async def get_active_jobs(self) -> List[IngestionJob]:
        """Get all jobs that are currently active (pending or in progress)."""
        with REPO_LATENCY.labels("get_active_jobs", "postgres").time():
            REPO_QUERIES.labels("get_active_jobs", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT payload FROM ingestion_jobs 
                        WHERE state IN ('PENDING', 'IN_PROGRESS') 
                        ORDER BY updated_at ASC
                        """
                    )

                return [self._deserialize_job_from_payload(row["payload"]) for row in rows]

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to get active jobs: {e}")
                raise IngestionRepositoryError(f"Failed to get active jobs: {e}") from e

    async def get_jobs_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[IngestionJob]:
        """Get jobs created within a date range."""
        with REPO_LATENCY.labels("get_jobs_by_date_range", "postgres").time():
            REPO_QUERIES.labels("get_jobs_by_date_range", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT payload FROM ingestion_jobs 
                        WHERE created_at BETWEEN $1 AND $2 
                        ORDER BY created_at DESC
                        """,
                        start_date,
                        end_date,
                    )

                return [self._deserialize_job_from_payload(row["payload"]) for row in rows]

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to get jobs by date range: {e}")
                raise IngestionRepositoryError(f"Failed to get jobs by date range: {e}") from e

    async def delete(self, job_id: IngestionJobId) -> bool:
        """Delete an ingestion job. Returns True if job was found and deleted."""
        with REPO_LATENCY.labels("delete", "postgres").time():
            REPO_QUERIES.labels("delete", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    result = await conn.execute(
                        "DELETE FROM ingestion_jobs WHERE payload->>'job_id' = $1", str(job_id)
                    )

                # Extract number of deleted rows from result
                deleted_count = int(result.split()[-1]) if result.startswith("DELETE") else 0
                return deleted_count > 0

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to delete job {job_id}: {e}")
                raise IngestionRepositoryError(f"Failed to delete job: {e}") from e

    async def get_job_history(self, limit: int = 100) -> List[IngestionJob]:
        """Get recent job history, ordered by creation date."""
        with REPO_LATENCY.labels("get_job_history", "postgres").time():
            REPO_QUERIES.labels("get_job_history", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT payload FROM ingestion_jobs ORDER BY created_at DESC LIMIT $1",
                        limit,
                    )

                return [self._deserialize_job_from_payload(row["payload"]) for row in rows]

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to get job history: {e}")
                raise IngestionRepositoryError(f"Failed to get job history: {e}") from e

    async def count_jobs_by_state(self) -> Dict[ProcessingState, int]:
        """Count jobs grouped by their processing state."""
        with REPO_LATENCY.labels("count_jobs_by_state", "postgres").time():
            REPO_QUERIES.labels("count_jobs_by_state", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT state, COUNT(*) as count FROM ingestion_jobs GROUP BY state"
                    )

                result = {}
                for row in rows:
                    try:
                        state = ProcessingState(row["state"])
                        result[state] = row["count"]
                    except ValueError:
                        # Skip invalid states
                        logger.warning(f"Invalid state found in database: {row['state']}")
                        continue

                return result

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to count jobs by state: {e}")
                raise IngestionRepositoryError(f"Failed to count jobs by state: {e}") from e

    async def fetch_and_lock(self, state: ProcessingState, limit: int) -> List[IngestionJob]:
        """
        Fetch jobs in specified state and lock them for processing.
        Uses PostgreSQL SELECT FOR UPDATE SKIP LOCKED for high concurrency.
        """
        with REPO_LATENCY.labels("fetch_and_lock", "postgres").time():
            REPO_QUERIES.labels("fetch_and_lock", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    async with conn.transaction():
                        # Select and lock jobs
                        rows = await conn.fetch(
                            """
                            SELECT id, payload FROM ingestion_jobs 
                            WHERE state = $1 
                            ORDER BY updated_at ASC 
                            LIMIT $2 
                            FOR UPDATE SKIP LOCKED
                            """,
                            state.value,
                            limit,
                        )

                        if not rows:
                            return []

                        # Update state to IN_PROGRESS
                        job_ids = [row["id"] for row in rows]
                        await conn.execute(
                            """
                            UPDATE ingestion_jobs 
                            SET state = $1, updated_at = $2 
                            WHERE id = ANY($3::int[])
                            """,
                            ProcessingState.IN_PROGRESS.value,
                            datetime.now(),
                            job_ids,
                        )

                        # Deserialize and update jobs
                        jobs = []
                        for row in rows:
                            job = self._deserialize_job_from_payload(row["payload"])
                            # Update the job state to reflect the database change
                            if hasattr(job, "_state"):
                                job._state = ProcessingState.IN_PROGRESS
                            jobs.append(job)

                        logger.info(f"Fetched and locked {len(jobs)} jobs in state {state}")
                        return jobs

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to fetch and lock jobs: {e}")
                raise IngestionRepositoryError(f"Failed to fetch and lock jobs: {e}") from e

    async def count_old_jobs(self, cutoff_date: str) -> int:
        """Count jobs older than cutoff date."""
        with REPO_LATENCY.labels("count_old_jobs", "postgres").time():
            REPO_QUERIES.labels("count_old_jobs", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    result = await conn.fetchval(
                        "SELECT COUNT(*) FROM ingestion_jobs WHERE day < $1", cutoff_date
                    )

                return result if result is not None else 0

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to count old jobs: {e}")
                raise IngestionRepositoryError(f"Failed to count old jobs: {e}") from e

    async def delete_old_jobs(self, cutoff_date: str) -> int:
        """Delete jobs older than cutoff date and run VACUUM."""
        with REPO_LATENCY.labels("delete_old_jobs", "postgres").time():
            REPO_QUERIES.labels("delete_old_jobs", "postgres").inc()

            try:
                pool = await self._get_pool()

                async with pool.acquire() as conn:
                    # Delete old jobs and get count
                    result = await conn.execute(
                        "DELETE FROM ingestion_jobs WHERE day < $1", cutoff_date
                    )

                    # Extract number of deleted rows from result
                    deleted_count = int(result.split()[-1]) if result.startswith("DELETE") else 0

                    if deleted_count > 0:
                        # Run VACUUM to reclaim space (PostgreSQL equivalent)
                        await conn.execute("VACUUM ingestion_jobs")
                        logger.info(f"Deleted {deleted_count} old jobs and ran VACUUM")

                    return deleted_count

            except asyncpg.PostgresError as e:
                logger.error(f"Failed to delete old jobs: {e}")
                raise IngestionRepositoryError(f"Failed to delete old jobs: {e}") from e

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("PostgreSQL connection pool closed")

    def _serialize_job_to_dict(self, job: IngestionJob) -> Dict[str, Any]:
        """
        Serialize IngestionJob to dictionary for JSONB storage.

        TODO: Monitor payload size in production. Large nested lists (e.g., many completed_partitions)
        could cause JSONB bloat. Consider stripping or summarizing large collections if needed.
        """
        return {
            "job_id": str(job.job_id),
            "symbols": [str(symbol) for symbol in job.symbols],
            "time_range": {
                "start": job.time_range.start.value.isoformat(),
                "end": job.time_range.end.value.isoformat(),
            },
            "configuration": {
                "output_path": str(job.configuration.output_path),
                "compression": job.configuration.compression,
                "max_workers": job.configuration.max_workers,
                "batch_size": job.configuration.batch_size,
                "rate_limit_per_minute": job.configuration.rate_limit_per_minute,
                "feed_type": job.configuration.feed_type,
            },
            "state": job.state.value,
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "failed_at": job.failed_at.isoformat() if job.failed_at else None,
            "error_message": job.error_message,
            "processed_symbols": [str(symbol) for symbol in job.processed_symbols],
            "total_bars_processed": job.total_bars_processed,
            "completed_partitions": [
                self._serialize_partition(p) for p in job.completed_partitions
            ],
        }

    def _serialize_partition(self, partition: IngestionPartition) -> Dict[str, Any]:
        """Serialize IngestionPartition to dictionary."""
        return {
            "symbol": str(partition.symbol),
            "file_path": str(partition.file_path),
            "record_count": partition.record_count,
            "file_size_bytes": partition.file_size_bytes,
            "created_at": partition.created_at.isoformat(),
        }

    def _deserialize_job_from_payload(self, payload: str) -> IngestionJob:
        """Deserialize IngestionJob from JSONB payload."""
        if isinstance(payload, str):
            job_dict = json.loads(payload)
        else:
            job_dict = payload

        # Parse datetime fields
        created_at = (
            datetime.fromisoformat(job_dict["created_at"])
            if job_dict.get("created_at")
            else datetime.now()
        )
        started_at = (
            datetime.fromisoformat(job_dict["started_at"]) if job_dict.get("started_at") else None
        )
        completed_at = (
            datetime.fromisoformat(job_dict["completed_at"])
            if job_dict.get("completed_at")
            else None
        )
        failed_at = (
            datetime.fromisoformat(job_dict["failed_at"]) if job_dict.get("failed_at") else None
        )

        # Reconstruct configuration
        config_data = job_dict.get("configuration", {})
        configuration = IngestionConfiguration(
            output_path=config_data.get("output_path", "/tmp"),
            compression=config_data.get("compression", "snappy"),
            max_workers=config_data.get("max_workers", 4),
            batch_size=config_data.get("batch_size", 1000),
            rate_limit_per_minute=config_data.get("rate_limit_per_minute", 200),
            feed_type=config_data.get("feed_type", "iex"),
        )

        # Reconstruct time range
        time_range_data = job_dict.get("time_range", {})
        time_range = TimeRange(
            start=Timestamp(datetime.fromisoformat(time_range_data["start"])),
            end=Timestamp(datetime.fromisoformat(time_range_data["end"])),
        )

        # Create the job
        job = IngestionJob(
            job_id=IngestionJobId.from_string(job_dict["job_id"]),
            configuration=configuration,
            symbols=[Symbol(s) for s in job_dict.get("symbols", [])],
            time_range=time_range,
        )

        # Restore internal state (accessing private attributes for reconstruction)
        job._state = ProcessingState(job_dict.get("state", "PENDING"))
        job._created_at = created_at
        job._started_at = started_at
        job._completed_at = completed_at
        job._failed_at = failed_at
        job._error_message = job_dict.get("error_message")
        job._processed_symbols = set(Symbol(s) for s in job_dict.get("processed_symbols", []))
        job._total_bars_processed = job_dict.get("total_bars_processed", 0)

        # Restore completed partitions
        partitions_data = job_dict.get("completed_partitions", [])
        job._completed_partitions = [self._deserialize_partition(p) for p in partitions_data]

        return job

    def _deserialize_partition(self, partition_data: Dict[str, Any]) -> IngestionPartition:
        """Deserialize IngestionPartition from dictionary."""
        from pathlib import Path

        return IngestionPartition(
            symbol=Symbol(partition_data["symbol"]),
            file_path=Path(partition_data["file_path"]),
            record_count=partition_data["record_count"],
            file_size_bytes=partition_data["file_size_bytes"],
            created_at=datetime.fromisoformat(partition_data["created_at"]),
        )

    def _extract_day_from_job(self, job: IngestionJob) -> date:
        """Extract trading day from job time range for simple queries."""
        # Use the start of the time range as the trading day
        return job.time_range.start.value.date()
