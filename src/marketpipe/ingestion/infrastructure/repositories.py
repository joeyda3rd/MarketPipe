# SPDX-License-Identifier: Apache-2.0
"""Infrastructure repository implementations for ingestion domain."""

from __future__ import annotations

import json
import aiosqlite
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from pathlib import Path

from ..domain.entities import IngestionJob, IngestionJobId, ProcessingState
from ..domain.repositories import (
    IIngestionJobRepository,
    IIngestionCheckpointRepository,
    IIngestionMetricsRepository,
    IngestionRepositoryError,
)
from ..domain.value_objects import IngestionCheckpoint, ProcessingMetrics
from marketpipe.domain.value_objects import Symbol
from marketpipe.infrastructure.sqlite_async_mixin import SqliteAsyncMixin
from prometheus_client import Counter, Histogram


# Shared metrics for all repository backends
REPO_QUERIES = Counter(
    'ingestion_repo_queries_total',
    'Total number of repository queries',
    ['operation', 'backend']
)

REPO_LATENCY = Histogram(
    'ingestion_repo_latency_seconds',
    'Repository operation latency',
    ['operation', 'backend']
)


class SqliteIngestionJobRepository(SqliteAsyncMixin, IIngestionJobRepository):
    """SQLite implementation of ingestion job repository."""

    def __init__(self, db_path: str | Path | None = None):
        """Initialize repository with database path.
        
        Args:
            db_path: Path to SQLite database file. Defaults to data/db/ingestion_jobs.db
        """
        self._db_path = db_path or Path("data/db/ingestion_jobs.db")
        self.db_path = str(self._db_path)  # For SqliteAsyncMixin
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the database schema using migrations."""
        from marketpipe.migrations import apply_pending
        apply_pending(self._db_path)

    def _domain_state_to_db_state(self, state: ProcessingState) -> str:
        """Transform domain state (lowercase) to database state (uppercase)."""
        return state.value.upper()

    def _db_state_to_domain_state(self, db_state: str) -> ProcessingState:
        """Transform database state (uppercase) to domain state (lowercase)."""
        return ProcessingState(db_state.lower())

    async def save(self, job: IngestionJob) -> None:
        """Save an ingestion job."""
        with REPO_LATENCY.labels('save', 'sqlite').time():
            REPO_QUERIES.labels('save', 'sqlite').inc()
            
        try:
            payload = self._serialize_job_to_json(job)
            now = datetime.now()

            async with self._conn() as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO ingestion_jobs 
                    (symbol, day, state, payload, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (str(job.job_id.symbol), job.job_id.day, self._domain_state_to_db_state(job.state), payload, job.created_at, now),
                )
                await db.commit()

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(
                f"Failed to save job {job.job_id}: {e}"
            ) from e

    async def get_by_id(self, job_id: IngestionJobId) -> Optional[IngestionJob]:
        """Retrieve an ingestion job by its ID."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM ingestion_jobs WHERE symbol = ? AND day = ?",
                    (str(job_id.symbol), job_id.day),
                )
                row = await cursor.fetchone()

                if row is None:
                    return None

                return self._deserialize_job_from_row(row)

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get job {job_id}: {e}") from e

    async def get_by_state(self, state: ProcessingState) -> List[IngestionJob]:
        """Get all jobs in a specific state."""
        with REPO_LATENCY.labels('get_by_state', 'sqlite').time():
            REPO_QUERIES.labels('get_by_state', 'sqlite').inc()
            
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM ingestion_jobs WHERE state = ? ORDER BY created_at DESC",
                    (self._domain_state_to_db_state(state),),
                )
                rows = await cursor.fetchall()

                return [self._deserialize_job_from_row(row) for row in rows]

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(
                f"Failed to get jobs by state {state}: {e}"
            ) from e

    async def get_active_jobs(self) -> List[IngestionJob]:
        """Get all jobs that are currently active."""
        active_states = [
            self._domain_state_to_db_state(ProcessingState.PENDING),
            self._domain_state_to_db_state(ProcessingState.IN_PROGRESS),
        ]

        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                placeholders = ",".join("?" * len(active_states))
                cursor = await db.execute(
                    f"SELECT * FROM ingestion_jobs WHERE state IN ({placeholders}) ORDER BY created_at",
                    active_states,
                )
                rows = await cursor.fetchall()

                return [self._deserialize_job_from_row(row) for row in rows]

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get active jobs: {e}") from e

    async def get_jobs_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> List[IngestionJob]:
        """Get jobs created within a date range."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT * FROM ingestion_jobs 
                    WHERE created_at BETWEEN ? AND ? 
                    ORDER BY created_at DESC
                """,
                    (start_date, end_date),
                )
                rows = await cursor.fetchall()

                return [self._deserialize_job_from_row(row) for row in rows]

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(
                f"Failed to get jobs by date range: {e}"
            ) from e

    async def delete(self, job_id: IngestionJobId) -> bool:
        """Delete an ingestion job."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    "DELETE FROM ingestion_jobs WHERE symbol = ? AND day = ?", 
                    (str(job_id.symbol), job_id.day)
                )
                await db.commit()
                return cursor.rowcount > 0

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to delete job {job_id}: {e}") from e

    async def get_job_history(self, limit: int = 100) -> List[IngestionJob]:
        """Get recent job history."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM ingestion_jobs ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                )
                rows = await cursor.fetchall()

                return [self._deserialize_job_from_row(row) for row in rows]

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get job history: {e}") from e

    async def count_jobs_by_state(self) -> Dict[ProcessingState, int]:
        """Count jobs grouped by their processing state."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    "SELECT state, COUNT(*) as count FROM ingestion_jobs GROUP BY state"
                )
                rows = await cursor.fetchall()

                result = {}
                for row in rows:
                    state = self._db_state_to_domain_state(row[0])
                    count = row[1]
                    result[state] = count

                return result

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to count jobs by state: {e}") from e

    async def fetch_and_lock(self, limit: int = 1) -> List[IngestionJob]:
        """Fetch and lock pending jobs for processing (SQLite version)."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                
                # SQLite doesn't have SELECT FOR UPDATE, so we use a transaction
                await db.execute("BEGIN IMMEDIATE")
                
                cursor = await db.execute(
                    """
                    SELECT * FROM ingestion_jobs 
                    WHERE state = ? 
                    ORDER BY created_at 
                    LIMIT ?
                    """,
                    (self._domain_state_to_db_state(ProcessingState.PENDING), limit),
                )
                rows = await cursor.fetchall()
                
                if not rows:
                    await db.rollback()
                    return []
                
                # Mark jobs as IN_PROGRESS
                job_ids = [(row["symbol"], row["day"]) for row in rows]
                for symbol, day in job_ids:
                    await db.execute(
                        "UPDATE ingestion_jobs SET state = ?, updated_at = ? WHERE symbol = ? AND day = ?",
                        (self._domain_state_to_db_state(ProcessingState.IN_PROGRESS), datetime.now(), symbol, day),
                    )
                
                await db.commit()
                
                # Return the jobs with updated state
                jobs = []
                for row in rows:
                    job = self._deserialize_job_from_row(row)
                    job._state = ProcessingState.IN_PROGRESS  # Update state in memory
                    jobs.append(job)
                
                return jobs

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to fetch and lock jobs: {e}") from e

    async def count_old_jobs(self, cutoff_date: str) -> int:
        """Count jobs older than cutoff date."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    "SELECT COUNT(*) FROM ingestion_jobs WHERE day < ?",
                    (cutoff_date,)
                )
                result = await cursor.fetchone()
                return result[0] if result else 0

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to count old jobs: {e}") from e

    async def delete_old_jobs(self, cutoff_date: str) -> int:
        """Delete jobs older than cutoff date and run VACUUM."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    "DELETE FROM ingestion_jobs WHERE day < ?",
                    (cutoff_date,)
                )
                deleted = cursor.rowcount
                
                if deleted > 0:
                    # Run VACUUM to reclaim space
                    await db.execute("VACUUM")
                
                await db.commit()
                return deleted

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to delete old jobs: {e}") from e

    def _serialize_job_to_json(self, job: IngestionJob) -> str:
        """Serialize an IngestionJob to JSON string for new schema."""
        job_dict = {
            "job_id": str(job.job_id),
            "symbols": [str(symbol) for symbol in job.symbols],
            "start_timestamp": job.time_range.start.to_nanoseconds(),
            "end_timestamp": job.time_range.end.to_nanoseconds(),
            "provider": getattr(job.configuration, 'provider', 'unknown'),
            "feed": getattr(job.configuration, 'feed_type', 'unknown'),
            "state": job.state.value,
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "processed_symbols": [str(symbol) for symbol in job.processed_symbols],
            "error_message": job.error_message,
        }
        return json.dumps(job_dict)

    def _deserialize_job_from_row(self, row) -> IngestionJob:
        """Deserialize database row to IngestionJob."""
        # Parse the JSON payload
        payload = json.loads(row["payload"]) if row["payload"] else {}
        
        # Create job ID from symbol and day
        job_id = IngestionJobId(Symbol(row["symbol"]), row["day"])
        
        # Extract data from payload with defaults
        symbols = [Symbol(s) for s in payload.get("symbols", [row["symbol"]])]
        
        # Import required classes
        from marketpipe.domain.value_objects import TimeRange, Timestamp
        from ..domain.value_objects import IngestionConfiguration
        from pathlib import Path
        
        # Create time range from timestamps
        start_timestamp_ns = payload.get("start_timestamp", 0)
        end_timestamp_ns = payload.get("end_timestamp", 0)
        time_range = TimeRange(
            start=Timestamp.from_nanoseconds(start_timestamp_ns),
            end=Timestamp.from_nanoseconds(end_timestamp_ns)
        )
        
        # Create configuration from stored values
        configuration = IngestionConfiguration(
            output_path=Path("data/raw"),  # Default path
            compression="snappy",
            max_workers=3,
            batch_size=1000,
            rate_limit_per_minute=200,
            feed_type=payload.get("feed", "iex"),
        )
        
        # Create job
        job = IngestionJob(
            job_id=job_id,
            configuration=configuration,
            symbols=symbols,
            time_range=time_range,
        )

        # Restore state
        job._state = self._db_state_to_domain_state(row["state"])
        job._created_at = datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"]
        job._started_at = datetime.fromisoformat(payload["started_at"]) if payload.get("started_at") else None
        job._completed_at = datetime.fromisoformat(payload["completed_at"]) if payload.get("completed_at") else None
        job._processed_symbols = set(Symbol(s) for s in payload.get("processed_symbols", []))
        job._error_message = payload.get("error_message")

        return job

    def _serialize_job(self, job: IngestionJob) -> str:
        """Serialize an IngestionJob to JSON string."""
        job_dict = {
            "job_id": str(job.job_id),
            "symbols": [str(symbol) for symbol in job.symbols],
            "start_timestamp": job.time_range.start.to_nanoseconds(),
            "end_timestamp": job.time_range.end.to_nanoseconds(),
            "provider": getattr(job.configuration, 'provider', 'unknown'),
            "feed": getattr(job.configuration, 'feed_type', 'unknown'),
            "state": job.state.value,
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "processed_symbols": [str(symbol) for symbol in job.processed_symbols],
            "error_message": job.error_message,
        }
        return json.dumps(job_dict)

    def _deserialize_job(self, job_data: str) -> IngestionJob:
        """Deserialize JSON string to IngestionJob."""
        job_dict = json.loads(job_data)

        # Parse datetimes
        created_at = datetime.fromisoformat(job_dict["created_at"])
        started_at = (
            datetime.fromisoformat(job_dict["started_at"])
            if job_dict["started_at"]
            else None
        )
        completed_at = (
            datetime.fromisoformat(job_dict["completed_at"])
            if job_dict["completed_at"]
            else None
        )

        # Import required classes
        from marketpipe.domain.value_objects import TimeRange, Timestamp
        from ..domain.value_objects import IngestionConfiguration
        from pathlib import Path
        
        # Create time range from timestamps
        time_range = TimeRange(
            start=Timestamp.from_nanoseconds(job_dict["start_timestamp"]),
            end=Timestamp.from_nanoseconds(job_dict["end_timestamp"])
        )
        
        # Create configuration from stored values
        configuration = IngestionConfiguration(
            output_path=Path("data/raw"),  # Default path
            compression="snappy",
            max_workers=3,
            batch_size=1000,
            rate_limit_per_minute=200,
            feed_type=job_dict.get("feed", "iex"),
        )

        # Create job
        job = IngestionJob(
            job_id=IngestionJobId.from_string(job_dict["job_id"]),
            configuration=configuration,
            symbols=[Symbol(s) for s in job_dict["symbols"]],
            time_range=time_range,
        )

        # Restore state
        job._state = ProcessingState(job_dict["state"])
        job._created_at = created_at
        job._started_at = started_at
        job._completed_at = completed_at
        job._processed_symbols = set(Symbol(s) for s in job_dict["processed_symbols"])
        job._error_message = job_dict["error_message"]

        # Handle state transitions that might require calling internal methods
        if job.state == ProcessingState.IN_PROGRESS and not job.started_at:
            # If job is marked as in progress but doesn't have started_at,
            # it means we're restoring state, so we should restore it properly
            job._started_at = started_at or datetime.now()

        if job.state in [ProcessingState.COMPLETED, ProcessingState.FAILED]:
            if job.state == ProcessingState.FAILED and job_dict["error_message"]:
                # If job failed with an error, restore that
                job._error_message = job_dict["error_message"]
            if not job.completed_at:
                # If job is marked complete but no completion time, set it now
                job._completed_at = completed_at or datetime.now()

        # Note: We don't call job.start(), job.complete(), etc. here because
        # those methods change state and emit events. We're just restoring
        # the serialized state.

        # Handle any state inconsistencies
        if job.state == ProcessingState.CANCELLED:
            if job.completed_at is None:
                job._completed_at = datetime.now()
            # For cancelled jobs, we may need to clean up
            if hasattr(job, "cancel"):
                job.cancel()

        # TODO: Restore other state like processed_symbols, timestamps, etc.
        # For now, just restore the basic state which is enough for our use case

        return job

    async def close_connections(self) -> None:
        """Close all database connections gracefully."""
        try:
            # Force close any remaining connections in the pool
            if hasattr(self, '_pool') and self._pool:
                await self._pool.close()
            
            # Also close any direct connections if they exist
            if hasattr(self, '_db_connection') and self._db_connection:
                await self._db_connection.close()
                
        except Exception as e:
            # Log but don't raise - this is cleanup
            import logging
            logger = logging.getLogger(self.__class__.__name__)
            logger.warning(f"Error during connection cleanup: {e}")

    def __del__(self):
        """Destructor to attempt cleanup if not done explicitly."""
        # Note: This won't work for async cleanup, but provides a fallback
        try:
            # Check if we have any connections that need cleanup
            if hasattr(self, '_pool') or hasattr(self, '_db_connection'):
                import warnings
                warnings.warn(
                    f"{self.__class__.__name__} was not properly closed. "
                    "Call close_connections() before destruction.",
                    ResourceWarning,
                    stacklevel=2
                )
        except Exception:
            pass  # Ignore errors in destructor


class SqliteCheckpointRepository(SqliteAsyncMixin, IIngestionCheckpointRepository):
    """SQLite implementation of checkpoint repository."""

    def __init__(self, db_path: str | Path | None = None):
        """Initialize repository with database path.
        
        Args:
            db_path: Path to SQLite database file. Defaults to data/db/ingestion_checkpoints.db
        """
        self._db_path = db_path or Path("data/db/ingestion_checkpoints.db")
        self.db_path = str(self._db_path)  # For SqliteAsyncMixin
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the database schema."""
        # For now, keep the sync initialization but use the connection pool
        from marketpipe.infrastructure.sqlite_pool import connection
        with connection(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
                    job_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    last_processed_timestamp INTEGER NOT NULL,
                    records_processed INTEGER NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (job_id, symbol)
                )
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_checkpoints_symbol 
                ON ingestion_checkpoints(symbol)
            """
            )

    async def save_checkpoint(
        self, job_id: IngestionJobId, checkpoint: IngestionCheckpoint
    ) -> None:
        """Save a checkpoint for a specific job and symbol."""
        try:
            async with self._conn() as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO ingestion_checkpoints 
                    (job_id, symbol, last_processed_timestamp, records_processed, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        str(job_id),
                        checkpoint.symbol.value,
                        checkpoint.last_processed_timestamp,
                        checkpoint.records_processed,
                        checkpoint.updated_at,
                    ),
                )
                await db.commit()

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to save checkpoint: {e}") from e

    async def get_checkpoint(
        self, job_id: IngestionJobId, symbol: Symbol
    ) -> Optional[IngestionCheckpoint]:
        """Get the latest checkpoint for a job and symbol."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT last_processed_timestamp, records_processed, updated_at
                    FROM ingestion_checkpoints 
                    WHERE job_id = ? AND symbol = ?
                """,
                    (str(job_id), symbol.value),
                )

                row = await cursor.fetchone()
                if row is None:
                    return None

                return IngestionCheckpoint(
                    symbol=symbol,
                    last_processed_timestamp=row["last_processed_timestamp"],
                    records_processed=row["records_processed"],
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                )

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get checkpoint: {e}") from e

    async def get_all_checkpoints(
        self, job_id: IngestionJobId
    ) -> List[IngestionCheckpoint]:
        """Get all checkpoints for a specific job."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT symbol, last_processed_timestamp, records_processed, updated_at
                    FROM ingestion_checkpoints 
                    WHERE job_id = ?
                """,
                    (str(job_id),),
                )

                rows = await cursor.fetchall()
                checkpoints = []

                for row in rows:
                    checkpoint = IngestionCheckpoint(
                        symbol=Symbol(row["symbol"]),
                        last_processed_timestamp=row["last_processed_timestamp"],
                        records_processed=row["records_processed"],
                        updated_at=datetime.fromisoformat(row["updated_at"]),
                    )
                    checkpoints.append(checkpoint)

                return checkpoints

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(
                f"Failed to get checkpoints for job {job_id}: {e}"
            ) from e

    async def delete_checkpoints(self, job_id: IngestionJobId) -> None:
        """Delete all checkpoints for a specific job."""
        try:
            async with self._conn() as db:
                await db.execute(
                    "DELETE FROM ingestion_checkpoints WHERE job_id = ?", (str(job_id),)
                )
                await db.commit()

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(
                f"Failed to delete checkpoints for job {job_id}: {e}"
            ) from e

    async def get_global_checkpoint(
        self, symbol: Symbol
    ) -> Optional[IngestionCheckpoint]:
        """Get the most recent checkpoint for a symbol across all jobs."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT symbol, last_processed_timestamp, records_processed, updated_at
                    FROM ingestion_checkpoints 
                    WHERE symbol = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                """,
                    (symbol.value,),
                )

                row = await cursor.fetchone()
                if row is None:
                    return None

                return IngestionCheckpoint(
                    symbol=symbol,
                    last_processed_timestamp=row["last_processed_timestamp"],
                    records_processed=row["records_processed"],
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                )

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(
                f"Failed to get global checkpoint for {symbol}: {e}"
            ) from e

    async def cleanup_old_checkpoints(self, older_than: datetime) -> int:
        """Remove checkpoints older than the specified date."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    "DELETE FROM ingestion_checkpoints WHERE updated_at < ?",
                    (older_than,),
                )
                await db.commit()
                return cursor.rowcount

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(
                f"Failed to cleanup old checkpoints: {e}"
            ) from e


class SqliteMetricsRepository(SqliteAsyncMixin, IIngestionMetricsRepository):
    """SQLite implementation of metrics repository."""

    def __init__(self, db_path: str | Path | None = None):
        """Initialize repository with database path.
        
        Args:
            db_path: Path to SQLite database file. Defaults to data/db/ingestion_metrics.db
        """
        self._db_path = db_path or Path("data/db/ingestion_metrics.db")
        self.db_path = str(self._db_path)  # For SqliteAsyncMixin
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the database schema."""
        # For now, keep the sync initialization but use the connection pool
        from marketpipe.infrastructure.sqlite_pool import connection
        with connection(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_metrics (
                    job_id TEXT NOT NULL,
                    start_timestamp INTEGER NOT NULL,
                    end_timestamp INTEGER NOT NULL,
                    total_records INTEGER NOT NULL,
                    processed_records INTEGER NOT NULL,
                    failed_records INTEGER NOT NULL,
                    processing_time_seconds REAL NOT NULL,
                    throughput_records_per_second REAL NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (job_id)
                )
            """
            )

    async def save_metrics(
        self, job_id: IngestionJobId, metrics: ProcessingMetrics
    ) -> None:
        """Save processing metrics for a job."""
        try:
            async with self._conn() as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO ingestion_metrics 
                    (job_id, start_timestamp, end_timestamp, total_records, 
                     processed_records, failed_records, processing_time_seconds, 
                     throughput_records_per_second, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        str(job_id),
                        metrics.start_timestamp,
                        metrics.end_timestamp,
                        metrics.total_records,
                        metrics.processed_records,
                        metrics.failed_records,
                        metrics.processing_time_seconds,
                        metrics.throughput_records_per_second,
                        datetime.now(),
                    ),
                )
                await db.commit()

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to save metrics: {e}") from e

    async def get_metrics(self, job_id: IngestionJobId) -> Optional[ProcessingMetrics]:
        """Get processing metrics for a specific job."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT start_timestamp, end_timestamp, total_records, 
                           processed_records, failed_records, processing_time_seconds, 
                           throughput_records_per_second
                    FROM ingestion_metrics 
                    WHERE job_id = ?
                """,
                    (str(job_id),),
                )

                row = await cursor.fetchone()
                if row is None:
                    return None

                return ProcessingMetrics(
                    start_timestamp=row["start_timestamp"],
                    end_timestamp=row["end_timestamp"],
                    total_records=row["total_records"],
                    processed_records=row["processed_records"],
                    failed_records=row["failed_records"],
                    processing_time_seconds=row["processing_time_seconds"],
                    throughput_records_per_second=row["throughput_records_per_second"],
                )

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get metrics: {e}") from e

    async def get_metrics_history(
        self, start_date: datetime, end_date: datetime
    ) -> List[tuple[IngestionJobId, ProcessingMetrics]]:
        """Get metrics history within a date range."""
        try:
            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT job_id, start_timestamp, end_timestamp, total_records, 
                           processed_records, failed_records, processing_time_seconds, 
                           throughput_records_per_second
                    FROM ingestion_metrics 
                    WHERE created_at BETWEEN ? AND ?
                    ORDER BY created_at DESC
                """,
                    (start_date, end_date),
                )

                rows = await cursor.fetchall()
                result = []

                for row in rows:
                    job_id = IngestionJobId.from_string(row["job_id"])
                    metrics = ProcessingMetrics(
                        start_timestamp=row["start_timestamp"],
                        end_timestamp=row["end_timestamp"],
                        total_records=row["total_records"],
                        processed_records=row["processed_records"],
                        failed_records=row["failed_records"],
                        processing_time_seconds=row["processing_time_seconds"],
                        throughput_records_per_second=row["throughput_records_per_second"],
                    )
                    result.append((job_id, metrics))

                return result

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get metrics history: {e}") from e

    async def get_average_metrics(
        self, start_date: datetime, end_date: datetime
    ) -> Optional[ProcessingMetrics]:
        """Get average processing metrics over a date range."""
        try:
            async with self._conn() as db:
                cursor = await db.execute(
                    """
                    SELECT 
                        AVG(start_timestamp) as avg_start_timestamp,
                        AVG(end_timestamp) as avg_end_timestamp,
                        AVG(total_records) as avg_total_records,
                        AVG(processed_records) as avg_processed_records,
                        AVG(failed_records) as avg_failed_records,
                        AVG(processing_time_seconds) as avg_processing_time_seconds,
                        AVG(throughput_records_per_second) as avg_throughput_records_per_second
                    FROM ingestion_metrics 
                    WHERE created_at BETWEEN ? AND ?
                """,
                    (start_date, end_date),
                )

                row = await cursor.fetchone()
                if row is None or row[0] is None:
                    return None

                return ProcessingMetrics(
                    start_timestamp=int(row[0]),
                    end_timestamp=int(row[1]),
                    total_records=int(row[2]),
                    processed_records=int(row[3]),
                    failed_records=int(row[4]),
                    processing_time_seconds=float(row[5]),
                    throughput_records_per_second=float(row[6]),
                )

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get average metrics: {e}") from e

    async def get_performance_trends(
        self, days: int = 30
    ) -> List[tuple[datetime, float]]:
        """Get performance trends over time."""
        try:
            start_date = datetime.now() - timedelta(days=days)

            async with self._conn() as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT DATE(created_at) as day, AVG(throughput_records_per_second) as avg_throughput
                    FROM ingestion_metrics 
                    WHERE created_at >= ?
                    GROUP BY DATE(created_at)
                    ORDER BY day
                """,
                    (start_date,),
                )

                rows = await cursor.fetchall()
                return [
                    (datetime.fromisoformat(row["day"]), float(row["avg_throughput"]))
                    for row in rows
                ]

        except aiosqlite.Error as e:
            raise IngestionRepositoryError(f"Failed to get performance trends: {e}") from e
