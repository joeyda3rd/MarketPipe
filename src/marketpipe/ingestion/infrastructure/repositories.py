# SPDX-License-Identifier: Apache-2.0
"""Infrastructure repository implementations for ingestion domain."""

from __future__ import annotations

import json
import sqlite3
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from pathlib import Path

from ..domain.entities import IngestionJob, IngestionJobId, ProcessingState
from ..domain.repositories import (
    IIngestionJobRepository,
    IIngestionCheckpointRepository, 
    IIngestionMetricsRepository,
    IngestionJobNotFoundError,
    IngestionRepositoryError
)
from ..domain.value_objects import IngestionCheckpoint, ProcessingMetrics
from marketpipe.domain.value_objects import Symbol
from marketpipe.infrastructure.sqlite_pool import connection


class SqliteIngestionJobRepository(IIngestionJobRepository):
    """SQLite implementation of ingestion job repository."""
    
    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = db_path or Path("ingestion_jobs.db")
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize the database schema."""
        with connection(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_data TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_state 
                ON ingestion_jobs(state)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_created_at 
                ON ingestion_jobs(created_at)
            """)
    
    async def save(self, job: IngestionJob) -> None:
        """Save an ingestion job."""
        try:
            job_data = self._serialize_job(job)
            now = datetime.now()
            
            with connection(self._db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO ingestion_jobs 
                    (job_id, job_data, state, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    str(job.job_id),
                    job_data,
                    job.state.value,
                    job.created_at,
                    now
                ))
                conn.commit()
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to save job {job.job_id}: {e}") from e
    
    async def get_by_id(self, job_id: IngestionJobId) -> Optional[IngestionJob]:
        """Retrieve an ingestion job by its ID."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT job_data FROM ingestion_jobs WHERE job_id = ?",
                    (str(job_id),)
                )
                row = cursor.fetchone()
                
                if row is None:
                    return None
                
                return self._deserialize_job(row["job_data"])
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get job {job_id}: {e}") from e
    
    async def get_by_state(self, state: ProcessingState) -> List[IngestionJob]:
        """Get all jobs in a specific state."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT job_data FROM ingestion_jobs WHERE state = ? ORDER BY created_at DESC",
                    (state.value,)
                )
                rows = cursor.fetchall()
                
                return [self._deserialize_job(row["job_data"]) for row in rows]
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get jobs by state {state}: {e}") from e
    
    async def get_active_jobs(self) -> List[IngestionJob]:
        """Get all jobs that are currently active."""
        active_states = [ProcessingState.PENDING.value, ProcessingState.IN_PROGRESS.value]
        
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                placeholders = ",".join("?" * len(active_states))
                cursor = conn.execute(
                    f"SELECT job_data FROM ingestion_jobs WHERE state IN ({placeholders}) ORDER BY created_at",
                    active_states
                )
                rows = cursor.fetchall()
                
                return [self._deserialize_job(row["job_data"]) for row in rows]
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get active jobs: {e}") from e
    
    async def get_jobs_by_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[IngestionJob]:
        """Get jobs created within a date range."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT job_data FROM ingestion_jobs 
                    WHERE created_at BETWEEN ? AND ? 
                    ORDER BY created_at DESC
                """, (start_date, end_date))
                rows = cursor.fetchall()
                
                return [self._deserialize_job(row["job_data"]) for row in rows]
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get jobs by date range: {e}") from e
    
    async def delete(self, job_id: IngestionJobId) -> bool:
        """Delete an ingestion job."""
        try:
            with connection(self._db_path) as conn:
                cursor = conn.execute(
                    "DELETE FROM ingestion_jobs WHERE job_id = ?",
                    (str(job_id),)
                )
                conn.commit()
                return cursor.rowcount > 0
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to delete job {job_id}: {e}") from e
    
    async def get_job_history(self, limit: int = 100) -> List[IngestionJob]:
        """Get recent job history."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT job_data FROM ingestion_jobs ORDER BY created_at DESC LIMIT ?",
                    (limit,)
                )
                rows = cursor.fetchall()
                
                return [self._deserialize_job(row["job_data"]) for row in rows]
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get job history: {e}") from e
    
    async def count_jobs_by_state(self) -> Dict[ProcessingState, int]:
        """Count jobs grouped by their processing state."""
        try:
            with connection(self._db_path) as conn:
                cursor = conn.execute(
                    "SELECT state, COUNT(*) as count FROM ingestion_jobs GROUP BY state"
                )
                rows = cursor.fetchall()
                
                counts = {}
                for row in rows:
                    state = ProcessingState(row[0])
                    counts[state] = row[1]
                
                # Ensure all states are represented
                for state in ProcessingState:
                    if state not in counts:
                        counts[state] = 0
                
                return counts
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to count jobs by state: {e}") from e
    
    def _serialize_job(self, job: IngestionJob) -> str:
        """Serialize job to JSON for storage."""
        # This is a simplified serialization - in production you'd want
        # a more robust serialization strategy
        job_dict = {
            "job_id": str(job.job_id),
            "symbols": [symbol.value for symbol in job.symbols],
            "time_range": {
                "start": job.time_range.start.value.isoformat(),
                "end": job.time_range.end.value.isoformat()
            },
            "configuration": job.configuration.to_dict(),
            "state": job.state.value,
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "failed_at": job.failed_at.isoformat() if job.failed_at else None,
            "error_message": job.error_message,
            "processed_symbols": [symbol.value for symbol in job.processed_symbols],
            "total_bars_processed": job.total_bars_processed
        }
        return json.dumps(job_dict)
    
    def _deserialize_job(self, job_data: str) -> IngestionJob:
        """Deserialize job from JSON storage."""
        # This is a simplified deserialization - in production you'd want
        # proper reconstruction of the domain entity
        job_dict = json.loads(job_data)
        
        # For this example, we're returning a simplified job reconstruction
        # In reality, you'd fully reconstruct the IngestionJob entity
        from ..domain.value_objects import IngestionConfiguration
        from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
        
        job_id = IngestionJobId.from_string(job_dict["job_id"])
        symbols = [Symbol(symbol_str) for symbol_str in job_dict["symbols"]]
        
        time_range = TimeRange(
            start=Timestamp.from_iso(job_dict["time_range"]["start"]),
            end=Timestamp.from_iso(job_dict["time_range"]["end"])
        )
        
        config = IngestionConfiguration.from_dict(job_dict["configuration"])
        
        # Create job and restore state
        job = IngestionJob(job_id, config, symbols, time_range)
        
        # This is simplified - you'd need to properly restore all state
        # including processed symbols, error messages, timestamps, etc.
        
        return job


class SqliteCheckpointRepository(IIngestionCheckpointRepository):
    """SQLite implementation of checkpoint repository."""
    
    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = db_path or Path("ingestion_checkpoints.db")
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize the database schema."""
        with connection(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
                    job_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    last_processed_timestamp INTEGER NOT NULL,
                    records_processed INTEGER NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (job_id, symbol)
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_checkpoints_symbol 
                ON ingestion_checkpoints(symbol)
            """)
    
    async def save_checkpoint(
        self, 
        job_id: IngestionJobId, 
        checkpoint: IngestionCheckpoint
    ) -> None:
        """Save a checkpoint for a specific job and symbol."""
        try:
            with connection(self._db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO ingestion_checkpoints 
                    (job_id, symbol, last_processed_timestamp, records_processed, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    str(job_id),
                    checkpoint.symbol.value,
                    checkpoint.last_processed_timestamp,
                    checkpoint.records_processed,
                    checkpoint.updated_at
                ))
                conn.commit()
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to save checkpoint: {e}") from e
    
    async def get_checkpoint(
        self, 
        job_id: IngestionJobId, 
        symbol: Symbol
    ) -> Optional[IngestionCheckpoint]:
        """Get the latest checkpoint for a job and symbol."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT last_processed_timestamp, records_processed, updated_at
                    FROM ingestion_checkpoints 
                    WHERE job_id = ? AND symbol = ?
                """, (str(job_id), symbol.value))
                
                row = cursor.fetchone()
                if row is None:
                    return None
                
                return IngestionCheckpoint(
                    symbol=symbol,
                    last_processed_timestamp=row["last_processed_timestamp"],
                    records_processed=row["records_processed"],
                    updated_at=datetime.fromisoformat(row["updated_at"])
                )
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get checkpoint: {e}") from e
    
    async def get_all_checkpoints(
        self, 
        job_id: IngestionJobId
    ) -> List[IngestionCheckpoint]:
        """Get all checkpoints for a specific job."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT symbol, last_processed_timestamp, records_processed, updated_at
                    FROM ingestion_checkpoints 
                    WHERE job_id = ?
                """, (str(job_id),))
                
                rows = cursor.fetchall()
                checkpoints = []
                
                for row in rows:
                    checkpoint = IngestionCheckpoint(
                        symbol=Symbol(row["symbol"]),
                        last_processed_timestamp=row["last_processed_timestamp"],
                        records_processed=row["records_processed"],
                        updated_at=datetime.fromisoformat(row["updated_at"])
                    )
                    checkpoints.append(checkpoint)
                
                return checkpoints
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get checkpoints for job {job_id}: {e}") from e
    
    async def delete_checkpoints(self, job_id: IngestionJobId) -> None:
        """Delete all checkpoints for a specific job."""
        try:
            with connection(self._db_path) as conn:
                conn.execute(
                    "DELETE FROM ingestion_checkpoints WHERE job_id = ?",
                    (str(job_id),)
                )
                conn.commit()
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to delete checkpoints for job {job_id}: {e}") from e
    
    async def get_global_checkpoint(self, symbol: Symbol) -> Optional[IngestionCheckpoint]:
        """Get the most recent checkpoint for a symbol across all jobs."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT symbol, last_processed_timestamp, records_processed, updated_at
                    FROM ingestion_checkpoints 
                    WHERE symbol = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                """, (symbol.value,))
                
                row = cursor.fetchone()
                if row is None:
                    return None
                
                return IngestionCheckpoint(
                    symbol=symbol,
                    last_processed_timestamp=row["last_processed_timestamp"],
                    records_processed=row["records_processed"],
                    updated_at=datetime.fromisoformat(row["updated_at"])
                )
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get global checkpoint for {symbol}: {e}") from e
    
    async def cleanup_old_checkpoints(self, older_than: datetime) -> int:
        """Remove checkpoints older than the specified date."""
        try:
            with connection(self._db_path) as conn:
                cursor = conn.execute(
                    "DELETE FROM ingestion_checkpoints WHERE updated_at < ?",
                    (older_than,)
                )
                conn.commit()
                return cursor.rowcount
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to cleanup old checkpoints: {e}") from e


class SqliteMetricsRepository(IIngestionMetricsRepository):
    """SQLite implementation of metrics repository."""
    
    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = db_path or Path("ingestion_metrics.db")
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize the database schema."""
        with connection(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_metrics (
                    job_id TEXT PRIMARY KEY,
                    metrics_data TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
            """)
    
    async def save_metrics(
        self, 
        job_id: IngestionJobId, 
        metrics: ProcessingMetrics
    ) -> None:
        """Save processing metrics for a job."""
        try:
            metrics_data = json.dumps(metrics.to_dict())
            
            with connection(self._db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO ingestion_metrics 
                    (job_id, metrics_data, created_at)
                    VALUES (?, ?, ?)
                """, (
                    str(job_id),
                    metrics_data,
                    datetime.now()
                ))
                conn.commit()
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to save metrics for job {job_id}: {e}") from e
    
    async def get_metrics(self, job_id: IngestionJobId) -> Optional[ProcessingMetrics]:
        """Get metrics for a specific job."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT metrics_data FROM ingestion_metrics WHERE job_id = ?",
                    (str(job_id),)
                )
                row = cursor.fetchone()
                
                if row is None:
                    return None
                
                metrics_dict = json.loads(row["metrics_data"])
                return ProcessingMetrics(
                    symbols_processed=metrics_dict["symbols_processed"],
                    symbols_failed=metrics_dict["symbols_failed"],
                    total_bars_ingested=metrics_dict["total_bars_ingested"],
                    total_processing_time_seconds=metrics_dict["total_processing_time_seconds"],
                    average_processing_time_per_symbol=metrics_dict["average_processing_time_per_symbol"],
                    peak_memory_usage_mb=metrics_dict.get("peak_memory_usage_mb")
                )
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get metrics for job {job_id}: {e}") from e
    
    async def get_metrics_history(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[tuple[IngestionJobId, ProcessingMetrics]]:
        """Get metrics for jobs within a date range."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT job_id, metrics_data
                    FROM ingestion_metrics 
                    WHERE created_at BETWEEN ? AND ?
                    ORDER BY created_at DESC
                """, (start_date, end_date))
                
                results = []
                for row in cursor.fetchall():
                    job_id = IngestionJobId(row["job_id"])
                    metrics_dict = json.loads(row["metrics_data"])
                    metrics = ProcessingMetrics(
                        symbols_processed=metrics_dict["symbols_processed"],
                        symbols_failed=metrics_dict["symbols_failed"],
                        total_bars_ingested=metrics_dict["total_bars_ingested"],
                        total_processing_time_seconds=metrics_dict["total_processing_time_seconds"],
                        average_processing_time_per_symbol=metrics_dict["average_processing_time_per_symbol"],
                        peak_memory_usage_mb=metrics_dict.get("peak_memory_usage_mb")
                    )
                    results.append((job_id, metrics))
                
                return results
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get metrics history: {e}") from e
    
    async def get_average_metrics(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> Optional[ProcessingMetrics]:
        """Calculate average metrics across jobs in a date range."""
        try:
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT metrics_data
                    FROM ingestion_metrics 
                    WHERE created_at BETWEEN ? AND ?
                """, (start_date, end_date))
                
                all_metrics = []
                for row in cursor.fetchall():
                    metrics_dict = json.loads(row["metrics_data"])
                    all_metrics.append(metrics_dict)
                
                if not all_metrics:
                    return None
                
                # Calculate averages
                avg_symbols_processed = sum(m["symbols_processed"] for m in all_metrics) / len(all_metrics)
                avg_symbols_failed = sum(m["symbols_failed"] for m in all_metrics) / len(all_metrics)
                avg_bars_ingested = sum(m["total_bars_ingested"] for m in all_metrics) / len(all_metrics)
                avg_processing_time = sum(m["total_processing_time_seconds"] for m in all_metrics) / len(all_metrics)
                avg_time_per_symbol = sum(m["average_processing_time_per_symbol"] for m in all_metrics) / len(all_metrics)
                
                # Average memory usage (only for jobs that have it)
                memory_values = [m.get("peak_memory_usage_mb") for m in all_metrics if m.get("peak_memory_usage_mb") is not None]
                avg_memory = sum(memory_values) / len(memory_values) if memory_values else None
                
                return ProcessingMetrics(
                    symbols_processed=int(avg_symbols_processed),
                    symbols_failed=int(avg_symbols_failed),
                    total_bars_ingested=int(avg_bars_ingested),
                    total_processing_time_seconds=avg_processing_time,
                    average_processing_time_per_symbol=avg_time_per_symbol,
                    peak_memory_usage_mb=avg_memory
                )
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to calculate average metrics: {e}") from e
    
    async def get_performance_trends(
        self, 
        days: int = 30
    ) -> List[tuple[datetime, float]]:
        """Get daily average processing performance over time."""
        try:
            # Calculate start date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            with connection(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT DATE(created_at) as date, 
                           AVG(json_extract(metrics_data, '$.total_processing_time_seconds')) as avg_time
                    FROM ingestion_metrics 
                    WHERE created_at BETWEEN ? AND ?
                    GROUP BY DATE(created_at)
                    ORDER BY date
                """, (start_date, end_date))
                
                trends = []
                for row in cursor.fetchall():
                    date_obj = datetime.fromisoformat(row["date"])
                    avg_time = float(row["avg_time"]) if row["avg_time"] else 0.0
                    trends.append((date_obj, avg_time))
                
                return trends
                
        except sqlite3.Error as e:
            raise IngestionRepositoryError(f"Failed to get performance trends: {e}") from e