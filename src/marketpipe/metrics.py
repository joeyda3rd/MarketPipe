# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import sqlite3
import asyncio
import os

from prometheus_client import Counter, Histogram, Gauge, Summary

from marketpipe.infrastructure.sqlite_pool import connection
from marketpipe.migrations import apply_pending

# Existing metrics
REQUESTS = Counter("mp_requests_total", "API requests", ["source"])
ERRORS = Counter("mp_errors_total", "Errors", ["source", "code"])
LATENCY = Histogram("mp_request_latency_seconds", "Latency", ["source"])
BACKLOG = Gauge("mp_backlog_jobs", "Coordinator queue size")

# New metrics for ingestion/validation/aggregation
INGEST_ROWS = Counter("mp_ingest_rows_total", "Rows ingested", ["symbol"])
VALIDATION_ERRORS = Counter("mp_validation_errors_total", "Validation errors", ["symbol", "error_type"])
AGG_ROWS = Counter("mp_aggregation_rows_total", "Rows aggregated", ["frame", "symbol"])

# Summary metrics for tracking operational data
PROCESSING_TIME = Summary("mp_processing_time_seconds", "Processing time", ["operation"])

__all__ = [
    "REQUESTS", "ERRORS", "LATENCY", "BACKLOG", 
    "INGEST_ROWS", "VALIDATION_ERRORS", "AGG_ROWS", "PROCESSING_TIME",
    "record_metric", "MetricPoint", "TrendPoint", "SqliteMetricsRepository"
]


@dataclass(frozen=True)
class MetricPoint:
    """Represents a single metric data point."""
    timestamp: datetime
    metric: str
    value: float


@dataclass(frozen=True)
class TrendPoint:
    """Represents a trend data point with bucket information."""
    bucket_start: datetime
    bucket_end: datetime
    average_value: float
    sample_count: int


class SqliteMetricsRepository:
    """SQLite-based repository for storing and querying metric history."""
    
    def __init__(self, db_path: Optional[str] = None):
        # Check environment variable first, then use provided path, then default
        if db_path is None:
            db_path = os.environ.get('METRICS_DB_PATH', "data/db/core.db")
        
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # Apply migrations on first use
        apply_pending(self._db_path)
    
    async def record(self, name: str, value: float) -> None:
        """Record a metric data point."""
        timestamp = int(datetime.now().timestamp())
        
        # Use asyncio to avoid blocking if possible
        def _record():
            with connection(self._db_path) as conn:
                conn.execute(
                    "INSERT INTO metrics (ts, name, value) VALUES (?, ?, ?)",
                    (timestamp, name, value)
                )
                conn.commit()
        
        # Run in thread pool if we're in async context
        if asyncio.get_event_loop().is_running():
            await asyncio.get_event_loop().run_in_executor(None, _record)
        else:
            _record()
    
    async def get_metrics_history(
        self, 
        metric: str, 
        *, 
        since: Optional[datetime] = None
    ) -> List[MetricPoint]:
        """Get metric history, optionally filtered by time."""
        def _query():
            with connection(self._db_path) as conn:
                if since:
                    since_ts = int(since.timestamp())
                    cursor = conn.execute("""
                        SELECT ts, name, value FROM metrics 
                        WHERE name = ? AND ts >= ?
                        ORDER BY ts
                    """, (metric, since_ts))
                else:
                    cursor = conn.execute("""
                        SELECT ts, name, value FROM metrics 
                        WHERE name = ?
                        ORDER BY ts
                    """, (metric,))
                
                return [
                    MetricPoint(
                        timestamp=datetime.fromtimestamp(row[0]),
                        metric=row[1],
                        value=row[2]
                    )
                    for row in cursor.fetchall()
                ]
        
        # Run in thread pool if we're in async context  
        if asyncio.get_event_loop().is_running():
            return await asyncio.get_event_loop().run_in_executor(None, _query)
        else:
            return _query()
    
    async def get_average_metrics(
        self, 
        metric: str, 
        *, 
        window_minutes: int
    ) -> float:
        """Get average metric value over a time window."""
        since = datetime.now().timestamp() - (window_minutes * 60)
        
        def _query():
            with connection(self._db_path) as conn:
                cursor = conn.execute("""
                    SELECT AVG(value) FROM metrics 
                    WHERE name = ? AND ts >= ?
                """, (metric, since))
                
                result = cursor.fetchone()[0]
                return result if result is not None else 0.0
        
        if asyncio.get_event_loop().is_running():
            return await asyncio.get_event_loop().run_in_executor(None, _query)
        else:
            return _query()
    
    async def get_performance_trends(
        self, 
        metric: str, 
        *, 
        buckets: int = 24
    ) -> List[TrendPoint]:
        """Get performance trends over time divided into buckets."""
        now = datetime.now()
        bucket_size_minutes = (24 * 60) // buckets  # Distribute 24 hours across buckets
        
        def _query():
            trends = []
            with connection(self._db_path) as conn:
                for i in range(buckets):
                    bucket_start_ts = now.timestamp() - ((buckets - i) * bucket_size_minutes * 60)
                    bucket_end_ts = now.timestamp() - ((buckets - i - 1) * bucket_size_minutes * 60)
                    
                    cursor = conn.execute("""
                        SELECT AVG(value), COUNT(*) FROM metrics 
                        WHERE name = ? AND ts >= ? AND ts < ?
                    """, (metric, bucket_start_ts, bucket_end_ts))
                    
                    avg_value, count = cursor.fetchone()
                    
                    trend_point = TrendPoint(
                        bucket_start=datetime.fromtimestamp(bucket_start_ts),
                        bucket_end=datetime.fromtimestamp(bucket_end_ts),
                        average_value=avg_value if avg_value is not None else 0.0,
                        sample_count=count or 0
                    )
                    trends.append(trend_point)
            
            return trends
        
        if asyncio.get_event_loop().is_running():
            return await asyncio.get_event_loop().run_in_executor(None, _query)
        else:
            return _query()
    
    def list_metric_names(self) -> List[str]:
        """List all available metric names."""
        with connection(self._db_path) as conn:
            cursor = conn.execute("SELECT DISTINCT name FROM metrics ORDER BY name")
            return [row[0] for row in cursor.fetchall()]


# Global repository instance for record_metric function
_metrics_repo: Optional[SqliteMetricsRepository] = None


def get_metrics_repository() -> SqliteMetricsRepository:
    """Get or create the global metrics repository."""
    global _metrics_repo
    # Always recreate if environment variable might have changed
    _metrics_repo = SqliteMetricsRepository()
    return _metrics_repo


def record_metric(name: str, value: float) -> None:
    """Record a metric to both Prometheus and SQLite persistence.
    
    This function updates Prometheus counters/summaries and persists
    the data to SQLite for historical analysis.
    """
    # Update Prometheus metrics based on metric name
    if "ingest" in name.lower():
        PROCESSING_TIME.labels(operation="ingestion").observe(value)
    elif "validation" in name.lower():
        PROCESSING_TIME.labels(operation="validation").observe(value)
    elif "aggregation" in name.lower():
        PROCESSING_TIME.labels(operation="aggregation").observe(value)
    
    # Persist to SQLite (run async if possible)
    repo = get_metrics_repository()
    try:
        # Try to run async if we're in an event loop
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Schedule for later execution to avoid blocking
            loop.create_task(repo.record(name, value))
        else:
            # Run synchronously if no event loop
            asyncio.run(repo.record(name, value))
    except RuntimeError:
        # No event loop, run synchronously
        asyncio.run(repo.record(name, value))
