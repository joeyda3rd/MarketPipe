# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from prometheus_client import Counter, Gauge, Histogram, Summary

from marketpipe.infrastructure.sqlite_async_mixin import SqliteAsyncMixin

# Rate limiter metrics (imported from rate_limit module)
from marketpipe.ingestion.infrastructure.rate_limit import RATE_LIMITER_WAITS

# Event loop lag monitoring (imported from metrics_server module)
from marketpipe.metrics_server import EVENT_LOOP_LAG
from marketpipe.migrations import apply_pending

# Core metrics with full label set: source, provider, feed
REQUESTS = Counter("mp_requests_total", "API requests", ["source", "provider", "feed"])
ERRORS = Counter("mp_errors_total", "Errors", ["source", "provider", "feed", "code"])
LATENCY = Histogram("mp_request_latency_seconds", "Latency", ["source", "provider", "feed"])

# Legacy metrics for backward compatibility (deprecated)
LEGACY_REQUESTS = Counter("mp_requests_legacy_total", "API requests (legacy)", ["source"])
LEGACY_ERRORS = Counter("mp_errors_legacy_total", "Errors (legacy)", ["source", "code"])
LEGACY_LATENCY = Histogram("mp_request_legacy_latency_seconds", "Latency (legacy)", ["source"])

BACKLOG = Gauge("mp_backlog_jobs", "Coordinator queue size")

# New metrics for ingestion/validation/aggregation
INGEST_ROWS = Counter("mp_ingest_rows_total", "Rows ingested", ["symbol"])
VALIDATION_ERRORS = Counter(
    "mp_validation_errors_total", "Validation errors", ["symbol", "error_type"]
)
AGG_ROWS = Counter("mp_aggregation_rows_total", "Rows aggregated", ["frame", "symbol"])

# Summary metrics for tracking operational data
PROCESSING_TIME = Summary("mp_processing_time_seconds", "Processing time", ["operation"])

# Symbol pipeline metrics
SYMBOLS_ROWS = Counter(
    "mp_symbols_rows_total", "SCD rows written to symbols_master parquet dataset", ["action"]
)

SYMBOLS_SNAPSHOT_RECORDS = Counter(
    "mp_symbols_snapshot_records_total", "Raw provider symbol rows staged for dedupe"
)

SYMBOLS_NULL_RATIO = Gauge(
    "mp_symbols_null_ratio", "Share of NULLs per column in v_symbol_latest", ["column"]
)

# Backfill metrics
BACKFILL_GAPS_FOUND_TOTAL = Counter(
    "mp_backfill_gaps_found_total",
    "Total number of per-symbol gaps detected for backfill",
    ["symbol"],
)
BACKFILL_GAP_LATENCY_SECONDS = Histogram(
    "mp_backfill_gap_latency_seconds",
    "Duration of individual gap back-fill runs",
    ["symbol"],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120],
)

# Data pruning metrics
DATA_PRUNED_BYTES_TOTAL = Counter(
    "mp_data_pruned_bytes_total",
    "Total bytes of data pruned/deleted",
    ["type"],  # parquet, sqlite, etc.
)
DATA_PRUNED_ROWS_TOTAL = Counter(
    "mp_data_pruned_rows_total", "Total rows of data pruned/deleted", ["type"]  # sqlite, etc.
)

__all__ = [
    "REQUESTS",
    "ERRORS",
    "LATENCY",
    "LEGACY_REQUESTS",
    "LEGACY_ERRORS",
    "LEGACY_LATENCY",
    "BACKLOG",
    "INGEST_ROWS",
    "VALIDATION_ERRORS",
    "AGG_ROWS",
    "PROCESSING_TIME",
    "RATE_LIMITER_WAITS",
    "EVENT_LOOP_LAG",
    "BACKFILL_GAPS_FOUND_TOTAL",
    "BACKFILL_GAP_LATENCY_SECONDS",
    "DATA_PRUNED_BYTES_TOTAL",
    "DATA_PRUNED_ROWS_TOTAL",
    # Symbol pipeline metrics
    "SYMBOLS_ROWS",
    "SYMBOLS_SNAPSHOT_RECORDS",
    "SYMBOLS_NULL_RATIO",
    # Repository and utilities
    "record_metric",
    "MetricPoint",
    "TrendPoint",
    "SqliteMetricsRepository",
]


@dataclass(frozen=True)
class MetricPoint:
    """Represents a single metric data point."""

    timestamp: datetime
    metric: str
    value: float
    provider: str = "unknown"
    feed: str = "unknown"


@dataclass(frozen=True)
class TrendPoint:
    """Represents a trend data point with bucket information."""

    bucket_start: datetime
    bucket_end: datetime
    average_value: float
    sample_count: int


class SqliteMetricsRepository(SqliteAsyncMixin):
    """SQLite-based repository for storing and querying metric history."""

    def __init__(self, db_path: Optional[str] = None):
        # Check environment variable first, then use provided path, then default
        if db_path is None:
            db_path = os.environ.get("METRICS_DB_PATH", "data/db/core.db")

        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(self._db_path)  # For async connection helper
        # Apply migrations on first use
        apply_pending(self._db_path)

    async def record(
        self, name: str, value: float, provider: str = "unknown", feed: str = "unknown"
    ) -> None:
        """Record a metric data point with provider and feed labels."""
        timestamp = int(datetime.now().timestamp())

        async with self._conn() as db:
            await db.execute(
                "INSERT INTO metrics (ts, name, value, provider, feed) VALUES (?, ?, ?, ?, ?)",
                (timestamp, name, value, provider, feed),
            )
            await db.commit()

    async def get_metrics_history(
        self, metric: str, *, since: Optional[datetime] = None
    ) -> list[MetricPoint]:
        """Get metric history, optionally filtered by time."""
        async with self._conn() as db:
            if since:
                since_ts = int(since.timestamp())
                cursor = await db.execute(
                    """
                    SELECT ts, name, value,
                           COALESCE(provider, 'unknown') as provider,
                           COALESCE(feed, 'unknown') as feed
                    FROM metrics
                    WHERE name = ? AND ts >= ?
                    ORDER BY ts
                """,
                    (metric, since_ts),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT ts, name, value,
                           COALESCE(provider, 'unknown') as provider,
                           COALESCE(feed, 'unknown') as feed
                    FROM metrics
                    WHERE name = ?
                    ORDER BY ts
                """,
                    (metric,),
                )

            rows = await cursor.fetchall()
            return [
                MetricPoint(
                    timestamp=datetime.fromtimestamp(row[0]),
                    metric=row[1],
                    value=row[2],
                    provider=row[3],
                    feed=row[4],
                )
                for row in rows
            ]

    async def get_average_metrics(self, metric: str, *, window_minutes: int) -> float:
        """Get average metric value over a time window."""
        since = datetime.now().timestamp() - (window_minutes * 60)

        async with self._conn() as db:
            cursor = await db.execute(
                """
                SELECT AVG(value) FROM metrics
                WHERE name = ? AND ts >= ?
            """,
                (metric, since),
            )

            row = await cursor.fetchone()
            result = row[0] if row else None
            return result if result is not None else 0.0

    async def get_performance_trends(self, metric: str, *, buckets: int = 24) -> list[TrendPoint]:
        """Get performance trends over time divided into buckets."""
        now = datetime.now()
        bucket_size_minutes = (24 * 60) // buckets  # Distribute 24 hours across buckets
        trends = []

        async with self._conn() as db:
            for i in range(buckets):
                bucket_start_ts = now.timestamp() - ((buckets - i) * bucket_size_minutes * 60)
                bucket_end_ts = now.timestamp() - ((buckets - i - 1) * bucket_size_minutes * 60)

                cursor = await db.execute(
                    """
                    SELECT AVG(value), COUNT(*) FROM metrics
                    WHERE name = ? AND ts >= ? AND ts < ?
                """,
                    (metric, bucket_start_ts, bucket_end_ts),
                )

                row = await cursor.fetchone()
                avg_value, count = row if row else (None, 0)

                trend_point = TrendPoint(
                    bucket_start=datetime.fromtimestamp(bucket_start_ts),
                    bucket_end=datetime.fromtimestamp(bucket_end_ts),
                    average_value=avg_value if avg_value is not None else 0.0,
                    sample_count=count or 0,
                )
                trends.append(trend_point)

        return trends

    async def list_metric_names(self) -> list[str]:
        """List all available metric names."""
        async with self._conn() as db:
            cursor = await db.execute("SELECT DISTINCT name FROM metrics ORDER BY name")
            rows = await cursor.fetchall()
            return [row[0] for row in rows]


# Global repository instance for record_metric function
_metrics_repo: Optional[SqliteMetricsRepository] = None


def get_metrics_repository() -> SqliteMetricsRepository:
    """Get or create the global metrics repository.

    Respects METRICS_DB_PATH and resets the cached repository if the target
    database path has changed (e.g., in tests that set the env var).
    """
    global _metrics_repo
    desired_path_env = os.environ.get("METRICS_DB_PATH")
    desired_path = desired_path_env or "data/db/core.db"
    desired_abs = str(Path(desired_path).resolve())

    if _metrics_repo is not None:
        try:
            current_abs = str(Path(_metrics_repo.db_path).resolve())
        except Exception:
            current_abs = ""

        # Recreate if the resolved target path changed (e.g., different CWD or env)
        if current_abs != desired_abs:
            _metrics_repo = SqliteMetricsRepository(desired_path)
        return _metrics_repo

    # Create new repository at the resolved path
    _metrics_repo = SqliteMetricsRepository(desired_path)
    return _metrics_repo


def record_metric(
    name: str,
    value: float,
    *,
    provider: str = "unknown",
    feed: str = "unknown",
    source: str = "unknown",
) -> None:
    """Record a metric to both Prometheus and SQLite persistence.

    This function updates Prometheus counters/summaries and persists
    the data to SQLite for historical analysis.

    Args:
        name: The metric name
        value: The metric value
        provider: The data provider (e.g., "alpaca", "polygon")
        feed: The data feed type (e.g., "iex", "sip")
        source: The source component (for backward compatibility)
    """
    # Update Prometheus metrics with full label set
    if "request" in name.lower():
        REQUESTS.labels(source=source, provider=provider, feed=feed).inc(value)
        # Also update legacy metric for backward compatibility
        LEGACY_REQUESTS.labels(source=source).inc(value)
    elif "error" in name.lower():
        error_code = "unknown"
        # Try to extract error code from metric name
        if "_" in name:
            parts = name.split("_")
            error_code = parts[-1] if parts[-1] not in ["total", "count"] else "unknown"
        ERRORS.labels(source=source, provider=provider, feed=feed, code=error_code).inc(value)
        LEGACY_ERRORS.labels(source=source, code=error_code).inc(value)
    elif "latency" in name.lower() or "duration" in name.lower():
        LATENCY.labels(source=source, provider=provider, feed=feed).observe(value)
        LEGACY_LATENCY.labels(source=source).observe(value)

    # Update operation-specific metrics
    if "ingest" in name.lower():
        PROCESSING_TIME.labels(operation="ingestion").observe(value)
    elif "validation" in name.lower():
        PROCESSING_TIME.labels(operation="validation").observe(value)
    elif "aggregation" in name.lower():
        PROCESSING_TIME.labels(operation="aggregation").observe(value)

    # Check environment variable for SQLite persistence
    if os.environ.get("MP_DISABLE_SQLITE_METRICS", "").lower() in ("1", "true", "yes"):
        # SQLite metrics disabled via environment variable
        return

    # Persist to SQLite - handle event loop contexts carefully
    repo = get_metrics_repository()

    try:
        # Try to determine if we're in an async context
        loop = asyncio.get_running_loop()
        # We're in an async context, schedule the task
        loop.create_task(repo.record(name, value, provider, feed))
        # Don't wait for completion to avoid blocking
    except RuntimeError:
        # No running event loop; perform a synchronous write using a temporary loop
        try:
            asyncio.run(repo.record(name, value, provider, feed))
        except Exception:
            # Swallow persistence errors in non-critical contexts (e.g., CLI/help runs)
            pass
