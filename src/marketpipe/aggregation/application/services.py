# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from marketpipe.bootstrap import get_event_bus
from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.domain.value_objects import Symbol

from ..domain.events import AggregationCompleted, AggregationFailed
from ..domain.services import AggregationDomainService
from ..domain.value_objects import DEFAULT_SPECS
from ..infrastructure.duckdb_engine import DuckDBAggregationEngine


class AggregationRunnerService:
    """Application service for coordinating aggregation operations."""

    def __init__(self, engine: DuckDBAggregationEngine, domain: AggregationDomainService):
        """Initialize aggregation runner service.

        Args:
            engine: DuckDB aggregation engine
            domain: Domain service for SQL generation
        """
        self._engine = engine
        self._domain = domain
        self.log = logging.getLogger(self.__class__.__name__)

    def _extract_provider_feed_info(self, event: IngestionJobCompleted) -> tuple[str, str]:
        """Extract provider and feed information from event or defaults."""
        # Try to get provider/feed from event attributes
        provider = getattr(event, "provider", "unknown")
        feed = getattr(event, "feed", "unknown")

        # If not available in event, try to infer from job context or use defaults
        if provider == "unknown" or feed == "unknown":
            # For now, use defaults - in the future we could look up job configuration
            provider = "unknown"
            feed = "unknown"

        return provider, feed

    def handle_ingestion_completed(self, event: IngestionJobCompleted) -> None:
        """Handle IngestionJobCompleted event by running aggregation.

        Args:
            event: Ingestion job completed event
        """
        try:
            self.log.info(f"Starting aggregation for job {event.job_id}")

            # Extract provider/feed info for metrics
            provider, feed = self._extract_provider_feed_info(event)

            # Record aggregation start metrics
            from marketpipe.metrics import record_metric

            record_metric("aggregation_jobs_started", 1, provider=provider, feed=feed)

            # Generate SQL for each frame
            sql_pairs = [(spec, self._domain.duckdb_sql(spec)) for spec in DEFAULT_SPECS]

            # Run aggregation
            result = self._engine.aggregate_job(event.job_id, sql_pairs)

            # Refresh DuckDB views to pick up new data
            from ..infrastructure.duckdb_views import refresh_views

            refresh_views()

            # Record success metrics
            frames_processed = len(DEFAULT_SPECS)
            record_metric("aggregation_jobs_success", 1, provider=provider, feed=feed)
            record_metric(
                "aggregation_frames_processed", frames_processed, provider=provider, feed=feed
            )

            # If engine returns row counts, record those too
            if hasattr(result, "total_rows_aggregated"):
                record_metric(
                    "aggregation_rows_processed",
                    result.total_rows_aggregated,
                    provider=provider,
                    feed=feed,
                )

            # Publish success event
            success_event = AggregationCompleted(event.job_id, frames_processed)
            get_event_bus().publish(success_event)

            self.log.info(
                f"Aggregation completed for job {event.job_id}: {frames_processed} frames processed"
            )

        except Exception as e:
            self.log.error(f"Aggregation failed for job {event.job_id}: {e}")

            # Extract provider/feed for error metrics too
            provider, feed = self._extract_provider_feed_info(event)

            # Record failure metrics
            from marketpipe.metrics import record_metric

            record_metric("aggregation_jobs_failed", 1, provider=provider, feed=feed)

            # Publish failure event
            failure_event = AggregationFailed(event.job_id, str(e))
            get_event_bus().publish(failure_event)

            # Re-raise to maintain error visibility
            raise

    def run_manual_aggregation(self, job_id: str) -> None:
        """Run aggregation manually for a specific job.

        Args:
            job_id: Job identifier to aggregate
        """
        self.log.info(f"Running manual aggregation for job {job_id}")

        # Record manual aggregation metrics with defaults
        from marketpipe.metrics import record_metric

        record_metric("aggregation_manual_runs", 1, provider="manual", feed="manual")

        # Create fake event and handle it
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("MANUAL"),
            trading_date=date.today(),
            bars_processed=0,
            success=True,
        )
        self.handle_ingestion_completed(event)

    @classmethod
    def build_default(cls) -> AggregationRunnerService:
        """Build service with default configuration.

        Returns:
            Configured aggregation runner service
        """
        engine = DuckDBAggregationEngine(raw_root=Path("data/raw"), agg_root=Path("data/agg"))
        domain = AggregationDomainService()

        return cls(engine=engine, domain=domain)

    @classmethod
    def register(cls) -> None:
        """Register event listener for ingestion completed events."""
        service = cls.build_default()
        get_event_bus().subscribe(IngestionJobCompleted, service.handle_ingestion_completed)

        logging.getLogger(cls.__name__).info(
            "Aggregation service registered for IngestionJobCompleted events"
        )
