# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from pathlib import Path
from datetime import date

from marketpipe.events import EventBus, IngestionJobCompleted
from marketpipe.domain.value_objects import Symbol
from ..domain.services import AggregationDomainService
from ..domain.value_objects import DEFAULT_SPECS
from ..domain.events import AggregationCompleted, AggregationFailed
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

    def handle_ingestion_completed(self, event: IngestionJobCompleted) -> None:
        """Handle IngestionJobCompleted event by running aggregation.
        
        Args:
            event: Ingestion job completed event
        """
        try:
            self.log.info(f"Starting aggregation for job {event.job_id}")
            
            # Record aggregation start metrics
            from marketpipe.metrics import record_metric
            record_metric("aggregation_jobs_started", 1)
            
            # Generate SQL for each frame
            sql_pairs = [(spec, self._domain.duckdb_sql(spec)) for spec in DEFAULT_SPECS]
            
            # Run aggregation
            result = self._engine.aggregate_job(event.job_id, sql_pairs)
            
            # Record success metrics
            frames_processed = len(DEFAULT_SPECS)
            record_metric("aggregation_jobs_success", 1)
            record_metric("aggregation_frames_processed", frames_processed)
            
            # If engine returns row counts, record those too
            if hasattr(result, 'total_rows_aggregated'):
                record_metric("aggregation_rows_processed", result.total_rows_aggregated)
            
            # Publish success event
            success_event = AggregationCompleted(event.job_id, frames_processed)
            EventBus.publish(success_event)
            
            self.log.info(f"Aggregation completed for job {event.job_id}: {frames_processed} frames processed")
            
        except Exception as e:
            self.log.error(f"Aggregation failed for job {event.job_id}: {e}")
            
            # Record failure metrics
            from marketpipe.metrics import record_metric
            record_metric("aggregation_jobs_failed", 1)
            
            # Publish failure event
            failure_event = AggregationFailed(event.job_id, str(e))
            EventBus.publish(failure_event)
            
            # Re-raise to maintain error visibility
            raise

    def run_manual_aggregation(self, job_id: str) -> None:
        """Run aggregation manually for a specific job.
        
        Args:
            job_id: Job identifier to aggregate
        """
        self.log.info(f"Running manual aggregation for job {job_id}")
        
        # Record manual aggregation metrics
        from marketpipe.metrics import record_metric
        record_metric("aggregation_manual_runs", 1)
        
        # Create fake event and handle it
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("MANUAL"),
            trading_date=date.today(),
            bars_processed=0,
            success=True
        )
        self.handle_ingestion_completed(event)

    @classmethod
    def build_default(cls) -> AggregationRunnerService:
        """Build service with default configuration.
        
        Returns:
            Configured aggregation runner service
        """
        engine = DuckDBAggregationEngine(
            raw_root=Path("data/raw"), 
            agg_root=Path("data/agg")
        )
        domain = AggregationDomainService()
        
        return cls(engine=engine, domain=domain)

    @classmethod
    def register(cls) -> None:
        """Register event listener for ingestion completed events."""
        service = cls.build_default()
        EventBus.subscribe(IngestionJobCompleted, service.handle_ingestion_completed)
        
        logging.getLogger(cls.__name__).info("Aggregation service registered for IngestionJobCompleted events") 