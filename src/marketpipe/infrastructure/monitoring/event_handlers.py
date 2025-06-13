# SPDX-License-Identifier: Apache-2.0
"""Event handlers for automatic metrics collection.

This module subscribes to domain events and automatically records
metrics to both Prometheus and SQLite for historical analysis.

This is part of the infrastructure layer and contains all concrete
monitoring implementations that were previously mixed into the domain.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from marketpipe.events import (
    EventBus,
    IngestionJobCompleted,
    ValidationFailed,
)
from marketpipe.aggregation.domain.events import (
    AggregationCompleted,
    AggregationFailed,
)
from marketpipe.validation.domain.events import ValidationCompleted
from marketpipe.metrics import (
    record_metric,
    INGEST_ROWS,
    VALIDATION_ERRORS,
    AGG_ROWS,
    REQUESTS,
    ERRORS,
)
from marketpipe.domain.events import (
    IngestionJobCompleted as DomainIngestionJobCompleted,
    ValidationFailed as DomainValidationFailed,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _handle_ingestion_completed(event: IngestionJobCompleted) -> None:
    """Handle ingestion job completion events."""
    try:
        # Record general ingestion metrics
        record_metric("ingest_jobs", 1)

        # Update Prometheus counters
        if event.success:
            REQUESTS.labels(source="ingestion").inc()
            # Record per-symbol ingestion
            if hasattr(event, "symbol") and event.symbol:
                INGEST_ROWS.labels(symbol=str(event.symbol)).inc(event.bars_processed)
                record_metric(f"ingest_bars_{event.symbol}", event.bars_processed)
        else:
            ERRORS.labels(source="ingestion", code="job_failed").inc()
            record_metric("ingest_failures", 1)

        # Record job processing metrics
        if hasattr(event, "processing_time_seconds"):
            record_metric("ingest_processing_time", event.processing_time_seconds)

        logger.debug(f"Recorded metrics for ingestion job completion: {event.job_id}")

    except Exception as e:
        logger.error(f"Failed to record ingestion metrics: {e}")


def _handle_validation_completed(event: ValidationCompleted) -> None:
    """Handle validation completion events."""
    try:
        record_metric("validation_jobs", 1)

        # Record validation success
        REQUESTS.labels(source="validation").inc()

        # Extract symbol from the validation result
        if hasattr(event, "result") and hasattr(event.result, "symbol"):
            symbol = event.result.symbol
            record_metric(f"validation_completed_{symbol}", 1)

        logger.debug("Recorded metrics for validation completion")

    except Exception as e:
        logger.error(f"Failed to record validation metrics: {e}")


def _handle_validation_failed(event: ValidationFailed) -> None:
    """Handle validation failure events."""
    try:
        # Record validation error metrics with proper labels
        symbol = str(event.symbol)
        error_type = getattr(event, "rule_id", "unknown")

        VALIDATION_ERRORS.labels(symbol=symbol, error_type=error_type).inc()
        record_metric("validation_errors", 1)
        record_metric(f"validation_errors_{symbol}", 1)

        # Update Prometheus error counter
        ERRORS.labels(source="validation", code="validation_failed").inc()

        logger.debug(f"Recorded metrics for validation failure: {symbol}")

    except Exception as e:
        logger.error(f"Failed to record validation failure metrics: {e}")


def _handle_aggregation_completed(event: AggregationCompleted) -> None:
    """Handle aggregation completion events."""
    try:
        record_metric("aggregation_jobs", 1)

        # Record aggregation success
        REQUESTS.labels(source="aggregation").inc()

        # Record frames processed with proper labels
        if hasattr(event, "frames_processed"):
            # Use default values if specific attributes aren't available
            frame = getattr(event, "frame", "1m")  # Default frame
            symbol = getattr(event, "symbol", "ALL")  # Default symbol

            AGG_ROWS.labels(frame=frame, symbol=symbol).inc(event.frames_processed)
            record_metric("aggregation_frames", event.frames_processed)

        logger.debug(f"Recorded metrics for aggregation completion: {event.job_id}")

    except Exception as e:
        logger.error(f"Failed to record aggregation metrics: {e}")


def _handle_aggregation_failed(event: AggregationFailed) -> None:
    """Handle aggregation failure events."""
    try:
        record_metric("aggregation_failures", 1)

        # Record aggregation error
        ERRORS.labels(source="aggregation", code="job_failed").inc()

        # Record frame-specific failure if available
        frame = getattr(event, "frame", "unknown")
        record_metric(f"aggregation_failures_{frame}", 1)

        logger.debug(f"Recorded metrics for aggregation failure: {event.job_id}")

    except Exception as e:
        logger.error(f"Failed to record aggregation failure metrics: {e}")


def _track_domain_ingestion_metrics(event: DomainIngestionJobCompleted) -> None:
    """Track ingestion job metrics from domain events."""
    try:
        if event.success:
            REQUESTS.labels(vendor="ingestion").inc()
        else:
            ERRORS.labels(vendor="ingestion", status_code="job_failed").inc()
        
        logger.debug(f"Recorded domain ingestion metrics for job: {event.job_id}")
    except Exception as e:
        logger.error(f"Failed to record domain ingestion metrics: {e}")


def _track_domain_validation_metrics(event: DomainValidationFailed) -> None:
    """Track validation failure metrics from domain events."""
    try:
        ERRORS.labels(vendor="validation", status_code="validation_failed").inc()
        
        logger.debug(f"Recorded domain validation metrics for: {event.symbol}")
    except Exception as e:
        logger.error(f"Failed to record domain validation metrics: {e}")


def register() -> None:
    """Register all event handlers for metrics collection.

    This function registers event handlers with the EventBus to automatically
    collect metrics when domain events are published.
    
    This is the main public API function that should be called during bootstrap
    to set up all monitoring infrastructure.
    """
    try:
        # Subscribe to application-level ingestion events
        EventBus.subscribe(IngestionJobCompleted, _handle_ingestion_completed)

        # Subscribe to validation events
        EventBus.subscribe(ValidationCompleted, _handle_validation_completed)
        EventBus.subscribe(ValidationFailed, _handle_validation_failed)

        # Subscribe to aggregation events
        EventBus.subscribe(AggregationCompleted, _handle_aggregation_completed)
        EventBus.subscribe(AggregationFailed, _handle_aggregation_failed)

        # Subscribe to domain-level events
        EventBus.subscribe(DomainIngestionJobCompleted, _track_domain_ingestion_metrics)
        EventBus.subscribe(DomainValidationFailed, _track_domain_validation_metrics)

        logger.info("Monitoring event handlers registered successfully")

    except Exception as e:
        logger.warning(f"Failed to setup monitoring event handlers: {e}")


__all__ = [
    "register",
] 