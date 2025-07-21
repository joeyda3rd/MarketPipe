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

from marketpipe.aggregation.domain.events import AggregationCompleted, AggregationFailed
from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.domain.events import IngestionJobCompleted as DomainIngestionJobCompleted
from marketpipe.domain.events import ValidationFailed
from marketpipe.domain.events import ValidationFailed as DomainValidationFailed
from marketpipe.metrics import (
    AGG_ROWS,
    ERRORS,
    INGEST_ROWS,
    REQUESTS,
    VALIDATION_ERRORS,
    record_metric,
)
from marketpipe.validation.domain.events import ValidationCompleted

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _extract_provider_feed_from_event(event) -> tuple[str, str]:
    """Extract provider and feed information from any event."""
    provider = getattr(event, "provider", "unknown")
    feed = getattr(event, "feed", "unknown")
    return provider, feed


def _handle_ingestion_completed(event: IngestionJobCompleted) -> None:
    """Handle ingestion job completion events."""
    try:
        # Extract provider/feed info
        provider, feed = _extract_provider_feed_from_event(event)

        # Record general ingestion metrics
        record_metric("ingest_jobs", 1, provider=provider, feed=feed)

        # Update Prometheus counters
        if event.success:
            REQUESTS.labels(source="ingestion", provider=provider, feed=feed).inc()
            # Record per-symbol ingestion
            if hasattr(event, "symbol") and event.symbol:
                INGEST_ROWS.labels(symbol=str(event.symbol)).inc(event.bars_processed)
                record_metric(
                    f"ingest_bars_{event.symbol}",
                    event.bars_processed,
                    provider=provider,
                    feed=feed,
                )
        else:
            ERRORS.labels(source="ingestion", provider=provider, feed=feed, code="job_failed").inc()
            record_metric("ingest_failures", 1, provider=provider, feed=feed)

        # Record job processing metrics
        if hasattr(event, "processing_time_seconds"):
            record_metric(
                "ingest_processing_time",
                event.processing_time_seconds,
                provider=provider,
                feed=feed,
            )

        logger.debug(f"Recorded metrics for ingestion job completion: {event.job_id}")

    except Exception as e:
        logger.error(f"Failed to record ingestion metrics: {e}")


def _handle_validation_completed(event: ValidationCompleted) -> None:
    """Handle validation completion events."""
    try:
        # Extract provider/feed info
        provider, feed = _extract_provider_feed_from_event(event)

        record_metric("validation_jobs", 1, provider=provider, feed=feed)

        # Record validation success
        REQUESTS.labels(source="validation", provider=provider, feed=feed).inc()

        # Extract symbol from the validation result
        if hasattr(event, "result") and hasattr(event.result, "symbol"):
            symbol = event.result.symbol
            record_metric(f"validation_completed_{symbol}", 1, provider=provider, feed=feed)

        logger.debug("Recorded metrics for validation completion")

    except Exception as e:
        logger.error(f"Failed to record validation metrics: {e}")


def _handle_validation_failed(event: ValidationFailed) -> None:
    """Handle validation failure events."""
    try:
        # Extract provider/feed info
        provider, feed = _extract_provider_feed_from_event(event)

        # Record validation error metrics with proper labels
        symbol = str(event.symbol)
        error_type = getattr(event, "rule_id", "unknown")

        VALIDATION_ERRORS.labels(symbol=symbol, error_type=error_type).inc()
        record_metric("validation_errors", 1, provider=provider, feed=feed)
        record_metric(f"validation_errors_{symbol}", 1, provider=provider, feed=feed)

        # Update Prometheus error counter
        ERRORS.labels(
            source="validation", provider=provider, feed=feed, code="validation_failed"
        ).inc()

        logger.debug(f"Recorded metrics for validation failure: {symbol}")

    except Exception as e:
        logger.error(f"Failed to record validation failure metrics: {e}")


def _handle_aggregation_completed(event: AggregationCompleted) -> None:
    """Handle aggregation completion events."""
    try:
        # Extract provider/feed info
        provider, feed = _extract_provider_feed_from_event(event)

        record_metric("aggregation_jobs", 1, provider=provider, feed=feed)

        # Record aggregation success
        REQUESTS.labels(source="aggregation", provider=provider, feed=feed).inc()

        # Record frames processed with proper labels
        if hasattr(event, "frames_processed"):
            # Use default values if specific attributes aren't available
            frame = getattr(event, "frame", "1m")  # Default frame
            symbol = getattr(event, "symbol", "ALL")  # Default symbol

            AGG_ROWS.labels(frame=frame, symbol=symbol).inc(event.frames_processed)
            record_metric(
                "aggregation_frames", event.frames_processed, provider=provider, feed=feed
            )

        logger.debug(f"Recorded metrics for aggregation completion: {event.job_id}")

    except Exception as e:
        logger.error(f"Failed to record aggregation metrics: {e}")


def _handle_aggregation_failed(event: AggregationFailed) -> None:
    """Handle aggregation failure events."""
    try:
        # Extract provider/feed info
        provider, feed = _extract_provider_feed_from_event(event)

        record_metric("aggregation_failures", 1, provider=provider, feed=feed)

        # Record aggregation error
        ERRORS.labels(source="aggregation", provider=provider, feed=feed, code="job_failed").inc()

        # Record frame-specific failure if available
        frame = getattr(event, "frame", "unknown")
        record_metric(f"aggregation_failures_{frame}", 1, provider=provider, feed=feed)

        logger.debug(f"Recorded metrics for aggregation failure: {event.job_id}")

    except Exception as e:
        logger.error(f"Failed to record aggregation failure metrics: {e}")


def _track_domain_ingestion_metrics(event: DomainIngestionJobCompleted) -> None:
    """Track ingestion job metrics from domain events."""
    try:
        # Extract provider/feed info
        provider, feed = _extract_provider_feed_from_event(event)

        if event.success:
            REQUESTS.labels(source="ingestion", provider=provider, feed=feed).inc()
        else:
            ERRORS.labels(source="ingestion", provider=provider, feed=feed, code="job_failed").inc()

        logger.debug(f"Recorded domain ingestion metrics for job: {event.job_id}")
    except Exception as e:
        logger.error(f"Failed to record domain ingestion metrics: {e}")


def _track_domain_validation_metrics(event: DomainValidationFailed) -> None:
    """Track validation failure metrics from domain events."""
    try:
        # Extract provider/feed info
        provider, feed = _extract_provider_feed_from_event(event)

        ERRORS.labels(
            source="validation", provider=provider, feed=feed, code="validation_failed"
        ).inc()

        logger.debug(f"Recorded domain validation metrics for symbol: {event.symbol}")
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
        from marketpipe.bootstrap import get_event_bus

        event_bus = get_event_bus()

        # Subscribe to application-level ingestion events
        event_bus.subscribe(IngestionJobCompleted, _handle_ingestion_completed)

        # Subscribe to validation events
        event_bus.subscribe(ValidationCompleted, _handle_validation_completed)
        event_bus.subscribe(ValidationFailed, _handle_validation_failed)

        # Subscribe to aggregation events
        event_bus.subscribe(AggregationCompleted, _handle_aggregation_completed)
        event_bus.subscribe(AggregationFailed, _handle_aggregation_failed)

        # Subscribe to domain-level events
        event_bus.subscribe(DomainIngestionJobCompleted, _track_domain_ingestion_metrics)
        event_bus.subscribe(DomainValidationFailed, _track_domain_validation_metrics)

        logger.info("Monitoring event handlers registered successfully")

    except Exception as e:
        logger.warning(f"Failed to setup monitoring event handlers: {e}")


__all__ = [
    "register",
]
