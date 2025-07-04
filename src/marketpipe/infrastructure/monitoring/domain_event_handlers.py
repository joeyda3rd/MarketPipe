# SPDX-License-Identifier: Apache-2.0
"""Infrastructure event handlers for domain events.

This module provides infrastructure implementations for handling domain events,
primarily for logging and observability.
"""

from __future__ import annotations

import logging

from marketpipe.domain.events import (
    BarCollectionCompleted,
    BarCollectionStarted,
    DataStored,
    IngestionJobCompleted,
    IngestionJobStarted,
    MarketDataReceived,
    RateLimitExceeded,
    SymbolActivated,
    SymbolDeactivated,
    ValidationFailed,
)

logger = logging.getLogger(__name__)


def log_bar_collection_started(event: BarCollectionStarted) -> None:
    """Log when bar collection starts for a symbol/date."""
    logger.info(f"Started collecting bars for {event.symbol} on {event.trading_date}")


def log_bar_collection_completed(event: BarCollectionCompleted) -> None:
    """Log when bar collection completes for a symbol/date."""
    gaps_msg = " (with gaps)" if event.has_gaps else ""
    logger.info(
        f"Completed collecting {event.bar_count} bars for {event.symbol} on {event.trading_date}{gaps_msg}"
    )


def log_validation_failed(event: ValidationFailed) -> None:
    """Log validation failures."""
    logger.warning(
        f"Validation failed for {event.symbol} at {event.timestamp}: {event.error_message}"
    )


def log_ingestion_job_started(event: IngestionJobStarted) -> None:
    """Log when ingestion job starts."""
    logger.info(f"Started ingestion job {event.job_id} for {event.symbol} on {event.trading_date}")


def log_ingestion_job_completed(event: IngestionJobCompleted) -> None:
    """Log when ingestion job completes."""
    status = "successfully" if event.success else "with failure"
    duration_msg = f" in {event.duration_seconds:.2f}s" if event.duration_seconds else ""
    logger.info(
        f"Completed ingestion job {event.job_id} {status}, processed {event.bars_processed} bars{duration_msg}"
    )

    if not event.success and event.error_message:
        logger.error(f"Ingestion job {event.job_id} failed: {event.error_message}")


def log_market_data_received(event: MarketDataReceived) -> None:
    """Log when market data is received."""
    logger.debug(
        f"Received {event.record_count} records for {event.symbol} from {event.provider_id} ({event.data_feed})"
    )


def log_data_stored(event: DataStored) -> None:
    """Log when data is successfully stored."""
    size_mb = event.file_size_bytes / (1024 * 1024)
    logger.info(
        f"Stored {event.record_count} records for {event.symbol} on {event.trading_date} "
        f"({size_mb:.2f} MB {event.storage_format} with {event.compression} compression)"
    )


def log_rate_limit_exceeded(event: RateLimitExceeded) -> None:
    """Log when rate limits are exceeded."""
    logger.warning(
        f"Rate limit exceeded for {event.provider_id}: {event.current_usage}/{event.limit_value} "
        f"({event.rate_limit_type}), resets at {event.reset_time}"
    )


def log_symbol_activated(event: SymbolActivated) -> None:
    """Log when a symbol is activated."""
    logger.info(f"Activated symbol {event.symbol} in universe {event.universe_id}")


def log_symbol_deactivated(event: SymbolDeactivated) -> None:
    """Log when a symbol is deactivated."""
    logger.info(f"Deactivated symbol {event.symbol} in universe {event.universe_id}")


def register_logging_handlers() -> None:
    """Register logging event handlers with the event bus."""
    from marketpipe.bootstrap import get_event_bus

    event_bus = get_event_bus()

    # Register all logging handlers
    event_bus.subscribe(BarCollectionStarted, log_bar_collection_started)
    event_bus.subscribe(BarCollectionCompleted, log_bar_collection_completed)
    event_bus.subscribe(ValidationFailed, log_validation_failed)
    event_bus.subscribe(IngestionJobStarted, log_ingestion_job_started)
    event_bus.subscribe(IngestionJobCompleted, log_ingestion_job_completed)
    event_bus.subscribe(MarketDataReceived, log_market_data_received)
    event_bus.subscribe(DataStored, log_data_stored)
    event_bus.subscribe(RateLimitExceeded, log_rate_limit_exceeded)
    event_bus.subscribe(SymbolActivated, log_symbol_activated)
    event_bus.subscribe(SymbolDeactivated, log_symbol_deactivated)

    logger.info("Domain event logging handlers registered")


__all__ = [
    "register_logging_handlers",
    "log_bar_collection_started",
    "log_bar_collection_completed",
    "log_validation_failed",
    "log_ingestion_job_started",
    "log_ingestion_job_completed",
    "log_market_data_received",
    "log_data_stored",
    "log_rate_limit_exceeded",
    "log_symbol_activated",
    "log_symbol_deactivated",
]
