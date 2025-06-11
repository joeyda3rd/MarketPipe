"""Tests for domain event handlers."""

from unittest.mock import patch
from datetime import date, datetime, timezone
from marketpipe.domain.event_handlers import (
    log_ingestion_job_completed,
    log_validation_failed,
    log_bar_collection_completed,
    setup_default_event_handlers,
    setup_metrics_event_handlers,
)
from marketpipe.domain.events import (
    IngestionJobCompleted,
    ValidationFailed,
    BarCollectionCompleted,
)
from marketpipe.domain.value_objects import Symbol


def test_log_ingestion_job_completed_success():
    """Test logging successful ingestion job completion."""
    with patch("marketpipe.domain.event_handlers.logger") as mock_logger:
        event = IngestionJobCompleted(
            job_id="test-job",
            symbol=Symbol("AAPL"),
            trading_date=date(2024, 1, 15),
            bars_processed=100,
            success=True,
        )

        log_ingestion_job_completed(event)

        # Should log success message
        mock_logger.info.assert_called_once()
        assert "successfully" in mock_logger.info.call_args[0][0]
        assert "100 bars" in mock_logger.info.call_args[0][0]


def test_log_ingestion_job_completed_failure():
    """Test logging failed ingestion job completion."""
    with patch("marketpipe.domain.event_handlers.logger") as mock_logger:
        event = IngestionJobCompleted(
            job_id="test-job",
            symbol=Symbol("AAPL"),
            trading_date=date(2024, 1, 15),
            bars_processed=50,
            success=False,
            error_message="Connection timeout",
        )

        log_ingestion_job_completed(event)

        # Should log both info and error messages
        mock_logger.info.assert_called_once()
        mock_logger.error.assert_called_once()
        assert "with failure" in mock_logger.info.call_args[0][0]
        assert "Connection timeout" in mock_logger.error.call_args[0][0]


def test_log_validation_failed():
    """Test logging validation failures."""
    with patch("marketpipe.domain.event_handlers.logger") as mock_logger:
        event = ValidationFailed(
            symbol=Symbol("AAPL"),
            timestamp=datetime.now(timezone.utc),
            error_message="Invalid OHLC values",
        )

        log_validation_failed(event)

        # Should log warning message
        mock_logger.warning.assert_called_once()
        assert "Invalid OHLC values" in mock_logger.warning.call_args[0][0]


def test_log_bar_collection_completed():
    """Test logging bar collection completion."""
    with patch("marketpipe.domain.event_handlers.logger") as mock_logger:
        event = BarCollectionCompleted(
            symbol=Symbol("AAPL"),
            trading_date=date(2024, 1, 15),
            bar_count=100,
            has_gaps=False,
        )

        log_bar_collection_completed(event)

        # Should log info message
        mock_logger.info.assert_called_once()
        assert "100 bars" in mock_logger.info.call_args[0][0]
        assert "AAPL" in mock_logger.info.call_args[0][0]


def test_setup_default_event_handlers():
    """Test setting up default event handlers."""
    with patch("marketpipe.events.EventBus") as mock_event_bus:
        with patch("marketpipe.domain.event_handlers.logger") as mock_logger:
            setup_default_event_handlers()

            # Should register multiple handlers
            assert mock_event_bus.subscribe.call_count >= 5
            mock_logger.info.assert_called_with("Default event handlers registered")


def test_setup_metrics_event_handlers():
    """Test setting up metrics event handlers."""
    with patch("marketpipe.events.EventBus") as mock_event_bus:
        with patch("marketpipe.domain.event_handlers.logger") as mock_logger:
            setup_metrics_event_handlers()

            # The function may either succeed or fail with ImportError
            # Check that it handled the case properly
            if mock_event_bus.subscribe.call_count >= 1:
                # Success case: metrics were available
                mock_logger.info.assert_called_with("Metrics event handlers registered")
            else:
                # ImportError case: metrics not available
                mock_logger.warning.assert_called_with(
                    "Metrics module not available, skipping metrics event handlers"
                )
