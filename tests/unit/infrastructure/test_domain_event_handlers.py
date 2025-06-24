"""Tests for infrastructure domain event handlers."""

from datetime import date, datetime, timezone
from unittest.mock import Mock, patch

from marketpipe.domain.events import (
    BarCollectionCompleted,
    IngestionJobCompleted,
    ValidationFailed,
)
from marketpipe.domain.value_objects import Symbol
from marketpipe.infrastructure.monitoring.domain_event_handlers import (
    log_bar_collection_completed,
    log_ingestion_job_completed,
    log_validation_failed,
    register_logging_handlers,
)


def test_log_ingestion_job_completed_success():
    """Test logging successful ingestion job completion."""
    with patch("marketpipe.infrastructure.monitoring.domain_event_handlers.logger") as mock_logger:
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
    with patch("marketpipe.infrastructure.monitoring.domain_event_handlers.logger") as mock_logger:
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
    with patch("marketpipe.infrastructure.monitoring.domain_event_handlers.logger") as mock_logger:
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
    with patch("marketpipe.infrastructure.monitoring.domain_event_handlers.logger") as mock_logger:
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


def test_register_logging_handlers():
    """Test registering logging event handlers."""
    with patch("marketpipe.bootstrap.get_event_bus") as mock_get_event_bus:
        mock_event_bus = Mock()
        mock_get_event_bus.return_value = mock_event_bus
        with patch(
            "marketpipe.infrastructure.monitoring.domain_event_handlers.logger"
        ) as mock_logger:
            register_logging_handlers()

            # Should register multiple handlers
            assert mock_event_bus.subscribe.call_count >= 5
            mock_logger.info.assert_called_with("Domain event logging handlers registered")


def test_setup_metrics_event_handlers():
    """Test setting up metrics event handlers via infrastructure module."""
    with patch("marketpipe.bootstrap.get_event_bus") as mock_get_event_bus:
        mock_event_bus = Mock()
        mock_get_event_bus.return_value = mock_event_bus
        with patch("marketpipe.infrastructure.monitoring.event_handlers.logger") as mock_logger:
            from marketpipe.infrastructure.monitoring.event_handlers import register

            register()

            # Should register multiple handlers
            assert mock_event_bus.subscribe.call_count >= 2
            mock_logger.info.assert_called_with("Monitoring event handlers registered successfully")
