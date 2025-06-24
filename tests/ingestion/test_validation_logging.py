"""Test structured logging of validation errors in symbol providers."""

from __future__ import annotations

import logging
from datetime import date

import anyio

from marketpipe.ingestion.symbol_providers.dummy import DummyProvider


def test_validation_warning_logged(caplog):
    """Test that validation errors are logged with structured format."""
    # Create broken symbol data that will trigger validation error
    broke = {
        "ticker": "",  # empty ticker triggers validation error
        "exchange_mic": "XNAS",
        "asset_class": "EQUITY",
        "currency": "USD",
        "status": "ACTIVE",
        "as_of": "2025-06-19",
    }

    provider = DummyProvider(as_of=date(2025, 6, 19))

    async def mock_fetch_raw():
        return [broke]

    provider._fetch_raw = mock_fetch_raw  # monkey-patch with async function

    # Set up logging capture
    caplog.set_level(logging.WARNING, logger="marketpipe.symbols.validation")

    # Run the provider - should return empty list due to validation failure
    recs = anyio.run(provider.fetch_symbols)
    assert recs == []  # dropped due to validation error

    # Verify structured log message was emitted
    assert len(caplog.records) == 1
    log_record = caplog.records[0]
    assert log_record.levelno == logging.WARNING
    assert log_record.name == "marketpipe.symbols.validation"

    # Check log message contains expected structured fields
    message = log_record.getMessage()
    assert "provider=dummy" in message
    assert "ticker=" in message  # Empty ticker case
    assert "field=ticker" in message
    assert "error=" in message


def test_validation_warning_with_valid_ticker(caplog):
    """Test validation error logging when ticker is present but other fields invalid."""
    # Create data with valid ticker but invalid currency (too long)
    broke = {
        "ticker": "TEST",
        "exchange_mic": "XNAS",
        "asset_class": "EQUITY",
        "currency": "TOOLONG",  # Too long, should be 3 chars
        "status": "ACTIVE",
        "as_of": "2025-06-19",
    }

    provider = DummyProvider(as_of=date(2025, 6, 19))

    async def mock_fetch_raw():
        return [broke]

    provider._fetch_raw = mock_fetch_raw

    caplog.set_level(logging.WARNING, logger="marketpipe.symbols.validation")

    recs = anyio.run(provider.fetch_symbols)
    assert recs == []

    # Verify structured log message
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "provider=dummy" in message
    assert "ticker=TEST" in message
    assert "field=currency" in message


def test_successful_validation_no_log(caplog):
    """Test that successful validation doesn't generate log messages."""
    # Use valid data - should not generate any log messages
    provider = DummyProvider(as_of=date(2025, 6, 19))
    # Don't monkey-patch _fetch_raw, use default valid data

    caplog.set_level(logging.WARNING, logger="marketpipe.symbols.validation")

    recs = anyio.run(provider.fetch_symbols)
    assert len(recs) == 1  # Should get one valid record

    # Should be no validation warning logs
    validation_logs = [r for r in caplog.records if r.name == "marketpipe.symbols.validation"]
    assert len(validation_logs) == 0


def test_mixed_valid_invalid_records(caplog):
    """Test logging when some records are valid and some invalid."""
    valid_record = {
        "ticker": "VALID",
        "exchange_mic": "XNAS",
        "asset_class": "EQUITY",
        "currency": "USD",
        "status": "ACTIVE",
        "as_of": "2025-06-19",
    }

    invalid_record = {
        "ticker": "",  # Invalid empty ticker
        "exchange_mic": "XNAS",
        "asset_class": "EQUITY",
        "currency": "USD",
        "status": "ACTIVE",
        "as_of": "2025-06-19",
    }

    provider = DummyProvider(as_of=date(2025, 6, 19))

    async def mock_fetch_raw():
        return [valid_record, invalid_record]

    provider._fetch_raw = mock_fetch_raw

    caplog.set_level(logging.WARNING, logger="marketpipe.symbols.validation")

    recs = anyio.run(provider.fetch_symbols)
    assert len(recs) == 1  # Only valid record should be returned
    assert recs[0].ticker == "VALID"

    # Should have one validation error log for the invalid record
    validation_logs = [r for r in caplog.records if r.name == "marketpipe.symbols.validation"]
    assert len(validation_logs) == 1
