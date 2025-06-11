# SPDX-License-Identifier: Apache-2.0
"""Integration tests for the full MarketPipe pipeline."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock
import pandas as pd

from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp, Price, Volume
from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.ingestion.domain.entities import IngestionJobId
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.validation.infrastructure.repositories import CsvReportRepository
from marketpipe.validation.application.services import ValidationRunnerService
from marketpipe.events import IngestionJobCompleted


class FakeMarketDataProvider:
    """Fake market data provider for integration testing."""

    def __init__(self):
        self.call_count = 0

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> list[OHLCVBar]:
        """Return fake OHLCV bars for testing."""
        self.call_count += 1

        # Create test bars with some intentional validation issues
        bars = []

        # Bar 1: Valid bar
        bars.append(
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(
                    datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
                ),
                open_price=Price.from_float(100.0),
                high_price=Price.from_float(101.0),
                low_price=Price.from_float(99.5),
                close_price=Price.from_float(100.5),
                volume=Volume(1000),
            )
        )

        # Bar 2: OHLC inconsistency (will be caught by validation)
        # Note: This will actually fail during OHLCVBar creation due to _validate_ohlc_consistency
        # So we'll create a bar that passes creation but fails domain validation
        bars.append(
            OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(
                    datetime(2024, 1, 15, 9, 31, 0, tzinfo=timezone.utc)
                ),
                open_price=Price.from_float(100.5),
                high_price=Price.from_float(102.0),  # Will be valid for entity creation
                low_price=Price.from_float(99.0),
                close_price=Price.from_float(101.0),
                volume=Volume(1500),
            )
        )

        return bars


@pytest.mark.integration
def test_full_pipeline_with_validation_reports(tmp_path):
    """Test complete pipeline: fake ingestion → storage → validation → CSV report."""

    # Setup test infrastructure
    storage_dir = tmp_path / "storage"
    reports_dir = tmp_path / "validation_reports"
    storage_dir.mkdir()
    reports_dir.mkdir()

    # Initialize components
    storage_engine = ParquetStorageEngine(storage_dir)
    csv_repository = CsvReportRepository(reports_dir)
    fake_provider = FakeMarketDataProvider()

    # Simulate ingestion by directly storing data
    job_id = IngestionJobId("test-pipeline-job-123")
    symbol = Symbol("AAPL")

    # Get bars from fake provider
    import asyncio

    time_range = TimeRange(
        start=Timestamp(datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)),
        end=Timestamp(datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc)),
    )
    bars = asyncio.run(fake_provider.fetch_bars_for_symbol(symbol, time_range))

    # Store bars using storage engine
    # Convert OHLCVBar entities to DataFrame format
    rows = []
    for bar in bars:
        rows.append(
            {
                "ts_ns": bar.timestamp_ns,
                "symbol": bar.symbol.value,
                "open": bar.open_price.to_float(),
                "high": bar.high_price.to_float(),
                "low": bar.low_price.to_float(),
                "close": bar.close_price.to_float(),
                "volume": bar.volume.value,
                "trade_count": getattr(bar, "trade_count", None),
                "vwap": (
                    bar.vwap.to_float() if hasattr(bar, "vwap") and bar.vwap else None
                ),
            }
        )
    df = pd.DataFrame(rows)

    # Use the correct write method
    storage_engine.write(
        df=df,
        frame="1m",
        symbol=symbol.value,
        trading_day=datetime(2024, 1, 15).date(),
        job_id=str(job_id),
        overwrite=True,
    )

    # Verify data was stored
    job_bars = storage_engine.load_job_bars(str(job_id))
    jobs_list = storage_engine.list_jobs(frame="1m", symbol=symbol.value)
    assert str(job_id) in jobs_list
    assert symbol.value in job_bars
    stored_df = job_bars[symbol.value]
    assert len(stored_df) == 2  # Two bars were stored

    # Run validation
    validation_service = ValidationRunnerService(
        storage_engine=storage_engine,
        validator=Mock(),  # We'll mock the validator to focus on report generation
        reporter=csv_repository,
    )

    # Mock the validator to return some validation errors
    from marketpipe.validation.domain.value_objects import ValidationResult, BarError

    mock_validation_result = ValidationResult(
        symbol=symbol.value,
        total=2,
        errors=[
            BarError(
                ts_ns=1705312200000000000,
                reason="timestamp not aligned to minute boundary at index 0",
            ),
            BarError(
                ts_ns=1705312260000000000,
                reason="extreme price movement at index 1: 1.5% change",
            ),
        ],
    )
    validation_service._validator.validate_bars = Mock(
        return_value=mock_validation_result
    )

    # Trigger validation
    validation_service.handle_ingestion_completed(
        IngestionJobCompleted(
            job_id=str(job_id),
            symbol=symbol,
            trading_date=datetime(2024, 1, 15).date(),
            bars_processed=2,
            success=True,
        )
    )

    # Verify CSV report was created
    reports = csv_repository.list_reports(str(job_id))
    assert len(reports) == 1  # One report for AAPL

    report_path = reports[0]
    assert report_path.exists()
    assert report_path.name == f"{job_id}_AAPL.csv"

    # Verify report content
    df = csv_repository.load_report(report_path)
    assert len(df) == 2  # Two validation errors
    assert list(df.columns) == ["symbol", "ts_ns", "reason"]

    # Check specific errors
    assert df.iloc[0]["symbol"] == "AAPL"
    assert "timestamp not aligned" in df.iloc[0]["reason"]
    assert df.iloc[1]["symbol"] == "AAPL"
    assert "extreme price movement" in df.iloc[1]["reason"]

    # Test report summary
    summary = csv_repository.get_report_summary(report_path)
    assert summary["total_errors"] == 2
    assert summary["symbols"] == ["AAPL"]
    assert len(summary["most_common_errors"]) == 2


@pytest.mark.integration
def test_multiple_symbols_pipeline(tmp_path):
    """Test pipeline with multiple symbols generating multiple reports."""

    # Setup
    storage_dir = tmp_path / "storage"
    reports_dir = tmp_path / "validation_reports"
    storage_dir.mkdir()
    reports_dir.mkdir()

    storage_engine = ParquetStorageEngine(storage_dir)
    csv_repository = CsvReportRepository(reports_dir)
    fake_provider = FakeMarketDataProvider()

    job_id = IngestionJobId("multi-symbol-job-456")
    symbols = [Symbol("AAPL"), Symbol("GOOGL"), Symbol("MSFT")]

    # Store data for multiple symbols
    import asyncio

    time_range = TimeRange(
        start=Timestamp(datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)),
        end=Timestamp(datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc)),
    )

    for symbol in symbols:
        bars = asyncio.run(fake_provider.fetch_bars_for_symbol(symbol, time_range))
        # Convert OHLCVBar entities to DataFrame format
        rows = []
        for bar in bars:
            rows.append(
                {
                    "ts_ns": bar.timestamp_ns,
                    "symbol": bar.symbol.value,
                    "open": bar.open_price.to_float(),
                    "high": bar.high_price.to_float(),
                    "low": bar.low_price.to_float(),
                    "close": bar.close_price.to_float(),
                    "volume": bar.volume.value,
                    "trade_count": getattr(bar, "trade_count", None),
                    "vwap": (
                        bar.vwap.to_float()
                        if hasattr(bar, "vwap") and bar.vwap
                        else None
                    ),
                }
            )
        df = pd.DataFrame(rows)

        # Use the correct write method
        storage_engine.write(
            df=df,
            frame="1m",
            symbol=symbol.value,
            trading_day=datetime(2024, 1, 15).date(),
            job_id=str(job_id),
            overwrite=True,
        )

    # Setup validation service with mock validator
    validation_service = ValidationRunnerService(
        storage_engine=storage_engine, validator=Mock(), reporter=csv_repository
    )

    # Mock validator to return different results for each symbol
    def mock_validate_bars(symbol_name, bars):
        from marketpipe.validation.domain.value_objects import (
            ValidationResult,
            BarError,
        )

        if symbol_name == "AAPL":
            return ValidationResult(
                symbol=symbol_name,
                total=2,
                errors=[BarError(ts_ns=1705312200000000000, reason="AAPL error 1")],
            )
        elif symbol_name == "GOOGL":
            return ValidationResult(symbol=symbol_name, total=2, errors=[])  # No errors
        else:  # MSFT
            return ValidationResult(
                symbol=symbol_name,
                total=2,
                errors=[
                    BarError(ts_ns=1705312200000000000, reason="MSFT error 1"),
                    BarError(ts_ns=1705312260000000000, reason="MSFT error 2"),
                ],
            )

    validation_service._validator.validate_bars = mock_validate_bars

    # Run validation
    validation_service.handle_ingestion_completed(
        IngestionJobCompleted(
            job_id=str(job_id),
            symbol=symbols[0],  # Use first symbol as primary
            trading_date=datetime(2024, 1, 15).date(),
            bars_processed=6,  # 2 bars per symbol * 3 symbols
            success=True,
        )
    )

    # Verify reports were created for all symbols
    reports = csv_repository.list_reports(str(job_id))
    assert len(reports) == 3  # One report per symbol

    # Check individual reports
    report_names = [r.name for r in reports]
    assert f"{job_id}_AAPL.csv" in report_names
    assert f"{job_id}_GOOGL.csv" in report_names
    assert f"{job_id}_MSFT.csv" in report_names

    # Verify AAPL report (1 error)
    aapl_report = next(r for r in reports if "AAPL" in r.name)
    aapl_df = csv_repository.load_report(aapl_report)
    assert len(aapl_df) == 1

    # Verify GOOGL report (no errors)
    googl_report = next(r for r in reports if "GOOGL" in r.name)
    googl_df = csv_repository.load_report(googl_report)
    assert len(googl_df) == 0

    # Verify MSFT report (2 errors)
    msft_report = next(r for r in reports if "MSFT" in r.name)
    msft_df = csv_repository.load_report(msft_report)
    assert len(msft_df) == 2


@pytest.mark.integration
def test_pipeline_empty_validation_report(tmp_path):
    """Test pipeline generates empty report when no validation errors occur."""

    # Setup
    storage_dir = tmp_path / "storage"
    reports_dir = tmp_path / "validation_reports"
    storage_dir.mkdir()
    reports_dir.mkdir()

    storage_engine = ParquetStorageEngine(storage_dir)
    csv_repository = CsvReportRepository(reports_dir)

    job_id = IngestionJobId("clean-job-789")
    symbol = Symbol("TSLA")

    # Create and store valid bars
    bars = [
        OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)),
            open_price=Price.from_float(200.0),
            high_price=Price.from_float(201.0),
            low_price=Price.from_float(199.5),
            close_price=Price.from_float(200.5),
            volume=Volume(2000),
        )
    ]

    # Convert OHLCVBar entities to DataFrame format
    rows = []
    for bar in bars:
        rows.append(
            {
                "ts_ns": bar.timestamp_ns,
                "symbol": bar.symbol.value,
                "open": bar.open_price.to_float(),
                "high": bar.high_price.to_float(),
                "low": bar.low_price.to_float(),
                "close": bar.close_price.to_float(),
                "volume": bar.volume.value,
                "trade_count": getattr(bar, "trade_count", None),
                "vwap": (
                    bar.vwap.to_float() if hasattr(bar, "vwap") and bar.vwap else None
                ),
            }
        )
    df = pd.DataFrame(rows)

    # Use the correct write method
    storage_engine.write(
        df=df,
        frame="1m",
        symbol=symbol.value,
        trading_day=datetime(2024, 1, 15).date(),
        job_id=str(job_id),
        overwrite=True,
    )

    # Setup validation service
    validation_service = ValidationRunnerService(
        storage_engine=storage_engine, validator=Mock(), reporter=csv_repository
    )

    # Mock validator to return no errors
    from marketpipe.validation.domain.value_objects import ValidationResult

    clean_result = ValidationResult(symbol=symbol.value, total=1, errors=[])
    validation_service._validator.validate_bars = Mock(return_value=clean_result)

    # Run validation
    validation_service.handle_ingestion_completed(
        IngestionJobCompleted(
            job_id=str(job_id),
            symbol=symbol,
            trading_date=datetime(2024, 1, 15).date(),
            bars_processed=1,
            success=True,
        )
    )

    # Verify empty report was created
    reports = csv_repository.list_reports(str(job_id))
    assert len(reports) == 1

    report_path = reports[0]
    df = csv_repository.load_report(report_path)
    assert len(df) == 0  # No errors
    assert list(df.columns) == ["symbol", "ts_ns", "reason"]  # But correct columns

    # Verify summary
    summary = csv_repository.get_report_summary(report_path)
    assert summary["total_errors"] == 0
    assert summary["symbols"] == []
