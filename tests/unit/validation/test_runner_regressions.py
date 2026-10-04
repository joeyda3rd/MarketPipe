"""Validation must retain malformed rows in reports and surface operational errors."""

from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest

from marketpipe.domain.events import IngestionJobCompleted
from marketpipe.domain.value_objects import Symbol
from marketpipe.validation.application.services import ValidationRunnerService
from marketpipe.validation.domain.services import ValidationDomainService
from marketpipe.validation.infrastructure.repositories import CsvReportRepository


def _event():
    return IngestionJobCompleted(
        job_id="AAPL_2020-01-01",
        symbol=Symbol("AAPL"),
        trading_date=date(2020, 1, 1),
        bars_processed=1,
        success=True,
    )


@pytest.mark.parametrize(
    "invalid_fields", [{"open": -1}, {"high": 90}, {"volume": -1}, {"close": float("nan")}]
)
def test_malformed_rows_are_written_to_validation_report(tmp_path, invalid_fields):
    row = {
        "ts_ns": 1577885400000000000,
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100,
        "volume": 100,
    }
    row.update(invalid_fields)
    storage = Mock()
    storage.load_job_bars.return_value = {"AAPL": pd.DataFrame([row])}
    reporter = CsvReportRepository(tmp_path)
    service = ValidationRunnerService(storage, ValidationDomainService(), reporter)

    service.handle_ingestion_completed(_event())

    report = pd.read_csv(reporter.list_reports()[0])
    assert len(report) == 1
    assert "invalid bar" in report.iloc[0]["reason"]
    assert report.iloc[0]["ts_ns"] == row["ts_ns"]


def test_report_write_failure_is_not_reported_as_success():
    storage = Mock()
    storage.load_job_bars.return_value = {
        "AAPL": pd.DataFrame(
            [
                {
                    "ts_ns": 1577885400000000000,
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 100,
                }
            ]
        )
    }
    reporter = Mock()
    reporter.save.side_effect = OSError("disk full")
    service = ValidationRunnerService(storage, ValidationDomainService(), reporter)
    with pytest.raises(RuntimeError, match="Failed to validate symbols: AAPL"):
        service.handle_ingestion_completed(_event())


def test_missing_job_cannot_be_validated_successfully():
    storage = Mock()
    storage.load_job_bars.return_value = {}
    service = ValidationRunnerService(storage, ValidationDomainService(), Mock())
    with pytest.raises(FileNotFoundError, match="No data found"):
        service.handle_ingestion_completed(_event())


def test_default_runner_honors_raw_root(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETPIPE_RAW_ROOT", str(tmp_path))
    service = ValidationRunnerService.build_default()
    assert service._storage_engine._root == tmp_path
