"""Regression tests for historical ingestion and job administration."""

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from marketpipe.cli.jobs import jobs_app
from marketpipe.cli.ohlcv_aggregate import _get_recent_jobs as aggregation_jobs
from marketpipe.cli.ohlcv_validate import _get_recent_jobs as validation_jobs
from marketpipe.cli.validators import validate_date_range, validate_output_dir, validate_symbols
from marketpipe.migrations import apply_pending


@pytest.fixture
def jobs_db(tmp_path, monkeypatch):
    path = tmp_path / "jobs.db"
    apply_pending(path)
    monkeypatch.setenv("MARKETPIPE_INGESTION_DB_PATH", str(path))
    return path


def _insert_job(path, state, timestamp):
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO ingestion_jobs "
            "(symbol, day, state, payload, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("AAPL", "2020-01-01", state, "{}", timestamp, timestamp),
        )


def test_historical_date_range_is_accepted():
    validate_date_range("2000-01-01", "2000-01-02")


@pytest.mark.parametrize("command", ["status", "doctor"])
def test_jobs_commands_accept_naive_sqlite_timestamps(jobs_db, command):
    _insert_job(jobs_db, "IN_PROGRESS", "2000-01-01 12:00:00")
    result = CliRunner().invoke(jobs_app, [command])
    assert result.exit_code == 0, result.output
    assert "AAPL" in result.output


def test_doctor_does_not_mark_recent_sqlite_timestamp_as_stuck(jobs_db):
    recent = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    _insert_job(jobs_db, "IN_PROGRESS", recent)
    result = CliRunner().invoke(jobs_app, ["doctor", "--fix"])
    assert result.exit_code == 0, result.output
    with sqlite3.connect(jobs_db) as conn:
        assert conn.execute("SELECT state FROM ingestion_jobs").fetchone()[0] == "IN_PROGRESS"


def test_unfiltered_cleanup_cannot_execute(jobs_db):
    _insert_job(jobs_db, "COMPLETED", "2000-01-01 12:00:00")
    result = CliRunner().invoke(jobs_app, ["cleanup", "--execute"])
    assert result.exit_code == 2, result.output
    with sqlite3.connect(jobs_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ingestion_jobs").fetchone()[0] == 1


def test_output_validation_preserves_existing_probe_file(tmp_path):
    existing = tmp_path / ".marketpipe_write_test"
    existing.write_text("existing data")
    validate_output_dir(tmp_path / "output")
    assert existing.read_text() == "existing data"


@pytest.mark.parametrize("symbols", ["  ", ",", " , , "])
def test_separator_only_symbol_lists_are_rejected(symbols):
    import typer

    with pytest.raises(typer.Exit):
        validate_symbols(symbols)


@pytest.mark.parametrize("lookup", [aggregation_jobs, validation_jobs])
def test_recent_completed_jobs_include_recent_historical_ingestion(jobs_db, lookup):
    _insert_job(jobs_db, "COMPLETED", datetime.now(timezone.utc).isoformat())
    assert lookup(symbol="aapl") == ["AAPL_2020-01-01"]


def test_cleanup_removes_only_target_job_checkpoints(jobs_db):
    _insert_job(jobs_db, "COMPLETED", "2000-01-01 12:00:00")
    with sqlite3.connect(jobs_db) as conn:
        conn.execute(
            "INSERT INTO ingestion_jobs (symbol, day, state) VALUES ('AAPL', '2020-01-02', 'PENDING')"
        )
    checkpoint_path = jobs_db.parent / "db" / "core.db"
    checkpoint_path.parent.mkdir()
    with sqlite3.connect(checkpoint_path) as conn:
        conn.execute("CREATE TABLE ingestion_checkpoints (job_id TEXT, symbol TEXT)")
        conn.executemany(
            "INSERT INTO ingestion_checkpoints VALUES (?, ?)",
            [("AAPL_2020-01-01", "AAPL"), ("AAPL_2020-01-02", "AAPL")],
        )
    result = CliRunner().invoke(jobs_app, ["cleanup", "--job-id", "AAPL_2020-01-01", "--execute"])
    assert result.exit_code == 0, result.output
    with sqlite3.connect(checkpoint_path) as conn:
        assert conn.execute("SELECT job_id FROM ingestion_checkpoints").fetchall() == [
            ("AAPL_2020-01-02",)
        ]
    with sqlite3.connect(jobs_db) as conn:
        assert conn.execute("SELECT day FROM ingestion_jobs").fetchall() == [("2020-01-02",)]


def test_cleanup_preserves_legacy_checkpoint_for_unselected_job(jobs_db):
    _insert_job(jobs_db, "COMPLETED", "2000-01-01 12:00:00")
    with sqlite3.connect(jobs_db) as conn:
        conn.execute(
            "INSERT INTO ingestion_jobs (symbol, day, state) VALUES ('AAPL', '2020-01-02', 'PENDING')"
        )
        conn.execute("INSERT INTO checkpoints (symbol, checkpoint_data) VALUES ('AAPL', '{}')")
    result = CliRunner().invoke(jobs_app, ["cleanup", "--completed", "--execute"])
    assert result.exit_code == 0, result.output
    with sqlite3.connect(jobs_db) as conn:
        assert conn.execute("SELECT symbol FROM checkpoints").fetchall() == [("AAPL",)]


@pytest.mark.parametrize("command", [["ingest-ohlcv"], ["ohlcv", "ingest"]])
def test_fake_ingestion_writes_real_market_data_and_completed_job(tmp_path, monkeypatch, command):
    from marketpipe.cli import app

    monkeypatch.chdir(tmp_path)
    raw_root = tmp_path / "raw"
    job_db = tmp_path / "jobs.db"
    monkeypatch.setenv("MARKETPIPE_RAW_ROOT", str(raw_root))
    monkeypatch.setenv("MARKETPIPE_INGESTION_DB_PATH", str(job_db))
    monkeypatch.setenv("MARKETPIPE_DB_PATH", str(tmp_path / "core.db"))
    monkeypatch.setenv("MARKETPIPE_METRICS_DB_PATH", str(tmp_path / "metrics.db"))
    result = CliRunner().invoke(
        app,
        command
        + [
            "--provider",
            "fake",
            "--symbols",
            "AAPL",
            "--start",
            "2020-01-01",
            "--end",
            "2020-01-02",
            "--workers",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output
    files = list(raw_root.rglob("*.parquet"))
    assert files
    rows = pd.concat([pq.ParquetFile(path).read().to_pandas() for path in files])
    assert {"ts_ns", "open", "high", "low", "close", "volume"}.issubset(rows.columns)
    assert len(rows) == 1440
    assert pd.to_datetime(rows["ts_ns"], unit="ns").min().date().isoformat() == "2020-01-01"
    with sqlite3.connect(job_db) as conn:
        assert conn.execute("SELECT state FROM ingestion_jobs").fetchone()[0] == "COMPLETED"

    # Exercise the README's default downstream commands on that actual ingestion.
    monkeypatch.setattr("marketpipe.bootstrap.bootstrap", lambda: None)
    monkeypatch.setenv("MARKETPIPE_AGG_ROOT", str(tmp_path / "agg"))
    for downstream in ["validate-ohlcv", "aggregate-ohlcv"]:
        result = CliRunner().invoke(app, [downstream])
        assert result.exit_code == 0, result.output
        assert "Successful: 1" in result.output
    result = CliRunner().invoke(app, ["query", "SELECT COUNT(*) AS count FROM bars_4h", "--csv"])
    assert result.exit_code == 0, result.output
    assert "count\n6" in result.output


@pytest.mark.parametrize("status, failed", [("failed", 1), ("partial_success", 1), ("failed", 0)])
def test_failed_ingestion_cannot_report_cli_success(tmp_path, status, failed):
    from marketpipe.cli import app

    job_service = AsyncMock()
    coordinator = AsyncMock()
    job_service.create_job.return_value = "AAPL_2020-01-01"
    coordinator.execute_job.return_value = {"status": status, "symbols_failed": failed}
    with (
        patch(
            "marketpipe.cli.ohlcv_ingest._build_ingestion_services",
            return_value=(job_service, coordinator),
        ),
        patch("marketpipe.cli.ohlcv_ingest._check_boundaries") as check_boundaries,
    ):
        result = CliRunner().invoke(
            app,
            [
                "ingest-ohlcv",
                "--provider",
                "fake",
                "--symbols",
                "AAPL",
                "--start",
                "2020-01-01",
                "--end",
                "2020-01-02",
                "--output",
                str(tmp_path / "raw"),
            ],
        )
    assert result.exit_code == 1, result.output
    assert "Job completed successfully" not in result.output
    assert "Ingestion failed" in result.output
    check_boundaries.assert_not_called()
