"""Partial failures and database contention exercise persisted job boundaries."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from marketpipe.domain.value_objects import Symbol
from marketpipe.ingestion.domain.entities import IngestionJobId
from marketpipe.ingestion.domain.value_objects import IngestionCheckpoint
from marketpipe.ingestion.infrastructure.repositories import SqliteCheckpointRepository

pytestmark = pytest.mark.e2e


def test_partial_provider_failure_and_filtered_cleanup_preserve_other_symbol(cli, http_server):
    url, responses, _requests = http_server
    cli.env.update(
        ALPACA_KEY="isolation-key", ALPACA_SECRET="isolation-secret", ALPACA_BASE_URL=url
    )
    cli.env.update(HTTP_PROXY=url, HTTPS_PROXY=url, NO_PROXY="127.0.0.1,localhost")

    def response(_path, query):
        if query["symbols"] == ["MSFT"]:
            return 401, {}, {"message": "isolation-key isolation-secret"}
        return (
            200,
            {},
            {
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-15T14:30:00Z",
                            "o": 100,
                            "h": 101,
                            "l": 99,
                            "c": 100,
                            "v": 10,
                        }
                    ]
                }
            },
        )

    responses.append(response)
    cli.run(
        "ingest-ohlcv",
        "--provider",
        "alpaca",
        "--feed-type",
        "iex",
        "--symbols",
        "AAPL",
        "--start",
        "2024-01-15",
        "--end",
        "2024-01-16",
    )
    responses.extend([response, response])
    result = cli.run(
        "ingest-ohlcv",
        "--provider",
        "alpaca",
        "--feed-type",
        "iex",
        "--symbols",
        "MSFT,AAPL",
        "--start",
        "2024-01-15",
        "--end",
        "2024-01-16",
        success=False,
    )
    assert "isolation-key" not in result.stdout + result.stderr
    assert "isolation-secret" not in result.stdout + result.stderr
    listing = cli.run("jobs", "list").stdout
    assert "COMPLETED" in listing and "FAILED" in listing
    files = list((cli.root / "data/raw").rglob("*.parquet"))
    assert len(files) == 1
    assert pd.read_parquet(files[0])["volume"].tolist() == [10]
    checkpoint = Path(cli.env["MARKETPIPE_CHECKPOINT_DB_PATH"])
    with sqlite3.connect(checkpoint) as database:
        before = database.execute("SELECT * FROM ingestion_checkpoints").fetchall()
    before = [row for row in before if row[0] == "AAPL_2024-01-15"]
    assert before
    cli.run("jobs", "cleanup", "--failed")
    assert "FAILED" in cli.run("jobs", "list").stdout
    cli.run("jobs", "cleanup", "--failed", "--execute")
    listing = cli.run("jobs", "list").stdout
    assert "COMPLETED" in listing and "FAILED" not in listing
    with sqlite3.connect(checkpoint) as database:
        assert database.execute("SELECT * FROM ingestion_checkpoints").fetchall() == before
    assert pd.read_parquet(files[0])["volume"].tolist() == [10]


@pytest.mark.asyncio
async def test_checkpoint_writer_waits_for_real_sqlite_contention(tmp_path):
    path = tmp_path / "checkpoints.db"
    repository = SqliteCheckpointRepository(path)
    job = IngestionJobId("AAPL_2024-01-15")
    checkpoint = IngestionCheckpoint(
        updated_at=datetime.now(timezone.utc),
        symbol=Symbol("AAPL"),
        last_processed_timestamp=1705329000000000000,
        records_processed=1,
    )
    await repository.save_checkpoint(job, checkpoint)
    with sqlite3.connect(path) as lock:
        lock.execute("BEGIN IMMEDIATE")
        pending = asyncio.create_task(repository.save_checkpoint(job, checkpoint))
        # The public operation must remain pending while another transaction owns the write lock.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(pending), timeout=0.1)
        lock.rollback()
        await asyncio.wait_for(pending, timeout=5)
    saved = await repository.get_checkpoint(job, Symbol("AAPL"))
    assert saved.records_processed == 1
    assert saved.last_processed_timestamp == 1705329000000000000
