"""Interrupt actual installed workers at persistence boundaries and resume them."""

from __future__ import annotations

import subprocess

import pandas as pd
import pytest

pytestmark = pytest.mark.e2e

CRASH_WORKER = """
import os
import sys
replace = os.replace
stage = sys.argv.pop(1)
def crash_replace(source, destination):
    if str(destination).endswith('.parquet'):
        if stage == 'before_replace':
            os._exit(73)
        replace(source, destination)
        os._exit(73)
    return replace(source, destination)
os.replace = crash_replace
from marketpipe.cli import app
app()
"""


@pytest.mark.parametrize("stage,stored_rows", [("before_replace", 2), ("after_replace", 3)])
def test_interrupted_ingestion_resumes_without_losing_persisted_bars(
    cli, http_server, stage, stored_rows
):
    url, responses, _requests = http_server
    cli.env.update(ALPACA_KEY="recovery-key", ALPACA_SECRET="recovery-secret", ALPACA_BASE_URL=url)
    cli.env.update(HTTP_PROXY=url, HTTPS_PROXY=url, NO_PROXY="127.0.0.1,localhost")
    bars = [
        {"t": "2024-01-15T14:30:00Z", "o": 100, "h": 101, "l": 99, "c": 100, "v": 10},
        {"t": "2024-01-15T14:31:00Z", "o": 100, "h": 101, "l": 99, "c": 100, "v": 20},
        {"t": "2024-01-15T14:32:00Z", "o": 100, "h": 101, "l": 99, "c": 100, "v": 30},
    ]
    command = [
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
    ]
    responses.append((200, {}, {"bars": {"AAPL": bars[:2]}, "next_page_token": None}))
    cli.run(*command)
    path = next((cli.root / "data" / "raw").rglob("*.parquet"))
    responses.append((200, {}, {"bars": {"AAPL": bars[1:]}, "next_page_token": None}))
    crashed = subprocess.run(
        [str(cli.executable.parent / "python"), "-c", CRASH_WORKER, stage, *command],
        cwd=cli.root,
        env=cli.env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert crashed.returncode == 73, crashed.stdout + crashed.stderr
    assert len(pd.read_parquet(path)) == stored_rows
    assert "IN_PROGRESS" in cli.run("jobs", "list").stdout

    # Real checkpoint repository: a killed writer cannot advance the saved checkpoint.
    checkpoint = subprocess.run(
        [
            str(cli.executable.parent / "python"),
            "-c",
            """
import asyncio
import os
from marketpipe.domain.value_objects import Symbol
from marketpipe.ingestion.domain.entities import IngestionJobId
from marketpipe.ingestion.infrastructure.repositories import SqliteCheckpointRepository
async def check():
    repo = SqliteCheckpointRepository(os.environ['MARKETPIPE_CHECKPOINT_DB_PATH'])
    value = await repo.get_checkpoint(IngestionJobId('AAPL_2024-01-15'), Symbol('AAPL'))
    print(value.last_processed_timestamp)
asyncio.run(check())
""",
        ],
        cwd=cli.root,
        env=cli.env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert checkpoint.returncode == 0, checkpoint.stderr
    assert checkpoint.stdout.strip() == "1705329060000000000"
    cli.run("jobs", "doctor", "--timeout", "0", "--fix")
    assert "FAILED" in cli.run("jobs", "list").stdout
    responses.append((200, {}, {"bars": {"AAPL": bars[1:]}, "next_page_token": None}))
    cli.run(*command)
    recovered = pd.read_parquet(path).sort_values("ts_ns")
    assert recovered["volume"].tolist() == [10, 20, 30]
    assert recovered["ts_ns"].is_unique
    assert "COMPLETED" in cli.run("jobs", "list").stdout
    assert "IN_PROGRESS" not in cli.run("jobs", "list").stdout
