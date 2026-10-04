"""Literal provider data travels through the installed user-facing CLI."""

from __future__ import annotations

import csv
import io

import pandas as pd
import pytest

pytestmark = pytest.mark.e2e


def test_installed_ingest_validate_aggregate_query_and_cleanup(cli, http_server):
    url, responses, requests = http_server
    cli.env.update(
        ALPACA_KEY="synthetic-e2e-key", ALPACA_SECRET="synthetic-e2e-secret", ALPACA_BASE_URL=url
    )
    cli.env.update(HTTP_PROXY=url, HTTPS_PROXY=url, NO_PROXY="127.0.0.1,localhost")
    payload = {
        "bars": {
            "AAPL": [
                {"t": "2024-01-15T14:30:00Z", "o": 100, "h": 102, "l": 99, "c": 101, "v": 10},
                {"t": "2024-01-15T14:31:00Z", "o": 101, "h": 103, "l": 100, "c": 102, "v": 20},
            ]
        },
        "next_page_token": None,
    }
    responses.append((200, {}, payload))
    command = (
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
    cli.run(*command)
    assert len(requests) == 1
    files = list((cli.root / "data" / "raw").rglob("*.parquet"))
    assert len(files) == 1
    raw = pd.read_parquet(files[0])
    assert raw[["open", "high", "low", "close", "volume"]].values.tolist() == [
        [100, 102, 99, 101, 10],
        [101, 103, 100, 102, 20],
    ]

    cli.run("validate-ohlcv", "AAPL_2024-01-15")
    reports = list((cli.root / "data" / "validation_reports").rglob("*.csv"))
    assert len(reports) == 1
    assert pd.read_csv(reports[0]).empty
    cli.run("aggregate-ohlcv", "AAPL_2024-01-15")
    result = cli.run("query", "SELECT open, high, low, close, volume FROM bars_5m", "--csv")
    records = list(csv.DictReader(io.StringIO(result.stdout)))
    assert len(records) == 1
    assert [float(records[0][name]) for name in ("open", "high", "low", "close", "volume")] == [
        100,
        103,
        99,
        102,
        30,
    ]
    assert "COMPLETED" in cli.run("jobs", "list").stdout

    # A rerun of the same request must retain the same two unique bars.
    responses.append((200, {}, payload))
    cli.run(*command)
    assert len(pd.read_parquet(files[0])) == 2
    before = pd.read_parquet(files[0])
    cli.run("jobs", "cleanup", "--completed")
    assert "COMPLETED" in cli.run("jobs", "list").stdout
    cli.run("jobs", "cleanup", "--execute", success=False)
    cli.run("jobs", "cleanup", "--completed", "--execute")
    assert "COMPLETED" not in cli.run("jobs", "list").stdout
    pd.testing.assert_frame_equal(before, pd.read_parquet(files[0]))


def test_installed_cli_failure_exit_codes(cli):
    cli.run(
        "ingest-ohlcv",
        "--provider",
        "fake",
        "--symbols",
        "AAPL",
        "--start",
        "2024-01-15",
        "--end",
        "2024-01-15",
        success=False,
    )
    cli.run("aggregate-ohlcv", "AAPL_1900-01-01", success=False)
    cli.run("validate-ohlcv", "AAPL_1900-01-01", success=False)
    cli.run("query", "SELECT * FROM missing_table", success=False)


def test_default_commands_discover_every_symbol_and_day_of_installed_job(cli, http_server):
    url, responses, requests = http_server
    cli.env.update(ALPACA_KEY="multi-key", ALPACA_SECRET="multi-secret", ALPACA_BASE_URL=url)
    cli.env.update(HTTP_PROXY=url, HTTPS_PROXY=url, NO_PROXY="127.0.0.1,localhost")

    def payload(_path, query):
        ticker = query["symbols"][0]
        bars = [
            {"t": timestamp, "o": 100, "h": 102, "l": 99, "c": 101, "v": volume}
            for timestamp, volume in [
                ("2024-01-15T14:30:00Z", 10),
                ("2024-01-15T14:31:00Z", 20),
                ("2024-01-16T14:30:00Z", 30),
                ("2024-01-16T14:31:00Z", 40),
            ]
        ]
        return 200, {}, {"bars": {ticker: bars}}

    responses.extend([payload, payload])
    cli.run(
        "ingest-ohlcv",
        "--provider",
        "alpaca",
        "--feed-type",
        "iex",
        "--symbols",
        "AAPL,MSFT",
        "--start",
        "2024-01-15",
        "--end",
        "2024-01-17",
    )
    assert len(requests) == 2
    assert len(list((cli.root / "data/raw").rglob("*.parquet"))) == 4
    cli.run("validate-ohlcv")
    assert len(list((cli.root / "data/validation_reports").rglob("*.csv"))) == 4
    cli.run("aggregate-ohlcv")
    result = cli.run("query", "SELECT symbol, volume FROM bars_1d ORDER BY symbol, ts_ns", "--csv")
    assert list(csv.DictReader(io.StringIO(result.stdout))) == [
        {"symbol": "AAPL", "volume": "30.0"},
        {"symbol": "AAPL", "volume": "70.0"},
        {"symbol": "MSFT", "volume": "30.0"},
        {"symbol": "MSFT", "volume": "70.0"},
    ]
