# SPDX-License-Identifier: Apache-2.0
"""Documented commands execute against real components in temporary storage."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def command(tmp_path):
    env = {
        "PATH": os.defpath,
        "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
        "MARKETPIPE_DB_PATH": str(tmp_path / "data/db/core.db"),
        "MARKETPIPE_CHECKPOINT_DB_PATH": str(tmp_path / "data/db/core.db"),
        "MARKETPIPE_INGESTION_DB_PATH": str(tmp_path / "data/ingestion_jobs.db"),
        "MARKETPIPE_METRICS_DB_PATH": str(tmp_path / "data/metrics.db"),
    }

    def run(*args):
        result = subprocess.run(
            [sys.executable, "-m", "marketpipe", *args],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return result

    return run, env


def test_readme_quickstart_full_workflow(command, tmp_path):
    run, _env = command
    run(
        "ingest",
        "--provider",
        "fake",
        "--symbols",
        "AAPL,GOOGL",
        "--start",
        "2024-01-15",
        "--end",
        "2024-01-16",
    )
    raw_files = list((tmp_path / "data/raw").rglob("*.parquet"))
    assert len(raw_files) == 2
    assert {pd.read_parquet(path).iloc[0]["symbol"] for path in raw_files} == {"AAPL", "GOOGL"}
    run("validate-ohlcv")
    assert len(list((tmp_path / "data/validation_reports").rglob("*.csv"))) == 2
    run("aggregate-ohlcv")
    result = run("query", "SELECT symbol, count(*) AS bars FROM bars_1d GROUP BY symbol", "--csv")
    assert "AAPL,2" in result.stdout
    assert "GOOGL,2" in result.stdout
    assert "COMPLETED" in run("jobs", "list").stdout


def test_readme_metrics_command_serves_prometheus(command, tmp_path):
    _run, env = command
    # Reserve adjacent ephemeral ports for the metrics and dashboard listeners.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    process = subprocess.Popen(
        [sys.executable, "-m", "marketpipe", "metrics", "--port", str(port)],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and process.poll() is None:
            try:
                with urlopen(f"http://localhost:{port}/metrics", timeout=0.25) as response:
                    body = response.read().decode()
                assert "# HELP" in body
                assert "# TYPE" in body
                return
            except URLError:
                time.sleep(0.05)
        pytest.fail("Metrics endpoint did not become ready")
    finally:
        process.terminate()
        try:
            output, errors = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            output, errors = process.communicate(timeout=5)
        (tmp_path / "metrics-server.log").write_text(output + errors)


@pytest.mark.parametrize(
    "args", [[], ["ingest"], ["query"], ["validate"], ["aggregate"], ["metrics"]]
)
def test_all_readme_commands_exist(command, args):
    run, _env = command
    assert "Usage:" in run(*args, "--help").stdout
