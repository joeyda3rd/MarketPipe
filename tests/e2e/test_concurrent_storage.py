"""Separate processes append and readers observe complete files through public storage."""

from __future__ import annotations

import errno
import os
import selectors
import subprocess
import time
from datetime import date

import pandas as pd
import pytest

from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine

pytestmark = pytest.mark.e2e

WRITER = """
import sys
from datetime import date
import pandas as pd
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
engine = ParquetStorageEngine(sys.argv[1])
print('ready', flush=True)
sys.stdin.readline()
start = int(sys.argv[2])
for value in range(start, start + 5):
    frame = pd.DataFrame([dict(symbol='AAPL', ts_ns=value * 60000000000,
                               open=100., high=101., low=99., close=100., volume=value)])
    engine.append_to_job(frame, frame='1m', symbol='AAPL',
                         trading_day=date(2024, 1, 15), job_id='shared')
"""


def row(value):
    return pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "ts_ns": value * 60000000000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": value,
            }
        ]
    )


def test_process_appends_and_concurrent_reads_preserve_all_rows(cli):
    root = cli.root / "shared"
    engine = ParquetStorageEngine(root)
    engine.write(row(0), frame="1m", symbol="AAPL", trading_day=date(2024, 1, 15), job_id="shared")
    workers = []
    selector = selectors.DefaultSelector()
    try:
        for start in (1, 6, 11, 16):
            worker = subprocess.Popen(
                [str(cli.executable.parent / "python"), "-c", WRITER, str(root), str(start)],
                cwd=cli.root,
                env=cli.env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            workers.append(worker)
            selector.register(worker.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + 25
        while selector.get_map():
            ready = selector.select(timeout=max(0, deadline - time.monotonic()))
            assert ready, "writer failed to reach its start barrier"
            for key, _events in ready:
                assert key.fileobj.readline().strip() == "ready"
                selector.unregister(key.fileobj)
        for worker in workers:
            worker.stdin.write("start\n")
            worker.stdin.flush()
        while any(worker.poll() is None for worker in workers):
            assert time.monotonic() < deadline, "concurrent writers did not finish"
            snapshot = engine.load_job_bars("shared")["AAPL"]
            assert snapshot["ts_ns"].is_unique
            assert 1 <= len(snapshot) <= 21
        for worker in workers:
            _stdout, stderr = worker.communicate(timeout=5)
            assert worker.returncode == 0, stderr
        result = engine.load_job_bars("shared")["AAPL"].sort_values("ts_ns")
        assert result["volume"].tolist() == list(range(21))
    finally:
        selector.close()
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
            worker.communicate(timeout=5)


@pytest.mark.parametrize("error_number", [errno.ENOSPC, errno.EACCES])
def test_failed_append_retains_previous_file_and_cleans_temporary_files(
    tmp_path, monkeypatch, error_number
):
    engine = ParquetStorageEngine(tmp_path)
    path = engine.write(
        row(0), frame="1m", symbol="AAPL", trading_day=date(2024, 1, 15), job_id="safe"
    )
    original = path.read_bytes()

    def fail_replace(*_args):
        raise OSError(error_number, os.strerror(error_number))

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError) as failure:
        engine.append_to_job(
            row(1), frame="1m", symbol="AAPL", trading_day=date(2024, 1, 15), job_id="safe"
        )
    assert failure.value.errno == error_number
    assert path.read_bytes() == original
    assert engine.load_job_bars("safe")["AAPL"]["volume"].tolist() == [0]
    assert not list(tmp_path.rglob("*.tmp"))


def test_corrupt_partition_cannot_return_partial_job_data(tmp_path):
    engine = ParquetStorageEngine(tmp_path)
    engine.write(row(0), frame="1m", symbol="AAPL", trading_day=date(2024, 1, 15), job_id="mixed")
    corrupt = tmp_path / "frame=1m/symbol=MSFT/date=2024-01-15/mixed.parquet"
    corrupt.parent.mkdir(parents=True)
    corrupt.write_bytes(b"invalid parquet")
    with pytest.raises(OSError):
        engine.load_job_bars("mixed")
