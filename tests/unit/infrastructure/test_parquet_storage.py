# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import sys
from datetime import timezone
from pathlib import Path

import pyarrow.parquet as pq

from marketpipe.domain.value_objects import Symbol
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration
from marketpipe.ingestion.infrastructure.parquet_storage import ParquetDataStorage

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from fakes.adapters import create_test_ohlcv_bars


def test_store_bars_writes_parquet_and_returns_partition(tmp_path):
    storage = ParquetDataStorage(root=tmp_path)
    symbol = Symbol("AAPL")
    bars = create_test_ohlcv_bars(symbol, count=3)
    config = IngestionConfiguration(
        output_path=tmp_path,
        compression="snappy",
        max_workers=1,
        batch_size=1000,
        rate_limit_per_minute=None,
        feed_type="iex",
    )

    partition = asyncio.run(storage.store_bars(bars, config))

    assert partition.record_count == 3
    assert partition.file_path.exists()

    table = pq.ParquetFile(partition.file_path).read()
    assert table.num_rows == 3
    assert partition.file_size_bytes == partition.file_path.stat().st_size
    assert partition.created_at.tzinfo is timezone.utc


def test_resumed_storage_keeps_earlier_bars(tmp_path):
    storage = ParquetDataStorage(root=tmp_path)
    bars = create_test_ohlcv_bars(Symbol("AAPL"), count=5)
    config = IngestionConfiguration(tmp_path, "snappy", 1, 1000, None, "iex")
    asyncio.run(storage.store_bars(bars[:3], config))
    partition = asyncio.run(storage.store_bars(bars[2:], config))
    table = pq.ParquetFile(partition.file_path).read()
    assert table.num_rows == 5
    assert len(set(table.column("ts_ns").to_pylist())) == 5


def test_storage_separates_symbols_from_same_day(tmp_path):
    storage = ParquetDataStorage(root=tmp_path)
    bars = create_test_ohlcv_bars(Symbol("AAPL"), count=2)
    bars += create_test_ohlcv_bars(Symbol("MSFT"), count=2)
    config = IngestionConfiguration(tmp_path, "snappy", 1, 1000, None, "iex")
    asyncio.run(storage.store_bars(bars, config))
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 2
    for path in files:
        table = pq.ParquetFile(path).read()
        symbols = set(table.column("symbol").to_pylist())
        assert len(symbols) == 1
        assert path.parent.parent.name == f"symbol={symbols.pop()}"
