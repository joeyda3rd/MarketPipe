# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from datetime import timezone

import pyarrow.parquet as pq

from marketpipe.domain.value_objects import Symbol
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration
from marketpipe.ingestion.infrastructure.parquet_storage import ParquetDataStorage
from tests.fakes.adapters import create_test_ohlcv_bars


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
