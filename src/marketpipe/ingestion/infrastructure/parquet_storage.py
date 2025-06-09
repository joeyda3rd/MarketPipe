from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from marketpipe.domain.entities import OHLCVBar
from marketpipe.ingestion.domain.storage import IDataStorage
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration, IngestionPartition


class ParquetDataStorage(IDataStorage):
    """Store OHLCV bars as partitioned Parquet files."""

    async def store_bars(
        self, bars: List[OHLCVBar], config: IngestionConfiguration
    ) -> IngestionPartition:
        if not bars:
            raise ValueError("No bars provided for storage")

        symbol = bars[0].symbol
        ts = bars[0].timestamp.value
        partition_path = (
            config.output_path
            / f"symbol={symbol.value}"
            / f"year={ts.year:04d}"
            / f"month={ts.month:02d}"
            / f"day={ts.day:02d}.parquet"
        )

        Path(partition_path.parent).mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist([bar.to_dict() for bar in bars])
        pq.write_table(table, partition_path, compression=config.compression)

        file_size = os.path.getsize(partition_path)

        return IngestionPartition(
            symbol=symbol,
            file_path=partition_path,
            record_count=len(bars),
            file_size_bytes=file_size,
            created_at=datetime.now(timezone.utc),
        )
