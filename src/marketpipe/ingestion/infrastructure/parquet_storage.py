# SPDX-License-Identifier: Apache-2.0
"""Legacy ParquetDataStorage - re-exported from new storage engine."""

from __future__ import annotations

import logging
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import List

# Re-export the production storage engine to maintain backward compatibility
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from marketpipe.domain.entities import OHLCVBar
from ..domain.storage import IDataStorage
from ..domain.value_objects import IngestionConfiguration, IngestionPartition


class ParquetDataStorageAdapter(IDataStorage):
    """Adapter that implements IDataStorage using ParquetStorageEngine."""
    
    def __init__(self, root: Path | str, compression: str = "snappy"):
        self._engine = ParquetStorageEngine(root, compression)
        self.log = logging.getLogger(self.__class__.__name__)
    
    async def store_bars(
        self,
        bars: List[OHLCVBar],
        config: IngestionConfiguration
    ) -> IngestionPartition:
        """Persist bars and return information about the created partition."""
        if not bars:
            raise ValueError("Cannot store empty list of bars")
        
        # Convert OHLCVBar entities to DataFrame format expected by engine
        rows = []
        for bar in bars:
            rows.append({
                "ts_ns": bar.timestamp_ns,
                "symbol": bar.symbol.value,
                "open": bar.open_price.to_float(),
                "high": bar.high_price.to_float(),
                "low": bar.low_price.to_float(),
                "close": bar.close_price.to_float(),
                "volume": bar.volume.value,
                "trade_count": bar.trade_count,
                "vwap": bar.vwap.to_float() if bar.vwap else None,
            })
        
        df = pd.DataFrame(rows)
        
        # Use first bar for partition information
        first_bar = bars[0]
        symbol = first_bar.symbol.value
        trading_date = first_bar.timestamp.trading_date()
        
        # Generate a job ID for this storage operation
        job_id = f"store_{symbol}_{trading_date.isoformat()}_{datetime.now().strftime('%H%M%S')}"
        
        # Use the engine to write the data
        file_path = self._engine.write(
            df=df,
            frame="1m",  # Assume 1-minute bars
            symbol=symbol,
            trading_day=trading_date,
            job_id=job_id,
            overwrite=True
        )
        
        # Create and return partition info
        file_size = file_path.stat().st_size
        partition = IngestionPartition(
            symbol=first_bar.symbol,
            file_path=file_path,
            record_count=len(bars),
            file_size_bytes=file_size,
            created_at=datetime.now(timezone.utc)
        )
        
        return partition


# Use the adapter as ParquetDataStorage for backward compatibility
ParquetDataStorage = ParquetDataStorageAdapter

__all__ = ["ParquetDataStorage"]
