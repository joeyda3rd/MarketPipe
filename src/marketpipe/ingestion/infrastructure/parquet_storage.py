from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Symbol, Price, Volume, Timestamp
from marketpipe.ingestion.domain.storage import IDataStorage
from marketpipe.ingestion.domain.value_objects import IngestionConfiguration, IngestionPartition


class ParquetDataStorage(IDataStorage):
    """Store OHLCV bars as partitioned Parquet files."""
    
    def __init__(self, base_path: str = "data/raw"):
        """Initialize with base path for data storage."""
        self.base_path = Path(base_path)

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

    def load_job_bars(self, job_id: str) -> Dict[str, List[OHLCVBar]]:
        """Load all bars for a job by scanning the output directory."""
        # For simplicity, we'll scan all parquet files in the data directory
        # In a real implementation, we'd store job metadata to know which files belong to which job
        bars_by_symbol = {}
        
        # Scan the data directory for parquet files
        if not self.base_path.exists():
            return bars_by_symbol
        
        for symbol_dir in self.base_path.iterdir():
            if symbol_dir.is_dir() and symbol_dir.name.startswith("symbol="):
                symbol_name = symbol_dir.name.split("=")[1]
                symbol = Symbol(symbol_name)
                bars = []
                
                # Recursively find all parquet files for this symbol
                for parquet_file in symbol_dir.glob("**/*.parquet"):
                    try:
                        table = pq.read_table(parquet_file)
                        df = table.to_pandas()
                        
                        for _, row in df.iterrows():
                            bar = OHLCVBar(
                                id=EntityId.generate(),
                                symbol=symbol,
                                timestamp=Timestamp.from_nanoseconds(int(row['timestamp_ns'])),
                                open_price=Price.from_float(float(row['open'])),
                                high_price=Price.from_float(float(row['high'])),
                                low_price=Price.from_float(float(row['low'])),
                                close_price=Price.from_float(float(row['close'])),
                                volume=Volume(int(row['volume']))
                            )
                            bars.append(bar)
                    except Exception as e:
                        # Skip files that can't be read
                        print(f"Warning: Could not read {parquet_file}: {e}")
                        continue
                
                if bars:
                    # Sort bars by timestamp
                    bars.sort(key=lambda b: b.timestamp.value)
                    bars_by_symbol[symbol_name] = bars
        
        return bars_by_symbol

    def load_job_bars_as_dataframes(self, job_id: str) -> Dict[str, pd.DataFrame]:
        """Load all bars for a job as pandas DataFrames for aggregation."""
        # For simplicity, we'll scan all parquet files in the data directory
        # In a real implementation, we'd store job metadata to know which files belong to which job
        dataframes_by_symbol = {}
        
        # Scan the data directory for parquet files
        if not self.base_path.exists():
            return dataframes_by_symbol
        
        for symbol_dir in self.base_path.iterdir():
            if symbol_dir.is_dir() and symbol_dir.name.startswith("symbol="):
                symbol_name = symbol_dir.name.split("=")[1]
                dfs = []
                
                # Recursively find all parquet files for this symbol
                for parquet_file in symbol_dir.glob("**/*.parquet"):
                    try:
                        df = pd.read_parquet(parquet_file)
                        # Ensure we have the required columns and rename for aggregation
                        if all(col in df.columns for col in ['timestamp_ns', 'open', 'high', 'low', 'close', 'volume']):
                            # Add symbol column and rename timestamp column
                            df = df.copy()
                            df['symbol'] = symbol_name
                            df['ts_ns'] = df['timestamp_ns']
                            dfs.append(df)
                    except Exception as e:
                        # Skip files that can't be read
                        print(f"Warning: Could not read {parquet_file}: {e}")
                        continue
                
                if dfs:
                    # Concatenate all DataFrames for this symbol
                    combined_df = pd.concat(dfs, ignore_index=True)
                    # Sort by timestamp
                    combined_df = combined_df.sort_values('ts_ns')
                    dataframes_by_symbol[symbol_name] = combined_df
        
        return dataframes_by_symbol
