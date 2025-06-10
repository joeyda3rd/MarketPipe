from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple, Dict

import duckdb
import pandas as pd
import pyarrow as pa

from marketpipe.ingestion.infrastructure.parquet_storage import ParquetDataStorage
from ..domain.value_objects import FrameSpec


class DuckDBAggregationEngine:
    """DuckDB-powered aggregation engine for resampling 1-minute bars to higher timeframes."""
    
    def __init__(self, raw_root: Path, agg_root: Path):
        """Initialize aggregation engine.
        
        Args:
            raw_root: Path to raw 1-minute Parquet data
            agg_root: Path to write aggregated Parquet data
        """
        self._raw = ParquetDataStorage(str(raw_root))
        self._agg_root = Path(agg_root)
        self._agg_root.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger(self.__class__.__name__)

    def aggregate_job(self, job_id: str, frame_sql_pairs: List[Tuple[FrameSpec, str]]) -> None:
        """Aggregate 1-minute bars for a job to multiple timeframes.
        
        Args:
            job_id: Ingestion job identifier
            frame_sql_pairs: List of (FrameSpec, SQL) tuples for aggregation
        """
        try:
            # Load raw data for all symbols in the job
            symbol_dataframes = self._raw.load_job_bars_as_dataframes(job_id)
            
            if not symbol_dataframes:
                self.log.warning(f"No data found for job {job_id}")
                return
            
            # Create DuckDB connection
            con = duckdb.connect(":memory:")
            
            # Process each symbol
            for symbol, df in symbol_dataframes.items():
                self.log.info(f"Aggregating {len(df)} bars for symbol {symbol}")
                
                # Register DataFrame as table in DuckDB
                con.register("bars", pa.Table.from_pandas(df))
                
                # Execute aggregation for each timeframe
                for spec, sql in frame_sql_pairs:
                    self.log.debug(f"Executing aggregation for {spec.name} frame")
                    
                    try:
                        # Execute aggregation SQL
                        result_df = con.execute(sql).fetch_df()
                        
                        if result_df.empty:
                            self.log.warning(f"No aggregated data for {symbol} {spec.name}")
                            continue
                        
                        # Write aggregated data to partitioned Parquet
                        self._write_aggregated_data(result_df, symbol, spec, job_id)
                        
                        self.log.info(f"Aggregated {len(result_df)} {spec.name} bars for {symbol}")
                        
                    except Exception as e:
                        self.log.error(f"Failed to aggregate {symbol} to {spec.name}: {e}")
                        continue
            
            con.close()
            self.log.info(f"Completed aggregation for job {job_id}")
            
        except Exception as e:
            self.log.error(f"Aggregation failed for job {job_id}: {e}")
            raise

    def _write_aggregated_data(
        self, 
        df: pd.DataFrame, 
        symbol: str, 
        spec: FrameSpec, 
        job_id: str
    ) -> None:
        """Write aggregated DataFrame to partitioned Parquet file."""
        # Create partition directory structure
        partition_path = (
            self._agg_root
            / f"frame={spec.name}"
            / f"symbol={symbol}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Write Parquet file
        output_file = partition_path / f"{job_id}.parquet"
        df.to_parquet(output_file, index=False, compression="snappy")
        
        self.log.debug(f"Wrote {len(df)} rows to {output_file}")

    def get_aggregated_data(
        self, 
        symbol: str, 
        frame: FrameSpec, 
        start_ts: int = None, 
        end_ts: int = None
    ) -> pd.DataFrame:
        """Load aggregated data for a symbol and timeframe.
        
        Args:
            symbol: Symbol to load
            frame: Timeframe specification
            start_ts: Optional start timestamp (nanoseconds)
            end_ts: Optional end timestamp (nanoseconds)
            
        Returns:
            DataFrame with aggregated OHLCV data
        """
        partition_path = (
            self._agg_root
            / f"frame={frame.name}"
            / f"symbol={symbol}"
        )
        
        if not partition_path.exists():
            return pd.DataFrame()
        
        # Read all Parquet files for this symbol/frame
        dfs = []
        for parquet_file in partition_path.glob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                dfs.append(df)
            except Exception as e:
                self.log.warning(f"Could not read {parquet_file}: {e}")
                continue
        
        if not dfs:
            return pd.DataFrame()
        
        # Combine all data
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Sort by timestamp
        combined_df = combined_df.sort_values("ts_ns")
        
        # Apply time filtering if specified
        if start_ts is not None:
            combined_df = combined_df[combined_df["ts_ns"] >= start_ts]
        if end_ts is not None:
            combined_df = combined_df[combined_df["ts_ns"] <= end_ts]
        
        return combined_df 