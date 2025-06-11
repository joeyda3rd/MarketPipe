# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple

import duckdb
import pandas as pd
import pyarrow as pa

from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine
from ..domain.value_objects import FrameSpec


class DuckDBAggregationEngine:
    """DuckDB-powered aggregation engine for resampling 1-minute bars to higher timeframes."""

    def __init__(self, raw_root: Path, agg_root: Path):
        """Initialize aggregation engine.

        Args:
            raw_root: Path to raw 1-minute Parquet data
            agg_root: Path to write aggregated Parquet data
        """
        self._raw_storage = ParquetStorageEngine(raw_root)
        self._agg_storage = ParquetStorageEngine(agg_root)
        self.log = logging.getLogger(self.__class__.__name__)

    def aggregate_job(
        self, job_id: str, frame_sql_pairs: List[Tuple[FrameSpec, str]]
    ) -> None:
        """Aggregate 1-minute bars for a job to multiple timeframes.

        Args:
            job_id: Ingestion job identifier
            frame_sql_pairs: List of (FrameSpec, SQL) tuples for aggregation
        """
        try:
            # Load raw data for all symbols in the job using new engine
            symbol_dataframes = self._raw_storage.load_job_bars(job_id)

            if not symbol_dataframes:
                self.log.warning(f"No data found for job {job_id}")
                return

            # Create DuckDB connection
            con = duckdb.connect(":memory:")

            # Process each symbol
            for symbol, df in symbol_dataframes.items():
                self.log.info(f"Aggregating {len(df)} bars for symbol {symbol}")

                # Ensure we have the ts_ns column for aggregation
                if "ts_ns" not in df.columns and "timestamp_ns" in df.columns:
                    df = df.copy()
                    df["ts_ns"] = df["timestamp_ns"]

                # Add symbol column if missing
                if "symbol" not in df.columns:
                    df = df.copy()
                    df["symbol"] = symbol

                # Register DataFrame as table in DuckDB
                con.register("bars", pa.Table.from_pandas(df))

                # Execute aggregation for each timeframe
                for spec, sql in frame_sql_pairs:
                    self.log.debug(f"Executing aggregation for {spec.name} frame")

                    try:
                        # Execute aggregation SQL
                        result_df = con.execute(sql).fetch_df()

                        if result_df.empty:
                            self.log.warning(
                                f"No aggregated data for {symbol} {spec.name}"
                            )
                            continue

                        # Write aggregated data using the new storage engine
                        self._write_aggregated_data(result_df, symbol, spec, job_id)

                        self.log.info(
                            f"Aggregated {len(result_df)} {spec.name} bars for {symbol}"
                        )

                    except Exception as e:
                        self.log.error(
                            f"Failed to aggregate {symbol} to {spec.name}: {e}"
                        )
                        continue

            con.close()
            self.log.info(f"Completed aggregation for job {job_id}")

        except Exception as e:
            self.log.error(f"Aggregation failed for job {job_id}: {e}")
            raise

    def _write_aggregated_data(
        self, df: pd.DataFrame, symbol: str, spec: FrameSpec, job_id: str
    ) -> None:
        """Write aggregated DataFrame using the new storage engine."""
        # Determine trading day from the first timestamp
        if "ts_ns" in df.columns and not df.empty:
            first_ts_ns = df["ts_ns"].iloc[0]
            trading_day = pd.Timestamp(first_ts_ns, unit="ns").date()
        else:
            # Fallback to today if no timestamp data
            from datetime import date

            trading_day = date.today()

        # Use the new storage engine to write the data
        try:
            output_path = self._agg_storage.write(
                df,
                frame=spec.name,
                symbol=symbol,
                trading_day=trading_day,
                job_id=job_id,
                overwrite=True,
            )
            self.log.debug(f"Wrote {len(df)} rows to {output_path}")
        except Exception as e:
            self.log.error(
                f"Failed to write aggregated data for {symbol} {spec.name}: {e}"
            )
            raise

    def get_aggregated_data(
        self, symbol: str, frame: FrameSpec, start_ts: int = None, end_ts: int = None
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
        # Use the new storage engine to load data
        df = self._agg_storage.load_symbol_data(symbol=symbol, frame=frame.name)

        if df.empty:
            return df

        # Apply time filtering if specified
        if start_ts is not None and "ts_ns" in df.columns:
            df = df[df["ts_ns"] >= start_ts]
        if end_ts is not None and "ts_ns" in df.columns:
            df = df[df["ts_ns"] <= end_ts]

        return df
