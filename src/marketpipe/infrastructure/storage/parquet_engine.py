# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fasteners


class ParquetStorageEngine:
    """
    Production-ready Parquet storage engine with partitioned writes and concurrent reads.

    Partition layout:
        <root>/
            frame=<frame>/
                symbol=<SYMBOL>/
                    date=<YYYY-MM-DD>/   # optional day partition
                        <job_id>.parquet

    Features:
    - Thread-safe writes using file locking
    - Partitioned storage by frame/symbol/date
    - Compression support (zstd by default)
    - Concurrent read operations
    - Job-based file organization
    """

    def __init__(self, root: Path | str, compression: str = "zstd"):
        """Initialize storage engine.

        Args:
            root: Root directory for Parquet storage
            compression: Compression algorithm (zstd, snappy, gzip, etc.)
        """
        self._root = Path(root)
        self._compression = compression
        self._root.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger(self.__class__.__name__)

        # Validate compression algorithm
        if compression not in {"zstd", "snappy", "gzip", "lz4", "brotli"}:
            raise ValueError(f"Unsupported compression: {compression}")

    # ----- Write Operations -----

    def write(
        self,
        df: pd.DataFrame,
        *,
        frame: str,
        symbol: str,
        trading_day: date,
        job_id: str,
        overwrite: bool = False,
    ) -> Path:
        """Write DataFrame to partitioned Parquet file with concurrency safety.

        Args:
            df: DataFrame containing OHLCV data
            frame: Timeframe (e.g., "1m", "5m", "1h", "1d")
            symbol: Stock symbol (e.g., "AAPL")
            trading_day: Trading date for partitioning
            job_id: Unique job identifier for the file
            overwrite: Whether to overwrite existing files

        Returns:
            Path to the written Parquet file

        Raises:
            FileExistsError: If file exists and overwrite=False
            ValueError: If DataFrame is empty or invalid
        """
        if df.empty:
            raise ValueError("Cannot write empty DataFrame")

        # Validate required columns
        required_cols = {"ts_ns", "open", "high", "low", "close", "volume"}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise ValueError(f"DataFrame missing required columns: {missing}")

        # Create partition directory structure
        partition_path = (
            self._root
            / f"frame={frame}"
            / f"symbol={symbol}"
            / f"date={trading_day.isoformat()}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        # Define output file path
        file_path = partition_path / f"{job_id}.parquet"

        # Use file locking for concurrency safety
        lock_path = str(file_path) + ".lock"
        with fasteners.InterProcessLock(lock_path):
            if file_path.exists() and not overwrite:
                raise FileExistsError(f"File already exists: {file_path}")

            try:
                # Convert to Arrow table with explicit schema to avoid type incompatibilities
                table = pa.Table.from_pandas(df, preserve_index=False)

                # Write with compression and consistent schema
                pq.write_table(
                    table,
                    file_path,
                    compression=self._compression,
                    row_group_size=10000,  # Optimize for read performance
                    use_dictionary=False,  # Disable dictionary encoding to avoid type conflicts
                )

                self.log.info(f"Wrote {len(df)} rows to {file_path}")

            except Exception as e:
                # Clean up partial file on failure
                if file_path.exists():
                    file_path.unlink()
                self.log.error(f"Failed to write {file_path}: {e}")
                raise

        return file_path

    def append_to_job(
        self,
        df: pd.DataFrame,
        *,
        frame: str,
        symbol: str,
        trading_day: date,
        job_id: str,
    ) -> Path:
        """Append data to existing job file or create new one.

        Args:
            df: DataFrame to append
            frame: Timeframe identifier
            symbol: Stock symbol
            trading_day: Trading date
            job_id: Job identifier

        Returns:
            Path to the updated Parquet file
        """
        file_path = (
            self._root
            / f"frame={frame}"
            / f"symbol={symbol}"
            / f"date={trading_day.isoformat()}"
            / f"{job_id}.parquet"
        )

        if file_path.exists():
            # Load existing data and combine with new data
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)

            # Remove duplicates based on timestamp if present
            if "ts_ns" in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=["ts_ns"], keep="last")
                combined_df = combined_df.sort_values("ts_ns")

            return self.write(
                combined_df,
                frame=frame,
                symbol=symbol,
                trading_day=trading_day,
                job_id=job_id,
                overwrite=True,
            )
        else:
            return self.write(
                df,
                frame=frame,
                symbol=symbol,
                trading_day=trading_day,
                job_id=job_id,
                overwrite=False,
            )

    async def store_bars(self, bars, configuration):
        """Store OHLCV bars using the configured settings.

        This method provides compatibility with the coordinator service interface.
        """
        if not bars:
            # Return a dummy partition for empty data
            from marketpipe.ingestion.domain.value_objects import IngestionPartition
            from datetime import datetime, timezone

            return IngestionPartition(
                symbol=bars[0].symbol if bars else None,
                file_path=self._root / "empty.parquet",
                record_count=0,
                file_size_bytes=0,
                created_at=datetime.now(timezone.utc),
            )

        # Convert bars to DataFrame
        import pandas as pd
        from datetime import datetime, timezone

        data = []
        for bar in bars:
            data.append(
                {
                    "ts_ns": bar.timestamp_ns,
                    "open": float(bar.open_price.value),
                    "high": float(bar.high_price.value),
                    "low": float(bar.low_price.value),
                    "close": float(bar.close_price.value),
                    "volume": int(bar.volume.value),
                    "symbol": bar.symbol.value,
                }
            )

        df = pd.DataFrame(data)

        # Extract details from first bar
        first_bar = bars[0]
        symbol = first_bar.symbol.value
        trading_day = first_bar.timestamp.trading_date()

        # Generate job ID from configuration or create one
        import uuid

        job_id = str(uuid.uuid4())[:8]

        # Write to storage
        file_path = self.write(
            df,
            frame="1m",  # Default to 1-minute bars
            symbol=symbol,
            trading_day=trading_day,
            job_id=job_id,
            overwrite=True,
        )

        # Return partition information
        from marketpipe.ingestion.domain.value_objects import IngestionPartition

        file_size = file_path.stat().st_size if file_path.exists() else 0

        return IngestionPartition(
            symbol=first_bar.symbol,
            file_path=file_path,
            record_count=len(bars),
            file_size_bytes=file_size,
            created_at=datetime.now(timezone.utc),
        )

    # ----- Read Operations -----

    def load_partition(
        self, frame: str, symbol: str, trading_day: date
    ) -> pd.DataFrame:
        """Load all data for a specific partition (frame/symbol/date).

        Args:
            frame: Timeframe identifier
            symbol: Stock symbol
            trading_day: Trading date

        Returns:
            Combined DataFrame from all job files in the partition
        """
        partition_path = (
            self._root
            / f"frame={frame}"
            / f"symbol={symbol}"
            / f"date={trading_day.isoformat()}"
        )

        if not partition_path.exists():
            self.log.debug(f"Partition not found: {partition_path}")
            return pd.DataFrame()

        # Read all Parquet files in the partition
        dataframes = []
        for parquet_file in partition_path.glob("*.parquet"):
            try:
                # Read as DataFrame directly to avoid schema merge issues
                df = pd.read_parquet(parquet_file)
                dataframes.append(df)
            except Exception as e:
                self.log.warning(f"Could not read {parquet_file}: {e}")
                continue

        if not dataframes:
            return pd.DataFrame()

        # Combine all DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True)

        # Sort by timestamp if available
        if "ts_ns" in combined_df.columns:
            combined_df = combined_df.sort_values("ts_ns")

        return combined_df

    def load_job_bars(self, job_id: str) -> Dict[str, pd.DataFrame]:
        """Load all symbol DataFrames written by a specific job.

        Args:
            job_id: Job identifier to load

        Returns:
            Dictionary mapping symbol names to their DataFrames
        """
        symbol_dataframes: Dict[str, List[pd.DataFrame]] = {}

        # Search for all files with the job_id pattern
        for parquet_file in self._root.rglob(f"{job_id}.parquet"):
            try:
                # Extract symbol from path structure
                # Path format: .../frame={frame}/symbol={symbol}/date={date}/{job_id}.parquet
                symbol_part = parquet_file.parent.parent.name
                if not symbol_part.startswith("symbol="):
                    self.log.warning(f"Unexpected path structure: {parquet_file}")
                    continue

                symbol = symbol_part.split("symbol=")[1]

                # Read the DataFrame
                df = pd.read_parquet(parquet_file)

                # Group by symbol
                symbol_dataframes.setdefault(symbol, []).append(df)

            except Exception as e:
                self.log.error(f"Failed to read {parquet_file}: {e}")
                continue

        # Combine DataFrames for each symbol
        result = {}
        for symbol, dfs in symbol_dataframes.items():
            if len(dfs) == 1:
                result[symbol] = dfs[0]
            else:
                combined_df = pd.concat(dfs, ignore_index=True)
                # Sort by timestamp if available
                if "ts_ns" in combined_df.columns:
                    combined_df = combined_df.sort_values("ts_ns")
                result[symbol] = combined_df

        return result

    def load_symbol_data(
        self,
        symbol: str,
        frame: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """Load data for a symbol across multiple dates.

        Args:
            symbol: Stock symbol
            frame: Timeframe identifier
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            Combined DataFrame for the symbol
        """
        symbol_path = self._root / f"frame={frame}" / f"symbol={symbol}"

        if not symbol_path.exists():
            return pd.DataFrame()

        dfs = []
        for date_dir in symbol_path.iterdir():
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue

            # Extract date from directory name
            try:
                date_str = date_dir.name.split("date=")[1]
                dir_date = date.fromisoformat(date_str)
            except (ValueError, IndexError):
                self.log.warning(f"Invalid date directory: {date_dir}")
                continue

            # Apply date filtering
            if start_date and dir_date < start_date:
                continue
            if end_date and dir_date > end_date:
                continue

            # Load all Parquet files in this date directory
            for parquet_file in date_dir.glob("*.parquet"):
                try:
                    df = pd.read_parquet(parquet_file)
                    dfs.append(df)
                except Exception as e:
                    self.log.warning(f"Could not read {parquet_file}: {e}")
                    continue

        if not dfs:
            return pd.DataFrame()

        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)

        # Sort by timestamp if available
        if "ts_ns" in combined_df.columns:
            combined_df = combined_df.sort_values("ts_ns")

        return combined_df

    # ----- Utility Operations -----

    def delete_job(self, job_id: str) -> int:
        """Delete all files associated with a job.

        Args:
            job_id: Job identifier to delete

        Returns:
            Number of files removed
        """
        removed_count = 0

        for parquet_file in self._root.rglob(f"{job_id}.parquet"):
            try:
                parquet_file.unlink(missing_ok=True)
                removed_count += 1
                self.log.debug(f"Deleted {parquet_file}")
            except Exception as e:
                self.log.error(f"Failed to delete {parquet_file}: {e}")

        self.log.info(f"Deleted {removed_count} files for job {job_id}")
        return removed_count

    def list_jobs(self, frame: str, symbol: str) -> List[str]:
        """List all job IDs for a given frame and symbol.

        Args:
            frame: Timeframe identifier
            symbol: Stock symbol

        Returns:
            List of job IDs found
        """
        symbol_path = self._root / f"frame={frame}" / f"symbol={symbol}"

        if not symbol_path.exists():
            return []

        job_ids = set()
        for parquet_file in symbol_path.rglob("*.parquet"):
            job_id = parquet_file.stem  # filename without extension
            job_ids.add(job_id)

        return sorted(job_ids)

    def get_storage_stats(self) -> Dict[str, any]:
        """Get storage statistics.

        Returns:
            Dictionary with storage metrics
        """
        total_files = 0
        total_size = 0
        frames = set()
        symbols = set()

        for parquet_file in self._root.rglob("*.parquet"):
            total_files += 1
            try:
                total_size += parquet_file.stat().st_size

                # Extract frame and symbol from path
                parts = parquet_file.parts
                for part in parts:
                    if part.startswith("frame="):
                        frames.add(part.split("=")[1])
                    elif part.startswith("symbol="):
                        symbols.add(part.split("=")[1])

            except Exception as e:
                self.log.warning(f"Could not stat {parquet_file}: {e}")

        return {
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "unique_frames": len(frames),
            "unique_symbols": len(symbols),
            "frames": sorted(frames),
            "symbols": sorted(symbols),
        }

    def validate_integrity(self) -> Dict[str, any]:
        """Validate storage integrity and return diagnostics.

        Returns:
            Dictionary with validation results
        """
        corrupted_files = []
        valid_files = 0
        total_rows = 0

        for parquet_file in self._root.rglob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                valid_files += 1
                total_rows += len(df)
            except Exception as e:
                corrupted_files.append({"file": str(parquet_file), "error": str(e)})

        return {
            "valid_files": valid_files,
            "corrupted_files": len(corrupted_files),
            "corruption_details": corrupted_files,
            "total_rows": total_rows,
            "is_healthy": len(corrupted_files) == 0,
        }
