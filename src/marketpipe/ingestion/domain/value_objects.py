# SPDX-License-Identifier: Apache-2.0
"""Ingestion domain value objects."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

from marketpipe.domain.value_objects import Symbol


@dataclass(frozen=True)
class IngestionConfiguration:
    """Configuration for an ingestion job."""

    output_path: Path
    compression: str
    max_workers: int
    batch_size: int
    rate_limit_per_minute: Optional[int]
    feed_type: str

    def __post_init__(self):
        """Validate configuration values."""
        if self.max_workers <= 0:
            raise ValueError("max_workers must be positive")

        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

        if self.rate_limit_per_minute is not None and self.rate_limit_per_minute <= 0:
            raise ValueError("rate_limit_per_minute must be positive if specified")

        if self.compression not in ["snappy", "gzip", "lz4", "zstd"]:
            raise ValueError(f"Unsupported compression: {self.compression}")

        if self.feed_type not in ["iex", "sip"]:
            raise ValueError(f"Unsupported feed type: {self.feed_type}")

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> IngestionConfiguration:
        """Create configuration from dictionary."""
        return cls(
            output_path=Path(config_dict["output_path"]),
            compression=config_dict.get("compression", "snappy"),
            max_workers=config_dict.get("workers", 4),
            batch_size=config_dict.get("batch_size", 1000),
            rate_limit_per_minute=config_dict.get("rate_limit_per_min"),
            feed_type=config_dict.get("feed", "iex"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "output_path": str(self.output_path),
            "compression": self.compression,
            "workers": self.max_workers,
            "batch_size": self.batch_size,
            "rate_limit_per_min": self.rate_limit_per_minute,
            "feed": self.feed_type,
        }


@dataclass(frozen=True)
class BatchConfiguration:
    """Configuration for processing a batch of symbols."""

    symbols_per_batch: int
    retry_attempts: int
    retry_delay_seconds: float
    timeout_seconds: float

    def __post_init__(self):
        """Validate batch configuration."""
        if self.symbols_per_batch <= 0:
            raise ValueError("symbols_per_batch must be positive")

        if self.retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")

        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be non-negative")

        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

    @classmethod
    def default(cls) -> BatchConfiguration:
        """Create default batch configuration."""
        return cls(
            symbols_per_batch=10,
            retry_attempts=3,
            retry_delay_seconds=1.0,
            timeout_seconds=30.0,
        )


@dataclass(frozen=True)
class IngestionPartition:
    """Represents a completed data partition from ingestion."""

    symbol: Symbol
    file_path: Path
    record_count: int
    file_size_bytes: int
    created_at: datetime
    checksum: Optional[str] = None

    def __post_init__(self):
        """Validate partition data."""
        if self.record_count < 0:
            raise ValueError("record_count must be non-negative")

        if self.file_size_bytes < 0:
            raise ValueError("file_size_bytes must be non-negative")

        if not self.file_path.suffix == ".parquet":
            raise ValueError("file_path must be a .parquet file")

    @property
    def is_empty(self) -> bool:
        """Check if the partition contains no data."""
        return self.record_count == 0

    @property
    def average_record_size(self) -> float:
        """Calculate average record size in bytes."""
        if self.record_count == 0:
            return 0.0
        return self.file_size_bytes / self.record_count

    def get_storage_info(self) -> Dict[str, Any]:
        """Get storage information for the partition."""
        return {
            "symbol": self.symbol.value,
            "file_path": str(self.file_path),
            "record_count": self.record_count,
            "file_size_bytes": self.file_size_bytes,
            "average_record_size": self.average_record_size,
            "created_at": self.created_at.isoformat(),
            "checksum": self.checksum,
        }


@dataclass(frozen=True)
class ProcessingMetrics:
    """Metrics collected during ingestion processing."""

    symbols_processed: int
    symbols_failed: int
    total_bars_ingested: int
    total_processing_time_seconds: float
    average_processing_time_per_symbol: float
    peak_memory_usage_mb: Optional[float] = None

    def __post_init__(self):
        """Validate metrics."""
        if self.symbols_processed < 0:
            raise ValueError("symbols_processed must be non-negative")

        if self.symbols_failed < 0:
            raise ValueError("symbols_failed must be non-negative")

        if self.total_bars_ingested < 0:
            raise ValueError("total_bars_ingested must be non-negative")

        if self.total_processing_time_seconds < 0:
            raise ValueError("total_processing_time_seconds must be non-negative")

        if self.average_processing_time_per_symbol < 0:
            raise ValueError("average_processing_time_per_symbol must be non-negative")

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        total_symbols = self.symbols_processed + self.symbols_failed
        if total_symbols == 0:
            return 100.0
        return (self.symbols_processed / total_symbols) * 100.0

    @property
    def bars_per_second(self) -> float:
        """Calculate ingestion rate in bars per second."""
        if self.total_processing_time_seconds == 0:
            return 0.0
        return self.total_bars_ingested / self.total_processing_time_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "symbols_processed": self.symbols_processed,
            "symbols_failed": self.symbols_failed,
            "total_bars_ingested": self.total_bars_ingested,
            "total_processing_time_seconds": self.total_processing_time_seconds,
            "average_processing_time_per_symbol": self.average_processing_time_per_symbol,
            "peak_memory_usage_mb": self.peak_memory_usage_mb,
            "success_rate": self.success_rate,
            "bars_per_second": self.bars_per_second,
        }


@dataclass(frozen=True)
class IngestionCheckpoint:
    """Checkpoint for resumable ingestion operations."""

    symbol: Symbol
    last_processed_timestamp: int  # nanoseconds since epoch
    records_processed: int
    updated_at: datetime

    def __post_init__(self):
        """Validate checkpoint data."""
        if self.last_processed_timestamp < 0:
            raise ValueError("last_processed_timestamp must be non-negative")

        if self.records_processed < 0:
            raise ValueError("records_processed must be non-negative")

    @classmethod
    def initial_checkpoint(cls, symbol: Symbol) -> IngestionCheckpoint:
        """Create initial checkpoint for a symbol."""
        return cls(
            symbol=symbol,
            last_processed_timestamp=0,
            records_processed=0,
            updated_at=datetime.now(),
        )

    def advance(self, timestamp: int, records_count: int) -> IngestionCheckpoint:
        """Create new checkpoint with advanced position."""
        if timestamp < self.last_processed_timestamp:
            raise ValueError("Cannot advance checkpoint to earlier timestamp")

        return IngestionCheckpoint(
            symbol=self.symbol,
            last_processed_timestamp=timestamp,
            records_processed=self.records_processed + records_count,
            updated_at=datetime.now(),
        )

    def is_ahead_of(self, other: IngestionCheckpoint) -> bool:
        """Check if this checkpoint is ahead of another."""
        if self.symbol != other.symbol:
            raise ValueError("Cannot compare checkpoints for different symbols")

        return self.last_processed_timestamp > other.last_processed_timestamp
