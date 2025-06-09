"""Ingestion infrastructure module."""

from __future__ import annotations

from .adapters import AlpacaMarketDataAdapter, MarketDataProviderAdapter
from .repositories import SqliteIngestionJobRepository, SqliteCheckpointRepository
from .clients import AlpacaApiClientWrapper
from .parquet_storage import ParquetDataStorage

__all__ = [
    # Adapters (Anti-corruption layer)
    "AlpacaMarketDataAdapter",
    "MarketDataProviderAdapter",
    # Repository implementations
    "SqliteIngestionJobRepository", 
    "SqliteCheckpointRepository",
    # Client wrappers
    "AlpacaApiClientWrapper",
    # Storage implementations
    "ParquetDataStorage",
]