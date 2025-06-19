# SPDX-License-Identifier: Apache-2.0
"""Ingestion infrastructure module."""

from __future__ import annotations

from .adapters import AlpacaMarketDataAdapter
from .iex_adapter import IEXMarketDataAdapter
from .fake_adapter import FakeMarketDataAdapter
from .polygon_adapter import PolygonMarketDataAdapter
from .finnhub_adapter import FinnhubMarketDataAdapter
from .provider_registry import provider, register, get, list_providers
from .provider_loader import build_provider, get_available_providers
from .repositories import SqliteIngestionJobRepository, SqliteCheckpointRepository
from .clients import AlpacaApiClientWrapper
from .parquet_storage import ParquetDataStorage

__all__ = [
    # Adapters (Anti-corruption layer)
    "AlpacaMarketDataAdapter",
    "IEXMarketDataAdapter",
    "FakeMarketDataAdapter",
    "PolygonMarketDataAdapter",
    "FinnhubMarketDataAdapter",
    # Provider registry and loading
    "provider",
    "register",
    "get",
    "list_providers",
    "build_provider",
    "get_available_providers",
    # Repository implementations
    "SqliteIngestionJobRepository",
    "SqliteCheckpointRepository",
    # Client wrappers
    "AlpacaApiClientWrapper",
    # Storage implementations
    "ParquetDataStorage",
]
