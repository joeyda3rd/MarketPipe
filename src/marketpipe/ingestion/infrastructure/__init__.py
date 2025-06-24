# SPDX-License-Identifier: Apache-2.0
"""Ingestion infrastructure module."""

from __future__ import annotations

from .adapters import AlpacaMarketDataAdapter
from .clients import AlpacaApiClientWrapper
from .fake_adapter import FakeMarketDataAdapter
from .finnhub_adapter import FinnhubMarketDataAdapter
from .iex_adapter import IEXMarketDataAdapter
from .parquet_storage import ParquetDataStorage
from .polygon_adapter import PolygonMarketDataAdapter
from .provider_loader import build_provider, get_available_providers
from .provider_registry import get, list_providers, provider, register
from .repositories import SqliteCheckpointRepository, SqliteIngestionJobRepository

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
