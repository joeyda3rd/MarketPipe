# SPDX-License-Identifier: Apache-2.0
"""Legacy ParquetDataStorage - re-exported from new storage engine."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

from marketpipe.domain.entities import OHLCVBar

# Re-export the production storage engine to maintain backward compatibility
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine

from ..domain.storage import IDataStorage
from ..domain.value_objects import IngestionConfiguration, IngestionPartition


class ParquetDataStorageAdapter(IDataStorage):
    """Adapter that implements IDataStorage using ParquetStorageEngine."""

    def __init__(self, root: Union[Path, str], compression: str = "snappy"):
        self._engine = ParquetStorageEngine(root, compression)
        self.log = logging.getLogger(self.__class__.__name__)

    async def store_bars(
        self, bars: list[OHLCVBar], config: IngestionConfiguration
    ) -> IngestionPartition:
        """Persist bars and return information about the created partition."""
        if not bars:
            raise ValueError("Cannot store empty list of bars")

        # Use the engine's store_bars method which properly handles multi-day data
        return await self._engine.store_bars(bars, config)


# Use the adapter as ParquetDataStorage for backward compatibility
ParquetDataStorage = ParquetDataStorageAdapter

__all__ = ["ParquetDataStorage"]
