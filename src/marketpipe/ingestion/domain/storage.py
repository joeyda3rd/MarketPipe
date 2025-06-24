# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from marketpipe.domain.entities import OHLCVBar

from .value_objects import IngestionConfiguration, IngestionPartition


class IDataStorage(ABC):
    """Interface for storing validated OHLCV bars."""

    @abstractmethod
    async def store_bars(
        self, bars: List[OHLCVBar], config: IngestionConfiguration
    ) -> IngestionPartition:
        """Persist bars and return information about the created partition."""
        pass
