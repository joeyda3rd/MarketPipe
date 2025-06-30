from __future__ import annotations

import datetime as _dt
from abc import ABC, abstractmethod
from typing import Any

from marketpipe.domain import SymbolRecord


class SymbolProviderBase(ABC):
    """
    Abstract base class for symbol-list providers.

    Subclasses **must**:
      • declare a unique `name` class attribute (lower-snake).
      • implement `async _fetch_raw(...)` that contacts the API or reads a file.
      • implement `_map_to_records(...)` that converts raw payload to `SymbolRecord`.
    """

    #: override in subclasses: provider identifier used on CLI and registry
    name: str

    def __init__(self, *, as_of: _dt.date | None = None, **provider_cfg: Any) -> None:
        self.as_of = as_of or _dt.date.today()
        self.cfg = provider_cfg  # token, base_url, etc.

    # ---------- public API --------------------------------------------------

    async def fetch_symbols(self) -> list[SymbolRecord]:
        """Fetch, validate, and return a list of SymbolRecord."""
        raw = await self._fetch_raw()
        records = self._map_to_records(raw)
        # Pydantic validation happens in SymbolRecord construction
        return records

    # ---------- mandatory hooks for subclasses ------------------------------

    @abstractmethod
    async def _fetch_raw(self) -> Any:
        """Fetch raw data from provider source (API, file, etc.)."""
        ...

    @abstractmethod
    def _map_to_records(self, payload: Any) -> list[SymbolRecord]:
        """Convert raw payload to list of validated SymbolRecord objects."""
        ...

    # ---------- convenience -------------------------------------------------

    def fetch_symbols_sync(self) -> list[SymbolRecord]:
        """Blocking wrapper for non-async call sites."""
        import anyio

        return anyio.run(self.fetch_symbols)
