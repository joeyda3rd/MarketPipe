from __future__ import annotations

import datetime as _dt
from typing import List

from marketpipe.domain import SymbolRecord
from marketpipe.domain.symbol import safe_create
from .base import SymbolProviderBase
from . import register


@register("dummy")
class DummyProvider(SymbolProviderBase):
    """Dummy provider for testing symbol provider framework.
    
    Returns static test data without any network calls.
    Useful for unit tests and development.
    """

    async def _fetch_raw(self):
        """Return static test data without network calls."""
        # No network — just return static dict
        return [
            {
                "ticker": "TEST",
                "exchange_mic": "XNAS",
                "asset_class": "EQUITY",
                "currency": "USD",
                "status": "ACTIVE",
                "as_of": str(self.as_of),
            }
        ]

    def _map_to_records(self, payload) -> List[SymbolRecord]:
        """Convert payload to SymbolRecord objects."""
        out = []
        for row in payload:
            rec = safe_create(row, provider=self.name)
            if rec:
                out.append(rec)
        return out 