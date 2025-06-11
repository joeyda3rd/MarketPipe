# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pydantic import BaseModel, Field

# Path to the canonical OHLCV schema used throughout the package
SCHEMA_PATH = "schema/schema_v1.json"


class ClientConfig(BaseModel):
    """Configuration for API clients."""

    api_key: str = Field(..., description="Vendor API key or token")
    base_url: str
    timeout: float = 30.0
    max_retries: int = 3
    rate_limit_per_min: int | None = Field(None, description="Rate limit in requests per minute")
    burst_size: int | None = Field(None, description="Maximum burst size (defaults to rate_limit_per_min)")
    user_agent: str = "MarketPipe/0.1"


class OHLCVRow(BaseModel):
    """Stub schema for OHLCV data rows."""

    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int | None = None
    vwap: float | None = None


__all__ = ["ClientConfig", "OHLCVRow"]
