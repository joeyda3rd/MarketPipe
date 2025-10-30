# SPDX-License-Identifier: Apache-2.0
"""Fake market data adapter for testing and development."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.market_data import (
    IMarketDataProvider,
    InvalidSymbolError,
    MarketDataUnavailableError,
    ProviderMetadata,
)
from marketpipe.domain.value_objects import Price, Symbol, TimeRange, Timestamp, Volume

from .provider_registry import provider


@provider("fake")
class FakeMarketDataAdapter(IMarketDataProvider):
    """
    Fake market data provider for testing and development.

    Generates synthetic OHLCV bars with realistic-looking patterns.
    """

    def __init__(
        self,
        base_price: float = 100.0,
        volatility: float = 0.02,
        fail_probability: float = 0.0,
        supported_symbols: Optional[list[str]] = None,
    ):
        self._base_price = base_price
        self._volatility = volatility
        self._fail_probability = fail_probability
        self._supported_symbols = supported_symbols or [
            "AAPL",
            "GOOGL",
            "MSFT",
            "AMZN",
            "TSLA",
            "COST",
            "FAKE1",
            "FAKE2",
            "TEST",
        ]

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> FakeMarketDataAdapter:
        """
        Create adapter from configuration dictionary.

        Args:
            config: Configuration dictionary with keys:
                - base_price: Starting price for fake data (optional, default: 100.0)
                - volatility: Price volatility factor (optional, default: 0.02)
                - fail_probability: Probability of simulated failures (optional, default: 0.0)
                - supported_symbols: List of symbols to support (optional)
        """
        return cls(
            base_price=config.get("base_price", 100.0),
            volatility=config.get("volatility", 0.02),
            fail_probability=config.get("fail_probability", 0.0),
            supported_symbols=config.get("supported_symbols"),
        )

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
        timeframe: str = "1m",
    ) -> list[OHLCVBar]:
        """
        Generate fake OHLCV bars for the given symbol and time range.

        Args:
            symbol: Stock symbol
            time_range: Time range for data retrieval
            max_bars: Maximum number of bars to fetch
            timeframe: Bar timeframe (e.g., "1m", "5m", "15m", "1h", "1d")
        """
        # Simulate random failures if configured
        if random.random() < self._fail_probability:
            raise MarketDataUnavailableError("Simulated provider failure")

        # Check if symbol is supported
        if symbol.value not in self._supported_symbols:
            raise InvalidSymbolError(f"Symbol {symbol.value} not supported by fake provider")

        # Parse timeframe to minutes
        timeframe_minutes = self._parse_timeframe_to_minutes(timeframe)

        # Calculate expected number of bars for the time range
        start_time = time_range.start.value
        end_time = time_range.end.value
        time_diff = end_time - start_time
        expected_bars = int(time_diff.total_seconds() / (timeframe_minutes * 60))

        # Use a more generous max_bars limit to ensure we cover the full range
        # Allow up to 10,000 bars or the expected number, whichever is higher
        effective_max_bars = max(max_bars, expected_bars, 10000)

        # Generate bars for the time range
        bars = []
        current_time = start_time

        # Use symbol name to seed price variation
        symbol_seed = hash(symbol.value) % 1000
        current_price = self._base_price + (symbol_seed / 10)

        bar_count = 0
        while current_time < end_time and bar_count < effective_max_bars:
            # Generate OHLCV for this timeframe
            bar = self._generate_bar(symbol, current_time, current_price)
            bars.append(bar)

            # Update price for next bar (random walk)
            price_change = random.gauss(0, self._volatility * current_price)
            current_price = max(0.01, current_price + price_change)

            # Move to next timeframe interval
            current_time += timedelta(minutes=timeframe_minutes)
            bar_count += 1

        return bars

    def _parse_timeframe_to_minutes(self, timeframe: str) -> int:
        """Parse timeframe string to minutes."""
        timeframe_map = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440,
        }
        if timeframe not in timeframe_map:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        return timeframe_map[timeframe]

    def _generate_bar(self, symbol: Symbol, timestamp: datetime, base_price: float) -> OHLCVBar:
        """Generate a single OHLCV bar."""
        # Generate intrabar price movement
        volatility = self._volatility * base_price

        # Open price (with small random variation from base)
        open_price = base_price + random.gauss(0, volatility * 0.5)

        # Generate high and low around open
        high_price = open_price + abs(random.gauss(0, volatility))
        low_price = open_price - abs(random.gauss(0, volatility))

        # Close price (mean reversion towards open)
        close_price = open_price + random.gauss(0, volatility * 0.7)

        # Ensure OHLC consistency
        high_price = max(high_price, open_price, close_price)
        low_price = min(low_price, open_price, close_price)

        # Generate volume (log-normal distribution)
        volume = int(random.lognormvariate(8, 1.5))  # Mean around 3000, realistic variance

        return OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(timestamp.replace(tzinfo=timezone.utc)),
            open_price=Price(Decimal(f"{open_price:.2f}")),
            high_price=Price(Decimal(f"{high_price:.2f}")),
            low_price=Price(Decimal(f"{low_price:.2f}")),
            close_price=Price(Decimal(f"{close_price:.2f}")),
            volume=Volume(volume),
        )

    async def get_supported_symbols(self) -> list[Symbol]:
        """Get list of supported symbols."""
        return [Symbol.from_string(s) for s in self._supported_symbols]

    async def is_available(self) -> bool:
        """Fake provider is always available (unless configured to fail)."""
        return random.random() >= self._fail_probability

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get fake provider metadata."""
        return ProviderMetadata(
            provider_name="fake",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=None,  # No rate limits for fake data
            minimum_time_resolution="1m",
            maximum_history_days=None,  # Can generate data for any historical period
        )

    # Legacy method for backward compatibility
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000,
        timeframe: str = "1m",
    ) -> list[OHLCVBar]:
        """Legacy method for backward compatibility."""
        from marketpipe.domain.value_objects import TimeRange, Timestamp

        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size, timeframe)
