# SPDX-License-Identifier: Apache-2.0
"""Fake market data adapter for testing and development."""

from __future__ import annotations

import random
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Any

from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Symbol, Price, Timestamp, Volume, TimeRange
from marketpipe.domain.market_data import (
    IMarketDataProvider,
    ProviderMetadata,
    MarketDataUnavailableError,
    InvalidSymbolError,
)
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
        supported_symbols: List[str] = None,
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
            "FAKE1",
            "FAKE2",
            "TEST",
        ]

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> FakeMarketDataAdapter:
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
    ) -> List[OHLCVBar]:
        """
        Generate fake OHLCV bars for the given symbol and time range.
        """
        # Simulate random failures if configured
        if random.random() < self._fail_probability:
            raise MarketDataUnavailableError("Simulated provider failure")

        # Check if symbol is supported
        if symbol.value not in self._supported_symbols:
            raise InvalidSymbolError(
                f"Symbol {symbol.value} not supported by fake provider"
            )

        # Generate bars for the time range
        bars = []
        current_time = time_range.start.value
        end_time = time_range.end.value

        # Use symbol name to seed price variation
        symbol_seed = hash(symbol.value) % 1000
        current_price = self._base_price + (symbol_seed / 10)

        bar_count = 0
        while current_time < end_time and bar_count < max_bars:
            # Generate OHLCV for this minute
            bar = self._generate_bar(symbol, current_time, current_price)
            bars.append(bar)

            # Update price for next bar (random walk)
            price_change = random.gauss(0, self._volatility * current_price)
            current_price = max(0.01, current_price + price_change)

            # Move to next minute
            current_time += timedelta(minutes=1)
            bar_count += 1

        return bars

    def _generate_bar(
        self, symbol: Symbol, timestamp: datetime, base_price: float
    ) -> OHLCVBar:
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
        volume = int(
            random.lognormvariate(8, 1.5)
        )  # Mean around 3000, realistic variance

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

    async def get_supported_symbols(self) -> List[Symbol]:
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
    ) -> List[OHLCVBar]:
        """Legacy method for backward compatibility."""
        from marketpipe.domain.value_objects import Timestamp, TimeRange

        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size)
