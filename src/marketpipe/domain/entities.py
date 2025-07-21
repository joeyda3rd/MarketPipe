# SPDX-License-Identifier: Apache-2.0
"""Domain entities for MarketPipe.

Entities are objects that have identity and lifecycle. They are distinguished
by their identity rather than their attributes and can change over time while
maintaining their identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from uuid import UUID, uuid4

from .value_objects import Price, Symbol, Timestamp, Volume


@dataclass(frozen=True)
class EntityId:
    """Base class for entity identifiers."""

    value: UUID

    @classmethod
    def generate(cls) -> EntityId:
        """Generate a new unique entity ID."""
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


class Entity:
    """Base class for all domain entities.

    Entities are objects that have identity and can change over time.
    They are distinguished by their identity rather than their attributes.
    """

    def __init__(self, id: EntityId):
        self._id = id
        self._version = 1

    @property
    def id(self) -> EntityId:
        """Get the entity's unique identifier."""
        return self._id

    @property
    def version(self) -> int:
        """Get the entity's version for optimistic concurrency control."""
        return self._version

    def _increment_version(self) -> None:
        """Increment the entity version after changes."""
        self._version += 1

    def __eq__(self, other: object) -> bool:
        """Entities are equal if they have the same ID."""
        return isinstance(other, Entity) and self._id == other._id

    def __hash__(self) -> int:
        """Hash based on entity ID."""
        return hash(self._id)


class OHLCVBar(Entity):
    """Represents a single OHLCV (Open, High, Low, Close, Volume) bar.

    This is the core entity representing market data for a specific symbol
    and timestamp. It encapsulates business rules for price validation
    and provides methods for price calculations.
    """

    def __init__(
        self,
        id: EntityId,
        symbol: Symbol,
        timestamp: Timestamp,
        open_price: Price,
        high_price: Price,
        low_price: Price,
        close_price: Price,
        volume: Volume,
        trade_count: Optional[int] = None,
        vwap: Optional[Price] = None,
    ):
        super().__init__(id)
        self._symbol = symbol
        self._timestamp = timestamp
        self._open_price = open_price
        self._high_price = high_price
        self._low_price = low_price
        self._close_price = close_price
        self._volume = volume
        self._trade_count = trade_count
        self._vwap = vwap

        # Validate OHLC consistency on creation
        self._validate_ohlc_consistency()

    def _validate_ohlc_consistency(self) -> None:
        """Validate OHLC price relationships.

        Business Rule: High must be >= Open, Close, Low
                      Low must be <= Open, Close, High

        Raises:
            ValueError: If OHLC prices are inconsistent
        """
        if not (
            self._high_price >= self._open_price
            and self._high_price >= self._close_price
            and self._high_price >= self._low_price
            and self._low_price <= self._open_price
            and self._low_price <= self._close_price
        ):
            raise ValueError(
                f"OHLC prices are inconsistent for {self._symbol} at {self._timestamp}: "
                f"O={self._open_price}, H={self._high_price}, L={self._low_price}, C={self._close_price}"
            )

    @property
    def symbol(self) -> Symbol:
        """Get the financial instrument symbol."""
        return self._symbol

    @property
    def timestamp(self) -> Timestamp:
        """Get the bar timestamp."""
        return self._timestamp

    @property
    def timestamp_ns(self) -> int:
        """Get the bar timestamp in nanoseconds since epoch."""
        return self._timestamp.to_nanoseconds()

    @property
    def open_price(self) -> Price:
        """Get the opening price."""
        return self._open_price

    @property
    def high_price(self) -> Price:
        """Get the highest price."""
        return self._high_price

    @property
    def low_price(self) -> Price:
        """Get the lowest price."""
        return self._low_price

    @property
    def close_price(self) -> Price:
        """Get the closing price."""
        return self._close_price

    @property
    def volume(self) -> Volume:
        """Get the trading volume."""
        return self._volume

    @property
    def trade_count(self) -> Optional[int]:
        """Get the number of trades (if available)."""
        return self._trade_count

    @property
    def vwap(self) -> Optional[Price]:
        """Get the volume-weighted average price (if available)."""
        return self._vwap

    def calculate_price_range(self) -> Price:
        """Calculate the price range (high - low).

        Returns:
            Price difference between high and low
        """
        return Price(self._high_price.value - self._low_price.value)

    def calculate_price_change(self) -> Price:
        """Calculate the price change (close - open).

        Returns:
            Price difference between close and open
        """
        return Price(self._close_price.value - self._open_price.value)

    def calculate_price_change_percentage(self) -> float:
        """Calculate the percentage price change.

        Returns:
            Percentage change from open to close
        """
        if self._open_price.value == 0:
            return 0.0

        change = self.calculate_price_change()
        return float((change.value / self._open_price.value) * 100)

    def is_same_trading_day(self, other: OHLCVBar) -> bool:
        """Check if this bar is from the same trading day as another.

        Args:
            other: Another OHLCV bar to compare

        Returns:
            True if both bars are from the same trading date
        """
        return self._timestamp.trading_date() == other._timestamp.trading_date()

    def is_during_market_hours(self) -> bool:
        """Check if this bar occurred during regular market hours.

        Returns:
            True if the bar timestamp is during market hours
        """
        return self._timestamp.is_market_hours()

    def update_trade_count(self, trade_count: int) -> None:
        """Update the trade count for this bar.

        Args:
            trade_count: Number of trades in this bar
        """
        if trade_count < 0:
            raise ValueError("Trade count cannot be negative")

        self._trade_count = trade_count
        self._increment_version()

    def update_vwap(self, vwap: Price) -> None:
        """Update the volume-weighted average price.

        Args:
            vwap: Volume-weighted average price
        """
        self._vwap = vwap
        self._increment_version()

    def __str__(self) -> str:
        """String representation of the OHLCV bar."""
        return (
            f"OHLCVBar({self._symbol} @ {self._timestamp.value.isoformat()}: "
            f"O={self._open_price}, H={self._high_price}, "
            f"L={self._low_price}, C={self._close_price}, V={self._volume})"
        )

    def __repr__(self) -> str:
        """Detailed string representation for debugging."""
        return (
            f"OHLCVBar(id={self._id}, symbol={self._symbol}, "
            f"timestamp={self._timestamp}, open={self._open_price}, "
            f"high={self._high_price}, low={self._low_price}, "
            f"close={self._close_price}, volume={self._volume}, "
            f"trade_count={self._trade_count}, vwap={self._vwap})"
        )
