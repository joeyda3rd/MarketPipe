# SPDX-License-Identifier: Apache-2.0
"""Domain value objects for MarketPipe.

Value Objects are immutable objects that are defined by their values rather
than their identity. They represent concepts from the domain that are
characterized by their attributes rather than a unique identity.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Union


@dataclass(frozen=True)
class Symbol:
    """Stock symbol value object.

    Represents a financial instrument identifier (e.g., AAPL, GOOGL).
    Enforces business rules for valid symbol format.
    """

    value: str

    def __post_init__(self):
        """Validate symbol format on creation."""
        if not self.value:
            raise ValueError("Symbol cannot be empty")

        # Normalize to uppercase
        normalized = self.value.upper().strip()
        object.__setattr__(self, "value", normalized)

        # Allow uppercase letters, digits, and an optional dot (e.g., "BRK.A"); 1-10 chars total
        if not re.match(r"^[A-Z0-9\.]{1,10}$", self.value):
            raise ValueError(
                f"Invalid symbol format: {self.value}. Must be 1-10 characters (A-Z, 0-9, or '.')"
            )

    @classmethod
    def from_string(cls, symbol_str: str) -> Symbol:
        """Create Symbol from string with normalization.

        Args:
            symbol_str: String representation of symbol

        Returns:
            Symbol value object
        """
        return cls(symbol_str.upper().strip())

    def __str__(self) -> str:
        """String representation returns the symbol value."""
        return self.value


@dataclass(frozen=True)
class Price:
    """Monetary price value object with precision handling.

    Represents a financial price with 4 decimal places precision
    to handle typical stock price requirements.
    """

    value: Decimal

    def __post_init__(self):
        """Validate and normalize price value."""
        if self.value < 0:
            raise ValueError(f"Price cannot be negative: {self.value}")

        # Quantize to 4 decimal places for financial precision
        quantized = self.value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        object.__setattr__(self, "value", quantized)

    @classmethod
    def from_float(cls, value: float) -> Price:
        """Create price from float with proper precision handling.

        Args:
            value: Float price value

        Returns:
            Price value object with proper decimal precision
        """
        return cls(Decimal(str(value)))

    @classmethod
    def from_string(cls, value: str) -> Price:
        """Create price from string representation.

        Args:
            value: String price value (e.g., "123.45")

        Returns:
            Price value object
        """
        try:
            return cls(Decimal(value))
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid price format: {value}") from e

    @classmethod
    def zero(cls) -> Price:
        """Create a zero price value."""
        return cls(Decimal("0.0000"))

    def __add__(self, other: Price) -> Price:
        """Add two prices."""
        return Price(self.value + other.value)

    def __sub__(self, other: Price) -> Price:
        """Subtract two prices."""
        return Price(self.value - other.value)

    def __mul__(self, other: Union[Price, Decimal, int, float]) -> Price:
        """Multiply price by number or another price."""
        if isinstance(other, Price):
            return Price(self.value * other.value)
        return Price(self.value * Decimal(str(other)))

    def __truediv__(self, other: Union[Price, Decimal, int, float]) -> Price:
        """Divide price by number or another price."""
        if isinstance(other, Price):
            if other.value == 0:
                raise ValueError("Cannot divide by zero price")
            return Price(self.value / other.value)

        divisor = Decimal(str(other))
        if divisor == 0:
            raise ValueError("Cannot divide by zero")
        return Price(self.value / divisor)

    def __lt__(self, other: Price) -> bool:
        """Compare if this price is less than another."""
        return self.value < other.value

    def __le__(self, other: Price) -> bool:
        """Compare if this price is less than or equal to another."""
        return self.value <= other.value

    def __gt__(self, other: Price) -> bool:
        """Compare if this price is greater than another."""
        return self.value > other.value

    def __ge__(self, other: Price) -> bool:
        """Compare if this price is greater than or equal to another."""
        return self.value >= other.value

    def to_float(self) -> float:
        """Convert to float (use with caution due to precision loss)."""
        return float(self.value)

    def __str__(self) -> str:
        """String representation with dollar sign."""
        return f"${self.value}"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"Price({self.value})"


@dataclass(frozen=True)
class Timestamp:
    """Timestamp value object with timezone awareness.

    Represents a specific point in time, always with timezone information.
    Provides utilities for financial market time calculations.
    """

    value: datetime

    def __post_init__(self):
        """Ensure timestamp has timezone information."""
        if self.value.tzinfo is None:
            # Assume UTC if no timezone provided
            utc_dt = self.value.replace(tzinfo=timezone.utc)
            object.__setattr__(self, "value", utc_dt)

    @classmethod
    def now(cls) -> Timestamp:
        """Create timestamp for current time in UTC."""
        return cls(datetime.now(timezone.utc))

    @classmethod
    def from_iso(cls, iso_string: str) -> Timestamp:
        """Create timestamp from ISO 8601 string.

        Args:
            iso_string: ISO format timestamp string

        Returns:
            Timestamp value object
        """
        try:
            dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
            return cls(dt)
        except ValueError as e:
            raise ValueError(f"Invalid ISO timestamp format: {iso_string}") from e

    @classmethod
    def from_unix_timestamp(cls, unix_timestamp: float) -> Timestamp:
        """Create timestamp from Unix timestamp.

        Args:
            unix_timestamp: Seconds since Unix epoch

        Returns:
            Timestamp value object
        """
        dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
        return cls(dt)

    @classmethod
    def from_nanoseconds(cls, nanoseconds: int) -> Timestamp:
        """Create timestamp from nanoseconds since epoch (Arrow format).

        Args:
            nanoseconds: Nanoseconds since Unix epoch

        Returns:
            Timestamp value object
        """
        seconds = nanoseconds / 1_000_000_000
        return cls.from_unix_timestamp(seconds)

    def trading_date(self) -> date:
        """Get the trading date (useful for partitioning).

        Returns:
            Date in the timestamp's timezone
        """
        return self.value.date()

    def to_nanoseconds(self) -> int:
        """Convert to nanoseconds since epoch (Arrow/Parquet format).

        Returns:
            Nanoseconds since Unix epoch
        """
        return int(self.value.timestamp() * 1_000_000_000)

    def to_unix_timestamp(self) -> float:
        """Convert to Unix timestamp (seconds since epoch).

        Returns:
            Seconds since Unix epoch
        """
        return self.value.timestamp()

    def is_market_hours(self) -> bool:
        """Check if timestamp is during regular US market hours.

        Note: This is a simplified implementation.
        Real implementation would consider market holidays, etc.

        Returns:
            True if during regular trading hours (9:30 AM - 4:00 PM ET)
        """
        # Convert to Eastern Time (simplified - doesn't handle DST properly)
        # Real implementation would use proper timezone handling
        et_hour = (self.value.hour - 5) % 24  # Rough ET conversion
        return 9 <= et_hour < 16

    def is_same_minute(self, other: Timestamp) -> bool:
        """Check if this timestamp is in the same minute as another.

        Args:
            other: Another timestamp to compare

        Returns:
            True if both timestamps are in the same minute
        """
        return self.value.replace(second=0, microsecond=0) == other.value.replace(
            second=0, microsecond=0
        )

    def round_to_minute(self) -> Timestamp:
        """Round timestamp down to the nearest minute boundary.

        Returns:
            Timestamp rounded to minute boundary
        """
        rounded = self.value.replace(second=0, microsecond=0)
        return Timestamp(rounded)

    def __lt__(self, other: Timestamp) -> bool:
        """Compare if this timestamp is less than another."""
        return self.value < other.value

    def __le__(self, other: Timestamp) -> bool:
        """Compare if this timestamp is less than or equal to another."""
        return self.value <= other.value

    def __gt__(self, other: Timestamp) -> bool:
        """Compare if this timestamp is greater than another."""
        return self.value > other.value

    def __ge__(self, other: Timestamp) -> bool:
        """Compare if this timestamp is greater than or equal to another."""
        return self.value >= other.value

    def __eq__(self, other: object) -> bool:
        """Compare if this timestamp equals another."""
        return isinstance(other, Timestamp) and self.value == other.value

    def __hash__(self) -> int:
        """Hash based on timestamp value."""
        return hash(self.value)

    def __str__(self) -> str:
        """ISO format string representation."""
        return self.value.isoformat()

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"Timestamp({self.value.isoformat()})"


@dataclass(frozen=True)
class Volume:
    """Trading volume value object.

    Represents the number of shares traded, ensuring non-negative values.
    """

    value: int

    def __post_init__(self):
        """Validate volume value."""
        if self.value < 0:
            raise ValueError(f"Volume cannot be negative: {self.value}")

    @classmethod
    def zero(cls) -> Volume:
        """Create zero volume."""
        return cls(0)

    def __add__(self, other: Volume) -> Volume:
        """Add two volumes."""
        return Volume(self.value + other.value)

    def __sub__(self, other: Volume) -> Volume:
        """Subtract volumes (result cannot be negative)."""
        result = self.value - other.value
        if result < 0:
            raise ValueError("Volume subtraction cannot result in negative value")
        return Volume(result)

    def __mul__(self, factor: Union[int, float]) -> Volume:
        """Multiply volume by a factor."""
        result = int(self.value * factor)
        if result < 0:
            raise ValueError("Volume multiplication cannot result in negative value")
        return Volume(result)

    def __str__(self) -> str:
        """String representation with comma formatting for readability."""
        return f"{self.value:,}"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"Volume({self.value})"


@dataclass(frozen=True)
class TimeRange:
    """Time range value object representing a period between two timestamps.

    Useful for specifying data collection periods, query ranges, etc.
    """

    start: Timestamp
    end: Timestamp

    def __post_init__(self):
        """Validate that start is before end."""
        if self.start.value >= self.end.value:
            raise ValueError(f"Start time {self.start} must be before end time {self.end}")

    @classmethod
    def from_dates(cls, start_date: date, end_date: date) -> TimeRange:
        """Create time range from dates (assumes start of day to start of next day).

        Args:
            start_date: Start date (inclusive)
            end_date: End date (exclusive)

        Returns:
            TimeRange covering the specified dates
        """
        start_dt = datetime.combine(start_date, datetime.min.time(), timezone.utc)
        end_dt = datetime.combine(end_date, datetime.min.time(), timezone.utc)

        return cls(Timestamp(start_dt), Timestamp(end_dt))

    @classmethod
    def single_day(cls, trading_date: date) -> TimeRange:
        """Create time range for a single trading day.

        Args:
            trading_date: The trading date

        Returns:
            TimeRange covering the entire trading day
        """
        start_dt = datetime.combine(trading_date, datetime.min.time(), timezone.utc)
        end_dt = datetime.combine(trading_date, datetime.max.time(), timezone.utc)

        return cls(Timestamp(start_dt), Timestamp(end_dt))

    def duration_seconds(self) -> float:
        """Calculate duration in seconds.

        Returns:
            Number of seconds in the time range
        """
        return (self.end.value - self.start.value).total_seconds()

    def contains(self, timestamp: Timestamp) -> bool:
        """Check if a timestamp falls within this range (inclusive start, exclusive end).

        Args:
            timestamp: Timestamp to check

        Returns:
            True if timestamp is within the range
        """
        return self.start.value <= timestamp.value < self.end.value

    def overlaps(self, other: TimeRange) -> bool:
        """Check if this range overlaps with another range.

        Args:
            other: Another time range

        Returns:
            True if the ranges overlap
        """
        return self.start.value < other.end.value and self.end.value > other.start.value

    def __str__(self) -> str:
        """String representation of the time range."""
        return f"{self.start} to {self.end}"

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"TimeRange(start={self.start}, end={self.end})"
