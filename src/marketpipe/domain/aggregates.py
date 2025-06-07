"""Domain aggregates for MarketPipe.

Aggregates are consistency boundaries that group related entities and value objects.
They ensure business invariants are maintained and provide a clear interface
for operations that span multiple domain objects.
"""

from __future__ import annotations

from typing import List, Optional, Dict, Iterator
from datetime import date
from collections import defaultdict

from .entities import OHLCVBar, EntityId
from .value_objects import Symbol, Timestamp, Price, Volume, TimeRange
from .events import DomainEvent, BarCollectionCompleted, BarCollectionStarted


class SymbolBarsAggregate:
    """Aggregate root for managing OHLCV bars for a single symbol on a trading day.
    
    This aggregate ensures consistency for all bars related to a specific symbol
    and trading date. It enforces business rules such as no duplicate timestamps
    and provides operations for calculating daily summaries.
    """
    
    def __init__(self, symbol: Symbol, trading_date: date):
        self._symbol = symbol
        self._trading_date = trading_date
        self._bars: Dict[Timestamp, OHLCVBar] = {}
        self._events: List[DomainEvent] = []
        self._version = 1
        self._is_complete = False
        self._collection_started = False
    
    @property
    def symbol(self) -> Symbol:
        """Get the symbol for this aggregate."""
        return self._symbol
    
    @property
    def trading_date(self) -> date:
        """Get the trading date for this aggregate."""
        return self._trading_date
    
    @property
    def bar_count(self) -> int:
        """Get the total number of bars in this aggregate."""
        return len(self._bars)
    
    @property
    def is_complete(self) -> bool:
        """Check if the bar collection is marked as complete."""
        return self._is_complete
    
    @property
    def version(self) -> int:
        """Get the aggregate version for optimistic concurrency control."""
        return self._version
    
    def start_collection(self) -> None:
        """Mark the start of bar collection for this symbol/date.
        
        Raises:
            ValueError: If collection has already been started
        """
        if self._collection_started:
            raise ValueError(f"Collection already started for {self._symbol} on {self._trading_date}")
        
        self._collection_started = True
        self._version += 1
        
        # Raise domain event
        event = BarCollectionStarted(
            symbol=self._symbol,
            trading_date=self._trading_date
        )
        self._events.append(event)
    
    def add_bar(self, bar: OHLCVBar) -> None:
        """Add a bar to the collection with validation.
        
        Args:
            bar: OHLCV bar to add
            
        Raises:
            ValueError: If bar violates aggregate invariants
        """
        # Enforce aggregate invariants
        if bar.symbol != self._symbol:
            raise ValueError(
                f"Bar symbol {bar.symbol} doesn't match aggregate symbol {self._symbol}"
            )
        
        if bar.timestamp.trading_date() != self._trading_date:
            raise ValueError(
                f"Bar date {bar.timestamp.trading_date()} doesn't match aggregate date {self._trading_date}"
            )
        
        if self._is_complete:
            raise ValueError(
                f"Cannot add bars to completed collection for {self._symbol} on {self._trading_date}"
            )
        
        # Check for duplicate timestamps
        if bar.timestamp in self._bars:
            existing_bar = self._bars[bar.timestamp]
            raise ValueError(
                f"Bar already exists for {self._symbol} at {bar.timestamp}. "
                f"Existing: {existing_bar}, New: {bar}"
            )
        
        # Start collection if not already started
        if not self._collection_started:
            self.start_collection()
        
        # Add the bar
        self._bars[bar.timestamp] = bar
        self._version += 1
    
    def get_bar(self, timestamp: Timestamp) -> Optional[OHLCVBar]:
        """Retrieve a specific bar by timestamp.
        
        Args:
            timestamp: Timestamp to look up
            
        Returns:
            OHLCVBar if found, None otherwise
        """
        return self._bars.get(timestamp)
    
    def get_all_bars(self) -> List[OHLCVBar]:
        """Get all bars sorted by timestamp.
        
        Returns:
            List of bars sorted chronologically
        """
        return sorted(self._bars.values(), key=lambda b: b.timestamp.value)
    
    def get_bars_in_range(self, time_range: TimeRange) -> List[OHLCVBar]:
        """Get bars within a specific time range.
        
        Args:
            time_range: Time range to filter by
            
        Returns:
            List of bars within the specified range
        """
        return [
            bar for bar in self._bars.values()
            if time_range.contains(bar.timestamp)
        ]
    
    def has_gaps(self) -> bool:
        """Check if there are time gaps in the minute-by-minute data.
        
        Returns:
            True if there are missing minutes in the trading day
        """
        if len(self._bars) < 2:
            return False
        
        sorted_bars = self.get_all_bars()
        expected_minutes = 390  # 6.5 hours * 60 minutes (regular trading hours)
        
        # Simple gap detection - in reality would need market calendar
        return len(sorted_bars) < expected_minutes
    
    def complete_collection(self) -> None:
        """Mark the bar collection as complete and raise domain event.
        
        Raises:
            ValueError: If collection hasn't been started
        """
        if not self._collection_started:
            raise ValueError(f"Cannot complete collection that hasn't been started")
        
        if self._is_complete:
            return  # Already complete
        
        self._is_complete = True
        self._version += 1
        
        # Raise domain event
        event = BarCollectionCompleted(
            symbol=self._symbol,
            trading_date=self._trading_date,
            bar_count=self.bar_count,
            has_gaps=self.has_gaps()
        )
        self._events.append(event)
    
    def calculate_daily_summary(self) -> DailySummary:
        """Calculate daily OHLCV summary from minute bars.
        
        Returns:
            Daily summary calculated from all bars
            
        Raises:
            ValueError: If no bars available for calculation
        """
        if not self._bars:
            raise ValueError(f"Cannot calculate summary with no bars for {self._symbol}")
        
        sorted_bars = self.get_all_bars()
        first_bar = sorted_bars[0]
        last_bar = sorted_bars[-1]
        
        # Calculate daily values
        daily_high = max(bar.high_price for bar in sorted_bars)
        daily_low = min(bar.low_price for bar in sorted_bars)
        daily_volume = sum(bar.volume for bar in sorted_bars)
        
        # Calculate VWAP if available
        total_value = Price.zero()
        total_volume = Volume.zero()
        
        for bar in sorted_bars:
            if bar.vwap is not None:
                value = bar.vwap * bar.volume.value
                total_value = total_value + value
                total_volume = total_volume + bar.volume
        
        daily_vwap = None
        if total_volume.value > 0:
            daily_vwap = total_value / total_volume.value
        
        return DailySummary(
            symbol=self._symbol,
            trading_date=self._trading_date,
            open_price=first_bar.open_price,
            high_price=daily_high,
            low_price=daily_low,
            close_price=last_bar.close_price,
            volume=daily_volume,
            vwap=daily_vwap,
            bar_count=len(sorted_bars),
            first_bar_time=first_bar.timestamp,
            last_bar_time=last_bar.timestamp
        )
    
    def get_uncommitted_events(self) -> List[DomainEvent]:
        """Get domain events that haven't been published.
        
        Returns:
            List of unpublished domain events
        """
        return self._events.copy()
    
    def mark_events_committed(self) -> None:
        """Mark all events as committed (published).
        
        This should be called after events have been successfully published
        to external systems.
        """
        self._events.clear()
    
    def __str__(self) -> str:
        """String representation of the aggregate."""
        status = "complete" if self._is_complete else "in progress"
        return f"SymbolBarsAggregate({self._symbol} on {self._trading_date}: {self.bar_count} bars, {status})"
    
    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (f"SymbolBarsAggregate(symbol={self._symbol}, trading_date={self._trading_date}, "
                f"bar_count={self.bar_count}, is_complete={self._is_complete}, version={self._version})")


class UniverseAggregate:
    """Aggregate for managing the universe of symbols being tracked.
    
    This aggregate manages the set of financial instruments (symbols) that
    are actively being monitored and processed by the system.
    """
    
    def __init__(self, universe_id: str):
        self._universe_id = universe_id
        self._symbols: Dict[str, Symbol] = {}
        self._active_symbols: set[Symbol] = set()
        self._events: List[DomainEvent] = []
        self._version = 1
    
    @property
    def universe_id(self) -> str:
        """Get the universe identifier."""
        return self._universe_id
    
    @property
    def symbol_count(self) -> int:
        """Get total number of symbols in universe."""
        return len(self._symbols)
    
    @property
    def active_symbol_count(self) -> int:
        """Get number of active symbols."""
        return len(self._active_symbols)
    
    def add_symbol(self, symbol: Symbol) -> None:
        """Add a symbol to the universe.
        
        Args:
            symbol: Symbol to add
        """
        if symbol.value in self._symbols:
            return  # Already exists
        
        self._symbols[symbol.value] = symbol
        self._active_symbols.add(symbol)
        self._version += 1
    
    def remove_symbol(self, symbol: Symbol) -> None:
        """Remove a symbol from the universe.
        
        Args:
            symbol: Symbol to remove
            
        Raises:
            ValueError: If symbol not in universe
        """
        if symbol.value not in self._symbols:
            raise ValueError(f"Symbol {symbol} not in universe")
        
        del self._symbols[symbol.value]
        self._active_symbols.discard(symbol)
        self._version += 1
    
    def activate_symbol(self, symbol: Symbol) -> None:
        """Mark a symbol as active for processing.
        
        Args:
            symbol: Symbol to activate
            
        Raises:
            ValueError: If symbol not in universe
        """
        if symbol.value not in self._symbols:
            raise ValueError(f"Symbol {symbol} not in universe")
        
        self._active_symbols.add(symbol)
        self._version += 1
    
    def deactivate_symbol(self, symbol: Symbol) -> None:
        """Mark a symbol as inactive (stop processing).
        
        Args:
            symbol: Symbol to deactivate
        """
        self._active_symbols.discard(symbol)
        self._version += 1
    
    def get_active_symbols(self) -> List[Symbol]:
        """Get list of active symbols.
        
        Returns:
            List of active symbols sorted alphabetically
        """
        return sorted(self._active_symbols, key=lambda s: s.value)
    
    def get_all_symbols(self) -> List[Symbol]:
        """Get list of all symbols in universe.
        
        Returns:
            List of all symbols sorted alphabetically
        """
        return sorted(self._symbols.values(), key=lambda s: s.value)
    
    def is_symbol_active(self, symbol: Symbol) -> bool:
        """Check if a symbol is active.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            True if symbol is active
        """
        return symbol in self._active_symbols


@dataclass(frozen=True)
class DailySummary:
    """Value object representing daily OHLCV summary data.
    
    Calculated from minute-level bars to provide daily aggregates.
    """
    symbol: Symbol
    trading_date: date
    open_price: Price
    high_price: Price
    low_price: Price
    close_price: Price
    volume: Volume
    vwap: Optional[Price]
    bar_count: int
    first_bar_time: Timestamp
    last_bar_time: Timestamp
    
    def calculate_price_change(self) -> Price:
        """Calculate daily price change."""
        return Price(self.close_price.value - self.open_price.value)
    
    def calculate_price_change_percentage(self) -> float:
        """Calculate daily price change percentage."""
        if self.open_price.value == 0:
            return 0.0
        change = self.calculate_price_change()
        return float((change.value / self.open_price.value) * 100)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "symbol": str(self.symbol),
            "trading_date": self.trading_date.isoformat(),
            "open": float(self.open_price.value),
            "high": float(self.high_price.value),
            "low": float(self.low_price.value),
            "close": float(self.close_price.value),
            "volume": self.volume.value,
            "vwap": float(self.vwap.value) if self.vwap else None,
            "bar_count": self.bar_count,
            "first_bar_time": str(self.first_bar_time),
            "last_bar_time": str(self.last_bar_time),
            "price_change": float(self.calculate_price_change().value),
            "price_change_percentage": self.calculate_price_change_percentage(),
        }