"""Fake adapter implementations for testing."""

from __future__ import annotations

from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from decimal import Decimal

from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Symbol, Price, Timestamp, Volume, TimeRange
from marketpipe.domain.market_data import IMarketDataProvider, ProviderMetadata


class FakeMarketDataAdapter(IMarketDataProvider):
    """Fake market data adapter for testing."""
    
    def __init__(self, provider_name: str = "fake"):
        self.provider_name = provider_name
        self._bars_data: Dict[Symbol, List[OHLCVBar]] = {}
        self._fetch_calls: List[tuple[Symbol, TimeRange]] = []
        self._should_fail = False
        self._failure_message = "Simulated provider failure"
        self._connection_working = True
        self._supported_symbols = [
            Symbol.from_string("AAPL"),
            Symbol.from_string("GOOGL"),
            Symbol.from_string("MSFT")
        ]
    
    def set_bars_data(self, symbol: Symbol, bars: List[OHLCVBar]) -> None:
        """Set the bars data that will be returned for a symbol."""
        self._bars_data[symbol] = bars
    
    def set_failure_mode(self, should_fail: bool, message: str = "Simulated failure") -> None:
        """Configure the adapter to simulate failures."""
        self._should_fail = should_fail
        self._failure_message = message
    
    def set_connection_status(self, is_working: bool) -> None:
        """Configure the connection status for testing."""
        self._connection_working = is_working
    
    def set_supported_symbols(self, symbols: List[Symbol]) -> None:
        """Set the list of symbols supported by this provider."""
        self._supported_symbols = symbols
    
    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """Fetch OHLCV bars for a symbol within a time range."""
        # Record the call for testing
        self._fetch_calls.append((symbol, time_range))
        
        if self._should_fail:
            raise Exception(self._failure_message)
        
        # Return configured test data
        bars = self._bars_data.get(symbol, [])
        
        # Filter by timestamp range
        filtered_bars = []
        for bar in bars:
            if time_range.contains(bar.timestamp):
                filtered_bars.append(bar)
        
        return filtered_bars[:max_bars]
    
    async def get_supported_symbols(self) -> List[Symbol]:
        """Get list of symbols supported by this provider."""
        return self._supported_symbols.copy()
    
    async def is_available(self) -> bool:
        """Test if the provider is currently available."""
        return self._connection_working
    
    def get_provider_metadata(self) -> ProviderMetadata:
        """Get metadata about this provider's capabilities."""
        return ProviderMetadata(
            provider_name=self.provider_name,
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=None,
            minimum_time_resolution="1m",
            maximum_history_days=365
        )
    
    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about the provider (legacy method for compatibility)."""
        return {
            "provider": self.provider_name,
            "type": "fake",
            "supports_real_time": True,
            "supports_historical": True,
            "rate_limit_per_min": None
        }
    
    # Legacy method for backward compatibility
    async def fetch_bars(
        self,
        symbol: Symbol,
        start_timestamp: int,
        end_timestamp: int,
        batch_size: int = 1000
    ) -> List[OHLCVBar]:
        """Legacy method for backward compatibility."""
        start_ts = Timestamp.from_nanoseconds(start_timestamp)
        end_ts = Timestamp.from_nanoseconds(end_timestamp)
        time_range = TimeRange(start_ts, end_ts)
        return await self.fetch_bars_for_symbol(symbol, time_range, batch_size)
    
    async def test_connection(self) -> bool:
        """Legacy method for backward compatibility."""
        return await self.is_available()
    
    # Test helpers
    def get_fetch_calls(self) -> List[tuple[Symbol, TimeRange]]:
        """Get list of fetch calls made (for testing)."""
        return self._fetch_calls.copy()
    
    def clear_fetch_calls(self) -> None:
        """Clear the list of fetch calls (for testing)."""
        self._fetch_calls.clear()
    
    def get_configured_symbols(self) -> List[Symbol]:
        """Get list of symbols that have configured data (for testing)."""
        return list(self._bars_data.keys())


def create_test_ohlcv_bars(
    symbol: Symbol, 
    count: int = 10, 
    start_time: Optional[datetime] = None
) -> List[OHLCVBar]:
    """Create test OHLCV bars for testing purposes."""
    if start_time is None:
        start_time = datetime(2023, 1, 2, 13, 30, tzinfo=timezone.utc)
    
    bars = []
    for i in range(count):
        timestamp = start_time.replace(minute=start_time.minute + i)
        
        bar = OHLCVBar(
            id=EntityId.generate(),
            symbol=symbol,
            timestamp=Timestamp(timestamp),
            open_price=Price(Decimal("100.00")),
            high_price=Price(Decimal("101.00")),
            low_price=Price(Decimal("99.00")),
            close_price=Price(Decimal("100.50")),
            volume=Volume(1000 + i * 10)
        )
        bars.append(bar)
    
    return bars


def create_test_ohlcv_bar_dict(
    symbol: str,
    timestamp: datetime,
    open_price: float = 100.0,
    high_price: float = 101.0,
    low_price: float = 99.0,
    close_price: float = 100.5,
    volume: int = 1000
) -> Dict[str, Any]:
    """Create test OHLCV bar dictionary in the format expected by legacy tests."""
    timestamp_ns = int(timestamp.timestamp() * 1_000_000_000)
    
    return {
        "symbol": symbol,
        "timestamp": timestamp_ns,
        "date": timestamp.date(),
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
        "trade_count": 1,
        "vwap": None,
        "session": "regular",
        "currency": "USD",
        "status": "ok",
        "source": "alpaca",
        "frame": "1m",
        "schema_version": 1,
    }