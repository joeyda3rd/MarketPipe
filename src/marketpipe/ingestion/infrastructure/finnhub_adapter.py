# SPDX-License-Identifier: Apache-2.0
"""Finnhub market data provider adapter."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, AsyncIterator

import httpx

from marketpipe.domain.entities import OHLCVBar, EntityId
from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp, Price, Volume
from marketpipe.domain.market_data import IMarketDataProvider, ProviderMetadata
from .provider_registry import provider


@provider("finnhub")
class FinnhubMarketDataAdapter(IMarketDataProvider):
    """
    Anti-corruption layer for Finnhub API integration.
    
    Implements the complete Finnhub REST API for historical OHLC data.
    Free tier: 60 requests per minute, paid tier: unlimited.
    
    API Documentation: https://finnhub.io/docs/api/stock-candles
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://finnhub.io/api/v1",
        rate_limit_per_minute: int = 60,  # Free tier limit
        timeout: float = 30.0,
        max_retries: int = 3,
        logger: Optional[logging.Logger] = None,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.rate_limit_per_minute = rate_limit_per_minute
        self.timeout = timeout
        self.max_retries = max_retries
        self.log = logger or logging.getLogger(self.__class__.__name__)
        
        # Rate limiting state
        self._request_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()
        
        self.log.info(f"Finnhub adapter initialized with {rate_limit_per_minute} requests/min limit")

    async def fetch_bars_for_symbol(
        self,
        symbol: Symbol,
        time_range: TimeRange,
        max_bars: int = 1000,
    ) -> List[OHLCVBar]:
        """
        Fetch OHLCV bars from Finnhub API.
        
        Args:
            symbol: Stock symbol (e.g., AAPL)
            time_range: Time range for data retrieval
            max_bars: Maximum number of bars to fetch
            
        Returns:
            List of OHLCV bars
        """
        # Default to daily timeframe for interface compatibility
        timeframe = "1d"
        self.log.info(f"Fetching {timeframe} bars for {symbol.value} from {time_range.start} to {time_range.end}")
        
        # Parse timeframe for Finnhub
        resolution = self._parse_timeframe(timeframe)
        
        # Convert dates to Unix timestamps
        from_timestamp = int(time_range.start.value.timestamp())
        to_timestamp = int(time_range.end.value.timestamp())
        
        # Apply rate limiting
        await self._apply_rate_limit()
        
        # Build request URL and parameters
        url = f"{self.base_url}/stock/candle"
        params = {
            "symbol": symbol.value,
            "resolution": resolution,
            "from": from_timestamp,
            "to": to_timestamp,
            "token": self.api_key,
        }
        
        try:
            # Make HTTP request
            response_data = await self._make_request(url, params)
            
            # Parse response
            if response_data.get("s") == "ok" and "c" in response_data:
                bars = self._parse_finnhub_response(response_data, symbol, timeframe)
                # Respect max_bars parameter
                if len(bars) > max_bars:
                    bars = bars[:max_bars]
                self.log.info(f"Successfully fetched {len(bars)} bars for {symbol.value}")
                return bars
            elif response_data.get("s") == "no_data":
                self.log.warning(f"No data available for {symbol.value} in the specified time range")
                return []
            else:
                error_msg = f"Finnhub API returned status: {response_data.get('s', 'unknown')}"
                self.log.error(error_msg)
                raise ValueError(error_msg)
                
        except Exception as e:
            self.log.error(f"Failed to fetch data for {symbol.value}: {e}")
            raise

    async def get_supported_symbols(self) -> List[Symbol]:
        """Get list of supported US stock symbols from Finnhub."""
        try:
            await self._apply_rate_limit()
            
            url = f"{self.base_url}/stock/symbol"
            params = {"exchange": "US", "token": self.api_key}
            
            response_data = await self._make_request(url, params)
            
            symbols = []
            for stock_info in response_data:
                if "symbol" in stock_info:
                    symbols.append(Symbol.from_string(stock_info["symbol"]))
            
            self.log.info(f"Retrieved {len(symbols)} supported symbols from Finnhub")
            return symbols
            
        except Exception as e:
            self.log.error(f"Failed to get supported symbols: {e}")
            # Return common symbols as fallback
            common_symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX"]
            return [Symbol.from_string(s) for s in common_symbols]

    async def is_available(self) -> bool:
        """Test if the provider is currently available."""
        try:
            # Test with a simple quote request
            url = f"{self.base_url}/quote"
            params = {"symbol": "AAPL", "token": self.api_key}
            
            await self._apply_rate_limit()
            response_data = await self._make_request(url, params)
            
            # Check if we got valid quote data
            if "c" in response_data and response_data["c"] is not None:
                self.log.info("Finnhub connection validated successfully")
                return True
            else:
                self.log.error(f"Finnhub validation failed: {response_data}")
                return False
                
        except Exception as e:
            self.log.error(f"Finnhub connection validation failed: {e}")
            return False

    def get_provider_metadata(self) -> ProviderMetadata:
        """Get provider metadata."""
        return ProviderMetadata(
            provider_name="finnhub",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=self.rate_limit_per_minute,
            minimum_time_resolution="1m",
            maximum_history_days=365,  # Finnhub historical data availability
        )

    async def _apply_rate_limit(self) -> None:
        """Apply rate limiting based on free tier limits."""
        async with self._rate_limit_lock:
            now = time.time()
            
            # Remove requests older than 1 minute
            cutoff = now - 60.0
            self._request_times = [t for t in self._request_times if t > cutoff]
            
            # Check if we need to wait
            if len(self._request_times) >= self.rate_limit_per_minute:
                # Calculate wait time
                oldest_request = min(self._request_times)
                wait_time = 60.0 - (now - oldest_request)
                
                if wait_time > 0:
                    self.log.info(f"Rate limit reached, waiting {wait_time:.1f} seconds")
                    await asyncio.sleep(wait_time)
            
            # Record this request
            self._request_times.append(now)

    async def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        headers = {
            "User-Agent": "MarketPipe/1.0 (Finnhub.io Adapter)",
            "Accept": "application/json",
        }
        
        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(url, params=params, headers=headers)
                    
                    # Handle rate limiting
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        self.log.warning(f"Rate limited, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        continue
                    
                    # Handle authentication errors
                    if response.status_code == 401:
                        raise ValueError("Invalid Finnhub API key")
                    
                    # Handle forbidden
                    if response.status_code == 403:
                        raise ValueError("Finnhub API access forbidden - check subscription")
                    
                    # Handle other client errors
                    if response.status_code >= 400:
                        error_text = response.text
                        self.log.error(f"Finnhub API error {response.status_code}: {error_text}")
                        response.raise_for_status()
                    
                    # Parse JSON response
                    data = response.json()
                    return data
                    
            except httpx.TimeoutException:
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    self.log.warning(f"Request timeout, retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    self.log.warning(f"Request error: {e}, retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
        
        raise RuntimeError(f"Failed to complete request after {self.max_retries + 1} attempts")

    def _parse_timeframe(self, timeframe: str) -> str:
        """Parse timeframe into Finnhub resolution format."""
        timeframe_map = {
            "1m": "1",
            "5m": "5",
            "15m": "15",
            "30m": "30",
            "1h": "60",
            "1d": "D",
            "1w": "W",
            "1M": "M",
        }
        
        if timeframe not in timeframe_map:
            raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {list(timeframe_map.keys())}")
        
        return timeframe_map[timeframe]

    def _parse_finnhub_response(self, response_data: Dict[str, Any], symbol: Symbol, timeframe: str) -> List[OHLCVBar]:
        """Parse Finnhub API response into OHLCVBar objects."""
        bars = []
        
        # Finnhub response format:
        # {
        #   "c": [close_prices],
        #   "h": [high_prices],
        #   "l": [low_prices],
        #   "o": [open_prices],
        #   "s": "ok",
        #   "t": [timestamps],
        #   "v": [volumes]
        # }
        
        try:
            closes = response_data.get("c", [])
            highs = response_data.get("h", [])
            lows = response_data.get("l", [])
            opens = response_data.get("o", [])
            timestamps = response_data.get("t", [])
            volumes = response_data.get("v", [])
            
            # Ensure all arrays have the same length
            min_length = min(len(closes), len(highs), len(lows), len(opens), len(timestamps), len(volumes))
            
            for i in range(min_length):
                try:
                    # Convert Unix timestamp to datetime
                    timestamp_dt = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
                    
                    bar = OHLCVBar(
                        id=EntityId.generate(),
                        symbol=symbol,
                        timestamp=Timestamp(timestamp_dt),
                        open_price=Price.from_float(float(opens[i])),
                        high_price=Price.from_float(float(highs[i])),
                        low_price=Price.from_float(float(lows[i])),
                        close_price=Price.from_float(float(closes[i])),
                        volume=Volume(int(volumes[i])),
                        trade_count=None,  # Finnhub doesn't provide trade count
                        vwap=None,         # Finnhub doesn't provide VWAP
                    )
                    
                    bars.append(bar)
                    
                except (ValueError, TypeError, IndexError) as e:
                    self.log.warning(f"Failed to parse bar data for {symbol.value} at index {i}: {e}")
                    continue
                    
        except Exception as e:
            self.log.error(f"Failed to parse Finnhub response for {symbol.value}: {e}")
            raise
        
        return bars

    def __str__(self) -> str:
        return f"FinnhubMarketDataAdapter(rate_limit={self.rate_limit_per_minute}/min)" 