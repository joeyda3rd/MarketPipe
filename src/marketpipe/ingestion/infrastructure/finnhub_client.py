# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import random
import time
from typing import Any, Dict, List, Mapping, Optional, AsyncIterator
from datetime import datetime, timedelta, timezone
import pandas as pd

import httpx

from marketpipe.metrics import REQUESTS, ERRORS, LATENCY
from marketpipe.security.mask import safe_for_log
from .base_api_client import BaseApiClient
from .models import ClientConfig
from .auth import AuthStrategy


class FinnhubClient(BaseApiClient):
    """
    Finnhub API client with improvements based on latest API documentation.
    
    Enhancements:
    - Date range chunking for requests > 1 month
    - Dual rate limiting: 60 req/min + 30 req/sec
    - Header-based authentication (X-Finnhub-Token)
    - Proper status handling (ok/no_data/error)
    - Retry-After header support for 429 responses
    - Symbol preservation in response bars
    """

    _PATH_TEMPLATE = "/stock/candle"
    _RESOLUTION = "1"  # 1-minute bars
    _MAX_DAYS_PER_REQUEST = 30  # Finnhub intraday candle limit

    def __init__(self, *args, **kwargs):
        """Initialize FinnhubClient."""
        super().__init__(*args, **kwargs)
        self._current_symbol: Optional[str] = None  # Track current symbol for parsing
        self.log.info("FinnhubClient initialized with enhanced API compliance")

    # ---------- URL helpers ----------
    def endpoint_path(self) -> str:
        return self._PATH_TEMPLATE

    def build_request_params(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        cursor: Optional[str] = None,
    ) -> Mapping[str, str]:
        """
        Build Finnhub-specific query parameters.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL", "BRK.B")
            start_ts: Start timestamp in nanoseconds
            end_ts: End timestamp in nanoseconds
            cursor: Not used by Finnhub (pagination not supported)
        
        Returns:
            Query parameters for Finnhub API
        """
        # Convert nanoseconds to Unix seconds for Finnhub
        start_unix = int(start_ts // 1_000_000_000)
        end_unix = int(end_ts // 1_000_000_000)
        
        # Handle dot notation symbols (e.g., BRK.B) - Finnhub accepts as-is
        finnhub_symbol = symbol  # No conversion needed, preserve dots
        
        return {
            "symbol": finnhub_symbol,
            "resolution": self._RESOLUTION,
            "from": str(start_unix),
            "to": str(end_unix),
            # Note: token now goes in header, not query param
        }

    def next_cursor(self, raw_json: Dict[str, Any]) -> Optional[str]:
        """
        Extract pagination cursor from Finnhub response.
        
        Finnhub doesn't support pagination for candle data,
        so this always returns None.
        """
        return None

    # ---------- sync request ----------
    def _request(self, params: Mapping[str, str]) -> Dict[str, Any]:
        if self.rate_limiter:
            self.rate_limiter.acquire()

        url = f"{self.config.base_url}{self._PATH_TEMPLATE}"
        headers = {"Accept": "application/json", "User-Agent": self.config.user_agent, "X-Finnhub-Token": self.config.api_key}

        retries = 0
        while True:
            start = time.perf_counter()
            try:
                r = httpx.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.config.timeout,
                )
                duration = time.perf_counter() - start
                LATENCY.labels(source="finnhub", provider="finnhub", feed="standard").observe(duration)
                REQUESTS.labels(source="finnhub", provider="finnhub", feed="standard").inc()
                if r.status_code >= 400:
                    ERRORS.labels(source="finnhub", provider="finnhub", feed="standard", code=str(r.status_code)).inc()

                # Enhanced retry-after handling for 429 responses
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    if retry_after:
                        try:
                            sleep_time = float(retry_after)
                            self.log.warning(f"Finnhub rate limit hit, Retry-After: {sleep_time}s")
                        except ValueError:
                            sleep_time = 2.0  # Default fallback
                    else:
                        sleep_time = 2.0  # Default if no Retry-After header
                    
                    if retries < self.config.max_retries:
                        retries += 1
                        self.log.warning(f"Retry {retries} after rate limit, sleeping {sleep_time}s")
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise RuntimeError(f"Finnhub rate limit exceeded retry limit: {r.text}")

                # Handle response parsing
                try:
                    response_json = r.json()
                except (json.JSONDecodeError, ValueError) as e:
                    self.log.warning(f"Failed to parse JSON response: {e}. Status: {r.status_code}")
                    if self.should_retry(r.status_code, {}):
                        retries += 1
                        if retries > self.config.max_retries:
                            raise RuntimeError(f"Finnhub request exceeded retry limit: {r.text}")
                        sleep_time = self._backoff(retries)
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise RuntimeError(f"Failed to parse API response as JSON: {r.text}")

                if not self.should_retry(r.status_code, response_json):
                    return response_json

                retries += 1
                if retries > self.config.max_retries:
                    raise RuntimeError(f"Finnhub request exceeded retry limit: {r.text}")

                sleep_time = self._backoff(retries)
                self.log.warning(f"Retry {retries} sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)

            except httpx.TimeoutException as e:
                duration = time.perf_counter() - start
                LATENCY.labels(source="finnhub", provider="finnhub", feed="standard").observe(duration)
                ERRORS.labels(source="finnhub", provider="finnhub", feed="standard", code="timeout").inc()
                
                retries += 1
                if retries > self.config.max_retries:
                    raise RuntimeError(f"Finnhub request timeout after {retries} attempts") from e
                
                sleep_time = self._backoff(retries)
                self.log.warning(f"Finnhub timeout, retry {retries} sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)

    # ---------- async request ----------
    async def _async_request(self, params: Mapping[str, str]) -> Dict[str, Any]:
        if self.rate_limiter:
            await self.rate_limiter.async_acquire()

        url = f"{self.config.base_url}{self._PATH_TEMPLATE}"
        headers = {"Accept": "application/json", "User-Agent": self.config.user_agent, "X-Finnhub-Token": self.config.api_key}

        retries = 0
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            while True:
                start = time.perf_counter()
                try:
                    r = await client.get(
                        url,
                        params=params,
                        headers=headers,
                    )
                    duration = time.perf_counter() - start
                    LATENCY.labels(source="finnhub", provider="finnhub", feed="standard").observe(duration)
                    REQUESTS.labels(source="finnhub", provider="finnhub", feed="standard").inc()
                    if r.status_code >= 400:
                        ERRORS.labels(source="finnhub", provider="finnhub", feed="standard", code=str(r.status_code)).inc()

                    # Enhanced retry-after handling for 429 responses
                    if r.status_code == 429:
                        retry_after = r.headers.get("Retry-After")
                        if retry_after:
                            try:
                                sleep_time = float(retry_after)
                                self.log.warning(f"Finnhub async rate limit hit, Retry-After: {sleep_time}s")
                            except ValueError:
                                sleep_time = 2.0  # Default fallback
                        else:
                            sleep_time = 2.0  # Default if no Retry-After header
                        
                        if retries < self.config.max_retries:
                            retries += 1
                            self.log.warning(f"Async retry {retries} after rate limit, sleeping {sleep_time}s")
                            await asyncio.sleep(sleep_time)
                            continue
                        else:
                            raise RuntimeError(f"Finnhub async rate limit exceeded retry limit: {r.text}")

                    # Handle response parsing
                    try:
                        response_json = r.json()
                    except (json.JSONDecodeError, ValueError) as e:
                        self.log.warning(f"Failed to parse JSON response: {e}. Status: {r.status_code}")
                        if self.should_retry(r.status_code, {}):
                            retries += 1
                            if retries > self.config.max_retries:
                                raise RuntimeError(f"Finnhub async request exceeded retry limit: {r.text}")
                            sleep_time = self._backoff(retries)
                            await asyncio.sleep(sleep_time)
                            continue
                        else:
                            raise RuntimeError(f"Failed to parse async API response as JSON: {r.text}")

                    if not self.should_retry(r.status_code, response_json):
                        return response_json

                    retries += 1
                    if retries > self.config.max_retries:
                        raise RuntimeError(f"Finnhub async request exceeded retry limit: {r.text}")

                    sleep_time = self._backoff(retries)
                    self.log.warning(f"Async retry {retries} sleeping {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)

                except httpx.TimeoutException as e:
                    duration = time.perf_counter() - start
                    LATENCY.labels(source="finnhub", provider="finnhub", feed="standard").observe(duration)
                    ERRORS.labels(source="finnhub", provider="finnhub", feed="standard", code="timeout").inc()
                    
                    retries += 1
                    if retries > self.config.max_retries:
                        raise RuntimeError(f"Finnhub async request timeout after {retries} attempts") from e
                    
                    sleep_time = self._backoff(retries)
                    self.log.warning(f"Finnhub async timeout, retry {retries} sleeping {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)

    # ---------- parsing ----------
    def parse_response(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert Finnhub candle response to canonical OHLCV format.
        
        Finnhub returns:
        {
            "c": [close_prices],
            "h": [high_prices], 
            "l": [low_prices],
            "o": [open_prices],
            "t": [timestamps],
            "v": [volumes],
            "s": "ok"|"no_data"|"error"
        }
        """
        rows = []
        
        # Check status field first
        status = raw_json.get("s", "unknown")
        
        if status == "no_data":
            # Finnhub explicitly says no data available - return empty list silently
            self.log.debug("Finnhub returned no_data status - no bars available for this period")
            return rows
        elif status != "ok":
            # Any status other than "ok" or "no_data" indicates an error
            raise RuntimeError(f"Finnhub API error status: {status}")
        
        # Extract OHLCV arrays
        closes = raw_json.get("c", [])
        highs = raw_json.get("h", [])
        lows = raw_json.get("l", [])
        opens = raw_json.get("o", [])
        timestamps = raw_json.get("t", [])
        volumes = raw_json.get("v", [])
        
        # Validate all arrays have same length
        arrays = [closes, highs, lows, opens, timestamps, volumes]
        lengths = [len(arr) for arr in arrays]
        
        if not all(length == lengths[0] for length in lengths):
            raise RuntimeError(f"Finnhub response arrays have mismatched lengths: {lengths}")
        
        # Convert to canonical format
        for i in range(len(timestamps)):
            # Convert Unix timestamp to nanoseconds
            timestamp_ns = int(timestamps[i] * 1_000_000_000)
            
            bar = {
                "symbol": self._current_symbol or "UNKNOWN",  # Use tracked symbol since Finnhub omits it
                "timestamp": timestamp_ns,
                "date": datetime.fromtimestamp(timestamps[i], tz=timezone.utc).strftime("%Y-%m-%d"),
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "volume": int(volumes[i]),
                "trade_count": None,  # Finnhub doesn't provide this
                "vwap": None,  # Finnhub doesn't provide this in candle endpoint
                "session": "regular",
                "currency": "USD",
                "status": "ok",
                "source": "finnhub",
                "frame": "1m",
                "schema_version": 1,
            }
            
            rows.append(bar)
        
        return rows

    # ---------- helpers ----------
    def should_retry(self, status: int, body: Dict[str, Any]) -> bool:
        """
        Determine if request should be retried based on Finnhub-specific conditions.
        
        Args:
            status: HTTP status code
            body: Response body as dict
            
        Returns:
            True if request should be retried
        """
        # Standard HTTP errors that should be retried
        if status in {429, 500, 502, 503, 504}:
            return True
        
        # Finnhub-specific error conditions
        if status == 403:
            # Check if it's a rate limit (should retry) vs auth error (shouldn't retry)
            error_msg = str(body).lower()
            if any(phrase in error_msg for phrase in ["rate limit", "too many requests", "quota"]):
                return True
        
        # Check for transient errors in response body
        if isinstance(body, dict):
            finnhub_status = body.get("s")
            if finnhub_status == "error":
                # Don't retry on explicit API errors
                return False
        
        return False

    @staticmethod
    def _backoff(attempt: int) -> float:
        """
        Exponential backoff with jitter for retry delays.
        """
        base = min(1.5 ** attempt, 32.0)  # Cap at 32 seconds
        import random
        jitter = random.uniform(0, 0.2 * base)
        return base + jitter

    def fetch_batch(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV data with date range chunking for periods > 1 month.
        
        Finnhub limits intraday candles to 1-month windows, so we automatically
        split larger requests into 30-day chunks.
        """
        # Set current symbol for parse_response to use
        self._current_symbol = symbol
        all_rows = []
        
        # Convert to datetime for chunking logic
        start_dt = datetime.fromtimestamp(start_ts / 1_000_000_000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ts / 1_000_000_000, tz=timezone.utc)
        
        # Check if we need to chunk the request
        if (end_dt - start_dt).days <= self._MAX_DAYS_PER_REQUEST:
            # Single request - no chunking needed
            for page in self.paginate(symbol, start_ts, end_ts):
                rows = self.parse_response(page)
                all_rows.extend(rows)
        else:
            # Multi-chunk request for large date ranges
            self.log.info(f"Chunking large date range for {symbol}: {start_dt.date()} to {end_dt.date()}")
            
            current_start = start_dt
            while current_start < end_dt:
                # Calculate chunk end (max 30 days, but don't exceed original end)
                chunk_end = min(
                    current_start + timedelta(days=self._MAX_DAYS_PER_REQUEST - 1, hours=23, minutes=59),
                    end_dt
                )
                
                # Convert back to nanoseconds
                chunk_start_ns = int(current_start.timestamp() * 1_000_000_000)
                chunk_end_ns = int(chunk_end.timestamp() * 1_000_000_000)
                
                self.log.debug(f"Fetching chunk for {symbol}: {current_start.date()} to {chunk_end.date()}")
                
                # Fetch this chunk
                try:
                    for page in self.paginate(symbol, chunk_start_ns, chunk_end_ns):
                        rows = self.parse_response(page)
                        all_rows.extend(rows)
                except Exception as e:
                    self.log.warning(f"Failed to fetch chunk for {symbol} {current_start.date()}-{chunk_end.date()}: {e}")
                    # Continue with next chunk rather than failing entire request
                
                # Move to next chunk
                current_start = chunk_end + timedelta(seconds=1)
        
        self.log.info(f"Fetched {len(all_rows)} bars for {symbol} across {start_dt.date()} to {end_dt.date()}")
        return all_rows

    async def async_fetch_batch(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[Dict[str, Any]]:
        """
        Async version of fetch_batch with date range chunking.
        """
        # Set current symbol for parse_response to use
        self._current_symbol = symbol
        all_rows = []
        
        # Convert to datetime for chunking logic
        start_dt = datetime.fromtimestamp(start_ts / 1_000_000_000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ts / 1_000_000_000, tz=timezone.utc)
        
        # Check if we need to chunk the request
        if (end_dt - start_dt).days <= self._MAX_DAYS_PER_REQUEST:
            # Single request - no chunking needed
            async for page in self.async_paginate(symbol, start_ts, end_ts):
                rows = self.parse_response(page)
                all_rows.extend(rows)
        else:
            # Multi-chunk request for large date ranges
            self.log.info(f"Async chunking large date range for {symbol}: {start_dt.date()} to {end_dt.date()}")
            
            current_start = start_dt
            while current_start < end_dt:
                # Calculate chunk end (max 30 days, but don't exceed original end)
                chunk_end = min(
                    current_start + timedelta(days=self._MAX_DAYS_PER_REQUEST - 1, hours=23, minutes=59),
                    end_dt
                )
                
                # Convert back to nanoseconds
                chunk_start_ns = int(current_start.timestamp() * 1_000_000_000)
                chunk_end_ns = int(chunk_end.timestamp() * 1_000_000_000)
                
                self.log.debug(f"Async fetching chunk for {symbol}: {current_start.date()} to {chunk_end.date()}")
                
                # Fetch this chunk
                try:
                    async for page in self.async_paginate(symbol, chunk_start_ns, chunk_end_ns):
                        rows = self.parse_response(page)
                        all_rows.extend(rows)
                except Exception as e:
                    self.log.warning(f"Failed to async fetch chunk for {symbol} {current_start.date()}-{chunk_end.date()}: {e}")
                    # Continue with next chunk rather than failing entire request
                
                # Move to next chunk
                current_start = chunk_end + timedelta(seconds=1)
        
        self.log.info(f"Async fetched {len(all_rows)} bars for {symbol} across {start_dt.date()} to {end_dt.date()}")
        return all_rows


__all__ = ["FinnhubClient"] 