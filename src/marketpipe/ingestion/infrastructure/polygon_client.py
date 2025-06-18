# SPDX-License-Identifier: Apache-2.0
"""Polygon.io API client for market data retrieval."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import random
import time
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import parse_qs, urlparse

import httpx

from marketpipe.metrics import ERRORS, LATENCY, REQUESTS
from marketpipe.security.mask import safe_for_log

from .auth import AuthStrategy
from .base_api_client import BaseApiClient
from .models import PolygonClientConfig


class PolygonClient(BaseApiClient):
    """Polygon.io API client for v2 aggregates endpoint."""

    _PATH_TEMPLATE = "/v2/aggs/ticker/{symbol}/range/1/minute/{from_date}/{to_date}"

    def __init__(self, *args, **kwargs):
        """Initialize PolygonClient."""
        super().__init__(*args, **kwargs)
        self._current_symbol = None
        self.log.info("PolygonClient initialized for v2 aggregates endpoint")

    def endpoint_path(self):
        """Return base endpoint path template."""
        return self._PATH_TEMPLATE

    def build_request_params(self, symbol, start_ts, end_ts, cursor=None):
        """Build Polygon-specific query parameters."""
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": str(self.config.max_results),
            "apikey": self.config.api_key,
        }
        
        if cursor:
            params["cursor"] = cursor
            
        return params

    def build_url(self, symbol, start_ts, end_ts):
        """Build complete URL for Polygon aggregates endpoint."""
        start_dt = dt.datetime.fromtimestamp(start_ts / 1_000_000_000, tz=dt.timezone.utc)
        end_dt = dt.datetime.fromtimestamp(end_ts / 1_000_000_000, tz=dt.timezone.utc)
        
        path = self._PATH_TEMPLATE.format(
            symbol=symbol,
            from_date=start_dt.date().isoformat(),
            to_date=end_dt.date().isoformat()
        )
        
        return self.config.base_url + path

    def next_cursor(self, raw_json):
        """Extract pagination cursor from Polygon response."""
        next_url = raw_json.get("next_url")
        if not next_url:
            return None
            
        try:
            parsed = urlparse(next_url)
            query_params = parse_qs(parsed.query)
            cursor = query_params.get("cursor", [None])[0]
            return cursor
        except Exception as e:
            self.log.warning(f"Failed to parse cursor from next_url: {next_url}, error: {e}")
            return None

    def parse_response(self, raw_json):
        """Convert Polygon aggregates response to canonical OHLCV format."""
        rows = []
        
        status = raw_json.get("status", "").upper()
        if status == "ERROR":
            error_msg = raw_json.get("error", "Unknown Polygon API error")
            raise RuntimeError(f"Polygon API error: {error_msg}")
        
        if status != "OK":
            self.log.warning(f"Unexpected Polygon status: {status}")
            return rows
        
        results = raw_json.get("results", [])
        ticker = raw_json.get("ticker", self._current_symbol or "UNKNOWN")
        
        for result in results:
            try:
                timestamp_ns = int(result["t"] * 1_000_000)
                timestamp_dt = dt.datetime.fromtimestamp(result["t"] / 1000, tz=dt.timezone.utc)
                
                bar = {
                    "symbol": ticker,
                    "timestamp": timestamp_ns,
                    "date": timestamp_dt.strftime("%Y-%m-%d"),
                    "open": float(result["o"]),
                    "high": float(result["h"]),
                    "low": float(result["l"]),
                    "close": float(result["c"]),
                    "volume": int(result["v"]),
                    "trade_count": result.get("n"),
                    "vwap": result.get("vw"),
                    "session": "regular",
                    "currency": "USD",
                    "status": "ok",
                    "source": "polygon",
                    "frame": "1m",
                    "schema_version": 1,
                }
                
                rows.append(bar)
                
            except (KeyError, ValueError, TypeError) as e:
                self.log.warning(f"Failed to parse Polygon result: {result}, error: {e}")
                continue
        
        return rows

    def should_retry(self, status, body):
        """Determine if request should be retried."""
        if status in {429, 500, 502, 503, 504}:
            return True
        
        if status == 403:
            error_msg = str(body).lower()
            if any(phrase in error_msg for phrase in ["rate limit", "too many requests", "quota"]):
                return True
        
        if isinstance(body, dict):
            polygon_status = body.get("status", "").upper()
            if polygon_status == "ERROR":
                return False
        
        return False

    @staticmethod
    def _backoff(attempt):
        """Exponential backoff with jitter for retry delays."""
        base = min(1.5 ** attempt, 32.0)
        jitter = random.uniform(0, 0.2 * base)
        return base + jitter

    def fetch_batch(self, symbol, start_ts, end_ts):
        """Fetch OHLCV data from Polygon with pagination support."""
        self._current_symbol = symbol
        all_rows = []
        
        url = self.build_url(symbol, start_ts, end_ts)
        cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            self.log.debug(f"Fetching page {page_count} for {symbol}")
            
            params = self.build_request_params(symbol, start_ts, end_ts, cursor)
            
            if self.rate_limiter:
                self.rate_limiter.acquire()
            
            try:
                response = httpx.get(url, params=params, timeout=self.config.timeout)
                response.raise_for_status()
                page_data = response.json()
            except Exception as e:
                safe_msg = safe_for_log(f"Failed to fetch page {page_count} for {symbol}: {e}", self.config.api_key)
                self.log.error(safe_msg)
                break
            
            page_rows = self.parse_response(page_data)
            all_rows.extend(page_rows)
            
            cursor = self.next_cursor(page_data)
            if not cursor:
                break
                
            if page_count > 100:
                self.log.warning(f"Reached pagination limit for {symbol}, stopping")
                break
        
        self.log.info(f"Fetched {len(all_rows)} bars for {symbol} across {page_count} pages")
        return all_rows

    async def async_fetch_batch(self, symbol, start_ts, end_ts):
        """Async version of fetch_batch with pagination support."""
        self._current_symbol = symbol
        all_rows = []
        
        url = self.build_url(symbol, start_ts, end_ts)
        cursor = None
        page_count = 0
        
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            while True:
                page_count += 1
                self.log.debug(f"Async fetching page {page_count} for {symbol}")
                
                params = self.build_request_params(symbol, start_ts, end_ts, cursor)
                
                if self.rate_limiter:
                    await self.rate_limiter.async_acquire()
                
                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    page_data = response.json()
                except Exception as e:
                    safe_msg = safe_for_log(f"Failed to async fetch page {page_count} for {symbol}: {e}", self.config.api_key)
                    self.log.error(safe_msg)
                    break
                
                page_rows = self.parse_response(page_data)
                all_rows.extend(page_rows)
                
                cursor = self.next_cursor(page_data)
                if not cursor:
                    break
                    
                if page_count > 100:
                    self.log.warning(f"Reached async pagination limit for {symbol}, stopping")
                    break
        
        self.log.info(f"Async fetched {len(all_rows)} bars for {symbol} across {page_count} pages")
        return all_rows


__all__ = ["PolygonClient"] 