# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import datetime as dt
import json
import random
import time
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

import httpx

from marketpipe.metrics import ERRORS, LATENCY, REQUESTS
from marketpipe.security.mask import safe_for_log

from .base_api_client import BaseApiClient

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"


class AlpacaClient(BaseApiClient):
    """Alpaca Data v2 minute-bar connector with IEX support."""

    _PATH_TEMPLATE = "/stocks/bars"

    def __init__(self, *args, feed: str = "iex", **kwargs):
        """Initialize AlpacaClient with feed option.

        Args:
            feed: Data feed to use ("iex" for free tier, "sip" for paid tier)
        """
        super().__init__(*args, **kwargs)
        self.feed = feed
        self.log.info(f"AlpacaClient initialized with feed: {feed}")

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
        start = dt.datetime.utcfromtimestamp(start_ts / 1_000).strftime(ISO_FMT)
        end = dt.datetime.utcfromtimestamp(end_ts / 1_000).strftime(ISO_FMT)
        qp: Dict[str, str] = {
            "symbols": symbol,  # v2 API uses "symbols" not "symbol"
            "timeframe": "1Min",
            "start": start,
            "end": end,
            "limit": "10000",
            "feed": self.feed,  # Add feed parameter for IEX/SIP selection
        }
        if cursor:
            qp["page_token"] = cursor
        return qp

    def next_cursor(self, raw_json: Dict[str, Any]) -> Optional[str]:
        return raw_json.get("next_page_token")

    # ---------- sync request ----------
    def _request(self, params: Mapping[str, str]) -> Dict[str, Any]:
        if self.rate_limiter:
            self.rate_limiter.acquire()

        url = f"{self.config.base_url}{self._PATH_TEMPLATE}"  # v2 API doesn't need symbol in URL
        headers = {"Accept": "application/json", "User-Agent": self.config.user_agent}
        self.auth.apply(headers, params={})

        retries = 0
        while True:
            start = time.perf_counter()
            r = httpx.get(
                url,
                params=params,  # Include all params including symbols
                headers=headers,
                timeout=self.config.timeout,
            )
            duration = time.perf_counter() - start
            LATENCY.labels(source="alpaca", provider="alpaca", feed=self.feed).observe(duration)
            REQUESTS.labels(source="alpaca", provider="alpaca", feed=self.feed).inc()
            if r.status_code >= 400:
                ERRORS.labels(
                    source="alpaca", provider="alpaca", feed=self.feed, code=str(r.status_code)
                ).inc()

            # Handle JSON parsing safely
            try:
                response_json = r.json()
            except (json.JSONDecodeError, ValueError) as e:
                # If JSON parsing fails, check if we should retry based on status code only
                safe_msg = safe_for_log(
                    f"Failed to parse JSON response: {e}. Status: {r.status_code}, Text: {r.text[:200]}",
                    self.config.api_key,
                )
                self.log.warning(safe_msg)
                if self.should_retry(r.status_code, {}):
                    retries += 1
                    if retries > self.config.max_retries:
                        safe_error_msg = safe_for_log(
                            f"Alpaca request exceeded retry limit: {r.text}", self.config.api_key
                        )
                        raise RuntimeError(safe_error_msg)
                    sleep = self._backoff(retries)
                    self.log.warning("Retry %d sleeping %.2fs", retries, sleep)
                    time.sleep(sleep)
                    continue
                else:
                    safe_error_msg = safe_for_log(
                        f"Failed to parse Alpaca API response as JSON: {r.text}",
                        self.config.api_key,
                    )
                    raise RuntimeError(safe_error_msg)

            if not self.should_retry(r.status_code, response_json):
                return response_json

            # Handle Retry-After header for 429 responses
            if r.status_code == 429 and self.rate_limiter:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        retry_seconds = int(retry_after)
                        self.log.warning(f"Rate limited, respecting Retry-After: {retry_seconds}s")
                        self.rate_limiter.notify_retry_after(retry_seconds)
                        continue  # Try again after retry-after period
                    except (ValueError, TypeError):
                        self.log.warning(f"Invalid Retry-After header: {retry_after}")

            retries += 1
            if retries > self.config.max_retries:
                safe_error_msg = safe_for_log(
                    f"Alpaca request exceeded retry limit: {r.text}", self.config.api_key
                )
                raise RuntimeError(safe_error_msg)
            sleep = self._backoff(retries)
            self.log.warning("Retry %d sleeping %.2fs", retries, sleep)
            time.sleep(sleep)

    # ---------- async request ----------
    async def _async_request(self, params: Mapping[str, str]) -> Dict[str, Any]:
        if self.rate_limiter:
            await self.rate_limiter.async_acquire()

        url = f"{self.config.base_url}{self._PATH_TEMPLATE}"  # v2 API doesn't need symbol in URL
        headers = {"Accept": "application/json", "User-Agent": self.config.user_agent}
        self.auth.apply(headers, params={})

        retries = 0
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            while True:
                start = time.perf_counter()
                r = await client.get(
                    url,
                    params=params,  # Include all params including symbols
                    headers=headers,
                )
                duration = time.perf_counter() - start
                LATENCY.labels(source="alpaca", provider="alpaca", feed=self.feed).observe(duration)
                REQUESTS.labels(source="alpaca", provider="alpaca", feed=self.feed).inc()
                if r.status_code >= 400:
                    ERRORS.labels(
                        source="alpaca", provider="alpaca", feed=self.feed, code=str(r.status_code)
                    ).inc()

                # Handle JSON parsing safely
                try:
                    response_json = r.json()
                except (json.JSONDecodeError, ValueError) as e:
                    # If JSON parsing fails, check if we should retry based on status code only
                    safe_msg = safe_for_log(
                        f"Failed to parse JSON response: {e}. Status: {r.status_code}, Text: {r.text[:200]}",
                        self.config.api_key,
                    )
                    self.log.warning(safe_msg)
                    if self.should_retry(r.status_code, {}):
                        retries += 1
                        if retries > self.config.max_retries:
                            safe_error_msg = safe_for_log(
                                "Alpaca async retry limit hit", self.config.api_key
                            )
                            raise RuntimeError(safe_error_msg)
                        sleep = self._backoff(retries)
                        self.log.warning("Async retry %d sleeping %.2fs", retries, sleep)
                        await asyncio.sleep(sleep)
                        continue
                    else:
                        safe_error_msg = safe_for_log(
                            f"Failed to parse Alpaca API response as JSON: {r.text}",
                            self.config.api_key,
                        )
                        raise RuntimeError(safe_error_msg)

                if not self.should_retry(r.status_code, response_json):
                    return response_json

                # Handle Retry-After header for 429 responses
                if r.status_code == 429 and self.rate_limiter:
                    retry_after = r.headers.get("Retry-After")
                    if retry_after:
                        try:
                            retry_seconds = int(retry_after)
                            self.log.warning(
                                f"Rate limited, respecting Retry-After: {retry_seconds}s"
                            )
                            await self.rate_limiter.notify_retry_after_async(retry_seconds)
                            continue  # Try again after retry-after period
                        except (ValueError, TypeError):
                            self.log.warning(f"Invalid Retry-After header: {retry_after}")

                retries += 1
                if retries > self.config.max_retries:
                    safe_error_msg = safe_for_log(
                        "Alpaca async retry limit hit", self.config.api_key
                    )
                    raise RuntimeError(safe_error_msg)
                sleep = self._backoff(retries)
                self.log.warning("Async retry %d sleeping %.2fs", retries, sleep)
                await asyncio.sleep(sleep)

    # ---------- parsing ----------
    def parse_response(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        bars_obj = raw_json.get("bars", {})

        if isinstance(bars_obj, list):
            # Legacy format returned a list of bars with short field names
            for bar in bars_obj:
                symbol = bar.get("S") or bar.get("symbol", "")
                rows.append(
                    {
                        "symbol": symbol,
                        "t": bar["t"],
                        "timestamp": int(
                            dt.datetime.fromisoformat(bar["t"].replace("Z", "+00:00")).timestamp()
                            * 1_000_000_000
                        ),
                        "date": dt.date.fromisoformat(bar["t"][:10]),
                        "open": bar["o"],
                        "high": bar["h"],
                        "low": bar["l"],
                        "close": bar["c"],
                        "volume": bar["v"],
                        "trade_count": bar.get("n"),
                        "vwap": None,
                        "session": "regular",
                        "currency": "USD",
                        "status": "ok",
                        "source": "alpaca",
                        "frame": "1m",
                        "schema_version": 1,
                    }
                )
        else:
            # Current API returns a mapping of symbol -> list[bar]
            bars_dict = bars_obj or {}
            for symbol, bars in bars_dict.items():
                for bar in bars:
                    rows.append(
                        {
                            "symbol": symbol,
                            "t": bar["t"],
                            "timestamp": int(
                                dt.datetime.fromisoformat(
                                    bar["t"].replace("Z", "+00:00")
                                ).timestamp()
                                * 1_000_000_000
                            ),
                            "date": dt.date.fromisoformat(bar["t"][:10]),
                            "open": bar["o"],
                            "high": bar["h"],
                            "low": bar["l"],
                            "close": bar["c"],
                            "volume": bar["v"],
                            "trade_count": bar.get("n"),
                            "vwap": None,
                            "session": "regular",
                            "currency": "USD",
                            "status": "ok",
                            "source": "alpaca",
                            "frame": "1m",
                            "schema_version": 1,
                        }
                    )

        return rows

    # ---------- helpers ----------
    def should_retry(self, status: int, body: Dict[str, Any]) -> bool:
        if status in {429, 500, 502, 503, 504}:
            return True
        if status == 403 and "too many requests" in str(body).lower():
            return True
        return False

    @staticmethod
    def _backoff(attempt: int) -> float:
        base = 1.5**attempt
        return base + random.uniform(0, 0.2 * base)


__all__ = ["AlpacaClient"]
