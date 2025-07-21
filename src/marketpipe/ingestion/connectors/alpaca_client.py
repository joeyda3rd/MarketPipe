from __future__ import annotations

import asyncio
import datetime as dt
import random
import time
from collections.abc import Mapping
from typing import Any, Optional

import httpx

from .base_api_client import BaseApiClient

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"


class AlpacaClient(BaseApiClient):
    """Alpaca Data v2 minute-bar connector (sandbox)."""

    _PATH_TEMPLATE = "/stocks/{symbol}/bars"

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
        qp: dict[str, str] = {
            "symbol": symbol,
            "timeframe": "1Min",
            "start": start,
            "end": end,
            "limit": "10000",
        }
        if cursor:
            qp["page_token"] = cursor
        return qp

    def next_cursor(self, raw_json: dict[str, Any]) -> Optional[str]:
        return raw_json.get("next_page_token")

    # ---------- sync request ----------
    def _request(self, params: Mapping[str, str]) -> dict[str, Any]:
        if self.rate_limiter:
            self.rate_limiter.acquire()

        url = f"{self.config.base_url}{self._PATH_TEMPLATE.format(symbol=params['symbol'])}"
        headers = {"Accept": "application/json", "User-Agent": self.config.user_agent}
        self.auth.apply(headers, params={})

        retries = 0
        while True:
            r = httpx.get(
                url,
                params={k: v for k, v in params.items() if k != "symbol"},
                headers=headers,
                timeout=self.config.timeout,
            )
            if not self.should_retry(r.status_code, r.json()):
                return r.json()

            retries += 1
            if retries > self.config.max_retries:
                raise RuntimeError(f"Alpaca request exceeded retry limit: {r.text}")
            sleep = self._backoff(retries)
            self.log.warning("Retry %d sleeping %.2fs", retries, sleep)
            time.sleep(sleep)

    # ---------- async request ----------
    async def _async_request(self, params: Mapping[str, str]) -> dict[str, Any]:
        if self.rate_limiter:
            await self.rate_limiter.async_acquire()

        url = f"{self.config.base_url}{self._PATH_TEMPLATE.format(symbol=params['symbol'])}"
        headers = {"Accept": "application/json", "User-Agent": self.config.user_agent}
        self.auth.apply(headers, params={})

        retries = 0
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            while True:
                r = await client.get(
                    url,
                    params={k: v for k, v in params.items() if k != "symbol"},
                    headers=headers,
                )
                if not self.should_retry(r.status_code, r.json()):
                    return r.json()

                retries += 1
                if retries > self.config.max_retries:
                    raise RuntimeError("Alpaca async retry limit hit")
                sleep = self._backoff(retries)
                self.log.warning("Async retry %d sleeping %.2fs", retries, sleep)
                await asyncio.sleep(sleep)

    # ---------- parsing ----------
    def parse_response(self, raw_json: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for bar in raw_json.get("bars", []):
            rows.append(
                {
                    "symbol": bar["S"],
                    "timestamp": int(
                        dt.datetime.fromisoformat(bar["t"]).timestamp() * 1_000_000_000
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
    def should_retry(self, status: int, body: dict[str, Any]) -> bool:
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
