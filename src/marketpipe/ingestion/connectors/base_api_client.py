from __future__ import annotations

import abc
import logging
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .auth import AuthStrategy
from .rate_limit import RateLimiter
from .models import ClientConfig


class BaseApiClient(abc.ABC):
    """Abstract, vendor-agnostic API client.

    Usage:
        >>> cfg = ClientConfig(api_key="token", base_url="https://api.example.com")
        >>> client = ConcreteClient(cfg, auth=TokenAuth(cfg.api_key))
        >>> rows = client.fetch_batch("AAPL", 1690848000, 1690851600)
    """

    def __init__(
        self,
        config: ClientConfig,
        auth: AuthStrategy,
        rate_limiter: Optional[RateLimiter] = None,
        metrics_collector: Optional[callable] = None,
        state_backend: Optional["StateBackend"] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.auth = auth
        self.rate_limiter = rate_limiter
        self.metrics = metrics_collector or (lambda *a, **k: None)
        self.state = state_backend
        self.log = logger or logging.getLogger(self.__class__.__name__)

    # ---------- URL / request helpers ----------
    @abc.abstractmethod
    def build_request_params(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        cursor: Optional[str] = None,
    ) -> Mapping[str, str]:
        """Return query parameters dict specific to this vendor."""

    @abc.abstractmethod
    def endpoint_path(self) -> str:
        """Return the endpoint path (e.g. '/v2/aggs/ticker/{symbol}/range')."""

    # ---------- Pagination ----------
    def paginate(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        **kwargs: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Default page iterator.

        Args:
            symbol: Security identifier.
            start_ts: Start timestamp inclusive.
            end_ts: End timestamp exclusive.

        Yields:
            Raw JSON pages from the vendor API.
        """
        cursor: Optional[str] = None
        while True:
            params = self.build_request_params(symbol, start_ts, end_ts, cursor)
            raw_json = self._request(params)
            yield raw_json
            cursor = self.next_cursor(raw_json)
            if not cursor:
                break

    @abc.abstractmethod
    def next_cursor(self, raw_json: Dict[str, Any]) -> Optional[str]:
        """Extract pagination cursor/offset from response JSON."""

    # ---------- Low-level request ----------
    def _request(self, params: Mapping[str, str]) -> Dict[str, Any]:
        """Blocking HTTP request with rate-limit, retry, and auth handling."""
        raise NotImplementedError

    async def _async_request(self, params: Mapping[str, str]) -> Dict[str, Any]:
        """Async HTTP request (same semantics as :meth:`_request`)."""
        raise NotImplementedError

    # ---------- Public batch fetch ----------
    def fetch_batch(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[Dict[str, Any]]:
        """High-level helper returning a list of normalized OHLCV rows."""
        rows: List[Dict[str, Any]] = []
        for page in self.paginate(symbol, start_ts, end_ts):
            rows.extend(self.parse_response(page))
        return rows

    async def async_fetch_batch(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        async for page in self.async_paginate(symbol, start_ts, end_ts):
            rows.extend(self.parse_response(page))
        return rows

    async def async_paginate(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        **kwargs: Any,
    ):
        """Async generator version of :meth:`paginate`."""
        cursor: Optional[str] = None
        while True:
            params = self.build_request_params(symbol, start_ts, end_ts, cursor)
            raw_json = await self._async_request(params)
            yield raw_json
            cursor = self.next_cursor(raw_json)
            if not cursor:
                break

    # ---------- Response handling ----------
    @abc.abstractmethod
    def parse_response(self, raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert vendor response to list of canonical OHLCV rows."""

    # ---------- Error / rate-limit ----------
    @abc.abstractmethod
    def should_retry(self, status_code: int, json_body: Dict[str, Any]) -> bool:
        """Return True if the request should be retried."""

    # ---------- State checkpointing ----------
    def save_checkpoint(self, symbol: str, checkpoint: str | int) -> None:
        """Persist symbol checkpoint if a state backend is configured."""
        if self.state:
            self.state.set(symbol, checkpoint)

    def load_checkpoint(self, symbol: str) -> Optional[str | int]:
        """Load the last saved checkpoint for a symbol."""
        return self.state.get(symbol) if self.state else None


__all__ = ["BaseApiClient"]
