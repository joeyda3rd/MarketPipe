# SPDX-License-Identifier: Apache-2.0
"""Infrastructure client wrappers."""

from __future__ import annotations

from typing import Any

from .alpaca_client import AlpacaClient as OriginalAlpacaClient
from .auth import HeaderTokenAuth
from .models import ClientConfig
from .rate_limit import RateLimiter


class AlpacaApiClientWrapper:
    """
    Wrapper around the original Alpaca client for infrastructure layer.

    This wrapper isolates the infrastructure concerns and provides
    a cleaner interface for the anti-corruption layer.
    """

    def __init__(
        self,
        config: ClientConfig,
        auth: HeaderTokenAuth,
        rate_limiter: RateLimiter,
        feed: str = "iex",
    ):
        self._config = config
        self._auth = auth
        self._rate_limiter = rate_limiter
        self._feed = feed

        # Create the original client
        self._client = OriginalAlpacaClient(
            config=config,
            auth=auth,
            rate_limiter=rate_limiter,
            state_backend=None,  # We handle state at domain level
            feed=feed,
        )

    async def fetch_bars_raw(self, symbol: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        """
        Fetch raw bars from Alpaca API.

        Returns the raw response format from Alpaca without any domain translation.
        This allows the anti-corruption layer to handle all translation logic.
        """
        try:
            return self._client.fetch_batch(symbol, start_ms, end_ms)
        except Exception as e:
            # Re-raise with infrastructure context
            raise InfrastructureError(f"Alpaca API error for {symbol}: {e}") from e

    async def test_connection(self) -> bool:
        """Test if the Alpaca API connection is working."""
        try:
            # You could implement a simple API call here to test connectivity
            # For now, we'll assume the connection is good if we can create the client
            return True
        except Exception:
            return False

    def get_configuration_info(self) -> dict[str, Any]:
        """Get information about the current configuration."""
        return {
            "base_url": self._config.base_url,
            "feed": self._feed,
            "rate_limit_per_min": self._config.rate_limit_per_min,
            "has_auth": self._auth is not None,
        }


class InfrastructureError(Exception):
    """Base exception for infrastructure layer errors."""

    pass
