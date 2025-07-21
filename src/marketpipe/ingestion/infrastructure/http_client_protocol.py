# SPDX-License-Identifier: Apache-2.0
"""HTTP client protocol for dependency injection."""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class HttpResponse(Protocol):
    """Protocol for HTTP response objects."""

    status_code: int
    headers: Dict[str, str]
    text: str

    def json(self) -> Dict[str, Any]:
        """Parse response as JSON."""
        ...


@runtime_checkable
class HttpClientProtocol(Protocol):
    """Protocol for HTTP client implementations.

    This protocol allows for dependency injection of HTTP clients,
    enabling tests to inject fake HTTP clients instead of using
    real HTTP libraries.
    """

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> HttpResponse:
        """Make a synchronous GET request.

        Args:
            url: Request URL
            params: Query parameters
            headers: Request headers
            timeout: Request timeout in seconds

        Returns:
            HttpResponse: Response object with status, headers, and body
        """
        ...


@runtime_checkable
class AsyncHttpClientProtocol(Protocol):
    """Protocol for async HTTP client implementations."""

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> HttpResponse:
        """Make an asynchronous GET request.

        Args:
            url: Request URL
            params: Query parameters
            headers: Request headers
            timeout: Request timeout in seconds

        Returns:
            HttpResponse: Response object with status, headers, and body
        """
        ...


# Adapter classes to make httpx compatible with the protocol

class HttpxResponseAdapter:
    """Adapter to make httpx.Response compatible with HttpResponse protocol."""

    def __init__(self, httpx_response):
        self._response = httpx_response

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> Dict[str, str]:
        return dict(self._response.headers)

    @property
    def text(self) -> str:
        return self._response.text

    def json(self) -> Dict[str, Any]:
        return self._response.json()


class HttpxClientAdapter:
    """Adapter to make httpx.Client compatible with HttpClientProtocol."""

    def __init__(self, httpx_client=None):
        import httpx
        self._client = httpx_client or httpx

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> HttpResponse:
        """Make GET request using httpx."""
        response = self._client.get(
            url=url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
        return HttpxResponseAdapter(response)


class AsyncHttpxClientAdapter:
    """Adapter to make httpx.AsyncClient compatible with AsyncHttpClientProtocol."""

    def __init__(self, httpx_client=None):
        import httpx
        self._client = httpx_client or httpx

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> HttpResponse:
        """Make async GET request using httpx."""
        response = await self._client.get(
            url=url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
        return HttpxResponseAdapter(response)


# Default implementations

def get_default_http_client() -> HttpClientProtocol:
    """Get default HTTP client implementation."""
    return HttpxClientAdapter()


def get_default_async_http_client() -> AsyncHttpClientProtocol:
    """Get default async HTTP client implementation."""
    return AsyncHttpxClientAdapter()
