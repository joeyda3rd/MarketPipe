from __future__ import annotations


class RateLimiter:
    """Placeholder rate limiter."""

    def acquire(self) -> None:
        """Acquire a rate limit token (blocking)."""
        return None

    async def acquire_async(self) -> None:
        """Async token acquisition stub."""
        return None

    async def async_acquire(self) -> None:  # backward compat alias
        return await self.acquire_async()


__all__ = ["RateLimiter"]
