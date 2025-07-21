# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import threading
import time
from typing import Optional

from prometheus_client import Counter

# Metrics for rate limiter waits
RATE_LIMITER_WAITS = Counter(
    "mp_rate_limiter_waits_total", "Number of times rate limiter caused wait", ["provider", "mode"]
)


class RateLimiter:
    """Token bucket rate limiter supporting both sync and async operations.

    The token bucket algorithm allows for controlled bursts while maintaining
    an average rate. Tokens are added to the bucket at the specified refill_rate
    up to the capacity limit.

    Args:
        capacity: Maximum number of tokens in the bucket (burst size)
        refill_rate: Tokens added per second
    """

    def __init__(self, capacity: int, refill_rate: float):
        """Initialize token bucket rate limiter.

        Args:
            capacity: Maximum tokens in bucket (allows burst up to this amount)
            refill_rate: Rate of token refill in tokens per second
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if refill_rate <= 0:
            raise ValueError("Refill rate must be positive")

        self._capacity = capacity
        self._refill_rate = refill_rate
        self._tokens = float(capacity)  # Start with full bucket
        self._last_refill = time.monotonic()

        # Sync/async coordination primitives
        self._sync_condition = threading.Condition()
        self._async_condition: asyncio.Optional[Condition] = None

        # Provider name for metrics (set by clients)
        self._provider_name = "unknown"

        # Retry-After state
        self._retry_after_until: Optional[float] = None

    def set_provider_name(self, provider_name: str) -> None:
        """Set provider name for metrics labeling."""
        self._provider_name = provider_name

    def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens (blocking sync version).

        Args:
            tokens: Number of tokens to acquire

        Raises:
            ValueError: If tokens > capacity (impossible to fulfill)
        """
        if tokens > self._capacity:
            raise ValueError(f"Cannot acquire {tokens} tokens, capacity is {self._capacity}")

        with self._sync_condition:
            time.monotonic()

            while True:
                # Check if we're in retry-after period
                current_time = time.monotonic()
                if self._retry_after_until and current_time < self._retry_after_until:
                    wait_time = self._retry_after_until - current_time
                    RATE_LIMITER_WAITS.labels(provider=self._provider_name, mode="sync").inc()
                    time.sleep(wait_time)
                    continue

                # Refill tokens based on elapsed time
                self._refill_tokens()

                # Check if we have enough tokens
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return

                # Calculate wait time for next token availability
                tokens_needed = tokens - self._tokens
                wait_time = tokens_needed / self._refill_rate

                # Record that we're waiting due to rate limiting
                RATE_LIMITER_WAITS.labels(provider=self._provider_name, mode="sync").inc()

                # Use condition variable for interruptible wait
                self._sync_condition.wait(timeout=wait_time)

    async def acquire_async(self, tokens: int = 1) -> None:
        """Acquire tokens (async version).

        Args:
            tokens: Number of tokens to acquire

        Raises:
            ValueError: If tokens > capacity (impossible to fulfill)
        """
        if tokens > self._capacity:
            raise ValueError(f"Cannot acquire {tokens} tokens, capacity is {self._capacity}")

        # Create async condition lazily in the event loop
        if self._async_condition is None:
            self._async_condition = asyncio.Condition()

        async with self._async_condition:
            while True:
                # Check if we're in retry-after period
                current_time = time.monotonic()
                if self._retry_after_until and current_time < self._retry_after_until:
                    wait_time = self._retry_after_until - current_time
                    RATE_LIMITER_WAITS.labels(provider=self._provider_name, mode="async").inc()
                    await asyncio.sleep(wait_time)
                    continue

                # Refill tokens based on elapsed time
                self._refill_tokens()

                # Check if we have enough tokens
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return

                # Calculate wait time for next token availability
                tokens_needed = tokens - self._tokens
                wait_time = tokens_needed / self._refill_rate

                # Record that we're waiting due to rate limiting
                RATE_LIMITER_WAITS.labels(provider=self._provider_name, mode="async").inc()

                # Wait for tokens to be available
                await asyncio.sleep(wait_time)

    def notify_retry_after(self, seconds: int) -> None:
        """Handle Retry-After header (sync version).

        Clears the token bucket and enforces a wait period.

        Args:
            seconds: Number of seconds to wait before allowing requests
        """
        current_time = time.monotonic()
        self._retry_after_until = current_time + seconds

        with self._sync_condition:
            # Clear the bucket to prevent immediate bursts after retry period
            self._tokens = 0.0
            self._last_refill = current_time

            # Record retry-after event
            RATE_LIMITER_WAITS.labels(provider=self._provider_name, mode="retry_after").inc()

            # Sleep for the retry period
            time.sleep(seconds)

            # Clear retry-after state
            self._retry_after_until = None

            # Notify any waiting threads
            self._sync_condition.notify_all()

    async def notify_retry_after_async(self, seconds: int) -> None:
        """Handle Retry-After header (async version).

        Clears the token bucket and enforces a wait period.

        Args:
            seconds: Number of seconds to wait before allowing requests
        """
        current_time = time.monotonic()
        self._retry_after_until = current_time + seconds

        # Create async condition lazily in the event loop
        if self._async_condition is None:
            self._async_condition = asyncio.Condition()

        async with self._async_condition:
            # Clear the bucket to prevent immediate bursts after retry period
            self._tokens = 0.0
            self._last_refill = current_time

            # Record retry-after event
            RATE_LIMITER_WAITS.labels(provider=self._provider_name, mode="retry_after").inc()

            # Sleep for the retry period
            await asyncio.sleep(seconds)

            # Clear retry-after state
            self._retry_after_until = None

            # Notify any waiting coroutines
            self._async_condition.notify_all()

    # Backward compatibility alias
    async def async_acquire(self) -> None:
        """Backward compatibility alias for acquire_async()."""
        await self.acquire_async()

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time since last refill."""
        current_time = time.monotonic()
        elapsed = current_time - self._last_refill

        # Calculate tokens to add based on elapsed time
        tokens_to_add = elapsed * self._refill_rate

        # Add tokens up to capacity
        self._tokens = min(self._capacity, self._tokens + tokens_to_add)

        # Update last refill time
        self._last_refill = current_time

    def get_available_tokens(self) -> float:
        """Get current number of available tokens (for testing/debugging)."""
        self._refill_tokens()
        return self._tokens

    def get_capacity(self) -> int:
        """Get bucket capacity."""
        return self._capacity

    def get_refill_rate(self) -> float:
        """Get refill rate in tokens per second."""
        return self._refill_rate

    def reset(self) -> None:
        """Reset the rate limiter to initial state (for testing)."""
        with self._sync_condition:
            self._tokens = float(self._capacity)
            self._last_refill = time.monotonic()
            self._retry_after_until = None
            self._sync_condition.notify_all()


def create_rate_limiter_from_config(
    rate_limit_per_min: Optional[int] = None,
    burst_size: Optional[int] = None,
    provider_name: str = "unknown",
) -> Optional[RateLimiter]:
    """Create a RateLimiter from configuration values.

    Args:
        rate_limit_per_min: Rate limit in requests per minute
        burst_size: Maximum burst size (defaults to rate_limit_per_min if not specified)
        provider_name: Provider name for metrics

    Returns:
        RateLimiter instance or None if rate limiting is disabled
    """
    if rate_limit_per_min is None or rate_limit_per_min <= 0:
        return None

    # Convert per-minute to per-second
    refill_rate = rate_limit_per_min / 60.0

    # Default burst size to rate limit if not specified
    capacity = burst_size if burst_size is not None else rate_limit_per_min

    limiter = RateLimiter(capacity=capacity, refill_rate=refill_rate)
    limiter.set_provider_name(provider_name)

    return limiter


__all__ = ["RateLimiter", "create_rate_limiter_from_config", "RATE_LIMITER_WAITS"]
