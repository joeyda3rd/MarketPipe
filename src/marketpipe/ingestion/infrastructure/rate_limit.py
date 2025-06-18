# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import threading
import time
from typing import Optional
import logging

from prometheus_client import Counter

# Metrics for rate limiter waits
RATE_LIMITER_WAITS = Counter(
    "mp_rate_limiter_waits_total",
    "Number of times rate limiter caused wait",
    ["provider", "mode"]
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
        self._async_condition: Optional[asyncio.Condition] = None
        
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
            start_time = time.monotonic()
            
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


class DualRateLimiter:
    """
    Rate limiter that enforces both per-minute and per-second limits.
    
    This is useful for APIs like Finnhub that have both:
    - A plan-based limit (e.g., 60 requests/minute)
    - A burst/QPS limit (e.g., 30 requests/second)
    """
    
    def __init__(
        self,
        minute_limit: int,
        second_limit: int,
        minute_window: float = 60.0,
        second_window: float = 1.0,
    ):
        """
        Initialize dual rate limiter.
        
        Args:
            minute_limit: Max requests per minute
            second_limit: Max requests per second  
            minute_window: Window size for minute limit (default: 60.0)
            second_window: Window size for second limit (default: 1.0)
        """
        self.minute_limiter = RateLimiter(minute_limit, minute_window)
        self.second_limiter = RateLimiter(second_limit, second_window)
        self.log = logging.getLogger(self.__class__.__name__)
    
    def acquire(self) -> None:
        """Acquire from both rate limiters (synchronous)."""
        # Acquire from the more restrictive limiter first
        self.second_limiter.acquire()
        self.minute_limiter.acquire()
    
    async def async_acquire(self) -> None:
        """Acquire from both rate limiters (asynchronous)."""
        # Acquire from the more restrictive limiter first
        await self.second_limiter.async_acquire()
        await self.minute_limiter.async_acquire()


def create_rate_limiter_from_config(
    rate_limit_per_min: Optional[int] = None,
    rate_limit_per_sec: Optional[int] = None,
    provider_name: str = "unknown",
) -> Optional[RateLimiter | DualRateLimiter]:
    """
    Create appropriate rate limiter from configuration.
    
    Args:
        rate_limit_per_min: Requests per minute limit
        rate_limit_per_sec: Requests per second limit (for dual limiting)
        provider_name: Provider name for logging
        
    Returns:
        RateLimiter, DualRateLimiter, or None if no limits specified
    """
    if rate_limit_per_min and rate_limit_per_sec:
        # Dual rate limiting for providers with both limits
        logging.getLogger(__name__).info(
            f"Creating dual rate limiter for {provider_name}: "
            f"{rate_limit_per_min} req/min + {rate_limit_per_sec} req/sec"
        )
        return DualRateLimiter(
            minute_limit=rate_limit_per_min,
            second_limit=rate_limit_per_sec
        )
    elif rate_limit_per_min:
        # Standard per-minute rate limiting
        logging.getLogger(__name__).info(
            f"Creating rate limiter for {provider_name}: {rate_limit_per_min} req/min"
        )
        return RateLimiter(rate_limit_per_min, 60.0)
    else:
        # No rate limiting
        return None


__all__ = ["RateLimiter", "create_rate_limiter_from_config", "RATE_LIMITER_WAITS"]
