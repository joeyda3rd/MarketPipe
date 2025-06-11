# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import time
import pytest
import threading
from unittest.mock import patch

from marketpipe.ingestion.infrastructure.rate_limit import (
    RateLimiter,
    create_rate_limiter_from_config,
    RATE_LIMITER_WAITS
)


class TestRateLimiter:
    """Test suite for RateLimiter functionality."""

    def test_initialization(self):
        """Test RateLimiter initialization."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        
        assert limiter.get_capacity() == 10
        assert limiter.get_refill_rate() == 5.0
        assert limiter.get_available_tokens() == 10.0  # Starts full

    def test_initialization_validation(self):
        """Test RateLimiter initialization parameter validation."""
        with pytest.raises(ValueError, match="Capacity must be positive"):
            RateLimiter(capacity=0, refill_rate=1.0)
            
        with pytest.raises(ValueError, match="Capacity must be positive"):
            RateLimiter(capacity=-1, refill_rate=1.0)
            
        with pytest.raises(ValueError, match="Refill rate must be positive"):
            RateLimiter(capacity=10, refill_rate=0.0)
            
        with pytest.raises(ValueError, match="Refill rate must be positive"):
            RateLimiter(capacity=10, refill_rate=-1.0)

    def test_acquire_within_capacity(self):
        """Test acquiring tokens within capacity doesn't block."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        
        start_time = time.monotonic()
        limiter.acquire(5)  # Should not block
        elapsed = time.monotonic() - start_time
        
        assert elapsed < 0.1  # Should be nearly instantaneous
        assert abs(limiter.get_available_tokens() - 5.0) < 0.01  # Allow small timing differences

    def test_acquire_more_than_capacity_raises_error(self):
        """Test acquiring more tokens than capacity raises ValueError."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        
        with pytest.raises(ValueError, match="Cannot acquire 15 tokens, capacity is 10"):
            limiter.acquire(15)

    def test_acquire_blocks_when_insufficient_tokens(self):
        """Test that acquire blocks when insufficient tokens are available."""
        # Small capacity and slow refill rate
        limiter = RateLimiter(capacity=2, refill_rate=1.0)  # 1 token per second
        
        # Drain the bucket
        limiter.acquire(2)
        assert limiter.get_available_tokens() < 0.01  # Allow small timing differences
        
        # Now acquiring should block for ~1 second
        start_time = time.monotonic()
        limiter.acquire(1)
        elapsed = time.monotonic() - start_time
        
        # Should have waited approximately 1 second (within tolerance)
        assert 0.9 <= elapsed <= 1.5

    @pytest.mark.asyncio
    async def test_acquire_async_within_capacity(self):
        """Test async acquire within capacity doesn't block."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        
        start_time = time.monotonic()
        await limiter.acquire_async(5)  # Should not block
        elapsed = time.monotonic() - start_time
        
        assert elapsed < 0.1  # Should be nearly instantaneous
        assert abs(limiter.get_available_tokens() - 5.0) < 0.01  # Allow small timing differences

    @pytest.mark.asyncio
    async def test_acquire_async_more_than_capacity_raises_error(self):
        """Test async acquire more tokens than capacity raises ValueError."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        
        with pytest.raises(ValueError, match="Cannot acquire 15 tokens, capacity is 10"):
            await limiter.acquire_async(15)

    @pytest.mark.asyncio
    async def test_acquire_async_blocks_when_insufficient_tokens(self):
        """Test that async acquire blocks when insufficient tokens are available."""
        # Small capacity and slow refill rate
        limiter = RateLimiter(capacity=2, refill_rate=1.0)  # 1 token per second
        
        # Drain the bucket
        await limiter.acquire_async(2)
        assert limiter.get_available_tokens() < 0.01  # Allow small timing differences
        
        # Now acquiring should block for ~1 second
        start_time = time.monotonic()
        await limiter.acquire_async(1)
        elapsed = time.monotonic() - start_time
        
        # Should have waited approximately 1 second (within tolerance)
        assert 0.9 <= elapsed <= 1.5

    def test_sync_async_behavior_consistency(self):
        """Test that sync and async variants behave identically within tolerance."""
        # Test both sync and async on the same limiter for fairness
        limiter = RateLimiter(capacity=2, refill_rate=1.0)  # 1 token per second
        
        # Test sync behavior first
        limiter.acquire(2)  # Drain bucket
        
        start_time = time.monotonic()
        limiter.acquire(1)  # Should wait ~1 second
        sync_elapsed = time.monotonic() - start_time
        
        # Reset the limiter
        limiter.reset()
        
        # Test async behavior  
        async def async_test():
            await limiter.acquire_async(2)  # Drain bucket
            
            start = time.monotonic()
            await limiter.acquire_async(1)  # Should wait ~1 second
            return time.monotonic() - start
        
        async_elapsed = asyncio.run(async_test())
        
        # Both should take approximately 1 second
        assert 0.9 <= sync_elapsed <= 1.5
        assert 0.9 <= async_elapsed <= 1.5

    def test_notify_retry_after_forces_wait(self):
        """Test that notify_retry_after forces wait for specified duration."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        limiter.set_provider_name("test_provider")
        
        # Should normally acquire immediately
        limiter.acquire(1)
        assert abs(limiter.get_available_tokens() - 9.0) < 0.01  # Allow small timing differences
        
        # Trigger retry-after for 1 second
        start_time = time.monotonic()
        limiter.notify_retry_after(1)
        elapsed = time.monotonic() - start_time
        
        # Should have waited at least 1 second
        assert elapsed >= 1.0
        
        # Bucket should be cleared, but allow for refill during the elapsed time
        # After waiting 1 second with refill_rate=5.0, we expect ~5 tokens to have been added
        available_tokens = limiter.get_available_tokens()
        assert 4.5 <= available_tokens <= 5.5  # Allow tolerance for timing variations

    @pytest.mark.asyncio
    async def test_notify_retry_after_async_forces_wait(self):
        """Test that async notify_retry_after forces wait for specified duration."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        limiter.set_provider_name("test_provider")
        
        # Should normally acquire immediately
        await limiter.acquire_async(1)
        assert abs(limiter.get_available_tokens() - 9.0) < 0.01  # Allow small timing differences
        
        # Trigger retry-after for 1 second
        start_time = time.monotonic()
        await limiter.notify_retry_after_async(1)
        elapsed = time.monotonic() - start_time
        
        # Should have waited at least 1 second
        assert elapsed >= 1.0
        
        # Bucket should be cleared, but allow for refill during the elapsed time
        # After waiting 1 second with refill_rate=5.0, we expect ~5 tokens to have been added
        available_tokens = limiter.get_available_tokens()
        assert 4.5 <= available_tokens <= 5.5  # Allow tolerance for timing variations

    def test_token_refill_over_time(self):
        """Test that tokens are refilled over time at the correct rate."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)  # 5 tokens per second
        
        # Drain bucket
        limiter.acquire(10)
        assert abs(limiter.get_available_tokens()) < 0.01  # Allow small timing differences
        
        # Wait 1 second
        time.sleep(1.0)
        
        # Should have ~5 tokens (5 tokens per second)
        available = limiter.get_available_tokens()
        assert 4.5 <= available <= 5.5  # Allow for timing variation

    def test_capacity_limit(self):
        """Test that tokens don't exceed capacity even with long waits."""
        limiter = RateLimiter(capacity=5, refill_rate=10.0)  # Fast refill, small capacity
        
        # Start with full bucket
        assert limiter.get_available_tokens() == 5.0
        
        # Wait a while - tokens shouldn't exceed capacity
        time.sleep(2.0)
        assert limiter.get_available_tokens() == 5.0

    @pytest.mark.asyncio
    async def test_backward_compatibility_alias(self):
        """Test that async_acquire alias works correctly."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        
        start_time = time.monotonic()
        await limiter.async_acquire()  # Should use acquire_async(1)
        elapsed = time.monotonic() - start_time
        
        assert elapsed < 0.1  # Should be nearly instantaneous
        assert abs(limiter.get_available_tokens() - 9.0) < 0.01  # Allow small timing differences

    def test_metrics_recording(self):
        """Test that metrics are properly recorded during waits."""
        limiter = RateLimiter(capacity=2, refill_rate=10.0)  # Fast refill to minimize test time
        limiter.set_provider_name("test_provider")
        
        # Clear any existing metrics
        RATE_LIMITER_WAITS.clear()
        
        # Drain bucket to force a wait
        limiter.acquire(2)
        
        # This should cause a wait and increment metrics
        limiter.acquire(1)
        
        # Check that metric was recorded
        metric_samples = RATE_LIMITER_WAITS.collect()[0].samples
        test_provider_samples = [
            s for s in metric_samples 
            if s.labels.get('provider') == 'test_provider' and s.labels.get('mode') == 'sync'
        ]
        
        assert len(test_provider_samples) > 0
        assert test_provider_samples[0].value >= 1

    @pytest.mark.asyncio
    async def test_metrics_recording_async(self):
        """Test that metrics are properly recorded during async waits."""
        limiter = RateLimiter(capacity=2, refill_rate=10.0)  # Fast refill to minimize test time
        limiter.set_provider_name("test_provider_async")
        
        # Clear any existing metrics
        RATE_LIMITER_WAITS.clear()
        
        # Drain bucket to force a wait
        await limiter.acquire_async(2)
        
        # This should cause a wait and increment metrics
        await limiter.acquire_async(1)
        
        # Check that metric was recorded
        metric_samples = RATE_LIMITER_WAITS.collect()[0].samples
        test_provider_samples = [
            s for s in metric_samples 
            if s.labels.get('provider') == 'test_provider_async' and s.labels.get('mode') == 'async'
        ]
        
        assert len(test_provider_samples) > 0
        assert test_provider_samples[0].value >= 1

    def test_concurrent_access_sync(self):
        """Test that multiple threads can safely access the rate limiter."""
        limiter = RateLimiter(capacity=10, refill_rate=100.0)  # Fast refill
        results = []
        errors = []

        def worker(worker_id):
            try:
                for _ in range(5):
                    limiter.acquire(1)
                    results.append(worker_id)
            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 25  # 5 workers × 5 acquisitions each

    @pytest.mark.asyncio
    async def test_concurrent_access_async(self):
        """Test that multiple coroutines can safely access the rate limiter."""
        limiter = RateLimiter(capacity=10, refill_rate=100.0)  # Fast refill
        results = []

        async def worker(worker_id):
            for _ in range(5):
                await limiter.acquire_async(1)
                results.append(worker_id)

        # Start multiple coroutines
        tasks = [worker(i) for i in range(5)]
        await asyncio.gather(*tasks)

        # Check results
        assert len(results) == 25  # 5 workers × 5 acquisitions each

    def test_reset_functionality(self):
        """Test that reset() returns the limiter to initial state."""
        limiter = RateLimiter(capacity=10, refill_rate=5.0)
        
        # Modify state
        limiter.acquire(5)
        limiter.notify_retry_after(2)  # This will clear tokens and set retry state
        
        # Reset should restore initial state
        limiter.reset()
        
        assert limiter.get_available_tokens() == 10.0
        
        # Should be able to acquire immediately after reset
        start_time = time.monotonic()
        limiter.acquire(5)
        elapsed = time.monotonic() - start_time
        assert elapsed < 0.1


class TestCreateRateLimiterFromConfig:
    """Test suite for create_rate_limiter_from_config function."""

    def test_create_from_config_basic(self):
        """Test creating rate limiter from configuration."""
        limiter = create_rate_limiter_from_config(
            rate_limit_per_min=60,
            provider_name="test_provider"
        )
        
        assert limiter is not None
        assert limiter.get_capacity() == 60
        assert limiter.get_refill_rate() == 1.0  # 60 per minute = 1 per second

    def test_create_from_config_with_burst_size(self):
        """Test creating rate limiter with custom burst size."""
        limiter = create_rate_limiter_from_config(
            rate_limit_per_min=60,
            burst_size=20,
            provider_name="test_provider"
        )
        
        assert limiter is not None
        assert limiter.get_capacity() == 20  # Custom burst size
        assert limiter.get_refill_rate() == 1.0  # 60 per minute = 1 per second

    def test_create_from_config_disabled(self):
        """Test that None rate limit returns None."""
        limiter = create_rate_limiter_from_config(
            rate_limit_per_min=None,
            provider_name="test_provider"
        )
        assert limiter is None

    def test_create_from_config_zero_rate_limit(self):
        """Test that zero rate limit returns None."""
        limiter = create_rate_limiter_from_config(
            rate_limit_per_min=0,
            provider_name="test_provider"
        )
        assert limiter is None

    def test_create_from_config_negative_rate_limit(self):
        """Test that negative rate limit returns None."""
        limiter = create_rate_limiter_from_config(
            rate_limit_per_min=-10,
            provider_name="test_provider"
        )
        assert limiter is None


class TestRateLimiterIntegration:
    """Integration tests simulating real-world usage patterns."""

    def test_burst_then_steady_state(self):
        """Test burst traffic followed by steady state requests."""
        # Limiter allowing 30 requests per minute (0.5 per second)
        # with burst capacity of 10
        limiter = RateLimiter(capacity=10, refill_rate=0.5)
        
        # Burst: acquire 10 tokens immediately
        start_time = time.monotonic()
        for _ in range(10):
            limiter.acquire(1)
        burst_elapsed = time.monotonic() - start_time
        
        # Burst should be fast
        assert burst_elapsed < 0.1
        assert abs(limiter.get_available_tokens()) < 0.01  # Allow small timing differences
        
        # Subsequent requests should be rate limited
        start_time = time.monotonic()
        limiter.acquire(1)  # Should wait 2 seconds (1 token / 0.5 tokens per second)
        steady_elapsed = time.monotonic() - start_time
        
        assert 1.8 <= steady_elapsed <= 2.5  # Allow for timing variation

    @pytest.mark.asyncio
    async def test_retry_after_integration(self):
        """Test integration of retry-after with normal rate limiting."""
        limiter = RateLimiter(capacity=5, refill_rate=2.0)
        limiter.set_provider_name("integration_test")
        
        # Normal operation
        await limiter.acquire_async(2)
        assert abs(limiter.get_available_tokens() - 3.0) < 0.01  # Allow small timing differences
        
        # Simulate 429 response with Retry-After
        start_time = time.monotonic()
        await limiter.notify_retry_after_async(1)
        elapsed = time.monotonic() - start_time
        
        # Should have waited for retry period
        assert elapsed >= 1.0
        
        # Bucket should be cleared, but allow for refill during the elapsed time
        # After waiting 1 second with refill_rate=2.0, we expect ~2 tokens to have been added
        available_tokens = limiter.get_available_tokens()
        assert 1.5 <= available_tokens <= 2.5  # Allow tolerance for timing variations
        
        # Subsequent requests should work normally after refill
        time.sleep(0.5)  # Allow some refill
        available = limiter.get_available_tokens()
        assert available > 0.0 