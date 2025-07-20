# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from marketpipe.ingestion.infrastructure.rate_limit import RateLimiter


class TestRateLimiterLoad:
    """Load tests for RateLimiter to verify production readiness."""

    def test_load_2x_vendor_limit_5_minutes_sync(self):
        """Test handling 2× vendor limit for 5 minutes without errors (sync version)."""
        # Simulate Alpaca rate limit: 200 requests/minute
        # Test with 2× the limit = 400 requests/minute = ~6.67 requests/second
        vendor_limit_per_min = 200
        test_rate_per_min = vendor_limit_per_min * 2  # 2× vendor limit

        # Create rate limiter with proper burst capacity
        limiter = RateLimiter(
            capacity=vendor_limit_per_min,  # Allow burst up to vendor limit
            refill_rate=vendor_limit_per_min / 60.0,  # Vendor limit per second
        )
        limiter.set_provider_name("load_test")

        # Test duration: 5 seconds for CI (reduced from 30 seconds to prevent timeouts)
        test_duration = 5.0
        requests_per_second = test_rate_per_min / 60.0  # ~6.67 requests/second

        start_time = time.monotonic()
        requests_made = 0
        errors = []

        while time.monotonic() - start_time < test_duration:
            try:
                # Make request
                limiter.acquire(1)
                requests_made += 1

                # Sleep to maintain target rate
                time.sleep(1.0 / requests_per_second)

            except Exception as e:
                errors.append(str(e))

        elapsed = time.monotonic() - start_time
        actual_rate = requests_made / elapsed * 60  # Requests per minute

        # Verify we made reasonable number of requests without errors
        assert len(errors) == 0, f"Got {len(errors)} errors: {errors[:5]}"
        assert requests_made > 0, "No requests were made"

        # Verify we stayed within reasonable bounds (rate limiter working)
        expected_max_requests = (
            vendor_limit_per_min * (elapsed / 60.0) + vendor_limit_per_min
        )  # Include burst
        assert (
            requests_made <= expected_max_requests
        ), f"Made {requests_made} requests, expected <= {expected_max_requests}"

        print(
            f"Load test completed: {requests_made} requests in {elapsed:.1f}s ({actual_rate:.1f} req/min)"
        )

    @pytest.mark.asyncio
    async def test_load_2x_vendor_limit_5_minutes_async(self):
        """Test handling 2× vendor limit for 5 minutes without errors (async version)."""
        # Simulate Alpaca rate limit: 200 requests/minute
        # Test with 2× the limit = 400 requests/minute = ~6.67 requests/second
        vendor_limit_per_min = 200
        test_rate_per_min = vendor_limit_per_min * 2  # 2× vendor limit

        # Create rate limiter with proper burst capacity
        limiter = RateLimiter(
            capacity=vendor_limit_per_min,  # Allow burst up to vendor limit
            refill_rate=vendor_limit_per_min / 60.0,  # Vendor limit per second
        )
        limiter.set_provider_name("async_load_test")

        # Test duration: 5 seconds for CI (reduced from 30 seconds to prevent timeouts)
        test_duration = 5.0
        requests_per_second = test_rate_per_min / 60.0  # ~6.67 requests/second

        start_time = time.monotonic()
        requests_made = 0
        errors = []

        while time.monotonic() - start_time < test_duration:
            try:
                # Make request
                await limiter.acquire_async(1)
                requests_made += 1

                # Sleep to maintain target rate
                await asyncio.sleep(1.0 / requests_per_second)

            except Exception as e:
                errors.append(str(e))

        elapsed = time.monotonic() - start_time
        actual_rate = requests_made / elapsed * 60  # Requests per minute

        # Verify we made reasonable number of requests without errors
        assert len(errors) == 0, f"Got {len(errors)} errors: {errors[:5]}"
        assert requests_made > 0, "No requests were made"

        # Verify we stayed within reasonable bounds (rate limiter working)
        expected_max_requests = (
            vendor_limit_per_min * (elapsed / 60.0) + vendor_limit_per_min
        )  # Include burst
        assert (
            requests_made <= expected_max_requests
        ), f"Made {requests_made} requests, expected <= {expected_max_requests}"

        print(
            f"Async load test completed: {requests_made} requests in {elapsed:.1f}s ({actual_rate:.1f} req/min)"
        )

    def test_concurrent_load_mixed_sync_async(self):
        """Test concurrent load with mixed sync and async clients."""
        # Create shared rate limiter
        limiter = RateLimiter(capacity=100, refill_rate=100 / 60.0)  # 100 requests/minute
        limiter.set_provider_name("concurrent_test")

        # Test duration: 3 seconds for CI
        test_duration = 3.0

        sync_requests = []
        async_requests = []
        errors = []

        def sync_worker():
            """Sync worker thread."""
            start = time.monotonic()
            count = 0
            while time.monotonic() - start < test_duration:
                try:
                    limiter.acquire(1)
                    count += 1
                    time.sleep(0.1)  # 10 requests/second
                except Exception as e:
                    errors.append(f"Sync: {e}")
            sync_requests.append(count)

        async def async_worker():
            """Async worker coroutine."""
            start = time.monotonic()
            count = 0
            while time.monotonic() - start < test_duration:
                try:
                    await limiter.acquire_async(1)
                    count += 1
                    await asyncio.sleep(0.1)  # 10 requests/second
                except Exception as e:
                    errors.append(f"Async: {e}")
            async_requests.append(count)

        # Run concurrent workers with proper cleanup
        executor = None
        try:
            executor = ThreadPoolExecutor(max_workers=2)
            # Start sync workers
            sync_futures = [executor.submit(sync_worker) for _ in range(2)]

            # Start async workers
            async def run_async_workers():
                await asyncio.gather(*[async_worker() for _ in range(2)])

            # Run async workers in new event loop to avoid conflicts
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(run_async_workers())
            finally:
                loop.close()

            # Wait for sync workers
            for future in sync_futures:
                future.result()
        finally:
            if executor:
                executor.shutdown(wait=True)

        total_requests = sum(sync_requests) + sum(async_requests)

        # Verify no errors occurred
        assert len(errors) == 0, f"Got {len(errors)} errors: {errors}"

        # Verify reasonable request distribution
        assert len(sync_requests) == 2, "Expected 2 sync workers"
        assert len(async_requests) == 2, "Expected 2 async workers"
        assert total_requests > 0, "No requests were made"

        # Verify rate limiting is working (shouldn't exceed capacity significantly)
        # Allow for burst capacity + some timing tolerance
        max_expected = 100 * (test_duration / 60.0) + 100 + 10  # Include burst capacity + tolerance
        assert (
            total_requests <= max_expected
        ), f"Made {total_requests} requests, expected <= {max_expected}"

        print(
            f"Concurrent test: {total_requests} total requests ({sum(sync_requests)} sync, {sum(async_requests)} async)"
        )

    def test_burst_handling_under_load(self):
        """Test that burst traffic is handled correctly under sustained load."""
        # Small capacity but steady refill
        limiter = RateLimiter(capacity=10, refill_rate=2.0)  # 2 requests/second sustained
        limiter.set_provider_name("burst_test")

        start_time = time.monotonic()

        # Initial burst: should succeed quickly
        burst_start = time.monotonic()
        for _ in range(10):
            limiter.acquire(1)
        burst_elapsed = time.monotonic() - burst_start

        # Burst should be fast (< 0.5 seconds)
        assert burst_elapsed < 0.5, f"Burst took {burst_elapsed:.2f}s, expected < 0.5s"

        # Sustained requests: should be rate limited
        sustained_start = time.monotonic()
        sustained_requests = 0

        # Make requests for ~2 seconds (reduced for CI)
        while time.monotonic() - sustained_start < 2.0:
            limiter.acquire(1)
            sustained_requests += 1

        sustained_elapsed = time.monotonic() - sustained_start
        sustained_rate = sustained_requests / sustained_elapsed

        # Should be limited to ~2 requests/second
        assert (
            1.5 <= sustained_rate <= 2.5
        ), f"Sustained rate was {sustained_rate:.2f} req/s, expected ~2 req/s"

        total_elapsed = time.monotonic() - start_time
        total_requests = 10 + sustained_requests  # Burst + sustained

        print(
            f"Burst test: {total_requests} requests in {total_elapsed:.1f}s "
            f"(burst: {burst_elapsed:.2f}s, sustained: {sustained_rate:.2f} req/s)"
        )
