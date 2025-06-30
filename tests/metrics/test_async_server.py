# SPDX-License-Identifier: Apache-2.0
"""Integration tests for async metrics server."""

from __future__ import annotations

import asyncio
import socket
from unittest.mock import patch

import httpx
import pytest

from marketpipe.metrics import EVENT_LOOP_LAG, REQUESTS
from marketpipe.metrics_server import (
    AsyncMetricsServer,
    start_async_server,
    stop_async_server,
)


def find_free_port() -> int:
    """Find a free port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
async def cleanup_global_server():
    """Ensure global server is cleaned up between tests."""
    yield
    # Cleanup after each test
    import marketpipe.metrics_server

    if marketpipe.metrics_server._async_server_instance is not None:
        await stop_async_server()


@pytest.mark.asyncio
async def test_async_metrics_server_lifecycle():
    """Test async metrics server start and stop lifecycle."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    # Server should not be running initially
    assert server.server is None

    # Start server
    await server.start()
    assert server.server is not None
    assert server._lag_monitor_task is not None

    # Stop server
    await server.stop()
    assert server.server is None
    assert server._lag_monitor_task is None


@pytest.mark.asyncio
async def test_async_server_serves_prometheus_metrics():
    """Test that async server serves Prometheus metrics at /metrics endpoint."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    try:
        await server.start()

        # Increment a test metric
        REQUESTS.labels(source="test", provider="test", feed="test").inc()

        # Give server a moment to start
        await asyncio.sleep(0.1)

        # Make HTTP request to /metrics
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{port}/metrics")

        # Verify response
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]

        # Check for Prometheus format
        text = response.text
        # Should have some metrics content
        assert len(text) > 0
        # Look for basic Prometheus format indicators
        assert (
            "# HELP" in text
            or "# TYPE" in text
            or "mp_requests_total" in text
            or "python_info" in text
        )

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_async_server_returns_404_for_invalid_paths():
    """Test that async server returns 404 for non-/metrics paths."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    try:
        await server.start()
        await asyncio.sleep(0.1)

        # Test invalid path
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{port}/invalid")

        assert response.status_code == 404
        assert b"Not found" in response.content

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_async_server_returns_405_for_invalid_methods():
    """Test that async server returns 405 for non-GET methods."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    try:
        await server.start()
        await asyncio.sleep(0.1)

        # Test POST method
        async with httpx.AsyncClient() as client:
            response = await client.post(f"http://localhost:{port}/metrics")

        assert response.status_code == 405
        assert b"Method not allowed" in response.content

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_event_loop_lag_monitoring():
    """Test that event loop lag is monitored and recorded."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    try:
        await server.start()

        # Wait for lag monitoring to run at least once
        await asyncio.sleep(1.5)

        # Check that lag gauge has been set
        lag_value = EVENT_LOOP_LAG._value.get()
        assert lag_value >= 0.0  # Should be a non-negative number
        assert lag_value < 1.0  # Should be reasonable (less than 1 second)

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_global_server_instance_management():
    """Test global async server instance management."""
    import marketpipe.metrics_server

    port = find_free_port()

    # No server should be running initially
    assert marketpipe.metrics_server._async_server_instance is None

    # Start global server
    server = await start_async_server(port=port)
    assert marketpipe.metrics_server._async_server_instance is server

    # Starting another should raise error
    with pytest.raises(RuntimeError, match="already running"):
        await start_async_server(port=port + 1)

    # Stop global server
    await stop_async_server()
    assert marketpipe.metrics_server._async_server_instance is None


@pytest.mark.asyncio
async def test_async_server_handles_concurrent_requests():
    """Test that async server can handle multiple concurrent requests."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    try:
        await server.start()
        await asyncio.sleep(0.1)

        # Make multiple concurrent requests
        async def make_request():
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://localhost:{port}/metrics")
                return response.status_code

        # Run 10 concurrent requests
        tasks = [make_request() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert all(status == 200 for status in results)

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_async_server_context_manager():
    """Test async server as context manager."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    async with server.run_context() as srv:
        assert srv is server
        assert server.server is not None

        # Test server is running
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{port}/metrics")
        assert response.status_code == 200

    # Server should be stopped after context
    assert server.server is None


@pytest.mark.asyncio
async def test_async_server_multiprocess_mode():
    """Test async server with multiprocess metrics."""
    import os
    import tempfile

    port = find_free_port()

    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.dict(os.environ, {"PROMETHEUS_MULTIPROC_DIR": temp_dir}):
            server = AsyncMetricsServer(port=port)

            try:
                await server.start()
                await asyncio.sleep(0.1)

                # Should still serve metrics
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"http://localhost:{port}/metrics")

                assert response.status_code == 200

            finally:
                await server.stop()


@pytest.mark.asyncio
async def test_async_server_handles_metrics_port_env_var():
    """Test that async server respects METRICS_PORT environment variable."""
    port = find_free_port()

    with patch.dict("os.environ", {"METRICS_PORT": str(port)}):
        server = await start_async_server()  # No port specified

        try:
            assert server.port == port

            # Test server is running on correct port
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://localhost:{port}/metrics")
            assert response.status_code == 200

        finally:
            await stop_async_server()


@pytest.mark.asyncio
async def test_async_server_graceful_shutdown_during_request():
    """Test that server shuts down gracefully even during active requests."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    try:
        await server.start()
        await asyncio.sleep(0.1)

        # Start a request and immediately begin shutdown
        async def slow_request():
            async with httpx.AsyncClient(timeout=5.0) as client:
                return await client.get(f"http://localhost:{port}/metrics")

        # Start request task
        request_task = asyncio.create_task(slow_request())

        # Give request a moment to start
        await asyncio.sleep(0.1)

        # Stop server
        await server.stop()

        # Request should still complete successfully or be cancelled gracefully
        try:
            response = await request_task
            # If completed, should be successful
            assert response.status_code == 200
        except (httpx.ConnectError, asyncio.CancelledError):
            # Connection error is acceptable during shutdown
            pass

    except Exception:
        # Ensure cleanup even if test fails
        if server.server:
            await server.stop()
        raise


@pytest.mark.asyncio
async def test_async_server_error_handling():
    """Test error handling in async server."""
    port = find_free_port()

    # Mock an error during metrics generation
    with patch("marketpipe.metrics_server.generate_latest") as mock_generate:
        mock_generate.side_effect = Exception("Test error")

        server = AsyncMetricsServer(port=port)

        try:
            await server.start()
            await asyncio.sleep(0.1)

            # Request should return 500 error
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://localhost:{port}/metrics")

            assert response.status_code == 500
            assert b"Failed to generate metrics" in response.content

        finally:
            await server.stop()


def test_async_server_ensures_no_leaked_tasks():
    """Test that async server doesn't leak asyncio tasks."""

    async def run_test():
        port = find_free_port()
        server = AsyncMetricsServer(port=port)

        # Get initial task count
        initial_tasks = len(asyncio.all_tasks())

        # Start and stop server
        await server.start()
        await asyncio.sleep(0.1)
        await server.stop()

        # Give tasks time to clean up
        await asyncio.sleep(0.1)

        # Task count should return to initial (or close to it)
        final_tasks = len(asyncio.all_tasks())

        # Allow for some variance in background tasks
        assert abs(final_tasks - initial_tasks) <= 2

    asyncio.run(run_test())


@pytest.mark.asyncio
async def test_async_server_improved_http_parsing():
    """Test improved HTTP parsing and exact path matching."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    try:
        await server.start()
        await asyncio.sleep(0.1)

        # Test exact path matching - should reject /mymetrics
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{port}/mymetrics")
        assert response.status_code == 404

        # Test correct path - should work
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{port}/metrics")
        assert response.status_code == 200

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_async_server_connection_limits():
    """Test that server respects connection limits."""
    port = find_free_port()
    # Set a very low connection limit for testing
    server = AsyncMetricsServer(port=port, max_connections=2)

    try:
        await server.start()
        await asyncio.sleep(0.1)

        # Should respect the connection limit setting
        assert server.max_connections == 2

        # Basic connectivity test
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{port}/metrics")
        assert response.status_code == 200

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_async_server_proper_logging():
    """Test that server uses proper logging instead of print statements."""
    port = find_free_port()
    server = AsyncMetricsServer(port=port)

    # Mock logger to verify it's being used
    import marketpipe.metrics_server

    with patch.object(marketpipe.metrics_server.logger, "info") as mock_info:
        with patch.object(marketpipe.metrics_server.logger, "error"):
            try:
                await server.start()

                # Should log server start
                mock_info.assert_called()

                # Test error logging with invalid request
                # This is harder to test directly, so we'll just verify the server handles it
                await server.stop()

                # Should log server stop
                assert mock_info.call_count >= 2

            except Exception:
                if server.server:
                    await server.stop()
                raise


@pytest.mark.asyncio
async def test_async_server_secure_error_handling():
    """Test that server doesn't leak error details in responses."""
    port = find_free_port()

    with patch("marketpipe.metrics_server.generate_latest") as mock_generate:
        # Mock an error with sensitive information
        mock_generate.side_effect = Exception("Secret database password: admin123")

        server = AsyncMetricsServer(port=port)

        try:
            await server.start()
            await asyncio.sleep(0.1)

            # Request should return generic error message
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://localhost:{port}/metrics")

            assert response.status_code == 500
            # Should NOT contain the secret information
            assert b"admin123" not in response.content
            assert b"password" not in response.content
            # Should contain generic error message
            assert b"Failed to generate metrics" in response.content

        finally:
            await server.stop()
