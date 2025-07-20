"""Tests for the metrics server module."""

import asyncio
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
from marketpipe.metrics_server import AsyncMetricsServer, metrics_app, run


def test_metrics_app_wsgi_interface():
    """Test the WSGI metrics app interface."""
    # Set up temporary multiprocess directory for the test
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.dict(os.environ, {"PROMETHEUS_MULTIPROC_DIR": temp_dir}):
            with patch("prometheus_client.CollectorRegistry"):
                with patch("prometheus_client.multiprocess.MultiProcessCollector"):
                    with patch(
                        "marketpipe.metrics_server.generate_latest",
                        return_value=b"test_metrics",
                    ):

                        # Mock WSGI environ and start_response
                        environ = {}
                        responses = []

                        def start_response(status, headers):
                            responses.append((status, headers))

                        # Call the WSGI app
                        result = metrics_app(environ, start_response)

                        # Should return metrics data
                        assert result == [b"test_metrics"]
                        assert len(responses) == 1
                        assert responses[0][0] == "200 OK"


def test_metrics_server_run_legacy_multiprocess_mode():
    """Test metrics server startup in legacy multiprocess mode."""
    # Create a temporary directory for multiprocess metrics
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.dict(os.environ, {"PROMETHEUS_MULTIPROC_DIR": temp_dir}):
            with patch("marketpipe.metrics_server.make_server") as mock_make_server:
                mock_server = Mock()
                mock_make_server.return_value = mock_server

                # Mock serve_forever to avoid infinite loop
                mock_server.serve_forever.side_effect = KeyboardInterrupt()

                try:
                    run(port=8080, legacy=True)
                except KeyboardInterrupt:
                    pass

                # Should create WSGI server
                mock_make_server.assert_called_once_with("", 8080, metrics_app)
                mock_server.serve_forever.assert_called_once()


def test_metrics_server_run_legacy_single_process_mode():
    """Test metrics server startup in legacy single process mode."""
    # Ensure no multiprocess env var is set
    with patch.dict(os.environ, {}, clear=False):
        if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
            del os.environ["PROMETHEUS_MULTIPROC_DIR"]

        with patch("marketpipe.metrics_server.start_http_server") as mock_server:
            with patch("time.sleep") as mock_sleep:
                mock_sleep.side_effect = KeyboardInterrupt()

                try:
                    run(port=8080, legacy=True)
                except KeyboardInterrupt:
                    pass

                # Should use single process mode
                mock_server.assert_called_once_with(port=8080)


def test_metrics_server_run_legacy_default_port():
    """Test legacy metrics server with default port."""
    # Ensure no multiprocess env var is set
    with patch.dict(os.environ, {}, clear=False):
        if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
            del os.environ["PROMETHEUS_MULTIPROC_DIR"]

        with patch("marketpipe.metrics_server.start_http_server") as mock_server:
            with patch("time.sleep") as mock_sleep:
                mock_sleep.side_effect = KeyboardInterrupt()

                try:
                    run(legacy=True)  # No port specified
                except KeyboardInterrupt:
                    pass

                # Should use default port 8000
                mock_server.assert_called_once_with(port=8000)


def test_metrics_server_run_async_mode():
    """Test metrics server in async mode (non-legacy)."""

    async def mock_async_run():
        server = AsyncMetricsServer(port=8080)
        await server.start()
        await server.stop()

    with patch("marketpipe.metrics_server.start_async_server"):
        with patch("marketpipe.metrics_server.stop_async_server"):
            with patch("asyncio.run") as mock_asyncio_run:
                with patch("asyncio.sleep") as mock_async_sleep:
                    mock_async_sleep.side_effect = KeyboardInterrupt()

                    try:
                        run(port=8080, legacy=False)
                    except KeyboardInterrupt:
                        pass

                    # asyncio.run should have been called
                    mock_asyncio_run.assert_called_once()


@pytest.mark.asyncio
async def test_async_metrics_server_basic_functionality():
    """Test basic async metrics server functionality."""
    server = AsyncMetricsServer(port=9999)  # Use unlikely port

    # Should start with server=None
    assert server.server is None
    assert server._lag_monitor_task is None

    # Mock the lag monitoring method to return a proper task
    async def mock_lag_monitor():
        try:
            await asyncio.sleep(1000)  # Long sleep that will be cancelled
        except asyncio.CancelledError:
            raise

    # Mock asyncio.start_server to avoid actually binding
    with patch("asyncio.start_server") as mock_start_server:
        mock_server = Mock()
        mock_start_server.return_value = mock_server

        with patch.object(server, "_monitor_event_loop_lag", mock_lag_monitor):
            # Start server
            await server.start()

            # Verify state
            assert server.server is mock_server
            assert server._lag_monitor_task is not None

            # Mock server close for cleanup
            mock_server.close = Mock()
            mock_server.wait_closed = Mock()
            mock_server.wait_closed.return_value = asyncio.sleep(0)

            # Stop server
            await server.stop()

            # Verify cleanup
            mock_server.close.assert_called_once()
            assert server.server is None
            assert server._lag_monitor_task is None


def test_async_server_respects_multiprocess_environment():
    """Test that AsyncMetricsServer respects multiprocess environment."""
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.dict(os.environ, {"PROMETHEUS_MULTIPROC_DIR": temp_dir}):
            with patch("marketpipe.metrics_server.MultiProcessCollector") as mock_collector:
                server = AsyncMetricsServer()

                # MultiProcessCollector should have been called
                mock_collector.assert_called_once_with(server._registry)
