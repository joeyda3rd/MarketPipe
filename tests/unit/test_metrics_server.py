"""Tests for the metrics server module."""

import tempfile
import os
from unittest.mock import patch, Mock
from marketpipe.metrics_server import run, metrics_app


def test_metrics_app_wsgi_interface():
    """Test the WSGI metrics app interface."""
    # Set up temporary multiprocess directory for the test
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.dict(os.environ, {"PROMETHEUS_MULTIPROC_DIR": temp_dir}):
            with patch("prometheus_client.CollectorRegistry") as mock_registry:
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


def test_metrics_server_run_multiprocess_mode():
    """Test metrics server startup in multiprocess mode."""
    # Create a temporary directory for multiprocess metrics
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.dict(os.environ, {"PROMETHEUS_MULTIPROC_DIR": temp_dir}):
            with patch("marketpipe.metrics_server.make_server") as mock_make_server:
                mock_server = Mock()
                mock_make_server.return_value = mock_server

                # Mock serve_forever to avoid infinite loop
                mock_server.serve_forever.side_effect = KeyboardInterrupt()

                try:
                    run(port=8080)
                except KeyboardInterrupt:
                    pass

                # Should create WSGI server
                mock_make_server.assert_called_once_with("", 8080, metrics_app)
                mock_server.serve_forever.assert_called_once()


def test_metrics_server_run_single_process_mode():
    """Test metrics server startup in single process mode."""
    # Ensure no multiprocess env var is set
    with patch.dict(os.environ, {}, clear=False):
        if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
            del os.environ["PROMETHEUS_MULTIPROC_DIR"]

        with patch("marketpipe.metrics_server.start_http_server") as mock_server:
            run(port=8080)

            # Should use single process mode
            mock_server.assert_called_once_with(port=8080)


def test_metrics_server_run_default_port():
    """Test metrics server with default port."""
    # Ensure no multiprocess env var is set
    with patch.dict(os.environ, {}, clear=False):
        if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
            del os.environ["PROMETHEUS_MULTIPROC_DIR"]

        with patch("marketpipe.metrics_server.start_http_server") as mock_server:
            run()  # No port specified

            # Should use default port 8000
            mock_server.assert_called_once_with(port=8000)
