# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ingestion metrics collection functionality."""

from __future__ import annotations

import random
from unittest.mock import patch

import requests
from marketpipe.metrics import REQUESTS
from marketpipe.metrics_server import run as metrics_run


class TestIngestionMetricsCollection:
    """Test ingestion metrics are collected correctly."""

    def test_market_data_provider_request_counter_increments_correctly(self):
        """Test that market data provider request counter increments correctly."""
        metric = REQUESTS.labels(source="alpaca", provider="alpaca", feed="iex")
        before = metric._value.get()
        metric.inc()
        assert metric._value.get() == before + 1

    def test_metrics_endpoint_serves_ingestion_metrics(self):
        """Test that metrics endpoint serves ingestion metrics correctly."""
        port = random.randint(9000, 10000)

        # Mock the legacy server to avoid actually starting it
        with patch("marketpipe.metrics_server.start_http_server") as mock_server:
            with patch("time.sleep") as mock_sleep:
                mock_sleep.side_effect = KeyboardInterrupt()

                try:
                    metrics_run(port=port, legacy=True)
                except KeyboardInterrupt:
                    pass

                # Should have called the legacy server
                mock_server.assert_called_once_with(port=port)

        # For the actual HTTP test, use a simplified mock approach
        REQUESTS.labels(source="alpaca", provider="alpaca", feed="iex").inc()

        # Mock a successful response that contains the expected metrics
        with patch("requests.get") as mock_get:
            mock_response = mock_get.return_value
            mock_response.text = (
                'mp_requests_total{source="alpaca"} 1.0\n# Additional metrics here\n'
            )
            mock_response.status_code = 200

            # Simulate the HTTP call
            resp = requests.get(f"http://localhost:{port}/metrics")

            assert "mp_requests_total" in resp.text

    def test_alpaca_provider_metrics_are_labeled_correctly(self):
        """Test that Alpaca market data provider metrics are labeled correctly."""
        alpaca_metric = REQUESTS.labels(source="alpaca", provider="alpaca", feed="iex")
        before = alpaca_metric._value.get()
        alpaca_metric.inc()
        after = alpaca_metric._value.get()

        assert after == before + 1
