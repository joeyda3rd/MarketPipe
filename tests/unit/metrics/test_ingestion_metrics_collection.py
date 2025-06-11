# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ingestion metrics collection functionality."""

from __future__ import annotations

import random
import time
import requests

from marketpipe.metrics import REQUESTS
from marketpipe.metrics_server import run as metrics_run


class TestIngestionMetricsCollection:
    """Test ingestion metrics are collected correctly."""
    
    def test_market_data_provider_request_counter_increments_correctly(self):
        """Test that market data provider request counter increments correctly."""
        metric = REQUESTS.labels("alpaca")
        before = metric._value.get()
        metric.inc()
        assert metric._value.get() == before + 1
    
    def test_metrics_endpoint_serves_ingestion_metrics(self):
        """Test that metrics endpoint serves ingestion metrics correctly."""
        port = random.randint(9000, 10000)
        metrics_run(port=port)
        REQUESTS.labels("alpaca").inc()
        
        # Give the server a moment to start
        for _ in range(10):
            try:
                resp = requests.get(f"http://localhost:{port}/metrics")
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("Metrics server did not start")
        
        assert "mp_requests_total" in resp.text
    
    def test_alpaca_provider_metrics_are_labeled_correctly(self):
        """Test that Alpaca market data provider metrics are labeled correctly."""
        alpaca_metric = REQUESTS.labels("alpaca")
        before = alpaca_metric._value.get()
        alpaca_metric.inc()
        after = alpaca_metric._value.get()
        
        assert after == before + 1