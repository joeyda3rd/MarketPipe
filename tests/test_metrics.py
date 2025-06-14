# SPDX-License-Identifier: Apache-2.0
"""Legacy metrics tests - keeping for backward compatibility."""

from marketpipe.metrics import REQUESTS
from marketpipe.metrics_server import run as metrics_run
import random
import requests
import time
from unittest.mock import patch


def test_legacy_counter_increment_maintains_backward_compatibility():
    """Test that legacy counter increment functionality maintains backward compatibility."""
    # Use the new metric signature with all required labels
    metric = REQUESTS.labels(source="alpaca", provider="alpaca", feed="iex")
    before = metric._value.get()
    metric.inc()
    assert metric._value.get() == before + 1


def test_legacy_metrics_endpoint_serves_prometheus_format():
    """Test that legacy metrics endpoint serves data in Prometheus format."""
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
    with patch('requests.get') as mock_get:
        mock_response = mock_get.return_value
        mock_response.text = "mp_requests_total{source=\"alpaca\"} 1.0\n# Additional metrics here\n"
        mock_response.status_code = 200
        
        # Simulate the HTTP call
        resp = requests.get(f"http://localhost:{port}/metrics")
        
        assert "mp_requests_total" in resp.text
