"""Legacy metrics tests - keeping for backward compatibility."""

from marketpipe.metrics import REQUESTS
from marketpipe.metrics_server import run as metrics_run
import random
import requests
import time


def test_legacy_counter_increment_maintains_backward_compatibility():
    """Test that legacy counter increment functionality maintains backward compatibility."""
    metric = REQUESTS.labels("alpaca")
    before = metric._value.get()
    metric.inc()
    assert metric._value.get() == before + 1


def test_legacy_metrics_endpoint_serves_prometheus_format():
    """Test that legacy metrics endpoint serves data in Prometheus format."""
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
