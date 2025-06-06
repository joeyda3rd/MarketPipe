from marketpipe.metrics import REQUESTS
from marketpipe.metrics_server import run as metrics_run
import random
import requests
import time


def test_counter_increment():
    metric = REQUESTS.labels("alpaca")
    before = metric._value.get()
    metric.inc()
    assert metric._value.get() == before + 1


def test_metrics_endpoint_serves():
    port = random.randint(9000, 10000)
    metrics_run(port=port)
    REQUESTS.labels("alpaca").inc()
    # give the server a moment to start
    for _ in range(10):
        try:
            resp = requests.get(f"http://localhost:{port}/metrics")
            break
        except Exception:
            time.sleep(0.1)
    else:
        raise RuntimeError("server did not start")
    assert "mp_requests_total" in resp.text
