# SPDX-License-Identifier: Apache-2.0
import os
from prometheus_client import start_http_server, generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST
from prometheus_client.multiprocess import MultiProcessCollector
from wsgiref.simple_server import make_server


def metrics_app(environ, start_response):
    """WSGI app for multiprocess metrics."""
    registry = CollectorRegistry()
    MultiProcessCollector(registry)
    data = generate_latest(registry)
    status = '200 OK'
    response_headers = [
        ('Content-type', CONTENT_TYPE_LATEST),
        ('Content-Length', str(len(data)))
    ]
    start_response(status, response_headers)
    return [data]


def run(port: int = 8000) -> None:
    # Check if multiprocess mode is available
    if 'PROMETHEUS_MULTIPROC_DIR' in os.environ:
        # Use multiprocess mode
        httpd = make_server('', port, metrics_app)
        print(f"Prometheus metrics server (multiprocess mode) serving on port {port}")
        httpd.serve_forever()
    else:
        # Use regular single-process mode
        start_http_server(port=port)
