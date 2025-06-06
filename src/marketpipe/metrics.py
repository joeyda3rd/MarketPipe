from prometheus_client import Counter, Histogram, Gauge

REQUESTS = Counter("mp_requests_total", "API requests", ["source"])
ERRORS = Counter("mp_errors_total", "Errors", ["source", "code"])
LATENCY = Histogram("mp_request_latency_seconds", "Latency", ["source"])
BACKLOG = Gauge("mp_backlog_jobs", "Coordinator queue size")

__all__ = ["REQUESTS", "ERRORS", "LATENCY", "BACKLOG"]
