#!/usr/bin/env python3
"""
MarketPipe Metrics Server Information

This module provides a simple web interface to explain what the metrics server does
and provide links to useful resources.
"""

from __future__ import annotations

import asyncio


async def serve_metrics_info(port: int = 8001, host: str = "localhost") -> None:
    """Serve a simple info page about the metrics server."""

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MarketPipe Metrics</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white;
                     padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        .metrics-link {{ background: #3498db; color: white; padding: 10px 20px;
                        text-decoration: none; border-radius: 5px; display: inline-block;
                        margin: 10px 5px; }}
        .metrics-link:hover {{ background: #2980b9; }}
        .code {{ background: #f8f9fa; padding: 15px; border-radius: 5px;
                font-family: 'Monaco', monospace; font-size: 14px; overflow-x: auto; }}
        .status {{ color: #27ae60; font-weight: bold; }}
        .warning {{ background: #fff3cd; border: 1px solid #ffeaa7; padding: 15px;
                   border-radius: 5px; margin: 15px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 MarketPipe Metrics Server</h1>

        <div class="status">✅ Metrics server is running on port {port}</div>

        <h2>What is this?</h2>
        <p>This is a <strong>Prometheus metrics server</strong> that exposes MarketPipe performance and operational metrics.
        The metrics are designed to be consumed by monitoring systems like Prometheus, Grafana, or other time-series databases.</p>

        <h2>Available Endpoints</h2>
        <a href="http://localhost:8000/metrics" class="metrics-link">📈 /metrics (Raw Data)</a>
        <a href="http://localhost:{port}" class="metrics-link">ℹ️ / (This Page)</a>

        <h2>What you'll see at /metrics</h2>
        <p>The <code>/metrics</code> endpoint serves data in <strong>Prometheus exposition format</strong>.
        This is plain text that looks like:</p>

        <div class="code"># HELP mp_requests_total API requests
# TYPE mp_requests_total counter
mp_requests_total{{source="alpaca",provider="alpaca",feed="iex"}} 42.0

# HELP mp_latency_seconds Request latency
# TYPE mp_latency_seconds histogram
mp_latency_seconds_bucket{{le="0.1"}} 10.0</div>

        <div class="warning">
            <strong>Note:</strong> This raw metrics data is intended for monitoring systems, not direct human consumption.
            If you want a visual dashboard, consider setting up Grafana to visualize these metrics.
        </div>

        <h2>Available Metrics</h2>
        <ul>
            <li><strong>mp_requests_total</strong> - Total API requests by provider and source</li>
            <li><strong>mp_errors_total</strong> - Total errors by provider and error type</li>
            <li><strong>mp_latency_seconds</strong> - Request latency histograms</li>
            <li><strong>mp_backlog_size</strong> - Current ingestion queue backlog</li>
            <li><strong>marketpipe_event_loop_lag_seconds</strong> - Event loop performance</li>
        </ul>

        <h2>Using with Monitoring Systems</h2>

        <h3>Prometheus Configuration</h3>
        <p>Add this to your <code>prometheus.yml</code>:</p>
        <div class="code">scrape_configs:
  - job_name: 'marketpipe'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 15s</div>

        <h3>Quick Test with curl</h3>
        <div class="code">curl http://localhost:8000/metrics</div>

        <h2>Troubleshooting</h2>
        <ul>
            <li><strong>Port conflicts:</strong> If port 8000 is busy, start with <code>--port 8001</code></li>
            <li><strong>No data:</strong> Metrics are only generated during MarketPipe operations</li>
            <li><strong>Permission issues:</strong> Make sure the port isn't restricted by firewall</li>
        </ul>

        <h2>Command Line Usage</h2>
        <div class="code"># Start metrics server
marketpipe metrics --port 8000

# View metrics in terminal
marketpipe metrics --no-server --format text

# Export metrics to file
marketpipe metrics --no-server --format json > metrics.json</div>
    </div>
</body>
</html>"""

    async def handle_request(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle HTTP requests for the info server."""
        try:
            # Read the request (we don't need to parse it, just respond with info)
            _ = await reader.read(1024)

            # HTTP response
            response = f"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {len(html_content.encode())}\r\n\r\n{html_content}"

            writer.write(response.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        except Exception:
            # Silently handle any connection errors
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # Start the info server
    server = await asyncio.start_server(handle_request, host, port)
    print(f"📋 Metrics info server started on http://{host}:{port}")
    print("   Visit this URL for information about the metrics server")

    try:
        await server.serve_forever()
    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(serve_metrics_info())
