#!/usr/bin/env python3
"""
MarketPipe Metrics Dashboard

A proper human-friendly web interface for viewing MarketPipe metrics.
This provides an actual UI instead of raw Prometheus text.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import httpx


async def serve_metrics_dashboard(
    metrics_port: int = 8000, dashboard_port: int = 8001, host: str = "localhost"
) -> None:
    """Serve a proper metrics dashboard with real data visualization."""

    async def fetch_metrics() -> Dict[str, Any]:
        """Fetch and parse metrics from the Prometheus server."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"http://{host}:{metrics_port}/metrics", timeout=5.0
                )
                if response.status_code == 200:
                    return parse_prometheus_metrics(response.text)
                else:
                    return {"error": f"Metrics server returned {response.status_code}"}
        except Exception as e:
            return {"error": f"Failed to fetch metrics: {e}"}

    def parse_prometheus_metrics(text: str) -> Dict[str, Any]:
        """Parse Prometheus exposition format into structured data."""
        metrics = {}
        lines = text.strip().split("\n")

        current_metric = None
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                if line.startswith("# HELP "):
                    current_metric = line.split(" ", 2)[2].split(" ", 1)[0]
                    if current_metric not in metrics:
                        metrics[current_metric] = {
                            "help": " ".join(line.split(" ", 3)[3:])
                            if len(line.split(" ", 3)) > 3
                            else "",
                            "type": "unknown",
                            "values": [],
                        }
                elif line.startswith("# TYPE "):
                    parts = line.split(" ", 3)
                    if len(parts) >= 4:
                        metric_name = parts[2]
                        metric_type = parts[3]
                        if metric_name in metrics:
                            metrics[metric_name]["type"] = metric_type
                continue

            # Parse metric value lines
            if " " in line:
                parts = line.split(" ")
                if len(parts) >= 2:
                    metric_line = parts[0]
                    value = parts[1]

                    # Extract metric name and labels
                    if "{" in metric_line:
                        metric_name = metric_line.split("{")[0]
                        labels_part = metric_line.split("{")[1].rstrip("}")
                    else:
                        metric_name = metric_line
                        labels_part = ""

                    if metric_name not in metrics:
                        metrics[metric_name] = {
                            "help": "",
                            "type": "unknown",
                            "values": [],
                        }

                    try:
                        numeric_value = float(value)
                        metrics[metric_name]["values"].append(
                            {"labels": labels_part, "value": numeric_value}
                        )
                    except ValueError:
                        pass

        return metrics

    def generate_dashboard_html(metrics_data: Dict[str, Any]) -> str:
        """Generate the HTML dashboard with actual metrics data."""
        if "error" in metrics_data:
            error_msg = metrics_data["error"]
            content = f"""
            <div class="error">
                <h2>❌ Unable to fetch metrics</h2>
                <p>{error_msg}</p>
                <p>Make sure the metrics server is running on port {metrics_port}</p>
            </div>
            """
        else:
            # Group metrics by category
            marketpipe_metrics = {
                k: v for k, v in metrics_data.items() if k.startswith("mp_")
            }
            system_metrics = {
                k: v
                for k, v in metrics_data.items()
                if k.startswith(("python_", "process_"))
            }
            other_metrics = {
                k: v
                for k, v in metrics_data.items()
                if not k.startswith(("mp_", "python_", "process_"))
            }

            content = f"""
            <div class="metrics-summary">
                <div class="metric-card">
                    <h3>📊 MarketPipe Metrics</h3>
                    <div class="metric-count">{len(marketpipe_metrics)}</div>
                </div>
                <div class="metric-card">
                    <h3>🐍 Python Metrics</h3>
                    <div class="metric-count">{len(system_metrics)}</div>
                </div>
                <div class="metric-card">
                    <h3>🔧 Other Metrics</h3>
                    <div class="metric-count">{len(other_metrics)}</div>
                </div>
            </div>

            {generate_metrics_section("📊 MarketPipe Application Metrics", marketpipe_metrics)}
            {generate_metrics_section("🐍 Python Runtime Metrics", system_metrics)}
            {generate_metrics_section("🔧 Other Metrics", other_metrics)}
            """

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MarketPipe Metrics Dashboard</title>
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; padding: 20px; background: #f8f9fa; 
        }}
        .header {{ 
            background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        }}
        h1 {{ color: #2c3e50; margin: 0; }}
        .subtitle {{ color: #7f8c8d; margin-top: 5px; }}
        .metrics-summary {{ 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 15px; margin-bottom: 30px; 
        }}
        .metric-card {{ 
            background: white; padding: 20px; border-radius: 8px; text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        }}
        .metric-card h3 {{ margin: 0 0 10px 0; color: #34495e; }}
        .metric-count {{ font-size: 2em; font-weight: bold; color: #3498db; }}
        .metrics-section {{ 
            background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        }}
        .metrics-section h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .metric-item {{ 
            border: 1px solid #ecf0f1; border-radius: 4px; margin: 10px 0; padding: 15px;
            background: #fcfcfc; 
        }}
        .metric-name {{ font-weight: bold; color: #2980b9; font-size: 1.1em; }}
        .metric-help {{ color: #7f8c8d; font-style: italic; margin: 5px 0; }}
        .metric-type {{ 
            display: inline-block; padding: 2px 8px; border-radius: 12px; 
            font-size: 0.8em; font-weight: bold; color: white;
        }}
        .type-counter {{ background: #e74c3c; }}
        .type-gauge {{ background: #f39c12; }}
        .type-histogram {{ background: #9b59b6; }}
        .type-summary {{ background: #1abc9c; }}
        .type-unknown {{ background: #95a5a6; }}
        .metric-values {{ margin-top: 10px; }}
        .metric-value {{ 
            background: #f8f9fa; border-left: 3px solid #3498db; padding: 8px 12px; 
            margin: 5px 0; font-family: 'Monaco', monospace; 
        }}
        .value-number {{ font-weight: bold; color: #27ae60; }}
        .value-labels {{ color: #8e44ad; }}
        .error {{ 
            background: #ffe6e6; border: 1px solid #ff9999; padding: 20px; 
            border-radius: 8px; color: #d63031; 
        }}
        .refresh-info {{ 
            text-align: center; margin-top: 20px; color: #7f8c8d; 
            font-size: 0.9em; 
        }}
        .refresh-button {{ 
            background: #3498db; color: white; border: none; padding: 10px 20px; 
            border-radius: 5px; cursor: pointer; margin-left: 10px; 
        }}
        .refresh-button:hover {{ background: #2980b9; }}
    </style>
    <script>
        function refreshPage() {{
            window.location.reload();
        }}
        // Auto-refresh every 30 seconds
        setTimeout(refreshPage, 30000);
    </script>
</head>
<body>
    <div class="header">
        <h1>📊 MarketPipe Metrics Dashboard</h1>
        <div class="subtitle">Real-time monitoring data • Last updated: {timestamp}</div>
    </div>
    
    {content}
    
    <div class="refresh-info">
        Page auto-refreshes every 30 seconds
        <button class="refresh-button" onclick="refreshPage()">🔄 Refresh Now</button>
    </div>
</body>
</html>"""

    def generate_metrics_section(title: str, metrics: Dict[str, Any]) -> str:
        """Generate HTML for a section of metrics."""
        if not metrics:
            return f"""
            <div class="metrics-section">
                <h2>{title}</h2>
                <p style="color: #7f8c8d; font-style: italic;">No metrics available in this category</p>
            </div>
            """

        items = []
        for name, data in sorted(metrics.items()):
            type_class = f"type-{data['type']}"
            values_html = ""

            if data["values"]:
                # Show first few values, or summary for large lists
                display_values = data["values"][:5]
                for val in display_values:
                    labels_text = f" {{{val['labels']}}}" if val["labels"] else ""
                    values_html += f"""
                    <div class="metric-value">
                        <span class="value-labels">{labels_text}</span>
                        <span class="value-number">{val['value']}</span>
                    </div>
                    """
                if len(data["values"]) > 5:
                    values_html += f"<div style='text-align: center; color: #7f8c8d; font-style: italic;'>... and {len(data['values']) - 5} more values</div>"
            else:
                values_html = "<div style='color: #7f8c8d; font-style: italic;'>No values recorded</div>"

            items.append(
                f"""
            <div class="metric-item">
                <div class="metric-name">{name}</div>
                <span class="metric-type {type_class}">{data['type']}</span>
                {f'<div class="metric-help">{data["help"]}</div>' if data["help"] else ''}
                <div class="metric-values">{values_html}</div>
            </div>
            """
            )

        return f"""
        <div class="metrics-section">
            <h2>{title}</h2>
            {''.join(items)}
        </div>
        """

    async def handle_request(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle HTTP requests for the dashboard."""
        try:
            # Read the HTTP request
            request_data = await reader.read(4096)
            request_str = request_data.decode("utf-8", errors="ignore")

            # Extract the path
            if "\r\n" in request_str:
                request_line = request_str.split("\r\n")[0]
                if " " in request_line:
                    parts = request_line.split(" ")
                    if len(parts) >= 2:
                        method, path = parts[0], parts[1]

                        if method == "GET":
                            # Fetch current metrics
                            metrics_data = await fetch_metrics()

                            # Generate dashboard HTML
                            html_content = generate_dashboard_html(metrics_data)

                            # Send HTTP response
                            response = f"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {len(html_content.encode())}\r\n\r\n{html_content}"
                            writer.write(response.encode())
                            await writer.drain()
                        else:
                            # Method not allowed
                            error_response = "HTTP/1.1 405 Method Not Allowed\r\n\r\n"
                            writer.write(error_response.encode())
                            await writer.drain()
                    else:
                        # Bad request
                        error_response = "HTTP/1.1 400 Bad Request\r\n\r\n"
                        writer.write(error_response.encode())
                        await writer.drain()

            writer.close()
            await writer.wait_closed()

        except Exception:
            # Silently handle connection errors
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # Start the dashboard server
    server = await asyncio.start_server(handle_request, host, dashboard_port)
    print(f"📋 Metrics dashboard started on http://{host}:{dashboard_port}")
    print(f"   Fetching data from metrics server at http://{host}:{metrics_port}")

    try:
        await server.serve_forever()
    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(serve_metrics_dashboard())