# SPDX-License-Identifier: Apache-2.0
"""Utility commands for MarketPipe."""

from __future__ import annotations

import os
import asyncio
from typing import List, Optional
from datetime import datetime, timedelta
from pathlib import Path

import typer

from marketpipe.ingestion.infrastructure.provider_registry import list_providers
from marketpipe.metrics_server import run as metrics_server_run, start_async_server, stop_async_server


def metrics(
    port: int = typer.Option(
        None, "--port", "-p", help="Port to run Prometheus metrics server"
    ),
    legacy_metrics: bool = typer.Option(
        False, "--legacy-metrics", help="Use legacy blocking metrics server"
    ),
    metric: str = typer.Option(
        None, "--metric", "-m", help="Show specific metric history"
    ),
    since: str = typer.Option(
        None, "--since", help="Show metrics since timestamp (e.g., '2024-01-01 10:00')"
    ),
    avg: str = typer.Option(
        None, "--avg", help="Show average metrics over window (e.g., '1h', '1d')"
    ),
    plot: bool = typer.Option(False, "--plot", help="Show ASCII sparkline plots"),
    list_metrics: bool = typer.Option(False, "--list", help="List available metrics"),
):
    """Manage and view MarketPipe metrics.
    
    Examples:
        marketpipe metrics --port 8000                 # Start async Prometheus server
        marketpipe metrics --port 8000 --legacy-metrics # Start legacy blocking server
        marketpipe metrics --list                      # List available metrics
        marketpipe metrics --metric ingestion_bars     # Show metric history
        marketpipe metrics --avg 1h --plot             # Show hourly averages with plot
    """
    from marketpipe.bootstrap import bootstrap
    bootstrap()
    
    try:
        from marketpipe.metrics import SqliteMetricsRepository

        # If port is specified, start metrics server
        if port is not None:
            if legacy_metrics:
                print(f"📊 Starting legacy metrics server on http://localhost:{port}/metrics")
                print("Press Ctrl+C to stop the server")
                metrics_server_run(port=port, legacy=True)
            else:
                print(f"📊 Starting async metrics server on http://localhost:{port}/metrics")
                print("Press Ctrl+C to stop the server")
                
                # Run async server
                async def run_async_server():
                    server = await start_async_server(port=port)
                    try:
                        while True:
                            await asyncio.sleep(1)
                    except KeyboardInterrupt:
                        print("\n📊 Shutting down async metrics server...")
                    finally:
                        await stop_async_server()
                
                try:
                    asyncio.run(run_async_server())
                except KeyboardInterrupt:
                    print("📊 Async metrics server stopped")
            return

        # Setup metrics repository
        metrics_repo = SqliteMetricsRepository(
            os.getenv("METRICS_DB_PATH", "data/metrics.db")
        )

        # Parse since timestamp if provided
        since_ts = None
        if since:
            try:
                since_ts = datetime.fromisoformat(since)
            except ValueError:
                print(f"❌ Invalid timestamp format: {since}")
                print("💡 Use format: 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD'")
                raise typer.Exit(1)

        if list_metrics:
            # List all available metrics
            metrics_list = metrics_repo.list_metric_names()
            if not metrics_list:
                print("📊 No metrics found in database")
                return

            print("📊 Available Metrics:")
            print("=" * 40)
            for metric_name in sorted(metrics_list):
                print(f"  📈 {metric_name}")

            print(f"\nTotal: {len(metrics_list)} metrics")
            print("💡 Use --metric <name> to see history")
            return

        if avg:
            # Show average metrics over time window
            window_seconds = _parse_time_window(avg)
            if not window_seconds:
                print(f"❌ Invalid time window: {avg}")
                print("💡 Use format: '1h', '30m', '1d', etc.")
                raise typer.Exit(1)

            if metric:
                # Show average for specific metric
                averages = asyncio.run(
                    metrics_repo.get_average_metrics(metric, window_seconds, since_ts)
                )
                if not averages:
                    print(f"📊 No data found for metric: {metric}")
                    return

                print(f"📊 Average {metric} (over {avg} windows):")
                print("=" * 50)

                if plot:
                    values = [p.value for p in averages]
                    sparkline = _create_sparkline(values)
                    print(f"Sparkline: {sparkline}")
                    print()

                for point in averages[-20:]:  # Last 20 averages
                    timestamp_str = point.timestamp.strftime("%Y-%m-%d %H:%M")
                    print(f"  {timestamp_str}: {point.value:.2f}")

                if len(averages) > 20:
                    print(f"... and {len(averages) - 20} earlier averages")
            else:
                # Show averages for all metrics
                metrics_list = metrics_repo.list_metric_names()
                print(f"📊 Average metrics over {avg} windows:")
                print("=" * 50)

                for metric_name in sorted(metrics_list)[:10]:
                    averages = asyncio.run(
                        metrics_repo.get_average_metrics(metric_name, window_seconds, since_ts)
                    )
                    if averages:
                        latest_avg = averages[-1]
                        timestamp_str = latest_avg.timestamp.strftime("%Y-%m-%d %H:%M")
                        print(f"{metric_name:30s}: {latest_avg.value:>8.1f} ({timestamp_str})")

                if len(metrics_list) > 10:
                    print(f"... and {len(metrics_list) - 10} more metrics")

            return

        if metric:
            # Show history for specific metric
            points = asyncio.run(metrics_repo.get_metrics_history(metric, since=since_ts))
            if not points:
                print(f"📊 No data found for metric: {metric}")
                print("💡 Check metric name with --list")
                return

            print(f"📊 Metric History: {metric}")
            print("=" * 50)

            if plot:
                values = [p.value for p in points]
                sparkline = _create_sparkline(values)
                print(f"Sparkline: {sparkline}")
                print()

            # Show recent data points
            recent_points = points[-20:]  # Last 20 points
            for point in recent_points:
                timestamp_str = point.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                print(f"  {timestamp_str}: {point.value:.2f}")

            if len(points) > 20:
                print(f"... and {len(points) - 20} earlier points")

            # Show summary stats
            values = [p.value for p in points]
            print("\nSummary:")
            print(f"  Total points: {len(points)}")
            print(f"  Average: {sum(values) / len(values):.2f}")
            print(f"  Min: {min(values):.2f}")
            print(f"  Max: {max(values):.2f}")
            return

        # If no specific option, show recent metrics summary
        metrics_list = metrics_repo.list_metric_names()
        if not metrics_list:
            print("📊 No metrics found in database")
            print("💡 Try: marketpipe metrics --port 8000  # Start async metrics server")
            print("💡 Or:  marketpipe metrics --port 8000 --legacy-metrics  # Start legacy server")
            return

        print("📊 Recent Metrics Summary")
        print("=" * 50)

        # Show latest value for each metric
        for metric_name in sorted(metrics_list)[:10]:  # Top 10 metrics
            points = asyncio.run(metrics_repo.get_metrics_history(metric_name, since=since_ts))
            if points:
                latest = points[0]
                timestamp_str = latest.timestamp.strftime("%Y-%m-%d %H:%M")
                print(f"{metric_name:30s}: {latest.value:>8.1f} ({timestamp_str})")

        if len(metrics_list) > 10:
            print(f"... and {len(metrics_list) - 10} more metrics")

        print("\n💡 Use --list to see all metrics")
        print("💡 Use --metric <name> to see history")
        print("💡 Use --avg 1h to see hourly averages")
        print("💡 Use --port 8000 to start async metrics server")

    except Exception as e:
        print(f"❌ Error querying metrics: {e}")
        raise typer.Exit(1)


def providers():
    """List available market data providers."""
    try:
        available_providers = list_providers()
        if not available_providers:
            print("❌ No providers registered")
            return

        print("📊 Available market data providers:")
        for provider_name in sorted(available_providers):
            print(f"  • {provider_name}")

        print(f"\nTotal: {len(available_providers)} providers")
        print("\n💡 Usage: marketpipe ingest-ohlcv --provider <name> ...")

    except Exception as e:
        print(f"❌ Failed to list providers: {e}")
        raise typer.Exit(1)


def migrate(
    path: Path = typer.Option(
        Path("data/db/core.db"), "--path", "-p", help="Database path to migrate"
    )
):
    """Apply any pending SQLite migrations."""
    try:
        from marketpipe.migrations import apply_pending
        apply_pending(path)
        typer.echo("✅ Migrations up-to-date")
    except Exception as e:
        typer.echo(f"❌ Migration failed: {e}", err=True)
        raise typer.Exit(1)


def _parse_time_window(window_str: str) -> Optional[int]:
    """Parse time window string to seconds."""
    import re

    match = re.match(r"(\d+)([smhdw])", window_str.lower())
    if not match:
        return None

    value, unit = match.groups()
    value = int(value)

    multipliers = {
        "s": 1,  # seconds
        "m": 60,  # minutes
        "h": 3600,  # hours
        "d": 86400,  # days
        "w": 604800,  # weeks
    }

    return value * multipliers.get(unit, 1)


def _create_sparkline(values: List[float]) -> str:
    """Create ASCII sparkline from list of values."""
    if not values:
        return ""

    # Normalize values to 0-7 range for sparkline characters
    min_val = min(values)
    max_val = max(values)

    if min_val == max_val:
        return "▄" * len(values)  # Flat line

    # Sparkline characters from lowest to highest
    chars = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]

    # Normalize and map to characters
    normalized = []
    for val in values:
        norm = (val - min_val) / (max_val - min_val)
        char_index = min(int(norm * len(chars)), len(chars) - 1)
        normalized.append(chars[char_index])

    return "".join(normalized) 