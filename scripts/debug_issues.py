#!/usr/bin/env python3
"""
Debug Issues Script for MarketPipe

This script helps debug the issues identified in the comprehensive pipeline:
1. Metrics server async issues
2. Missing validation reports
3. Aggregation problems
4. Data inspection

Usage:
    python scripts/debug_issues.py
"""

import asyncio
import os
import subprocess
import traceback
from datetime import datetime
from pathlib import Path


def log_and_print(message: str):
    """Print and log message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def run_command(cmd: list, description: str) -> tuple[bool, str, str]:
    """Run command and return success, stdout, stderr."""
    log_and_print(f"🔧 {description}")
    log_and_print(f"💻 Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)

def debug_metrics_async_issue():
    """Debug the metrics async issue."""
    log_and_print("\n🔍 Debugging Metrics Async Issue")
    log_and_print("-" * 40)

    try:
        # Try to import and test metrics directly
        from marketpipe.metrics import SqliteMetricsRepository

        # Test async method directly
        async def test_metrics():
            metrics_db = os.getenv("METRICS_DB_PATH", "data/metrics.db")
            log_and_print(f"📄 Using metrics DB: {metrics_db}")

            repo = SqliteMetricsRepository(metrics_db)
            try:
                # Test list_metric_names
                metrics = await repo.list_metric_names()
                log_and_print(f"✅ Successfully listed {len(metrics)} metrics")
                for metric in metrics[:5]:  # Show first 5
                    log_and_print(f"  📊 {metric}")
                if len(metrics) > 5:
                    log_and_print(f"  ... and {len(metrics) - 5} more")
                return True
            except Exception as e:
                log_and_print(f"❌ Error calling list_metric_names: {e}")
                log_and_print(f"🔍 Traceback: {traceback.format_exc()}")
                return False

        # Run the async test
        success = asyncio.run(test_metrics())
        return success

    except Exception as e:
        log_and_print(f"❌ Failed to import or test metrics: {e}")
        log_and_print(f"🔍 Traceback: {traceback.format_exc()}")
        return False

def debug_data_structure():
    """Debug the data structure and see what was actually created."""
    log_and_print("\n🔍 Debugging Data Structure")
    log_and_print("-" * 40)

    data_path = Path("data")
    if not data_path.exists():
        log_and_print("❌ No data directory found")
        return False

    # Count files and directories
    total_files = 0
    total_size = 0
    parquet_files = []

    for item in data_path.rglob("*"):
        if item.is_file():
            total_files += 1
            total_size += item.stat().st_size
            if item.suffix == ".parquet":
                parquet_files.append(item)

    log_and_print("📊 Data Directory Summary:")
    log_and_print(f"  📄 Total files: {total_files}")
    log_and_print(f"  💾 Total size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
    log_and_print(f"  📊 Parquet files: {len(parquet_files)}")

    # Show some parquet file details
    if parquet_files:
        log_and_print("\n📊 Sample Parquet Files:")
        for pf in parquet_files[:5]:
            size = pf.stat().st_size
            log_and_print(f"  📄 {pf} ({size:,} bytes)")
        if len(parquet_files) > 5:
            log_and_print(f"  ... and {len(parquet_files) - 5} more")

    # Check specific directories
    important_dirs = ["data/raw", "data/aggregated", "data/validation_reports", "data/db"]
    for dir_path in important_dirs:
        path = Path(dir_path)
        if path.exists():
            files = list(path.rglob("*"))
            file_count = len([f for f in files if f.is_file()])
            log_and_print(f"  📁 {dir_path}: {file_count} files")
        else:
            log_and_print(f"  📁 {dir_path}: ❌ Not found")

    return True

def debug_validation_reports():
    """Debug why validation reports are missing."""
    log_and_print("\n🔍 Debugging Validation Reports")
    log_and_print("-" * 40)

    # Check validation reports directory
    reports_dir = Path("data/validation_reports")
    if not reports_dir.exists():
        log_and_print("❌ Validation reports directory doesn't exist")
        return False

    reports = list(reports_dir.glob("*"))
    log_and_print(f"📊 Found {len(reports)} items in validation_reports")

    for report in reports:
        if report.is_file():
            size = report.stat().st_size
            log_and_print(f"  📄 {report.name} ({size:,} bytes)")

    # Try to run validation manually on existing data
    success, stdout, stderr = run_command(
        ["python", "-m", "marketpipe", "validate-ohlcv", "--list"],
        "Listing validation reports"
    )

    if not success:
        log_and_print("❌ Failed to list validation reports")
        log_and_print(f"📄 Stdout: {stdout}")
        log_and_print(f"🚨 Stderr: {stderr}")
    else:
        log_and_print("✅ Validation command succeeded")
        log_and_print(f"📄 Output: {stdout}")

    return success

def debug_aggregation():
    """Debug aggregation issues."""
    log_and_print("\n🔍 Debugging Aggregation")
    log_and_print("-" * 40)

    # Check aggregated data directory
    agg_dir = Path("data/aggregated")
    if not agg_dir.exists():
        log_and_print("❌ Aggregated data directory doesn't exist")
        return False

    agg_files = list(agg_dir.rglob("*"))
    file_count = len([f for f in agg_files if f.is_file()])
    log_and_print(f"📊 Found {file_count} files in aggregated directory")

    # Show some structure
    for item in sorted(agg_files)[:10]:
        if item.is_file():
            size = item.stat().st_size
            log_and_print(f"  📄 {item} ({size:,} bytes)")
        elif item.is_dir():
            log_and_print(f"  📁 {item}/")

    return True

def debug_query_issues():
    """Debug SQL query issues."""
    log_and_print("\n🔍 Debugging Query Issues")
    log_and_print("-" * 40)

    # Try simple queries that should work
    test_queries = [
        "SELECT COUNT(*) as total_files FROM duckdb_files()",
        "SHOW TABLES",
        "SELECT COUNT(*) FROM bars_1m LIMIT 1",
    ]

    for query in test_queries:
        success, stdout, stderr = run_command(
            ["python", "-m", "marketpipe", "query", query],
            f"Testing query: {query[:30]}..."
        )

        if success:
            log_and_print(f"✅ Query succeeded: {stdout.strip()}")
        else:
            log_and_print(f"❌ Query failed: {stderr.strip()}")

def debug_metrics_server():
    """Debug metrics server connection issues."""
    log_and_print("\n🔍 Debugging Metrics Server")
    log_and_print("-" * 40)

    # Try to start server in background briefly
    import threading
    import time

    import requests

    def start_server():
        try:
            from marketpipe.metrics_server import AsyncMetricsServer

            async def run_server():
                server = AsyncMetricsServer(port=8001)  # Use different port
                await server.start()
                await asyncio.sleep(3)  # Run for 3 seconds
                await server.stop()

            asyncio.run(run_server())
        except Exception as e:
            log_and_print(f"❌ Server error: {e}")

    # Start server in thread
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()

    # Give server time to start
    time.sleep(1)

    # Try to connect
    try:
        import requests
        response = requests.get("http://localhost:8001/metrics", timeout=2)
        log_and_print(f"✅ Metrics server responding: {response.status_code}")
        log_and_print(f"📄 Content length: {len(response.text)} bytes")
        return True
    except requests.exceptions.RequestException as e:
        log_and_print(f"❌ Failed to connect to metrics server: {e}")
        return False
    except ImportError:
        log_and_print("⚠️ requests not available, skipping HTTP test")
        return False

def main():
    """Run all debugging checks."""
    log_and_print("🚀 MarketPipe Debugging Script")
    log_and_print("=" * 50)

    # Create logs directory
    Path("logs").mkdir(exist_ok=True)

    # Run all debug checks
    debug_functions = [
        debug_data_structure,
        debug_metrics_async_issue,
        debug_validation_reports,
        debug_aggregation,
        debug_query_issues,
        debug_metrics_server,
    ]

    results = {}
    for debug_func in debug_functions:
        try:
            results[debug_func.__name__] = debug_func()
        except Exception as e:
            log_and_print(f"❌ {debug_func.__name__} failed: {e}")
            results[debug_func.__name__] = False

    # Summary
    log_and_print("\n📊 Debug Summary")
    log_and_print("=" * 50)

    for func_name, success in results.items():
        status = "✅" if success else "❌"
        log_and_print(f"{status} {func_name}: {'PASS' if success else 'FAIL'}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    log_and_print(f"\n🎯 Overall: {passed}/{total} checks passed")

    if passed < total:
        log_and_print("\n💡 Next steps to fix issues:")
        if not results.get("debug_metrics_async_issue", True):
            log_and_print("  • Fix metrics async issues in CLI")
        if not results.get("debug_validation_reports", True):
            log_and_print("  • Debug validation report generation")
        if not results.get("debug_aggregation", True):
            log_and_print("  • Check aggregation process")
        if not results.get("debug_query_issues", True):
            log_and_print("  • Fix SQL query schema issues")
        if not results.get("debug_metrics_server", True):
            log_and_print("  • Debug metrics server startup")

if __name__ == "__main__":
    main()
