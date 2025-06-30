#!/usr/bin/env python3
"""
Quick demo script to show the metrics server working properly.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path so we can import marketpipe
sys.path.insert(0, str(Path(__file__).parent / "src"))

from marketpipe.cli.metrics_info import serve_metrics_info
from marketpipe.metrics_server import start_async_server, stop_async_server


async def demo_metrics_server():
    """Demonstrate the fixed metrics server."""

    print("🚀 MarketPipe Metrics Server Demo")
    print("=" * 50)

    # Start metrics server on port 8000
    print("\n1. Starting metrics server on http://localhost:8000/metrics...")
    try:
        metrics_server = await start_async_server(port=8000, host="localhost")
        print("   ✅ Metrics server started successfully")
    except RuntimeError as e:
        print(f"   ❌ Error: {e}")
        print("   💡 Tip: Another server may be running. Try a different port.")
        return False

    # Start info server on port 8001
    print("\n2. Starting info server on http://localhost:8001...")
    info_task = asyncio.create_task(serve_metrics_info(port=8001, host="localhost"))
    await asyncio.sleep(1)  # Give it time to start

    print("\n✅ Both servers are now running!")
    print("\n📋 What to do next:")
    print("   • Visit http://localhost:8001 for user-friendly information")
    print("   • Visit http://localhost:8000/metrics for raw Prometheus data")
    print("   • Use curl http://localhost:8000/metrics to see metrics in terminal")
    print("   • Press Ctrl+C to stop both servers")

    try:
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down servers...")
    finally:
        # Clean shutdown
        info_task.cancel()
        await stop_async_server()
        print("✅ All servers stopped successfully")

    return True


if __name__ == "__main__":
    print("Starting MarketPipe metrics demo...")
    print("This will show you how the metrics server works after the fixes.")
    print("\nPress Ctrl+C anytime to stop.\n")

    try:
        success = asyncio.run(demo_metrics_server())
        if success:
            print("\n🎉 Demo completed successfully!")
        else:
            print("\n❌ Demo failed. Check the error messages above.")
    except KeyboardInterrupt:
        print("\n👋 Demo interrupted by user")
