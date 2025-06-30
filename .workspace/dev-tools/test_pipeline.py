#!/usr/bin/env python3
"""
Simple MarketPipe Pipeline Test

Test the basic MarketPipe functionality with fake data.
"""

import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path


def run_command(cmd, description):
    """Run a command and show results."""
    print(f"🔧 {description}")
    print(f"   Running: {' '.join(cmd)}")

    try:
        start_time = time.time()
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, cwd=Path(__file__).parent.parent
        )
        execution_time = time.time() - start_time

        if result.returncode == 0:
            print(f"✅ {description} completed in {execution_time:.1f}s")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()[:200]}...")
            return True
        else:
            print(f"❌ {description} failed (exit code {result.returncode})")
            if result.stderr.strip():
                print(f"   Error: {result.stderr.strip()[:200]}...")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()[:200]}...")
            return False

    except subprocess.TimeoutExpired:
        print(f"❌ {description} timed out after 60s")
        return False
    except Exception as e:
        print(f"❌ {description} failed with exception: {e}")
        return False


def main():
    """Run a simple pipeline test."""
    print("🧪 MarketPipe Simple Pipeline Test")
    print("=" * 50)

    # Test 1: Check MarketPipe CLI
    success1 = run_command(
        ["python", "-m", "marketpipe", "--help"], "Testing MarketPipe CLI access"
    )

    if not success1:
        print("❌ MarketPipe CLI not working - stopping test")
        return False

    # Test 2: List providers
    success2 = run_command(["python", "-m", "marketpipe", "providers"], "Testing provider listing")

    # Test 3: Run migrations
    success3 = run_command(["python", "-m", "marketpipe", "migrate"], "Testing database migrations")

    # Test 4: Try a simple ingestion with fake provider and recent date
    today = date.today()
    yesterday = today - timedelta(days=1)
    test_date = yesterday.strftime("%Y-%m-%d")

    success4 = run_command(
        [
            "python",
            "-m",
            "marketpipe",
            "ingest-ohlcv",
            "--provider",
            "fake",
            "--symbols",
            "MSFT",
            "--start",
            test_date,
            "--end",
            test_date,
            "--output",
            "./test_simple_output",
            "--workers",
            "1",
            "--batch-size",
            "100",
        ],
        f"Testing ingestion with fake provider for {test_date}",
    )

    # Summary
    print("\n" + "=" * 50)
    print("📋 Test Summary")
    print("=" * 50)

    tests = [
        ("CLI Access", success1),
        ("Provider Listing", success2),
        ("Database Migrations", success3),
        ("Fake Data Ingestion", success4),
    ]

    passed = sum(1 for _, success in tests if success)
    total = len(tests)

    for test_name, success in tests:
        print(f"   {test_name}: {'✅ PASS' if success else '❌ FAIL'}")

    print(f"\nResult: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! MarketPipe is working correctly.")
        print("   You can now set up Alpaca credentials for real data.")
    else:
        print("⚠️  Some tests failed. Check the errors above.")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
