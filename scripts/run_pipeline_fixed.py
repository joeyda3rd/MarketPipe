#!/usr/bin/env python3
"""
Fixed MarketPipe Pipeline Runner

This script addresses the known issues:
1. Sets PYTHONPATH to use source code instead of installed package
2. Loads .env file properly
3. Validates Alpaca credentials before running
4. Provides better error handling and diagnostics

Usage:
    python scripts/run_pipeline_fixed.py --execute
"""

import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path


def setup_environment():
    """Set up the environment for running MarketPipe."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent.absolute()

    # Set PYTHONPATH to use source code
    src_path = str(project_root / "src")
    current_pythonpath = os.environ.get("PYTHONPATH", "")
    if current_pythonpath:
        os.environ["PYTHONPATH"] = f"{src_path}:{current_pythonpath}"
    else:
        os.environ["PYTHONPATH"] = src_path

    print(f"✅ Set PYTHONPATH to: {os.environ['PYTHONPATH']}")

    # Load .env file if it exists
    env_file = project_root / ".env"
    if env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
            print(f"✅ Loaded environment from: {env_file}")
        except ImportError:
            print("⚠️ python-dotenv not available, loading .env manually")
            load_env_manually(env_file)
    else:
        print(f"⚠️ .env file not found at: {env_file}")
        print("   You may need to set ALPACA_KEY and ALPACA_SECRET environment variables")

    # Change to project root
    os.chdir(project_root)
    print(f"✅ Changed to project directory: {project_root}")

def load_env_manually(env_file):
    """Manually load .env file if python-dotenv is not available."""
    try:
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if value and not value.startswith('your_'):  # Skip template values
                        os.environ[key] = value
        print("✅ Loaded .env file manually")
    except Exception as e:
        print(f"⚠️ Error loading .env file: {e}")

def check_credentials():
    """Check if Alpaca credentials are available."""
    alpaca_key = os.environ.get("ALPACA_KEY", "")
    alpaca_secret = os.environ.get("ALPACA_SECRET", "")

    if not alpaca_key or alpaca_key.startswith("your_"):
        print("❌ ALPACA_KEY not set or using template value")
        return False

    if not alpaca_secret or alpaca_secret.startswith("your_"):
        print("❌ ALPACA_SECRET not set or using template value")
        return False

    print("✅ Alpaca credentials found")
    print(f"   ALPACA_KEY: {alpaca_key[:8]}..." if len(alpaca_key) > 8 else "   ALPACA_KEY: (too short)")
    print(f"   ALPACA_SECRET: {alpaca_secret[:8]}..." if len(alpaca_secret) > 8 else "   ALPACA_SECRET: (too short)")
    return True

def run_command(cmd, description, timeout=300):
    """Run a command with proper error handling."""
    print(f"\n🔧 {description}")
    print(f"   Running: {' '.join(cmd)}")

    try:
        start_time = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=Path.cwd(),
            env=os.environ.copy()  # Use updated environment
        )
        execution_time = time.time() - start_time

        if result.returncode == 0:
            print(f"✅ {description} completed in {execution_time:.1f}s")
            if result.stdout.strip():
                print("   Output:")
                for line in result.stdout.strip().split('\n'):
                    print(f"     {line}")
            return True
        else:
            print(f"❌ {description} failed (exit code {result.returncode})")
            if result.stderr.strip():
                print("   Error:")
                for line in result.stderr.strip().split('\n')[:10]:  # Limit error output
                    print(f"     {line}")
            if result.stdout.strip():
                print("   Output:")
                for line in result.stdout.strip().split('\n')[:10]:  # Limit output
                    print(f"     {line}")
            return False

    except subprocess.TimeoutExpired:
        print(f"❌ {description} timed out after {timeout}s")
        return False
    except Exception as e:
        print(f"❌ {description} failed with exception: {e}")
        return False

def test_simple_ingestion():
    """Test with a simple ingestion to verify the system works."""
    print("\n" + "="*60)
    print("🧪 TESTING WITH FAKE PROVIDER")
    print("="*60)

    # Use a recent date that should be valid
    test_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")

    cmd = [
        sys.executable, "-m", "marketpipe", "ingest-ohlcv",
        "--provider", "fake",
        "--symbols", "AAPL",
        "--start", test_date,
        "--end", test_date,
        "--output", "./test_output",
        "--workers", "1",
        "--batch-size", "100"
    ]

    return run_command(cmd, "Test ingestion with fake provider", timeout=60)

def main():
    """Main function."""
    print("🚀 MarketPipe Pipeline Runner (Fixed)")
    print("="*60)

    # Setup environment
    print("\n📦 Setting up environment...")
    setup_environment()

    # Check credentials
    print("\n🔑 Checking credentials...")
    credentials_ok = check_credentials()

    if not credentials_ok:
        print("\n" + "="*60)
        print("❌ CREDENTIAL SETUP REQUIRED")
        print("="*60)
        print("To get Alpaca credentials:")
        print("1. Go to https://alpaca.markets/")
        print("2. Sign up for a free account")
        print("3. Get your API Key and Secret from the dashboard")
        print("4. Create/update .env file:")
        print("   ALPACA_KEY=your_actual_key_here")
        print("   ALPACA_SECRET=your_actual_secret_here")
        print("\nFor now, testing with fake provider...")

        # Test with fake provider
        if test_simple_ingestion():
            print("\n✅ System is working! Set up real credentials to use Alpaca data.")
        else:
            print("\n❌ System test failed. There may be other issues.")
        return

    # Test system first
    print("\n🧪 Testing system...")
    if not test_simple_ingestion():
        print("❌ System test failed, aborting full pipeline")
        return

    # Check command line arguments
    execute = "--execute" in sys.argv
    if not execute:
        print("\n" + "="*60)
        print("🔍 DRY RUN MODE")
        print("="*60)
        print("Add --execute to run the full pipeline")
        print("Example: python scripts/run_pipeline_fixed.py --execute")
        return

    # Run the original pipeline script with fixed environment
    print("\n" + "="*60)
    print("🚀 RUNNING FULL PIPELINE")
    print("="*60)

    cmd = [sys.executable, "scripts/run_full_pipeline.py", "--execute"]
    success = run_command(cmd, "Full pipeline execution", timeout=1800)  # 30 minutes

    if success:
        print("\n🎉 Pipeline completed successfully!")
    else:
        print("\n❌ Pipeline failed. Check the logs above for details.")

if __name__ == "__main__":
    main()
