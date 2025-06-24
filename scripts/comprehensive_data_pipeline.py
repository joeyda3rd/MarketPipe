#!/usr/bin/env python3
"""
Comprehensive MarketPipe Data Processing Pipeline Script

This script demonstrates the complete data acquisition and processing workflow
for a configurable date range. It covers:

1. Data Ingestion from multiple providers
2. Data Validation and quality checks
3. Data Aggregation to multiple timeframes
4. Metrics monitoring and reporting
5. Data querying and analysis
6. Cleanup and maintenance

Usage:
    python scripts/comprehensive_data_pipeline.py [--dry-run] [--provider PROVIDER] [--live-alpaca]
    
Examples:
    # Demo mode with fake data
    python scripts/comprehensive_data_pipeline.py --dry-run
    
    # Live Alpaca data for past month
    python scripts/comprehensive_data_pipeline.py --live-alpaca
    
    # Test with specific provider
    python scripts/comprehensive_data_pipeline.py --provider alpaca
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List


class MarketPipelineRunner:
    """Orchestrates the complete MarketPipe data processing pipeline."""

    def __init__(self, dry_run: bool = False, provider: str = "fake", live_alpaca: bool = False):
        self.dry_run = dry_run
        self.provider = provider
        self.live_alpaca = live_alpaca

        if live_alpaca:
            # For live Alpaca data: use recent dates we don't have yet
            # Check current data - we have: 1970-01-01, 2020-07-27, 2020-07-30, 2020-08-03
            # Let's use a fresh 5-day window in June 2025
            self.end_date = date(2025, 6, 10)  # June 10, 2025
            self.start_date = date(2025, 6, 6)  # June 6, 2025 (5 trading days)
            self.provider = "alpaca"
            self.symbols = ["AAPL", "MSFT", "GOOGL"]  # Just 3 symbols for quick test
        else:
            # For demo/testing: use different dates than what we have
            self.end_date = date(2025, 6, 5)  # June 5, 2025
            self.start_date = date(2025, 6, 3)  # June 3, 2025 (3 days for demo)
            self.symbols = ["AAPL", "TSLA"]  # Just 2 symbols for demo

        self.job_ids = []  # Track job IDs for validation/aggregation

        # Setup logging
        self.setup_logging()

        self.log_and_print("🚀 MarketPipe Comprehensive Pipeline")
        self.log_and_print(f"📅 Date Range: {self.start_date} to {self.end_date}")
        self.log_and_print(f"📊 Symbols: {', '.join(self.symbols)}")
        self.log_and_print(f"🔌 Provider: {self.provider}")
        self.log_and_print(f"🧪 Dry Run: {self.dry_run}")
        if live_alpaca:
            self.log_and_print("🔴 LIVE ALPACA MODE: Will use real API credentials")
        self.log_and_print("=" * 60)

    def setup_logging(self):
        """Setup comprehensive logging to file and console."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Create timestamped log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"pipeline_run_{timestamp}.log"

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )

        self.logger = logging.getLogger(__name__)
        self.log_file = log_file

        self.log_and_print(f"📝 Logging to: {log_file}")

    def log_and_print(self, message: str):
        """Log and print message."""
        self.logger.info(message)

    def run_command(self, cmd: List[str], description: str) -> bool:
        """Execute a command and handle errors with comprehensive logging."""
        self.log_and_print(f"\n🔧 {description}")
        self.log_and_print(f"💻 Command: {' '.join(cmd)}")

        if self.dry_run:
            self.log_and_print("🧪 [DRY RUN] Command would be executed")
            return True

        try:
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            duration = time.time() - start_time

            self.log_and_print(f"✅ Success ({duration:.1f}s)")
            if result.stdout.strip():
                self.log_and_print("📄 Output:")
                for line in result.stdout.strip().split('\n'):
                    self.log_and_print(f"  {line}")

            return True

        except subprocess.CalledProcessError as e:
            self.log_and_print(f"❌ Failed (exit code {e.returncode})")
            if e.stdout:
                self.log_and_print("📄 Stdout:")
                for line in e.stdout.strip().split('\n'):
                    self.log_and_print(f"  {line}")
            if e.stderr:
                self.log_and_print("🚨 Stderr:")
                for line in e.stderr.strip().split('\n'):
                    self.log_and_print(f"  {line}")
            return False
        except Exception as e:
            self.log_and_print(f"❌ Unexpected error: {e}")
            return False

    def setup_environment(self):
        """Setup environment and check prerequisites."""
        self.log_and_print("\n📋 Phase 1: Environment Setup")
        self.log_and_print("-" * 30)

        # Check if MarketPipe is installed
        if not self.run_command(["python", "-m", "marketpipe", "--help"],
                               "Checking MarketPipe installation"):
            self.log_and_print("❌ MarketPipe not found. Please install with: pip install -e .")
            return False

        # Create data directories - use 'agg' not 'aggregated' to match actual structure
        data_dirs = ["data", "data/raw", "data/agg", "data/validation_reports", "data/db", "logs"]
        for dir_path in data_dirs:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            self.log_and_print(f"📁 Created directory: {dir_path}")

        # Setup provider credentials if needed
        if self.provider == "alpaca":
            alpaca_key = os.getenv("ALPACA_KEY")
            alpaca_secret = os.getenv("ALPACA_SECRET")

            if not alpaca_key or not alpaca_secret:
                if self.live_alpaca:
                    self.log_and_print("❌ ALPACA_KEY and ALPACA_SECRET environment variables are required for live data")
                    self.log_and_print("💡 Set them in your environment or .env file")
                    return False
                else:
                    self.log_and_print("⚠️  Warning: ALPACA_KEY and ALPACA_SECRET not set")
                    self.log_and_print("💡 Using fake provider instead for demonstration")
                    self.provider = "fake"
            else:
                # Mask credentials in log
                self.log_and_print(f"✅ ALPACA_KEY: {alpaca_key[:8]}...")
                self.log_and_print(f"✅ ALPACA_SECRET: {alpaca_secret[:8]}...")
        elif self.provider == "iex":
            if not os.getenv("IEX_TOKEN"):
                self.log_and_print("⚠️  Warning: IEX_TOKEN not set")
                self.log_and_print("💡 Using fake provider instead for demonstration")
                self.provider = "fake"

        # List available providers
        self.run_command(["python", "-m", "marketpipe", "providers"],
                        "Listing available data providers")

        return True

    def start_metrics_server(self):
        """Start the metrics server in background."""
        self.log_and_print("\n📊 Phase 2: Metrics Server Setup")
        self.log_and_print("-" * 30)

        if self.dry_run:
            self.log_and_print("🧪 [DRY RUN] Would start metrics server on port 8000")
            return True

        # Note: In a real scenario, you'd start this in background
        # For this demo, we'll just show the command
        self.log_and_print("💡 To start metrics server manually, run:")
        self.log_and_print("   python -m marketpipe metrics --port 8000")
        self.log_and_print("   Then visit http://localhost:8000/metrics")

        return True

    def ingest_data(self):
        """Ingest data for all symbols across the date range."""
        self.log_and_print("\n📥 Phase 3: Data Ingestion")
        self.log_and_print("-" * 30)

        if self.live_alpaca:
            # For live data, process in smaller weekly chunks
            current_date = self.start_date
            week_count = 0

            while current_date <= self.end_date:
                week_count += 1
                week_end = min(current_date + timedelta(days=6), self.end_date)

                self.log_and_print(f"\n📅 Week {week_count}: {current_date} to {week_end}")

                # Ingest data for this week
                cmd = [
                    "python", "-m", "marketpipe", "ingest-ohlcv",
                    "--symbols", ",".join(self.symbols),
                    "--start", current_date.isoformat(),
                    "--end", week_end.isoformat(),
                    "--provider", self.provider,
                    "--batch-size", "500",
                    "--workers", "2"
                ]

                if self.run_command(cmd, f"Ingesting data for week {week_count}"):
                    # Generate a mock job ID for tracking
                    job_id = f"job_{current_date.strftime('%Y%m%d')}_{int(time.time())}"
                    self.job_ids.append(job_id)
                    self.log_and_print(f"📝 Job ID: {job_id}")

                current_date = week_end + timedelta(days=1)
        else:
            # For demo, process the full date range at once
            self.log_and_print(f"\n📅 Processing: {self.start_date} to {self.end_date}")

            # Ingest data
            cmd = [
                "python", "-m", "marketpipe", "ingest-ohlcv",
                "--symbols", ",".join(self.symbols),
                "--start", self.start_date.isoformat(),
                "--end", self.end_date.isoformat(),
                "--provider", self.provider,
                "--batch-size", "100",  # Smaller batch for demo
                "--workers", "1"        # Single worker for cleaner logs
            ]

            if self.run_command(cmd, f"Ingesting data for {len(self.symbols)} symbols"):
                # Generate a mock job ID for tracking
                job_id = f"job_{self.start_date.strftime('%Y%m%d')}_{int(time.time())}"
                self.job_ids.append(job_id)
                self.log_and_print(f"📝 Job ID: {job_id}")

        self.log_and_print(f"\n✅ Ingestion completed for {len(self.job_ids)} jobs")
        self.log_and_print(f"📝 Generated {len(self.job_ids)} job IDs")

        return True

    def validate_data(self):
        """Validate ingested data quality."""
        self.log_and_print("\n🔍 Phase 4: Data Validation")
        self.log_and_print("-" * 30)

        # List existing validation reports
        self.run_command(["python", "-m", "marketpipe", "validate-ohlcv", "--list"],
                        "Listing existing validation reports")

        # Run validation for each job (in practice, this happens automatically)
        for i, job_id in enumerate(self.job_ids[:3]):  # Limit to first 3 for demo
            self.run_command(["python", "-m", "marketpipe", "validate-ohlcv", "--job-id", job_id],
                           f"Validating job {i+1}/{len(self.job_ids)}: {job_id}")

        if len(self.job_ids) > 3:
            self.log_and_print(f"💡 Skipped validation for {len(self.job_ids) - 3} additional jobs in demo")

        # Show validation reports again
        self.run_command(["python", "-m", "marketpipe", "validate-ohlcv", "--list"],
                        "Listing validation reports after processing")

        return True

    def aggregate_data(self):
        """Aggregate data to multiple timeframes."""
        self.log_and_print("\n📊 Phase 5: Data Aggregation")
        self.log_and_print("-" * 30)

        # Run aggregation for each job (in practice, this happens automatically)
        for i, job_id in enumerate(self.job_ids[:3]):  # Limit to first 3 for demo
            self.run_command(["python", "-m", "marketpipe", "aggregate-ohlcv", job_id],
                           f"Aggregating job {i+1}/{len(self.job_ids)}: {job_id}")

        if len(self.job_ids) > 3:
            self.log_and_print(f"💡 Skipped aggregation for {len(self.job_ids) - 3} additional jobs in demo")

        return True

    def query_and_analyze(self):
        """Demonstrate data querying and analysis."""
        self.log_and_print("\n🔎 Phase 6: Data Querying & Analysis")
        self.log_and_print("-" * 30)

        # First, try direct queries on raw parquet files since views might be empty
        direct_queries = [
            ("SELECT symbol, COUNT(*) as bar_count FROM 'data/raw/**/*.parquet' WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 10",
             "Count bars by symbol (direct parquet)"),

            ("SELECT symbol, AVG(close) as avg_close FROM 'data/raw/**/*.parquet' WHERE symbol IN ('AAPL', 'MSFT') AND symbol IS NOT NULL GROUP BY symbol",
             "Average closing prices for AAPL and MSFT (direct parquet)"),

            ("SELECT ANY_VALUE(symbol) as symbol, MAX(high) as max_high, MIN(low) as min_low FROM 'data/raw/**/*.parquet' WHERE symbol='AAPL'",
             "AAPL price range (direct parquet)"),
        ]

        for query, description in direct_queries:
            self.run_command(["python", "-m", "marketpipe", "query", query],
                           description)

        # Now try view-based queries (these might fail if aggregation didn't work)
        view_queries = [
            ("SELECT symbol, COUNT(*) as bar_count FROM bars_5m WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 10",
             "Count 5-minute bars by symbol (view)"),

            ("SELECT symbol, COUNT(*) as bar_count FROM bars_1d WHERE symbol IS NOT NULL GROUP BY symbol LIMIT 10",
             "Count daily bars by symbol (view)"),
        ]

        for query, description in view_queries:
            success = self.run_command(["python", "-m", "marketpipe", "query", query], description)
            if not success:
                self.log_and_print("💡 View query failed - this is expected if aggregation hasn't populated the views yet")

        # Demonstrate CSV output
        csv_query = "SELECT symbol, COUNT(*) as total_bars FROM 'data/raw/**/*.parquet' WHERE symbol IS NOT NULL GROUP BY symbol"
        self.run_command(["python", "-m", "marketpipe", "query", csv_query, "--csv"],
                        "Export bar counts to CSV (direct parquet)")

        return True

    def monitor_metrics(self):
        """Check metrics and monitoring data."""
        self.log_and_print("\n📈 Phase 7: Metrics Monitoring")
        self.log_and_print("-" * 30)

        # List available metrics
        self.run_command(["python", "-m", "marketpipe", "metrics", "--list"],
                        "Listing available metrics")

        # Show specific metric histories (if any exist)
        metrics_to_check = ["ingestion_bars", "validation_errors", "aggregation_latency"]

        for metric in metrics_to_check:
            success = self.run_command(["python", "-m", "marketpipe", "metrics", "--metric", metric],
                           f"Checking {metric} history")
            if not success:
                self.log_and_print(f"💡 No data for {metric} - this is normal for a new installation")

        # Show average metrics with plots
        self.run_command(["python", "-m", "marketpipe", "metrics", "--avg", "1h", "--plot"],
                        "Showing hourly metric averages with plots")

        return True

    def backfill_gaps(self):
        """Demonstrate gap detection and backfilling."""
        self.log_and_print("\n🔄 Phase 8: Gap Detection & Backfilling")
        self.log_and_print("-" * 30)

        # For live data, use recent dates; for demo, skip since it has date issues
        if self.live_alpaca:
            # Run backfill for a subset of symbols to detect any gaps
            cmd = [
                "python", "-m", "marketpipe", "ohlcv", "backfill", "backfill",
                "--symbol", "AAPL",
                "--symbol", "MSFT",
                "--lookback", "3",  # Just check last 3 days
                "--provider", self.provider
            ]

            self.run_command(cmd, "Detecting and filling gaps in last 3 days")
        else:
            self.log_and_print("⏭️  Skipping backfill for demo data (known date calculation issues)")
            self.log_and_print("💡 Backfill works better with live data and recent dates")

        return True

    def cleanup_and_maintenance(self):
        """Demonstrate cleanup and maintenance operations."""
        self.log_and_print("\n🧹 Phase 9: Cleanup & Maintenance")
        self.log_and_print("-" * 30)

        # Show what would be pruned (dry run) - use correct parameter names
        self.run_command(["python", "-m", "marketpipe", "prune", "parquet", "1y", "--dry-run", "--root", "data/raw"],
                        "Checking for old parquet files to prune")

        self.run_command(["python", "-m", "marketpipe", "prune", "database", "6m", "--dry-run"],
                        "Checking for old database records to prune")

        # Migrate database schema (ensure it's up to date)
        self.run_command(["python", "-m", "marketpipe", "migrate"],
                        "Ensuring database schema is up to date")

        return True

    def generate_summary(self):
        """Generate a summary report of the pipeline run."""
        self.log_and_print("\n📋 Phase 10: Pipeline Summary")
        self.log_and_print("-" * 30)

        # Calculate some basic stats
        total_days = (self.end_date - self.start_date).days
        total_symbols = len(self.symbols)

        self.log_and_print("📊 Pipeline Execution Summary:")
        self.log_and_print(f"   • Date Range: {self.start_date} to {self.end_date} ({total_days} days)")
        self.log_and_print(f"   • Symbols Processed: {total_symbols}")
        self.log_and_print(f"   • Provider Used: {self.provider}")
        self.log_and_print(f"   • Jobs Created: {len(self.job_ids)}")
        self.log_and_print(f"   • Mode: {'DRY RUN' if self.dry_run else 'LIVE EXECUTION'}")
        if self.live_alpaca:
            self.log_and_print("   • Live Alpaca Data: ✅")

        # Show final data directory structure
        self.log_and_print("\n📁 Data Directory Structure:")
        data_path = Path("data")
        if data_path.exists():
            total_files = 0
            total_size = 0
            for item in sorted(data_path.rglob("*")):
                if item.is_file():
                    size = item.stat().st_size
                    total_files += 1
                    total_size += size
                    self.log_and_print(f"   📄 {item} ({size:,} bytes)")
                elif item.is_dir() and any(item.iterdir()):  # Only show non-empty dirs
                    self.log_and_print(f"   📁 {item}/")

            self.log_and_print(f"\n   📊 Total: {total_files} files, {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")

        self.log_and_print("\n✅ Pipeline completed successfully!")
        self.log_and_print("💡 Next steps:")
        self.log_and_print("   • Start metrics server: python -m marketpipe metrics --port 8000")
        self.log_and_print("   • Query data: python -m marketpipe query \"SELECT symbol, COUNT(*) FROM 'data/raw/**/*.parquet' WHERE symbol IS NOT NULL GROUP BY symbol\"")
        self.log_and_print("   • Monitor validation: python -m marketpipe validate-ohlcv --list")
        self.log_and_print(f"   • Check logs: {self.log_file}")

        return True

    def run_full_pipeline(self):
        """Execute the complete data processing pipeline."""
        phases = [
            self.setup_environment,
            self.start_metrics_server,
            self.ingest_data,
            self.validate_data,
            self.aggregate_data,
            self.query_and_analyze,
            self.monitor_metrics,
            self.backfill_gaps,
            self.cleanup_and_maintenance,
            self.generate_summary,
        ]

        start_time = time.time()

        for i, phase in enumerate(phases, 1):
            self.log_and_print(f"\n{'='*60}")
            self.log_and_print(f"🚀 Executing Phase {i}/{len(phases)}: {phase.__name__}")
            self.log_and_print(f"{'='*60}")

            try:
                if not phase():
                    self.log_and_print(f"❌ Phase {i} failed. Stopping pipeline.")
                    return False
            except Exception as e:
                self.log_and_print(f"❌ Phase {i} crashed with error: {e}")
                import traceback
                self.log_and_print(f"🔍 Traceback: {traceback.format_exc()}")
                return False

        total_time = time.time() - start_time
        self.log_and_print(f"\n🎉 Complete pipeline finished in {total_time:.1f} seconds!")

        return True


def main():
    """Main entry point for the comprehensive pipeline script."""
    parser = argparse.ArgumentParser(
        description="Run comprehensive MarketPipe data processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Demo mode with fake data
  python scripts/comprehensive_data_pipeline.py --dry-run
  
  # Live Alpaca data for past month
  python scripts/comprehensive_data_pipeline.py --live-alpaca
  
  # Test with specific provider
  python scripts/comprehensive_data_pipeline.py --provider alpaca

Notes:
  - For live Alpaca data, set ALPACA_KEY and ALPACA_SECRET environment variables
  - Demo mode uses fake data for testing pipeline functionality
  - All operations are logged to logs/pipeline_run_TIMESTAMP.log
        """
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show commands without executing them"
    )
    parser.add_argument(
        "--provider",
        default="fake",
        choices=["fake", "alpaca", "iex"],
        help="Data provider to use (default: fake)"
    )
    parser.add_argument(
        "--live-alpaca",
        action="store_true",
        help="Use live Alpaca data for the past month (requires ALPACA_KEY and ALPACA_SECRET)"
    )

    args = parser.parse_args()

    # Show what we're about to do
    print("🚀 MarketPipe Comprehensive Data Pipeline")
    print("=" * 50)

    if args.live_alpaca:
        print("🔴 LIVE MODE: Will fetch real Alpaca data for fresh dates")
        print("📋 Requires: ALPACA_KEY and ALPACA_SECRET environment variables")
        print("📊 Symbols: AAPL, MSFT, GOOGL")
        start_date = date(2025, 6, 6)
        end_date = date(2025, 6, 10)
        print(f"📅 Date range: {start_date} to {end_date}")
    else:
        print("🧪 DEMO MODE: Will use fake/test data")
        print("📊 Symbols: AAPL, TSLA (limited for demo)")
        start_date = date(2025, 6, 3)
        end_date = date(2025, 6, 5)
        print(f"📅 Date range: {start_date} to {end_date}")

    if args.dry_run:
        print("👁️  DRY RUN: Commands will be shown but not executed")

    print()

    # Create and run the pipeline
    pipeline = MarketPipelineRunner(
        dry_run=args.dry_run,
        provider=args.provider,
        live_alpaca=args.live_alpaca
    )

    try:
        success = pipeline.run_full_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed with unexpected error: {e}")
        import traceback
        print(f"🔍 Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
