#!/usr/bin/env python3
"""
Comprehensive MarketPipe Data Processing Pipeline Script

This script demonstrates the complete data acquisition and processing workflow
for a 3-month window ending yesterday. It covers:

1. Data Ingestion from multiple providers
2. Data Validation and quality checks
3. Data Aggregation to multiple timeframes
4. Metrics monitoring and reporting
5. Data querying and analysis
6. Cleanup and maintenance

Usage:
    python scripts/comprehensive_data_pipeline.py [--dry-run] [--provider PROVIDER]
"""

import os
import sys
import subprocess
import time
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import List, Optional
import argparse


class MarketPipelineRunner:
    """Orchestrates the complete MarketPipe data processing pipeline."""
    
    def __init__(self, dry_run: bool = False, provider: str = "fake"):
        self.dry_run = dry_run
        self.provider = provider
        self.symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "AMZN", "META", "NFLX"]
        
        # Calculate 3-month window ending yesterday
        self.end_date = date.today() - timedelta(days=1)
        self.start_date = self.end_date - timedelta(days=90)
        
        self.job_ids = []  # Track job IDs for validation/aggregation
        
        print(f"🚀 MarketPipe Comprehensive Pipeline")
        print(f"📅 Date Range: {self.start_date} to {self.end_date}")
        print(f"📊 Symbols: {', '.join(self.symbols)}")
        print(f"🔌 Provider: {self.provider}")
        print(f"🧪 Dry Run: {self.dry_run}")
        print("=" * 60)
    
    def run_command(self, cmd: List[str], description: str) -> bool:
        """Execute a command and handle errors."""
        print(f"\n🔧 {description}")
        print(f"💻 Command: {' '.join(cmd)}")
        
        if self.dry_run:
            print("🧪 [DRY RUN] Command would be executed")
            return True
        
        try:
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            duration = time.time() - start_time
            
            print(f"✅ Success ({duration:.1f}s)")
            if result.stdout.strip():
                print(f"📄 Output:\n{result.stdout}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed (exit code {e.returncode})")
            if e.stdout:
                print(f"📄 Stdout:\n{e.stdout}")
            if e.stderr:
                print(f"🚨 Stderr:\n{e.stderr}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    def setup_environment(self):
        """Setup environment and check prerequisites."""
        print("\n📋 Phase 1: Environment Setup")
        print("-" * 30)
        
        # Check if MarketPipe is installed
        if not self.run_command(["python", "-m", "marketpipe", "--help"], 
                               "Checking MarketPipe installation"):
            print("❌ MarketPipe not found. Please install with: pip install -e .")
            return False
        
        # Create data directories
        data_dirs = ["data", "data/raw", "data/aggregated", "data/validation_reports", "data/db"]
        for dir_path in data_dirs:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            print(f"📁 Created directory: {dir_path}")
        
        # Setup provider credentials if needed
        if self.provider == "alpaca":
            if not os.getenv("ALPACA_KEY") or not os.getenv("ALPACA_SECRET"):
                print("⚠️  Warning: ALPACA_KEY and ALPACA_SECRET not set")
                print("💡 Using fake provider instead for demonstration")
                self.provider = "fake"
        elif self.provider == "iex":
            if not os.getenv("IEX_TOKEN"):
                print("⚠️  Warning: IEX_TOKEN not set")
                print("💡 Using fake provider instead for demonstration")
                self.provider = "fake"
        
        # List available providers
        self.run_command(["python", "-m", "marketpipe", "providers"], 
                        "Listing available data providers")
        
        return True
    
    def start_metrics_server(self):
        """Start the metrics server in background."""
        print("\n📊 Phase 2: Metrics Server Setup")
        print("-" * 30)
        
        if self.dry_run:
            print("🧪 [DRY RUN] Would start metrics server on port 8000")
            return True
        
        # Note: In a real scenario, you'd start this in background
        # For this demo, we'll just show the command
        print("💡 To start metrics server manually, run:")
        print("   python -m marketpipe metrics --port 8000")
        print("   Then visit http://localhost:8000/metrics")
        
        return True
    
    def ingest_data(self):
        """Ingest data for all symbols across the date range."""
        print("\n📥 Phase 3: Data Ingestion")
        print("-" * 30)
        
        # Split the 3-month period into weekly chunks for better progress tracking
        current_date = self.start_date
        week_count = 0
        
        while current_date <= self.end_date:
            week_end = min(current_date + timedelta(days=6), self.end_date)
            week_count += 1
            
            print(f"\n📅 Week {week_count}: {current_date} to {week_end}")
            
            # Ingest data for this week
            cmd = [
                "python", "-m", "marketpipe", "ingest-ohlcv",
                "--symbols", ",".join(self.symbols),
                "--start", current_date.isoformat(),
                "--end", week_end.isoformat(),
                "--provider", self.provider,
                "--batch-size", "1000",
                "--workers", "3"
            ]
            
            if self.run_command(cmd, f"Ingesting data for week {week_count}"):
                # Generate a mock job ID for tracking
                job_id = f"job_{current_date.strftime('%Y%m%d')}_{int(time.time())}"
                self.job_ids.append(job_id)
                print(f"📝 Job ID: {job_id}")
            
            current_date = week_end + timedelta(days=1)
        
        print(f"\n✅ Ingestion completed for {week_count} weeks")
        print(f"📝 Generated {len(self.job_ids)} job IDs")
        
        return True
    
    def validate_data(self):
        """Validate ingested data quality."""
        print("\n🔍 Phase 4: Data Validation")
        print("-" * 30)
        
        # List existing validation reports
        self.run_command(["python", "-m", "marketpipe", "validate-ohlcv", "--list"],
                        "Listing existing validation reports")
        
        # Run validation for each job (in practice, this happens automatically)
        for i, job_id in enumerate(self.job_ids[:3]):  # Limit to first 3 for demo
            self.run_command(["python", "-m", "marketpipe", "validate-ohlcv", "--job-id", job_id],
                           f"Validating job {i+1}/{len(self.job_ids)}: {job_id}")
        
        if len(self.job_ids) > 3:
            print(f"💡 Skipped validation for {len(self.job_ids) - 3} additional jobs in demo")
        
        # Show validation reports again
        self.run_command(["python", "-m", "marketpipe", "validate-ohlcv", "--list"],
                        "Listing validation reports after processing")
        
        return True
    
    def aggregate_data(self):
        """Aggregate data to multiple timeframes."""
        print("\n📊 Phase 5: Data Aggregation")
        print("-" * 30)
        
        # Run aggregation for each job (in practice, this happens automatically)
        for i, job_id in enumerate(self.job_ids[:3]):  # Limit to first 3 for demo
            self.run_command(["python", "-m", "marketpipe", "aggregate-ohlcv", job_id],
                           f"Aggregating job {i+1}/{len(self.job_ids)}: {job_id}")
        
        if len(self.job_ids) > 3:
            print(f"💡 Skipped aggregation for {len(self.job_ids) - 3} additional jobs in demo")
        
        return True
    
    def query_and_analyze(self):
        """Demonstrate data querying and analysis."""
        print("\n🔎 Phase 6: Data Querying & Analysis")
        print("-" * 30)
        
        # Sample queries to demonstrate different timeframes and analysis
        queries = [
            ("SELECT symbol, COUNT(*) as bar_count FROM bars_5m GROUP BY symbol LIMIT 10",
             "Count 5-minute bars by symbol"),
            
            ("SELECT symbol, AVG(close) as avg_close FROM bars_1d WHERE symbol IN ('AAPL', 'MSFT') GROUP BY symbol",
             "Average closing prices for AAPL and MSFT"),
            
            ("SELECT symbol, MAX(high) as max_high, MIN(low) as min_low FROM bars_1h WHERE symbol='AAPL'",
             "AAPL price range in hourly data"),
            
            ("SELECT DATE(timestamp) as date, symbol, close FROM bars_1d WHERE symbol='TSLA' ORDER BY date DESC LIMIT 5",
             "Recent TSLA daily closes"),
        ]
        
        for query, description in queries:
            self.run_command(["python", "-m", "marketpipe", "query", query],
                           description)
        
        # Demonstrate CSV output
        csv_query = "SELECT symbol, COUNT(*) as total_bars FROM bars_1d GROUP BY symbol"
        self.run_command(["python", "-m", "marketpipe", "query", csv_query, "--csv"],
                        "Export daily bar counts to CSV")
        
        return True
    
    def monitor_metrics(self):
        """Check metrics and monitoring data."""
        print("\n📈 Phase 7: Metrics Monitoring")
        print("-" * 30)
        
        # List available metrics
        self.run_command(["python", "-m", "marketpipe", "metrics", "--list"],
                        "Listing available metrics")
        
        # Show specific metric histories (if any exist)
        metrics_to_check = ["ingestion_bars", "validation_errors", "aggregation_latency"]
        
        for metric in metrics_to_check:
            self.run_command(["python", "-m", "marketpipe", "metrics", "--metric", metric],
                           f"Checking {metric} history")
        
        # Show average metrics with plots
        self.run_command(["python", "-m", "marketpipe", "metrics", "--avg", "1h", "--plot"],
                        "Showing hourly metric averages with plots")
        
        return True
    
    def backfill_gaps(self):
        """Demonstrate gap detection and backfilling."""
        print("\n🔄 Phase 8: Gap Detection & Backfilling")
        print("-" * 30)
        
        # Run backfill for a subset of symbols to detect any gaps
        cmd = [
            "python", "-m", "marketpipe", "ohlcv", "backfill", "backfill",
            "--symbol", "AAPL",
            "--symbol", "MSFT", 
            "--lookback", "7",  # Just check last week
            "--provider", self.provider
        ]
        
        self.run_command(cmd, "Detecting and filling gaps in last 7 days")
        
        return True
    
    def cleanup_and_maintenance(self):
        """Demonstrate cleanup and maintenance operations."""
        print("\n🧹 Phase 9: Cleanup & Maintenance")
        print("-" * 30)
        
        # Show what would be pruned (dry run)
        self.run_command(["python", "-m", "marketpipe", "prune", "parquet", "1y", "--dry-run"],
                        "Checking for old parquet files to prune")
        
        self.run_command(["python", "-m", "marketpipe", "prune", "database", "6m", "--dry-run"],
                        "Checking for old database records to prune")
        
        # Migrate database schema (ensure it's up to date)
        self.run_command(["python", "-m", "marketpipe", "migrate"],
                        "Ensuring database schema is up to date")
        
        return True
    
    def generate_summary(self):
        """Generate a summary report of the pipeline run."""
        print("\n📋 Phase 10: Pipeline Summary")
        print("-" * 30)
        
        # Calculate some basic stats
        total_days = (self.end_date - self.start_date).days
        total_symbols = len(self.symbols)
        
        print(f"📊 Pipeline Execution Summary:")
        print(f"   • Date Range: {self.start_date} to {self.end_date} ({total_days} days)")
        print(f"   • Symbols Processed: {total_symbols}")
        print(f"   • Provider Used: {self.provider}")
        print(f"   • Jobs Created: {len(self.job_ids)}")
        print(f"   • Mode: {'DRY RUN' if self.dry_run else 'LIVE EXECUTION'}")
        
        # Show final data directory structure
        print(f"\n📁 Data Directory Structure:")
        data_path = Path("data")
        if data_path.exists():
            for item in sorted(data_path.rglob("*")):
                if item.is_file():
                    size = item.stat().st_size
                    print(f"   📄 {item} ({size:,} bytes)")
                elif item.is_dir():
                    print(f"   📁 {item}/")
        
        print(f"\n✅ Pipeline completed successfully!")
        print(f"💡 Next steps:")
        print(f"   • Start metrics server: python -m marketpipe metrics --port 8000")
        print(f"   • Query data: python -m marketpipe query \"SELECT * FROM bars_1d LIMIT 10\"")
        print(f"   • Monitor validation: python -m marketpipe validate-ohlcv --list")
        
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
            print(f"\n{'='*60}")
            print(f"🚀 Executing Phase {i}/{len(phases)}: {phase.__name__}")
            print(f"{'='*60}")
            
            if not phase():
                print(f"❌ Phase {i} failed. Stopping pipeline.")
                return False
        
        total_time = time.time() - start_time
        print(f"\n🎉 Complete pipeline finished in {total_time:.1f} seconds!")
        
        return True


def main():
    """Main entry point for the comprehensive pipeline script."""
    parser = argparse.ArgumentParser(
        description="Run comprehensive MarketPipe data processing pipeline"
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
    
    args = parser.parse_args()
    
    # Create and run the pipeline
    pipeline = MarketPipelineRunner(dry_run=args.dry_run, provider=args.provider)
    
    try:
        success = pipeline.run_full_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed with unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 