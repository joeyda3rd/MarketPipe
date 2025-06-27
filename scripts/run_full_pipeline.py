#!/usr/bin/env python3
"""
Complete MarketPipe Pipeline Runner for Top 10 Equities

This script runs the entire MarketPipe pipeline (ingest -> validate -> aggregate) 
for the top 10 US equities using live market data from a year prior to yesterday.

Usage:
    python scripts/run_full_pipeline.py --dry-run     # Test configuration
    python scripts/run_full_pipeline.py --execute    # Run live pipeline
    python scripts/run_full_pipeline.py --help       # Show help
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import yaml

# Top 10 US equities by market cap (as of 2024)
TOP_10_EQUITIES = [
    "AAPL",  # Apple Inc.
    "MSFT",  # Microsoft Corporation
    "GOOGL", # Alphabet Inc. Class A
    "AMZN",  # Amazon.com Inc.
    "NVDA",  # NVIDIA Corporation
    "META",  # Meta Platforms Inc.
    "TSLA",  # Tesla Inc.
    "BRK.B", # Berkshire Hathaway Inc. Class B
    "LLY",   # Eli Lilly and Company
    "V"      # Visa Inc.
]


class PipelineRunner:
    """Manages the complete MarketPipe pipeline execution."""

    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.base_dir = Path(__file__).parent.parent
        self.config_path = None
        self.job_id = None

        # Calculate date range (1 year prior to 2 days ago to ensure data availability)
        today = date.today()
        two_days_ago = today - timedelta(days=2)  # Conservative approach for data availability
        one_year_ago = two_days_ago - timedelta(days=365)

        self.start_date = one_year_ago.strftime("%Y-%m-%d")
        self.end_date = two_days_ago.strftime("%Y-%m-%d")

        print(f"📅 Date range: {self.start_date} to {self.end_date}")
        print(f"🔧 Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")

    def create_configuration(self) -> Path:
        """Create optimized configuration file for the pipeline."""
        config_data = {
            "config_version": "1",
            "symbols": TOP_10_EQUITIES,
            "start": self.start_date,
            "end": self.end_date,
            "provider": "alpaca",  # Primary provider with good coverage
            "feed_type": "iex",    # Free tier - use "sip" for premium
            "batch_size": 500,     # Conservative batch size for rate limiting
            "workers": 3,          # Moderate parallelism
            "output_path": str(self.base_dir / "data"),
            "validation": {
                "enabled": True,
                "strict": False    # Allow minor data quality issues
            },
            "aggregation": {
                "timeframes": ["1min", "5min", "15min", "1h", "1d"],
                "enabled": True
            },
            "metrics": {
                "enabled": True,
                "port": 8000
            }
        }

        # Create temporary config file
        config_file = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.yaml',
            prefix='pipeline_config_',
            delete=False,
            dir=self.base_dir / "config"
        )

        yaml.dump(config_data, config_file, default_flow_style=False, indent=2)
        config_file.close()

        self.config_path = Path(config_file.name)
        print(f"📄 Created configuration: {self.config_path}")

        return self.config_path

    def validate_prerequisites(self) -> bool:
        """Validate that all prerequisites are met for live data execution."""
        print("🔍 Validating prerequisites...")

        issues = []

        # Check MarketPipe installation
        try:
            result = subprocess.run(
                ["python", "-m", "marketpipe", "--help"],
                capture_output=True,
                text=True,
                timeout=10,
                cwd=self.base_dir
            )
            if result.returncode != 0:
                issues.append("MarketPipe CLI not accessible")
        except Exception as e:
            issues.append(f"MarketPipe CLI error: {e}")

        # Check for required environment variables (Alpaca credentials)
        required_env_vars = ["ALPACA_KEY", "ALPACA_SECRET"]
        for env_var in required_env_vars:
            if not os.getenv(env_var):
                issues.append(f"Missing environment variable: {env_var}")

        # Check available providers
        try:
            result = subprocess.run(
                ["python", "-m", "marketpipe", "providers"],
                capture_output=True,
                text=True,
                timeout=10,
                cwd=self.base_dir
            )
            if result.returncode == 0:
                if "alpaca" not in result.stdout.lower():
                    issues.append("Alpaca provider not available")
            else:
                issues.append("Could not check available providers")
        except Exception as e:
            issues.append(f"Provider check error: {e}")

        # Check data directory permissions
        data_dir = self.base_dir / "data"
        try:
            data_dir.mkdir(exist_ok=True)
            test_file = data_dir / ".write_test"
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            issues.append(f"Data directory not writable: {e}")

        # Estimate data requirements
        trading_days_per_year = 252
        symbols_count = len(TOP_10_EQUITIES)
        estimated_records = trading_days_per_year * symbols_count * 390  # ~390 minutes per trading day
        estimated_size_mb = estimated_records * 0.1  # Rough estimate

        print("📊 Estimated data:")
        print(f"   - Records: ~{estimated_records:,}")
        print(f"   - Storage: ~{estimated_size_mb:.1f} MB")
        print(f"   - Symbols: {symbols_count}")
        print(f"   - Date range: {(datetime.strptime(self.end_date, '%Y-%m-%d') - datetime.strptime(self.start_date, '%Y-%m-%d')).days} days")

        if issues:
            print("❌ Prerequisites validation failed:")
            for issue in issues:
                print(f"   - {issue}")
            return False

        print("✅ Prerequisites validation passed")
        return True

    def run_command(self, cmd: List[str], description: str, timeout: int = 300) -> Tuple[bool, str, str]:
        """Run a CLI command with comprehensive error handling."""
        print(f"🔧 {description}")

        if self.dry_run:
            print(f"   [DRY RUN] Would execute: {' '.join(cmd)}")
            return True, f"Dry run: {description}", ""

        try:
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=self.base_dir
            )
            execution_time = time.time() - start_time

            if result.returncode == 0:
                print(f"✅ {description} completed in {execution_time:.1f}s")
                return True, result.stdout, result.stderr
            else:
                print(f"❌ {description} failed (exit code {result.returncode})")
                print(f"   Error: {result.stderr}")
                return False, result.stdout, result.stderr

        except subprocess.TimeoutExpired:
            print(f"❌ {description} timed out after {timeout}s")
            return False, "", "Command timed out"
        except Exception as e:
            print(f"❌ {description} error: {e}")
            return False, "", str(e)

    def extract_job_id(self, output: str) -> Optional[str]:
        """Extract job ID from ingestion command output."""
        # Look for various patterns that might contain the job ID
        patterns = [
            "📊 Job ID:",      # Main pattern from CLI output
            "✅ Created job:", # Alternative pattern
            "Job ID:",
            "job_id:",
            "Job started with ID:",
            "Ingestion job:",
        ]

        for line in output.split('\n'):
            for pattern in patterns:
                if pattern in line:
                    # Extract the job ID (usually a UUID or similar)
                    parts = line.split(pattern)
                    if len(parts) > 1:
                        job_id = parts[1].strip().split()[0]
                        # Basic validation that it looks like a job ID
                        if len(job_id) > 5:
                            return job_id

        # If no explicit job ID found, look for UUID-like patterns
        import re
        uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        matches = re.findall(uuid_pattern, output)
        if matches:
            return matches[-1]  # Return the last UUID found

        return None

    def _get_latest_job_id(self) -> Optional[str]:
        """Get the latest job ID from the database."""
        try:
            cmd = [
                "python", "-m", "marketpipe", "query",
                "SELECT job_id FROM ingestion_jobs ORDER BY created_at DESC LIMIT 1",
                "--csv"
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=self.base_dir
            )

            if result.returncode == 0 and result.stdout.strip():
                # Parse CSV output to get job ID
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:  # Skip header
                    return lines[1].strip()

        except Exception as e:
            print(f"   Could not query latest job ID: {e}")

        return None

    def run_ingestion(self) -> bool:
        """Run the data ingestion phase."""
        print("\n" + "="*60)
        print("📥 PHASE 1: DATA INGESTION")
        print("="*60)

        cmd = [
            "python", "-m", "marketpipe", "ingest-ohlcv",
            "--config", str(self.config_path)
        ]

        # Extended timeout for ingestion (can take a long time for a year of data)
        success, stdout, stderr = self.run_command(
            cmd,
            f"Ingesting data for {len(TOP_10_EQUITIES)} symbols over 1 year",
            timeout=3600  # 1 hour timeout
        )

        if not success:
            return False

        # Extract job ID for subsequent phases
        self.job_id = self.extract_job_id(stdout)
        if self.job_id:
            print(f"🆔 Extracted Job ID: {self.job_id}")
        else:
            print("⚠️  Could not extract job ID from ingestion output")
            if not self.dry_run:
                # Try to get the latest job ID from database
                latest_job_id = self._get_latest_job_id()
                if latest_job_id:
                    print(f"🆔 Using latest job ID from database: {latest_job_id}")
                    self.job_id = latest_job_id
                else:
                    print("   You may need to manually run validation and aggregation")

        return True

    def run_validation(self) -> bool:
        """Run the data validation phase."""
        print("\n" + "="*60)
        print("🔍 PHASE 2: DATA VALIDATION")
        print("="*60)

        if not self.job_id:
            print("⚠️  No job ID available - trying to run validation without job ID")
            cmd = ["python", "-m", "marketpipe", "validate-ohlcv"]
        else:
            cmd = [
                "python", "-m", "marketpipe", "validate-ohlcv",
                "--job-id", self.job_id
            ]

        success, stdout, stderr = self.run_command(
            cmd,
            "Validating data quality and generating reports",
            timeout=600  # 10 minute timeout
        )

        return success

    def run_aggregation(self) -> bool:
        """Run the data aggregation phase."""
        print("\n" + "="*60)
        print("📊 PHASE 3: DATA AGGREGATION")
        print("="*60)

        if not self.job_id:
            print("❌ No job ID available - cannot run aggregation")
            print("   Please run aggregation manually with the correct job ID")
            return False

        cmd = [
            "python", "-m", "marketpipe", "aggregate-ohlcv",
            self.job_id
        ]

        success, stdout, stderr = self.run_command(
            cmd,
            "Aggregating data to multiple timeframes",
            timeout=1800  # 30 minute timeout
        )

        return success

    def run_health_check(self) -> bool:
        """Run health check to validate the installation."""
        print("\n" + "="*60)
        print("🏥 HEALTH CHECK")
        print("="*60)

        cmd = ["python", "-m", "marketpipe", "health-check", "--verbose"]

        success, stdout, stderr = self.run_command(
            cmd,
            "Running MarketPipe health check",
            timeout=120
        )

        return success

    def generate_summary(self, ingestion_success: bool, validation_success: bool, aggregation_success: bool):
        """Generate execution summary report."""
        print("\n" + "="*60)
        print("📋 EXECUTION SUMMARY")
        print("="*60)

        print(f"🔧 Mode: {'DRY RUN' if self.dry_run else 'LIVE EXECUTION'}")
        print(f"📅 Date Range: {self.start_date} to {self.end_date}")
        print(f"📈 Symbols: {', '.join(TOP_10_EQUITIES)}")
        print(f"📄 Configuration: {self.config_path}")

        if self.job_id:
            print(f"🆔 Job ID: {self.job_id}")

        print("\n📊 Phase Results:")
        print(f"   Ingestion:   {'✅ SUCCESS' if ingestion_success else '❌ FAILED'}")
        print(f"   Validation:  {'✅ SUCCESS' if validation_success else '❌ FAILED'}")
        print(f"   Aggregation: {'✅ SUCCESS' if aggregation_success else '❌ FAILED'}")

        overall_success = ingestion_success and validation_success and aggregation_success

        if overall_success:
            print("\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            if not self.dry_run:
                print(f"   Data available in: {self.base_dir / 'data'}")
                print("   Query data with: python -m marketpipe query 'SELECT * FROM aggregated_ohlcv LIMIT 10'")
        else:
            print("\n⚠️  PIPELINE COMPLETED WITH ISSUES")
            if not self.dry_run:
                print("   Check logs and re-run failed phases manually")

        # Cleanup temporary config file
        if self.config_path and self.config_path.exists():
            if not self.dry_run:
                print(f"\n🧹 Cleaning up temporary config file: {self.config_path}")
                self.config_path.unlink()
            else:
                print(f"\n📁 Config file (will be cleaned up in live run): {self.config_path}")

    def run_full_pipeline(self) -> bool:
        """Execute the complete pipeline."""
        print("🚀 Starting MarketPipe Full Pipeline for Top 10 Equities")
        print("="*60)

        # Create configuration
        self.create_configuration()

        # Validate prerequisites
        if not self.validate_prerequisites():
            return False

        # Run health check first
        health_success = self.run_health_check()
        if not health_success and not self.dry_run:
            print("⚠️  Health check failed - continuing anyway")

        # Execute pipeline phases
        ingestion_success = self.run_ingestion()
        validation_success = self.run_validation() if ingestion_success else False
        aggregation_success = self.run_aggregation() if ingestion_success else False

        # Generate summary
        self.generate_summary(ingestion_success, validation_success, aggregation_success)

        return ingestion_success and validation_success and aggregation_success


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run complete MarketPipe pipeline for top 10 US equities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Test the pipeline configuration (dry run)
    python scripts/run_full_pipeline.py --dry-run

    # Execute the full pipeline with live data
    python scripts/run_full_pipeline.py --execute

    # Show this help message
    python scripts/run_full_pipeline.py --help

Prerequisites:
    - MarketPipe installed and configured
    - Alpaca API credentials in environment variables:
      * ALPACA_KEY
      * ALPACA_SECRET
    - Sufficient disk space for ~1 year of OHLCV data

The script will:
    1. Create optimized configuration for top 10 equities
    2. Run health check to validate setup
    3. Ingest 1 year of OHLCV data (prior to yesterday)
    4. Validate data quality
    5. Aggregate to multiple timeframes (1min, 5min, 15min, 1h, 1d)
    6. Generate execution summary

Data will be stored in the 'data/' directory with partitioned Parquet files.
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Test configuration and show commands without executing"
    )
    group.add_argument(
        "--execute",
        action="store_true",
        help="Execute the full pipeline with live data"
    )

    args = parser.parse_args()

    try:
        runner = PipelineRunner(dry_run=args.dry_run)
        success = runner.run_full_pipeline()

        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\n🛑 Pipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
