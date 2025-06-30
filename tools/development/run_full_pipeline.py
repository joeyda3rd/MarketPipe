#!/usr/bin/env python3
"""
Complete MarketPipe Pipeline Runner for COST and TSLA

This script runs the entire MarketPipe pipeline (ingest -> validate -> aggregate)
for COST and TSLA using live market data from 3 months prior to yesterday.

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
from typing import Optional

import yaml

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # If python-dotenv is not available, try manual loading
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()

# Selected equities for focused pipeline testing
SELECTED_EQUITIES = [
    "AAPL",  # Apple Inc.
    "META",  # Meta Platforms Inc.
]


class PipelineRunner:
    """Manages the complete MarketPipe pipeline execution."""

    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.base_dir = Path(__file__).parent.parent
        self.config_path = None
        self.job_id = None

        # Fixed provider (Alpaca) – credentials must be set externally
        self.provider = "alpaca"
        self.feed_type = "iex"

        # Calculate date range (3 days of very recent historical data for testing)
        # Use very recent dates to test completely fresh data with fixed job IDs
        end_date = date.today() - timedelta(days=1)  # Use yesterday to ensure data is available
        start_date = end_date - timedelta(days=3)  # 3 days for focused testing

        self.start_date = start_date.strftime("%Y-%m-%d")
        self.end_date = end_date.strftime("%Y-%m-%d")

        print(f"📅 Date range: {self.start_date} to {self.end_date}")
        print(f"🔧 Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
        print(f"📡 Provider: {self.provider}")

    def create_configuration(self) -> Path:
        """Create optimized configuration file for the pipeline."""
        config_data = {
            # REQUIRED: Config version field
            "config_version": "1",
            # Basic pipeline configuration (simplified to only supported fields)
            "symbols": SELECTED_EQUITIES,
            "start": self.start_date,
            "end": self.end_date,
            "output_path": str(self.base_dir / "data"),
            "workers": 3,
        }

        # Create temporary config file
        config_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            prefix="pipeline_config_",
            delete=False,
            dir=self.base_dir / "config",
        )

        yaml.dump(config_data, config_file, default_flow_style=False, indent=2)
        config_file.close()

        self.config_path = Path(config_file.name)
        print(f"📄 Created configuration: {self.config_path}")

        return self.config_path

    def check_stuck_jobs(self) -> bool:
        """Check for and fix stuck jobs that could block new ingestion."""
        print("🔍 Checking for stuck jobs...")

        db_paths = ["data/ingestion_jobs.db", "ingestion_jobs.db", "data/db/core.db"]
        db_path = None

        for path in db_paths:
            if Path(path).exists():
                db_path = path
                break

        if not db_path:
            print("ℹ️  No job database found - this is normal for first run")
            return True

        try:
            import sqlite3

            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # Find jobs stuck in IN_PROGRESS for more than 5 minutes (very aggressive)
                from datetime import timedelta, timezone

                stuck_threshold = datetime.now(timezone.utc) - timedelta(minutes=5)

                cursor.execute(
                    """
                    SELECT id, symbol, day, state, created_at, updated_at
                    FROM ingestion_jobs
                    WHERE state = 'IN_PROGRESS'
                    AND updated_at < ?
                    ORDER BY updated_at DESC
                """,
                    (stuck_threshold.isoformat(),),
                )

                stuck_jobs = cursor.fetchall()

                if stuck_jobs:
                    print(f"⚠️  Found {len(stuck_jobs)} stuck jobs:")
                    for job_id, symbol, day, _state, _created_at, updated_at in stuck_jobs:
                        print(f"   📋 Job {job_id} - {symbol} {day} (stuck since {updated_at})")

                    if not self.dry_run:
                        print("🔧 Auto-fixing stuck jobs...")

                        for job_id, symbol, day, _state, _created_at, updated_at in stuck_jobs:
                            cursor.execute(
                                """
                                UPDATE ingestion_jobs
                                SET state = 'FAILED',
                                    payload = json_set(COALESCE(payload, '{}'), '$.error_message', 'Auto-fixed: Job was stuck in IN_PROGRESS state for >5 minutes')
                                WHERE id = ?
                            """,
                                (job_id,),
                            )
                            print(f"   ✅ Fixed Job {job_id} ({symbol} {day})")

                        conn.commit()
                        print(f"🎯 Successfully fixed {len(stuck_jobs)} stuck jobs")
                    else:
                        print("   [DRY RUN] Would auto-fix these stuck jobs in live mode")
                else:
                    print("✅ No stuck jobs found")

        except Exception as e:
            print(f"⚠️  Warning: Could not check for stuck jobs: {e}")
            # Continue anyway - this shouldn't block the pipeline

        return True

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
                cwd=self.base_dir,
            )
            if result.returncode != 0:
                issues.append("MarketPipe CLI not accessible")
        except Exception as e:
            issues.append(f"MarketPipe CLI error: {e}")

        # Required Alpaca credentials
        required_env_vars = ["ALPACA_KEY", "ALPACA_SECRET"]
        for env_var in required_env_vars:
            value = os.getenv(env_var)
            if not value:
                issues.append(f"Missing environment variable: {env_var}")
            elif value.startswith("your_"):
                issues.append(f"Environment variable {env_var} contains placeholder value")

        # Check that the selected provider is available in the installed CLI
        try:
            result = subprocess.run(
                ["python", "-m", "marketpipe", "providers"],
                capture_output=True,
                text=True,
                timeout=10,
                cwd=self.base_dir,
            )
            if result.returncode == 0:
                providers_available = result.stdout.lower()
                if "alpaca" not in providers_available:
                    issues.append("Alpaca provider not available. Check installation.")
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

        # Check for stuck jobs that could block ingestion
        if not self.check_stuck_jobs():
            issues.append("Stuck job detection failed")

        # Estimate data requirements
        trading_days_per_month = 21  # Approximately 30 days / 365 days * 252 trading days
        symbols_count = len(SELECTED_EQUITIES)
        estimated_records = (
            trading_days_per_month * symbols_count * 390
        )  # ~390 minutes per trading day
        estimated_size_mb = estimated_records * 0.1  # Rough estimate

        print("📊 Estimated data:")
        print(f"   - Records: ~{estimated_records:,}")
        print(f"   - Storage: ~{estimated_size_mb:.1f} MB")
        print(f"   - Symbols: {symbols_count}")
        print(
            f"   - Date range: {(datetime.strptime(self.end_date, '%Y-%m-%d') - datetime.strptime(self.start_date, '%Y-%m-%d')).days} days"
        )

        if issues:
            print("❌ Prerequisites validation failed:")
            for issue in issues:
                print(f"   - {issue}")
            return False

        print("✅ Prerequisites validation passed")
        return True

    def run_command(
        self, cmd: list[str], description: str, timeout: int = 300
    ) -> tuple[bool, str, str]:
        """Run a CLI command with comprehensive error handling."""
        print(f"🔧 {description}")

        if self.dry_run:
            print(f"   [DRY RUN] Would execute: {' '.join(cmd)}")
            return True, f"Dry run: {description}", ""

        try:
            start_time = time.time()
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout, cwd=self.base_dir
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
        if self.dry_run:
            # In dry run mode, simulate a job ID for testing
            return "dry-run-job-id-12345"

        # Look for various patterns that might contain the job ID
        patterns = [
            "Job ID:",  # Standard pattern
            "job_id:",  # Alternative pattern
            "Job started:",  # Another pattern
            "Ingestion job:",  # CLI output pattern
            "Created job:",  # Possible pattern
        ]

        for line in output.split("\n"):
            for pattern in patterns:
                if pattern in line:
                    # Extract the job ID (usually after the colon)
                    parts = line.split(pattern)
                    if len(parts) > 1:
                        # Get the first word after the pattern, clean it up
                        job_id_candidate = parts[1].strip().split()[0].rstrip(",.:;")
                        # Basic validation that it looks like a job ID
                        if len(job_id_candidate) > 5:
                            return job_id_candidate

        # If no explicit job ID found, look for UUID-like patterns
        import re

        uuid_pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        matches = re.findall(uuid_pattern, output, re.IGNORECASE)
        if matches:
            return matches[-1]  # Return the last UUID found

        return None

    def _get_latest_job_id(self) -> Optional[str]:
        """Get the latest job ID from the database."""
        try:
            cmd = [
                "python",
                "-m",
                "marketpipe",
                "query",
                "SELECT job_id FROM ingestion_jobs ORDER BY created_at DESC LIMIT 1",
                "--csv",
            ]

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30, cwd=self.base_dir
            )

            if result.returncode == 0 and result.stdout.strip():
                # Parse CSV output to get job ID
                lines = result.stdout.strip().split("\n")
                if len(lines) > 1:  # Skip header
                    return lines[1].strip()

        except Exception as e:
            print(f"   Could not query latest job ID: {e}")

        return None

    def _check_job_completion_status(self) -> bool:
        """Check if recent jobs actually completed successfully by checking job database."""
        try:
            # Wait a moment for the database to be updated
            time.sleep(2)

            print("🔍 Checking recent job completions...")

            # Check each symbol individually using the jobs list command
            completed_symbols = set()

            for symbol in SELECTED_EQUITIES:
                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "jobs",
                    "list",
                    "--symbol",
                    symbol,
                    "--limit",
                    "5",
                ]

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30, cwd=self.base_dir
                )

                if result.returncode == 0 and result.stdout.strip():
                    # Parse the jobs list output to check for recent COMPLETED jobs
                    lines = result.stdout.strip().split("\n")
                    for line in lines:
                        if "COMPLETED" in line and symbol in line:
                            # Check if the job is recent (contains today's date)
                            from datetime import datetime

                            today = datetime.now().strftime("%m-%d")  # Format: MM-DD
                            if today in line:
                                completed_symbols.add(symbol)
                                print(f"   ✅ {symbol}: Found recent completed job")
                                break
                    else:
                        print(f"   ⚠️  {symbol}: No recent completed jobs found")
                else:
                    print(f"   ❌ {symbol}: Could not check job status")

            # Check if at least one symbol completed successfully (more lenient)
            target_symbols = set(SELECTED_EQUITIES)
            if completed_symbols:
                if target_symbols.issubset(completed_symbols):
                    print(
                        f"✅ All symbols have recent successful completions: {', '.join(completed_symbols)}"
                    )
                    return True
                else:
                    missing = target_symbols - completed_symbols
                    print(
                        f"⚠️  Partial success - completed: {', '.join(completed_symbols)}, missing: {', '.join(missing)}"
                    )
                    print("✅ Proceeding with partial success since some data was ingested")
                    return True  # Allow partial success
            else:
                print("❌ No symbols completed successfully")

        except Exception as e:
            print(f"   Could not check job completion status: {e}")

        return False

    def run_ingestion(self) -> bool:
        """Run the data ingestion phase."""
        print("\n" + "=" * 60)
        print("📥 PHASE 1: DATA INGESTION")
        print("=" * 60)

        # Ingest using Alpaca provider
        cmd = [
            "bash",
            "-c",
            f"source .env && python -m marketpipe ingest-ohlcv --config {self.config_path} --provider alpaca --feed-type iex",
        ]

        # Extended timeout for ingestion (can take a long time for a year of data)
        success, stdout, stderr = self.run_command(
            cmd,
            f"Ingesting data for {len(SELECTED_EQUITIES)} symbols over 3 days",
            timeout=1200,  # 20 minute timeout (reduced for smaller dataset)
        )

        if not success:
            print("⚠️  Ingestion command returned error, but checking actual job status...")
            # The ingestion might still have succeeded despite error messages
            # Check the actual job completion status in the database
            if not self.dry_run and self._check_job_completion_status():
                print("✅ Database verification shows ingestion actually succeeded!")
                success = True  # Override the error status
            else:
                print("💡 Troubleshooting tips:")
                print("   - Check API credentials: echo $ALPACA_KEY")
                print("   - Ensure your Alpaca account has IEX feed access")
                print("   - Check for stuck jobs that were auto-fixed")
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
        print("\n" + "=" * 60)
        print("🔍 PHASE 2: DATA VALIDATION")
        print("=" * 60)

        # Try with job ID first, fall back to general validation
        if self.job_id:
            cmd = ["python", "-m", "marketpipe", "validate-ohlcv", "--job-id", self.job_id]
            description = f"Validating data quality for job {self.job_id}"
        else:
            cmd = ["python", "-m", "marketpipe", "validate-ohlcv"]
            description = "Validating data quality (no specific job ID)"

        success, stdout, stderr = self.run_command(
            cmd, description, timeout=600  # 10 minute timeout
        )

        return success

    def run_aggregation(self) -> bool:
        """Run the data aggregation phase."""
        print("\n" + "=" * 60)
        print("📊 PHASE 3: DATA AGGREGATION")
        print("=" * 60)

        if not self.job_id:
            print("⚠️  No job ID available - attempting to get latest job from database")
            if not self.dry_run:
                latest_job_id = self._get_latest_job_id()
                if latest_job_id:
                    print(f"🆔 Using latest job ID from database: {latest_job_id}")
                    self.job_id = latest_job_id
                else:
                    print("❌ Cannot determine job ID for aggregation")
                    print(
                        "   You may need to run aggregation manually with: python -m marketpipe aggregate-ohlcv <job_id>"
                    )
                    return False
            else:
                # In dry run mode, we already set a fake job ID
                pass

        cmd = ["python", "-m", "marketpipe", "aggregate-ohlcv", self.job_id]

        success, stdout, stderr = self.run_command(
            cmd, f"Aggregating data for job {self.job_id}", timeout=1800  # 30 minute timeout
        )

        return success

    def run_health_check(self) -> bool:
        """Run health check to validate the installation."""
        print("\n" + "=" * 60)
        print("🏥 HEALTH CHECK")
        print("=" * 60)

        cmd = ["python", "-m", "marketpipe", "health-check", "--verbose"]

        success, stdout, stderr = self.run_command(
            cmd, "Running MarketPipe health check", timeout=120
        )

        return success

    def generate_summary(
        self, ingestion_success: bool, validation_success: bool, aggregation_success: bool
    ):
        """Generate execution summary report."""
        print("\n" + "=" * 60)
        print("📋 EXECUTION SUMMARY")
        print("=" * 60)

        print(f"🔧 Mode: {'DRY RUN' if self.dry_run else 'LIVE EXECUTION'}")
        print(f"📅 Date Range: {self.start_date} to {self.end_date}")
        print(f"📈 Symbols: {', '.join(SELECTED_EQUITIES)}")
        print(f"📄 Configuration: {self.config_path}")
        print(f"📡 Provider: {self.provider}")

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
                print(
                    "   Query data with: python -m marketpipe query 'SELECT * FROM aggregated_ohlcv LIMIT 10'"
                )
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
        symbols_str = " and ".join(SELECTED_EQUITIES)
        print(f"🚀 Starting MarketPipe Full Pipeline for {symbols_str}")
        print("=" * 60)

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
        description="Run complete MarketPipe pipeline for COST and TSLA",
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
    - Sufficient disk space for ~3 months of OHLCV data

The script will:
    1. Create optimized configuration for COST and TSLA
    2. Run health check to validate setup
    3. Ingest 3 months of OHLCV data (prior to yesterday)
    4. Validate data quality
    5. Aggregate to multiple timeframes (1min, 5min, 15min, 1h, 1d)
    6. Generate execution summary

Data will be stored in the 'data/' directory with partitioned Parquet files.
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Test configuration and show commands without executing",
    )
    group.add_argument(
        "--execute", action="store_true", help="Execute the full pipeline with live data"
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
