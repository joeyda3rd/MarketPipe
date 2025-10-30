# SPDX-License-Identifier: Apache-2.0
"""OHLCV data validation commands."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import typer

from marketpipe.validation.application.services import ValidationRunnerService
from marketpipe.validation.infrastructure.repositories import CsvReportRepository


def _get_recent_jobs(symbol: Optional[str] = None, days: int = 7) -> list[str]:
    """Get recent completed jobs from the repository."""
    import asyncio

    from marketpipe.ingestion.domain.entities import ProcessingState
    from marketpipe.ingestion.infrastructure.simple_job_adapter import SimpleJobRepository

    repo = SimpleJobRepository()._repo

    async def fetch_jobs():
        # Get jobs from last N days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        jobs = await repo.get_jobs_by_date_range(start_date, end_date)

        # Filter for completed jobs
        completed_jobs = [j for j in jobs if j.state == ProcessingState.COMPLETED]

        # Filter by symbol if specified
        if symbol:
            completed_jobs = [
                j for j in completed_jobs if j.job_id.symbol and str(j.job_id.symbol) == symbol
            ]

        # Return job IDs as strings
        return [str(j.job_id) for j in completed_jobs]

    return asyncio.run(fetch_jobs())


def _validate_single_job(job_id: str, validation_service: ValidationRunnerService):
    """Validate a single job."""
    print(f"🔍 Validating job: {job_id}")

    try:
        # Create and trigger the ingestion completed event
        from datetime import date

        from marketpipe.domain.events import IngestionJobCompleted
        from marketpipe.domain.value_objects import Symbol

        # Create a mock event to trigger validation
        event = IngestionJobCompleted(
            job_id=job_id,
            symbol=Symbol("DUMMY"),  # Placeholder symbol
            trading_date=date.today(),
            bars_processed=0,
            success=True,
        )

        # Execute validation via event handler
        validation_service.handle_ingestion_completed(event)
        print(f"✅ Validation completed for job: {job_id}")
        return True
    except Exception as e:
        print(f"❌ Validation failed for job {job_id}: {e}")
        return False


def _validate_impl(
    job_id: Optional[str] = None,
    list_reports: bool = False,
    show: Optional[Path] = None,
    symbol: Optional[str] = None,
    days: int = 7,
    all_jobs: bool = False,
):
    """Implementation of the validate functionality."""
    from marketpipe.bootstrap import bootstrap

    bootstrap()

    try:
        # Setup validation service - use the build_default factory method
        validation_service = ValidationRunnerService.build_default()

        # Setup report repository for CLI functionality
        report_repo = CsvReportRepository("data/validation_reports")

        if list_reports:
            # List available validation reports
            print("📊 Available Validation Reports:")
            print("=" * 50)

            reports = report_repo.list_reports()
            if not reports:
                print("No validation reports found.")
                print("💡 Run validation with: marketpipe validate-ohlcv --job-id <job_id>")
                return

            # Group by job_id
            jobs: dict[str, list[Path]] = {}
            for report in reports:
                job_id_part = report.stem.split("_")[0]  # Extract job_id prefix
                if job_id_part not in jobs:
                    jobs[job_id_part] = []
                jobs[job_id_part].append(report)

            for job_id_part, job_reports in jobs.items():
                print(f"Job {job_id_part}:")
                for report_file in sorted(job_reports):
                    symbol = (
                        report_file.stem.split("_", 1)[1] if "_" in report_file.stem else "unknown"
                    )
                    size_kb = report_file.stat().st_size / 1024
                    print(f"  📄 {report_file.name} ({symbol}, {size_kb:.1f} KB)")
                print()

            print(f"Total: {len(reports)} reports across {len(jobs)} jobs")
            print("💡 Use --show <filename> to view a specific report")
            return

        if show:
            # Display specific validation report
            if not show.exists():
                print(f"❌ Report file not found: {show}")
                raise typer.Exit(1)

            print(f"📄 Validation Report: {show.name}")
            print("=" * 50)

            try:
                import pandas as pd

                df = pd.read_csv(show)

                if df.empty:
                    print("✅ No validation errors found in this report")
                    return

                print(f"Total validation errors: {len(df)}")
                print()

                # Group by reason to show summary
                if "reason" in df.columns:
                    error_summary = df["reason"].value_counts()
                    print("Error Summary:")
                    for reason, count in error_summary.items():
                        print(f"  {reason}: {count}")
                    print()

                # Show first few rows
                print("First 10 errors:")
                try:
                    # Try to format as table
                    print(df.head(10).to_string(index=False))
                except Exception:
                    # Fallback to CSV display
                    print(df.head(10).to_csv(index=False))

                if len(df) > 10:
                    print(f"\n... and {len(df) - 10} more errors")

            except Exception as e:
                print(f"❌ Error reading report: {e}")
                # Fallback: show raw file contents
                print("Raw contents:")
                with open(show) as f:
                    print(f.read())

            return

        if job_id:
            # Validate specific job
            job_ids = [job_id]
            print(f"🔍 Validating job: {job_id}")
        elif all_jobs:
            # Get all recent completed jobs
            print(f"🔍 Finding all completed jobs from last {days} days...")
            job_ids = _get_recent_jobs(symbol=symbol, days=days if days != 7 else 30)
            if not job_ids:
                print("ℹ️  No completed jobs found")
                return
            print(f"🔍 Found {len(job_ids)} job(s) to validate")
        elif not list_reports and not show:
            # Default: Get recent jobs (last 7 days)
            print(f"🔍 Finding recent completed jobs from last {days} days...")
            job_ids = _get_recent_jobs(symbol=symbol, days=days)
            if not job_ids:
                print("ℹ️  No completed jobs found")
                print("💡 Tip: Use --all to validate all recent jobs, or specify a JOB_ID")
                # Show status as fallback
                job_ids = []
            else:
                print(f"🔍 Found {len(job_ids)} job(s) to validate")
        else:
            job_ids = []

        if job_ids:
            # Validate each job
            success_count = 0
            failed_count = 0
            for jid in job_ids:
                if _validate_single_job(jid, validation_service):
                    success_count += 1
                else:
                    failed_count += 1

            # Print summary
            print("\n🔍 Validation Summary:")
            print(f"  ✅ Successful: {success_count}")
            if failed_count > 0:
                print(f"  ❌ Failed: {failed_count}")
            print("📄 Check 'data/validation_reports/' for generated reports")

            if failed_count > 0:
                raise typer.Exit(1)
            return

        if not list_reports and not show and not job_ids:
            # Show validation status and recent activity
            print("📊 Validation Status:")
            print("=" * 30)

            reports = report_repo.list_reports()
            if reports:
                recent_reports = sorted(reports, key=lambda x: x.stat().st_mtime, reverse=True)[:5]
                print("Recent validation reports:")
                for report in recent_reports:
                    import datetime

                    mod_time = datetime.datetime.fromtimestamp(report.stat().st_mtime)
                    size_kb = report.stat().st_size / 1024
                    print(
                        f"  📄 {report.name} ({mod_time.strftime('%Y-%m-%d %H:%M')}, {size_kb:.1f} KB)"
                    )
            else:
                print("No validation reports found.")

            print("\n💡 Usage:")
            print("  marketpipe validate-ohlcv --job-id <job_id>    # Re-run validation")
            print("  marketpipe validate-ohlcv --list               # List all reports")
            print("  marketpipe validate-ohlcv --show <file>        # Show specific report")

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        raise typer.Exit(1) from e


def validate_ohlcv(
    job_id: Optional[str] = typer.Argument(
        None,
        help="Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, validates recent completed jobs.",
    ),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter jobs by symbol"),
    days: int = typer.Option(7, "--days", "-d", help="Look back N days for jobs (default: 7)"),
    all_jobs: bool = typer.Option(False, "--all", "-a", help="Validate all recent jobs"),
    list_reports: bool = typer.Option(False, "--list", help="List available reports"),
    show: Optional[Path] = typer.Option(None, "--show", help="Show a report CSV"),
    help_flag: bool = typer.Option(
        False,
        "--help",
        "-h",
        is_flag=True,
        help="Show this message and exit",
        show_default=False,
    ),
):
    """Validate OHLCV data quality and generate reports.

    Without JOB_ID: Validates recent completed jobs from the last 7 days.
    With JOB_ID: Validates only the specified job.

    Examples:
      marketpipe ohlcv validate              # Validate recent jobs (last 7 days)
      marketpipe ohlcv validate --all        # Validate all jobs (last 30 days)
      marketpipe ohlcv validate --symbol AAPL  # Validate AAPL jobs only
      marketpipe ohlcv validate AAPL_2024-01-15  # Validate specific job
      marketpipe ohlcv validate --list       # List all reports
      marketpipe ohlcv validate --show report.csv  # Show specific report
    """
    # Skip all processing and show help immediately if help flag is set
    if help_flag:
        help_text = """
Usage: ohlcv validate [JOB_ID] [OPTIONS]

Validate OHLCV data quality and generate reports.

Arguments:
  JOB_ID  Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, validates recent completed jobs.

Options:
  -s, --symbol TEXT    Filter jobs by symbol
  -d, --days INTEGER   Look back N days for jobs (default: 7)
  -a, --all            Validate all recent jobs
  --list               List available reports
  --show PATH          Show a report CSV
  -h, --help           Show this message and exit

Examples:
  marketpipe ohlcv validate              # Validate recent jobs (last 7 days)
  marketpipe ohlcv validate --all        # Validate all jobs (last 30 days)
  marketpipe ohlcv validate --symbol AAPL  # Validate AAPL jobs only
  marketpipe ohlcv validate AAPL_2024-01-15  # Validate specific job
"""
        typer.echo(help_text.strip())
        raise typer.Exit(0)

    _validate_impl(
        job_id=job_id,
        list_reports=list_reports,
        show=show,
        symbol=symbol,
        days=days,
        all_jobs=all_jobs,
    )


def validate_ohlcv_convenience(
    job_id: Optional[str] = typer.Argument(
        None,
        help="Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, validates recent completed jobs.",
    ),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter jobs by symbol"),
    days: int = typer.Option(7, "--days", "-d", help="Look back N days for jobs (default: 7)"),
    all_jobs: bool = typer.Option(False, "--all", "-a", help="Validate all recent jobs"),
    list_reports: bool = typer.Option(False, "--list", help="List available reports"),
    show: Optional[Path] = typer.Option(None, "--show", help="Show a report CSV"),
    help_flag: bool = typer.Option(
        False,
        "--help",
        "-h",
        is_flag=True,
        help="Show this message and exit",
        show_default=False,
    ),
):
    """Validate OHLCV data quality and generate reports (convenience command)."""
    # Skip all processing and show help immediately if help flag is set
    if help_flag:
        help_text = """
Usage: validate-ohlcv [JOB_ID] [OPTIONS]

Validate OHLCV data quality and generate reports.

Arguments:
  JOB_ID  Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, validates recent completed jobs.

Options:
  -s, --symbol TEXT    Filter jobs by symbol
  -d, --days INTEGER   Look back N days for jobs (default: 7)
  -a, --all            Validate all recent jobs
  --list               List available reports
  --show PATH          Show a report CSV
  -h, --help           Show this message and exit
"""
        typer.echo(help_text.strip())
        raise typer.Exit(0)

    _validate_impl(
        job_id=job_id,
        list_reports=list_reports,
        show=show,
        symbol=symbol,
        days=days,
        all_jobs=all_jobs,
    )


def validate_deprecated(
    job_id: Optional[str] = typer.Argument(
        None,
        help="Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, validates recent completed jobs.",
    ),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter jobs by symbol"),
    days: int = typer.Option(7, "--days", "-d", help="Look back N days for jobs (default: 7)"),
    all_jobs: bool = typer.Option(False, "--all", "-a", help="Validate all recent jobs"),
    list_reports: bool = typer.Option(False, "--list", help="List available reports"),
    show: Optional[Path] = typer.Option(None, "--show", help="Show a report CSV"),
):
    """[DEPRECATED] Use 'validate-ohlcv' or 'ohlcv validate' instead."""
    print("⚠️  Warning: 'validate' is deprecated. Use 'validate-ohlcv' or 'ohlcv validate' instead.")
    _validate_impl(
        job_id=job_id,
        list_reports=list_reports,
        show=show,
        symbol=symbol,
        days=days,
        all_jobs=all_jobs,
    )
