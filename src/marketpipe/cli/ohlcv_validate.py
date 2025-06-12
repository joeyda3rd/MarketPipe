# SPDX-License-Identifier: Apache-2.0
"""OHLCV data validation commands."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from marketpipe.validation.application.services import ValidationRunnerService
from marketpipe.validation.infrastructure.repositories import (
    CsvReportRepository,
)
from marketpipe.domain.events import InMemoryEventPublisher


def _validate_impl(
    job_id: str = None,
    list_reports: bool = False,
    show: Path = None,
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
            jobs = {}
            for report in reports:
                job_id_part = report.stem.split('_')[0]  # Extract job_id prefix
                if job_id_part not in jobs:
                    jobs[job_id_part] = []
                jobs[job_id_part].append(report)
            
            for job_id_part, job_reports in jobs.items():
                print(f"Job {job_id_part}:")
                for report_file in sorted(job_reports):
                    symbol = report_file.stem.split('_', 1)[1] if '_' in report_file.stem else 'unknown'
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
                if 'reason' in df.columns:
                    error_summary = df['reason'].value_counts()
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
                with open(show, 'r') as f:
                    print(f.read())
            
            return

        if job_id:
            # Re-run validation for specific job
            print(f"🔍 Re-running validation for job: {job_id}")
            
            # Create and trigger the ingestion completed event
            from marketpipe.domain.events import IngestionJobCompleted
            from marketpipe.domain.value_objects import Symbol
            from datetime import date
            
            # Create a mock event to trigger validation
            # Use dummy values since this is just to trigger the validation
            event = IngestionJobCompleted(
                job_id=job_id,
                symbol=Symbol("DUMMY"),  # Placeholder symbol
                trading_date=date.today(),
                bars_processed=0,
                success=True
            )
            
            try:
                # Execute validation via event handler
                validation_service.handle_ingestion_completed(event)
                print(f"✅ Validation completed successfully!")
                print(f"📄 Check 'data/validation_reports/' for generated reports")
            except Exception as e:
                print(f"❌ Validation failed: {e}")
                raise typer.Exit(1)
        else:
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
                    print(f"  📄 {report.name} ({mod_time.strftime('%Y-%m-%d %H:%M')}, {size_kb:.1f} KB)")
            else:
                print("No validation reports found.")
            
            print("\n💡 Usage:")
            print("  marketpipe validate-ohlcv --job-id <job_id>    # Re-run validation")
            print("  marketpipe validate-ohlcv --list               # List all reports")
            print("  marketpipe validate-ohlcv --show <file>        # Show specific report")

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        raise typer.Exit(1)


def validate_ohlcv(
    job_id: str = typer.Option(None, "--job-id", help="Re-run validation for job"),
    list_reports: bool = typer.Option(False, "--list", help="List available reports"),
    show: Path = typer.Option(None, "--show", help="Show a report CSV"),
):
    """Validate OHLCV data quality and generate reports."""
    _validate_impl(job_id=job_id, list_reports=list_reports, show=show)


def validate_ohlcv_convenience(
    job_id: str = typer.Option(None, "--job-id", help="Re-run validation for job"),
    list_reports: bool = typer.Option(False, "--list", help="List available reports"),
    show: Path = typer.Option(None, "--show", help="Show a report CSV"),
):
    """Validate OHLCV data quality and generate reports (convenience command)."""
    _validate_impl(job_id=job_id, list_reports=list_reports, show=show)


def validate_deprecated(
    job_id: str = typer.Option(None, "--job-id", help="Re-run validation for job"),
    list_reports: bool = typer.Option(False, "--list", help="List available reports"),
    show: Path = typer.Option(None, "--show", help="Show a report CSV"),
):
    """[DEPRECATED] Use 'validate-ohlcv' or 'ohlcv validate' instead."""
    print("⚠️  Warning: 'validate' is deprecated. Use 'validate-ohlcv' or 'ohlcv validate' instead.")
    _validate_impl(job_id=job_id, list_reports=list_reports, show=show) 