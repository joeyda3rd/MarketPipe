# SPDX-License-Identifier: Apache-2.0
"""OHLCV data aggregation commands."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import typer

from marketpipe.aggregation.application.services import AggregationRunnerService


def _get_recent_jobs(symbol: Optional[str] = None, days: int = 7) -> list[str]:
    """Get recent completed jobs from the repository."""
    import asyncio

    from marketpipe.bootstrap import get_repository_adapter
    from marketpipe.ingestion.domain.entities import ProcessingState

    adapter = get_repository_adapter()
    repo = adapter.job_repository()

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


def _aggregate_single_job(job_id: str, aggregation_service: AggregationRunnerService):
    """Aggregate a single job."""
    print(f"📊 Aggregating job: {job_id}")

    try:
        # Execute aggregation via manual method
        aggregation_service.run_manual_aggregation(job_id)
        print(f"✅ Aggregation completed for job: {job_id}")
        return True
    except Exception as e:
        print(f"❌ Aggregation failed for job {job_id}: {e}")
        return False


def _aggregate_impl(
    job_id: Optional[str] = None,
    symbol: Optional[str] = None,
    days: int = 7,
    all_jobs: bool = False,
):
    """Implementation of the aggregate functionality."""
    from marketpipe.bootstrap import bootstrap

    bootstrap()

    try:
        # Setup aggregation service - use the build_default factory method
        aggregation_service = AggregationRunnerService.build_default()

        # Determine which jobs to aggregate
        if job_id:
            # Single job specified
            job_ids = [job_id]
            print(f"📊 Starting aggregation for job: {job_id}")
        elif all_jobs:
            # Get all recent completed jobs
            print(f"📊 Finding all completed jobs from last {days} days...")
            job_ids = _get_recent_jobs(symbol=symbol, days=days if days != 7 else 30)
            if not job_ids:
                print(f"ℹ️  No completed jobs found")
                return
            print(f"📊 Found {len(job_ids)} job(s) to aggregate")
        else:
            # Default: Get recent jobs (last 7 days)
            print(f"📊 Finding recent completed jobs from last {days} days...")
            job_ids = _get_recent_jobs(symbol=symbol, days=days)
            if not job_ids:
                print(f"ℹ️  No completed jobs found")
                print(f"💡 Tip: Use --all to aggregate all recent jobs, or specify a JOB_ID")
                return
            print(f"📊 Found {len(job_ids)} job(s) to aggregate")

        # Aggregate each job
        success_count = 0
        failed_count = 0
        for jid in job_ids:
            if _aggregate_single_job(jid, aggregation_service):
                success_count += 1
            else:
                failed_count += 1

        # Refresh DuckDB views once at the end
        try:
            from marketpipe.aggregation.infrastructure.duckdb_views import refresh_views

            refresh_views()
            print("🔄 DuckDB views refreshed")
        except Exception as e:
            print(f"⚠️  Warning: Failed to refresh DuckDB views: {e}")

        # Print summary
        print(f"\n📊 Aggregation Summary:")
        print(f"  ✅ Successful: {success_count}")
        if failed_count > 0:
            print(f"  ❌ Failed: {failed_count}")
        print(f"📄 Check 'data/agg/' for aggregated data")

        if failed_count > 0:
            raise typer.Exit(1)

    except Exception as e:
        print(f"❌ Aggregation failed: {e}")
        raise typer.Exit(1) from None


def aggregate_ohlcv(
    job_id: Optional[str] = typer.Argument(
        None,
        help="Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, aggregates recent completed jobs.",
    ),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter jobs by symbol"),
    days: int = typer.Option(7, "--days", "-d", help="Look back N days for jobs (default: 7)"),
    all_jobs: bool = typer.Option(False, "--all", "-a", help="Aggregate all recent jobs"),
):
    """Aggregate OHLCV data to multiple timeframes.

    Without JOB_ID: Aggregates recent completed jobs from the last 7 days.
    With JOB_ID: Aggregates only the specified job.

    Examples:
      marketpipe aggregate-ohlcv              # Aggregate recent jobs (last 7 days)
      marketpipe aggregate-ohlcv --all        # Aggregate all jobs (last 30 days)
      marketpipe aggregate-ohlcv --symbol AAPL  # Aggregate AAPL jobs only
      marketpipe aggregate-ohlcv AAPL_2024-01-15  # Aggregate specific job
    """
    _aggregate_impl(job_id, symbol, days, all_jobs)


def aggregate_ohlcv_convenience(
    job_id: Optional[str] = typer.Argument(
        None,
        help="Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, aggregates recent completed jobs.",
    ),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter jobs by symbol"),
    days: int = typer.Option(7, "--days", "-d", help="Look back N days for jobs (default: 7)"),
    all_jobs: bool = typer.Option(False, "--all", "-a", help="Aggregate all recent jobs"),
):
    """Aggregate OHLCV data to multiple timeframes (convenience command)."""
    _aggregate_impl(job_id, symbol, days, all_jobs)


def aggregate_deprecated(
    job_id: Optional[str] = typer.Argument(
        None,
        help="Specific job ID (format: SYMBOL_YYYY-MM-DD). If omitted, aggregates recent completed jobs.",
    ),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter jobs by symbol"),
    days: int = typer.Option(7, "--days", "-d", help="Look back N days for jobs (default: 7)"),
    all_jobs: bool = typer.Option(False, "--all", "-a", help="Aggregate all recent jobs"),
):
    """[DEPRECATED] Use 'aggregate-ohlcv' or 'ohlcv aggregate' instead."""
    print(
        "⚠️  Warning: 'aggregate' is deprecated. Use 'aggregate-ohlcv' or 'ohlcv aggregate' instead."
    )
    _aggregate_impl(job_id, symbol, days, all_jobs)
