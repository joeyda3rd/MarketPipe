"""Symbols CLI commands for managing symbol master data."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from marketpipe.ingestion.pipeline.symbol_pipeline import run_symbol_pipeline
from marketpipe.ingestion.symbol_providers import list_providers

console = Console()

# Create the Typer app instance that the main CLI imports
app = typer.Typer(help="Symbol-master related commands.")


def validate_date_format(date_string: str, flag_name: str) -> date:
    """Validate date format and return date object."""
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError:
        console.print(
            f"❌ Invalid date format for {flag_name}: {date_string}. Use YYYY-MM-DD.", style="red"
        )
        raise typer.Exit(1)


def validate_backfill_range(backfill_date: date, snapshot_date: date) -> None:
    """Validate backfill date range and provide warnings."""
    if backfill_date > snapshot_date:
        console.print(
            f"❌ Backfill date {backfill_date} cannot be after snapshot date {snapshot_date}.",
            style="red",
        )
        raise typer.Exit(1)

    # Calculate number of days
    days_to_process = (snapshot_date - backfill_date).days + 1

    if days_to_process > 365:
        console.print(
            f"⚠️  Large backfill detected: {days_to_process} days ({days_to_process/365:.1f} years).",
            style="yellow",
        )
        console.print(
            "This may take a significant amount of time. Consider breaking into smaller chunks.",
            style="yellow",
        )

        # Ask for confirmation for very large backfills
        if days_to_process > 1825:  # 5 years
            if not typer.confirm(
                f"Process {days_to_process} days? This is approximately {days_to_process/365:.1f} years of data."
            ):
                console.print("Backfill cancelled by user.", style="yellow")
                raise typer.Exit(0)


def check_diff_only_precondition(db_path: Path) -> None:
    """Check that symbols_snapshot table exists for diff-only mode."""
    import duckdb

    try:
        with duckdb.connect(str(db_path)) as conn:
            # Check if symbols_snapshot table exists and has data
            result = conn.execute("SELECT COUNT(*) FROM symbols_snapshot").fetchone()
            if result[0] == 0:
                console.print(
                    "❌ --diff-only requires existing symbols_snapshot table with data.",
                    style="red",
                )
                console.print(
                    "Run without --diff-only first to create initial snapshot.", style="yellow"
                )
                raise typer.Exit(1)
    except Exception as e:
        if "Table with name symbols_snapshot does not exist" in str(e):
            console.print("❌ --diff-only requires existing symbols_snapshot table.", style="red")
            console.print(
                "Run without --diff-only first to create initial snapshot.", style="yellow"
            )
            raise typer.Exit(1)
        else:
            # Re-raise other database errors
            raise


def show_progress_summary(
    current_date: date,
    total_days: int,
    current_day: int,
    inserts: int,
    updates: int,
    verbose: bool = True,
) -> None:
    """Show progress summary for backfill operations."""
    if verbose or total_days <= 50:
        # Show detailed progress for small backfills or when verbose
        console.print(
            f"[{current_day}/{total_days}] {current_date}: {inserts} inserts, {updates} updates",
            style="green",
        )
    elif current_day % 10 == 0 or current_day == total_days:
        # Show summary every 10 days for large backfills
        console.print(
            f"[{current_day}/{total_days}] Processed through {current_date}: {inserts} inserts, {updates} updates",
            style="green",
        )


@app.command("update")
def update(
    providers: list[str] = typer.Option(
        ...,
        "--provider",
        "-p",
        help="Symbol provider(s) to ingest. Available: " + ", ".join(list_providers()),
    ),
    snapshot_as_of: str = typer.Option(
        str(date.today()), "--snapshot-as-of", help="Snapshot date (YYYY-MM-DD). Default: today"
    ),
    db_path: Optional[Path] = typer.Option(
        None, "--db", help="DuckDB database path. Default: warehouse.duckdb"
    ),
    data_dir: Optional[Path] = typer.Option(
        None, "--data-dir", help="Parquet dataset root. Default: ./data"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run pipeline but skip Parquet writes"),
    diff_only: bool = typer.Option(
        False, "--diff-only", help="Skip provider fetch and SCD update only"
    ),
    backfill: Optional[str] = typer.Option(
        None, "--backfill", help="Back-fill symbols from this date (YYYY-MM-DD)"
    ),
    execute: bool = typer.Option(False, "--execute", help="Perform writes (not read-only)"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed progress for each day in backfill"
    ),
) -> None:
    """Update symbol master data from configured providers.

    This command fetches symbol data from external providers, normalizes it,
    and updates the symbol master tables with SCD-2 change tracking.

    Examples:
        mp symbols update -p polygon --execute
        mp symbols update -p nasdaq_dl --dry-run
        mp symbols update -p polygon --backfill 2025-01-01 --execute
        mp symbols update -p polygon --diff-only --execute
    """

    # These validations will be checked later when executing

    # Parse and validate dates
    snapshot_date = validate_date_format(snapshot_as_of, "--snapshot-as-of")

    backfill_date = None
    if backfill:
        backfill_date = validate_date_format(backfill, "--backfill")
        validate_backfill_range(backfill_date, snapshot_date)

    # Set defaults
    if db_path is None:
        db_path = Path("warehouse.duckdb")
    if data_dir is None:
        data_dir = Path("./data")

    # Handle --execute precedence over --dry-run
    show_precedence_preview = False
    if execute and dry_run:
        console.print("Both --dry-run and --execute specified", style="yellow")
        console.print("--execute takes precedence", style="yellow")
        dry_run = False
        show_precedence_preview = True

    # Show preview if not executing OR if showing precedence override
    if not execute or show_precedence_preview:
        console.print("Symbol update plan:", style="blue")
        console.print(f"  providers: {', '.join(providers)}")
        console.print(f"  snapshot_as_of: {snapshot_date}")
        if backfill_date:
            days_count = (snapshot_date - backfill_date).days + 1
            console.print(f"  backfill: {backfill_date}")
        console.print(f"  db: {db_path}")
        console.print(f"  data_dir: {data_dir}")
        console.print(f"  dry_run: {dry_run}")
        console.print(f"  diff_only: {diff_only}")
        console.print(f"  execute: {execute}")

        # Only exit if not executing (normal preview mode)
        if not execute:
            console.print("Dry preview complete. Re-run with --execute to perform writes.")
            return

    # Check diff-only precondition when executing
    if diff_only:
        check_diff_only_precondition(db_path)

    # Validate flag combinations when actually executing
    if dry_run and diff_only:
        console.print(
            "❌ `--diff-only` implies DB writes; cannot combine with --dry-run.", style="red"
        )
        raise typer.Exit(1)

    if backfill and diff_only:
        console.print(
            "❌ Back-fill requires provider fetch -> cannot use --diff-only.", style="red"
        )
        raise typer.Exit(1)

    # Determine date range for processing
    if backfill_date:
        # Generate list of dates from backfill_date to snapshot_date (inclusive)
        dates_to_process = []
        current = backfill_date
        while current <= snapshot_date:
            dates_to_process.append(current)
            current += timedelta(days=1)
    else:
        # Single date processing
        dates_to_process = [snapshot_date]

    total_days = len(dates_to_process)
    console.print(f"🚀 Starting symbol update pipeline for {total_days} date(s)...")

    # Process each date
    total_inserts = 0
    total_updates = 0

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
            transient=False,
        ) as progress:

            if total_days > 1:
                task = progress.add_task(f"Processing {total_days} dates", total=total_days)

            for day_num, process_date in enumerate(dates_to_process, 1):
                if total_days > 1:
                    progress.update(task, description=f"Processing {process_date}")

                # Run pipeline for this date
                inserts, updates = run_symbol_pipeline(
                    provider_names=providers,
                    snapshot_as_of=process_date,
                    db_path=db_path,
                    data_dir=data_dir,
                    dry_run=dry_run,
                    diff_only=diff_only,
                )

                total_inserts += inserts
                total_updates += updates

                # Show progress summary
                show_progress_summary(process_date, total_days, day_num, inserts, updates, verbose)

                if total_days > 1:
                    progress.update(task, advance=1)

        # Final summary
        console.print("✅ Pipeline complete.", style="green bold")
        console.print(f"  Total inserts: {total_inserts}")
        console.print(f"  Total updates: {total_updates}")
        if dry_run:
            console.print("  Mode: Dry run (no files written)")
        elif diff_only:
            console.print("  Mode: Diff only (no provider fetch)")

    except Exception as e:
        console.print(f"❌ Pipeline failed: {e}", style="red")
        raise typer.Exit(1)
