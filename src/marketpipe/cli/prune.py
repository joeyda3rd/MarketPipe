# SPDX-License-Identifier: Apache-2.0
"""Data retention (prune) commands for MarketPipe."""

from __future__ import annotations

import re
import time
import asyncio
import datetime as dt
from pathlib import Path
from typing import Optional

import typer
import humanize

from marketpipe.bootstrap import bootstrap


def _parse_age(expr: str) -> dt.date:
    """Parse age expression like '30d', '18m', '5y' into cutoff date."""
    m = re.fullmatch(r"(\d+)([dmy]?)", expr.lower())
    if not m:
        raise typer.BadParameter("Use formats like 30d, 12m, 5y, or just 30 (days).")
    
    value, unit = int(m.group(1)), m.group(2) or "d"  # Default to days
    today = dt.date.today()
    
    if unit == "d":
        delta = dt.timedelta(days=value)
    elif unit == "m":
        delta = dt.timedelta(days=value * 30)  # Approximate months
    elif unit == "y":
        delta = dt.timedelta(days=value * 365)  # Approximate years
    else:
        raise typer.BadParameter(f"Unknown unit '{unit}'. Use d, m, or y.")
    
    return today - delta


prune_app = typer.Typer(help="Data retention utilities", add_completion=False)


@prune_app.command("parquet")
def prune_parquet(
    older_than: str = typer.Argument(..., help="Age threshold (e.g., 5y, 30d, 18m)"),
    parquet_root: Path = typer.Option(
        Path("data/parquet"), 
        "--root", 
        help="Root directory containing parquet files"
    ),
    dry_run: bool = typer.Option(
        False, 
        "--dry-run", 
        "-n", 
        help="Show what would be deleted without making changes"
    ),
):
    """Delete parquet files older than the specified cutoff.
    
    Searches for parquet files in the directory structure and deletes
    entire partition files whose date folder is older than the cutoff.
    
    Examples:
        marketpipe prune parquet 5y                    # Delete files older than 5 years
        marketpipe prune parquet 30d --dry-run         # Preview 30-day cleanup
        marketpipe prune parquet 18m --root ./data     # Custom root directory
    """
    bootstrap()
    
    try:
        cutoff = _parse_age(older_than)
        
        typer.echo(f"🗑️ Pruning parquet files older than {cutoff}")
        typer.echo(f"📁 Searching in: {parquet_root}")
        
        if not parquet_root.exists():
            typer.echo(f"❌ Directory does not exist: {parquet_root}", err=True)
            raise typer.Exit(1)
        
        bytes_pruned = 0
        files_found = 0
        
        # Search for parquet files and extract dates from path structure
        for file in parquet_root.rglob("*.parquet"):
            try:
                # Try to extract date from path pattern like:
                # .../symbol=AAPL/2024/01/15.parquet
                # .../symbol=AAPL/date=2024-01-15/file.parquet
                # .../AAPL/2024-01-15.parquet
                
                path_parts = file.parts
                date_found = None
                
                # Look for date patterns in path parts
                for part in path_parts:
                    # Try YYYY-MM-DD format
                    if re.match(r'\d{4}-\d{2}-\d{2}', part):
                        try:
                            date_found = dt.date.fromisoformat(part)
                            break
                        except ValueError:
                            continue
                    
                    # Try date= prefix format
                    if part.startswith('date='):
                        try:
                            date_found = dt.date.fromisoformat(part[5:])
                            break
                        except ValueError:
                            continue
                    
                    # Try YYYY/MM/DD structure (check if we have year/month/day pattern)
                    if len(path_parts) >= 3:
                        try:
                            # Look for potential year/month/day structure
                            for i, p in enumerate(path_parts[:-2]):
                                if re.match(r'\d{4}$', p):  # Year
                                    year = int(p)
                                    month_part = path_parts[i + 1]
                                    day_part = path_parts[i + 2]
                                    
                                    if re.match(r'\d{1,2}$', month_part) and re.match(r'\d{1,2}', day_part.split('.')[0]):
                                        month = int(month_part)
                                        day = int(day_part.split('.')[0])
                                        date_found = dt.date(year, month, day)
                                        break
                                        
                        except (ValueError, IndexError):
                            continue
                
                if date_found is None:
                    # Skip files where we can't determine the date
                    continue
                
                files_found += 1
                
                if date_found < cutoff:
                    file_size = file.stat().st_size
                    
                    if dry_run:
                        typer.echo(f"[DRY RUN] Would delete: {file} ({humanize.naturalsize(file_size)}) - {date_found}")
                    else:
                        typer.echo(f"Deleting: {file} ({humanize.naturalsize(file_size)}) - {date_found}")
                        file.unlink(missing_ok=True)
                        bytes_pruned += file_size
                        
                        # Record metrics
                        try:
                            from marketpipe.metrics import record_metric
                            record_metric("data_pruned_bytes", file_size, source="prune", provider="parquet")
                        except ImportError:
                            pass  # Metrics not available
                        
                        # Emit domain event
                        try:
                            from marketpipe.domain.events import DataPruned
                            event = DataPruned(
                                data_type="parquet",
                                amount=file_size,
                                cutoff=cutoff
                            )
                            # For now, just log the event. In a full implementation,
                            # this would be published to an event bus
                            typer.echo(f"📊 Event: {event.event_type} - {file_size} bytes pruned")
                        except ImportError:
                            pass  # Domain events not available
                    
            except (ValueError, OSError) as e:
                typer.echo(f"⚠️ Warning: Could not process {file}: {e}", err=True)
                continue
        
        if not dry_run and bytes_pruned > 0:
            # Update Prometheus metrics
            try:
                from marketpipe.metrics import DATA_PRUNED_BYTES_TOTAL
                DATA_PRUNED_BYTES_TOTAL.labels(type="parquet").inc(bytes_pruned)
            except ImportError:
                pass  # Metrics not available
        
        # Report results
        if dry_run:
            typer.echo(f"\n🔍 Dry run complete")
            typer.echo(f"📁 Files examined: {files_found}")
            typer.echo(f"🗑️ Would remove files totaling: {humanize.naturalsize(bytes_pruned) if bytes_pruned else '0 bytes'}")
        else:
            if bytes_pruned > 0:
                typer.secho(f"\n✅ Removed {humanize.naturalsize(bytes_pruned)} from {files_found} files older than {cutoff}", fg="green")
            else:
                typer.echo(f"\n✨ No files found older than {cutoff}")
                
    except Exception as e:
        typer.echo(f"❌ Pruning failed: {e}", err=True)
        raise typer.Exit(1)


@prune_app.command("database")
def prune_database(
    older_than: str = typer.Argument(..., help="Age threshold (e.g., 18m, 90d)"),
    dry_run: bool = typer.Option(
        False, 
        "--dry-run", 
        "-n", 
        help="Show what would be deleted without making changes"
    ),
):
    """Delete old rows from the ingestion_jobs database (SQLite or PostgreSQL).
    
    Removes job records whose trading day is older than the cutoff date
    and runs VACUUM to reclaim space. Works with both SQLite and PostgreSQL backends.
    
    Examples:
        marketpipe prune database 18m          # Delete jobs older than 18 months
        marketpipe prune database 90d --dry-run # Preview 90-day cleanup
    """
    bootstrap()
    
    try:
        cutoff = _parse_age(older_than)
        
        # Import repository factory
        from marketpipe.ingestion.infrastructure.repository_factory import create_ingestion_job_repository
        
        # Get repository - works with both SQLite and PostgreSQL
        repo = create_ingestion_job_repository()
        
        # Determine backend type for user feedback
        backend_type = "Unknown"
        if hasattr(repo, '__class__'):
            if 'Sqlite' in repo.__class__.__name__:
                backend_type = "SQLite"
            elif 'Postgres' in repo.__class__.__name__:
                backend_type = "PostgreSQL"
        
        typer.echo(f"🗑️ Pruning {backend_type} job records older than {cutoff}")
        
        # Check if repository supports pruning methods
        if not hasattr(repo, 'count_old_jobs') or not hasattr(repo, 'delete_old_jobs'):
            typer.secho(f"⚠️ {backend_type} backend does not support pruning operations.", fg="yellow")
            raise typer.Exit(0)
        
        if dry_run:
            # Count rows that would be deleted
            try:
                count = asyncio.run(repo.count_old_jobs(cutoff.isoformat()))
                typer.echo(f"🔍 Dry run: Would delete {count} job records older than {cutoff}")
                return  # Exit successfully
            except Exception as e:
                typer.echo(f"❌ Failed to count old records: {e}", err=True)
                raise typer.Exit(1)
        else:
            # Actually delete the rows
            try:
                deleted = asyncio.run(repo.delete_old_jobs(cutoff.isoformat()))
                
                if deleted > 0:
                    # Record metrics
                    try:
                        from marketpipe.metrics import DATA_PRUNED_ROWS_TOTAL, record_metric
                        metric_type = backend_type.lower()
                        DATA_PRUNED_ROWS_TOTAL.labels(type=metric_type).inc(deleted)
                        record_metric("data_pruned_rows", deleted, source="prune", provider=metric_type)
                    except ImportError:
                        pass  # Metrics not available
                    
                    # Emit domain event
                    try:
                        from marketpipe.domain.events import DataPruned
                        event = DataPruned(
                            data_type=backend_type.lower(),
                            amount=deleted,
                            cutoff=cutoff
                        )
                        typer.echo(f"📊 Event: {event.event_type} - {deleted} rows pruned")
                    except ImportError:
                        pass  # Domain events not available
                    
                    typer.secho(f"✅ Deleted {deleted} job records older than {cutoff}", fg="green")
                else:
                    typer.echo(f"✨ No job records found older than {cutoff}")
                    
            except Exception as e:
                typer.echo(f"❌ Failed to delete old records: {e}", err=True)
                raise typer.Exit(1)
                
    except Exception as e:
        typer.echo(f"❌ Database pruning failed: {e}", err=True)
        raise typer.Exit(1)


# Legacy alias for backward compatibility
@prune_app.command("sqlite", hidden=True)
def prune_sqlite_legacy(
    older_than: str = typer.Argument(..., help="Age threshold (e.g., 18m, 90d)"),
    dry_run: bool = typer.Option(
        False, 
        "--dry-run", 
        "-n", 
        help="Show what would be deleted without making changes"
    ),
):
    """Legacy alias for 'prune database' command."""
    typer.secho("⚠️ 'prune sqlite' is deprecated. Use 'prune database' instead.", fg="yellow")
    prune_database(older_than, dry_run)


if __name__ == "__main__":
    prune_app() 