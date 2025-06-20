from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(help="Symbol-master related commands.")


@app.command("update")
def update(
    provider: Optional[list[str]] = typer.Option(
        None,
        "--provider",
        "-p",
        help="Symbol provider(s) to ingest. Omit for all.",
    ),
    db: Optional[Path] = typer.Option(
        None,
        "--db",
        help="DuckDB database path.",
        exists=False,
    ),
    data_dir: Optional[Path] = typer.Option(
        None,
        "--data-dir",
        help="Parquet dataset root.",
        exists=False,
    ),
    backfill: Optional[str] = typer.Option(
        None,
        help="Back-fill symbols starting this date (YYYY-MM-DD).",
    ),
    snapshot_as_of: Optional[str] = typer.Option(
        None,
        "--snapshot-as-of",
        help="Override provider snapshot date (YYYY-MM-DD).",
    ),
    dry_run: bool = typer.Option(
        False, help="Run pipeline but skip DB / Parquet writes."
    ),
    diff_only: bool = typer.Option(
        False, help="Skip provider fetch; run diff + SCD update only."
    ),
    execute: bool = typer.Option(
        False,
        help="Perform writes; without this flag command is read-only.",
    ),
) -> None:
    """
    Fetch symbol snapshots, diff against master table, and (optionally)
    update the SCD-history Parquet dataset.
    """

    # ------- Flag exclusivity handling ------ #
    if dry_run and execute:
        typer.secho(
            "⚠️  Both --dry-run and --execute specified. --execute takes precedence.",
            fg=typer.colors.YELLOW,
        )
        dry_run = False  # --execute wins

    # ------- Apply defaults for environment variables ------ #
    if db is None:
        db = Path(os.getenv("MP_DB", "warehouse.duckdb"))
    if data_dir is None:
        data_dir = Path(os.getenv("MP_DATA_DIR", "warehouse/symbols_master"))
    if snapshot_as_of is None:
        snapshot_as_of = _dt.date.today().isoformat()

    # ------- Date validation ------ #
    def _parse_date(date_str: str, field_name: str) -> _dt.date:
        """Parse date string with validation."""
        try:
            return _dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            typer.secho(f"Invalid date format for {field_name}: {date_str}. Use YYYY-MM-DD.", fg=typer.colors.RED)
            raise typer.Exit(code=1)

    if backfill:
        _parse_date(backfill, "--backfill")
    _parse_date(snapshot_as_of, "--snapshot-as-of")

    # ------- Sanity checks on CLI input ------ #
    # Lazy import to avoid slow startup for help text
    from marketpipe.ingestion.symbol_providers import list_providers
    avail = set(list_providers())
    chosen = set(provider) if provider else avail
    unknown = chosen - avail
    if unknown:
        typer.secho(f"Unknown provider(s): {', '.join(sorted(unknown))}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    plan = {
        "providers": sorted(chosen),
        "db": str(db),
        "data_dir": str(data_dir),
        "backfill": backfill if backfill else "-",
        "snapshot_as_of": snapshot_as_of,
        "dry_run": dry_run,
        "diff_only": diff_only,
        "execute": execute,
    }
    typer.echo("Symbol update plan:")
    for k, v in plan.items():
        typer.echo(f"  • {k:>15}: {v}")

    if not execute:
        typer.secho(
            "\nDry preview complete. Re-run with --execute to perform writes.",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit()

    # Import and run the actual pipeline
    from marketpipe.ingestion.pipeline.symbol_pipeline import run_symbol_pipeline
    
    typer.echo()
    snapshot_date = _dt.date.fromisoformat(snapshot_as_of)
    try:
        run_symbol_pipeline(
            db_path=db,
            data_dir=data_dir,
            provider_names=sorted(chosen),
            snapshot_as_of=snapshot_date,
        )
        typer.secho("✅ Pipeline complete.", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"❌ Pipeline failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1) 