"""Back-fill gaps in daily OHLCV parquet partitions.

Implements the ``mp ohlcv backfill`` command that:
1. Detects missing *per-symbol* / *per-day* gaps within a look-back window.
2. Creates ingestion jobs **only** for those gaps (idempotent).
3. Executes the coordinator for each gap synchronously.
4. Emits Prometheus metrics (gap counter / latency histogram).
5. Publishes domain events for success / failure.

The implementation builds on the existing ingestion CLI – we simply
invoke the private ``_ingest_impl`` helper with *start=end=gap_day* which
already takes care of repository wiring, validation, storage, etc.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from typing import Optional

import typer

from marketpipe.bootstrap import bootstrap
from marketpipe.cli.ohlcv_ingest import _ingest_impl  # pylint: disable=protected-access
from marketpipe.config import ConfigVersionError, load_config
from marketpipe.domain.events import BackfillJobCompleted, BackfillJobFailed
from marketpipe.infrastructure.events import InMemoryEventPublisher
from marketpipe.ingestion.services.gap_detector import GapDetectorService
from marketpipe.metrics import BACKFILL_GAP_LATENCY_SECONDS, BACKFILL_GAPS_FOUND_TOTAL

# ---------------------------------------------------------------------------
# Typer sub-app will be attached from ``marketpipe.cli.__init__``
# ---------------------------------------------------------------------------

app = typer.Typer(name="backfill", help="Detect and ingest missing daily gaps")


@app.command("backfill")
def backfill_ohlcv(  # noqa: PLR0913 – CLI has many options
    # NOTE: Typer automatically converts YYYY-MM-DD string to datetime.date
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Pipeline YAML config. If omitted the command relies solely on CLI flags.",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    lookback: Optional[int] = typer.Option(
        None,
        "--lookback",
        help="Look-back window in days (default 365). Ignored if --from is given.",
    ),
    since: Optional[str] = typer.Option(
        None,
        "--from",
        help="Start date (YYYY-MM-DD) overriding --lookback.",
    ),
    symbol: list[str] = typer.Option(
        None,
        "--symbol",
        "-s",
        help="Repeatable symbol filter. When omitted the configured universe is used.",
    ),
    provider: Optional[str] = typer.Option(
        None,
        "--provider",
        help="Provider override passed through to ingestion command.",
    ),
) -> None:
    """Fill historical gaps by (re-)ingesting only the missing days."""
    bootstrap()

    # ------------------------------------------------------------------
    # Determine temporal bounds
    # ------------------------------------------------------------------
    today = dt.date.today()
    if since:
        try:
            start_date = dt.datetime.fromisoformat(since).date()
        except ValueError:
            typer.echo("❌ --from date must be in YYYY-MM-DD format", err=True)
            raise typer.Exit(1) from None
    else:
        start_date = today - dt.timedelta(days=lookback or 365)
    end_date = today - dt.timedelta(days=1)  # yesterday – do not process current day

    # ------------------------------------------------------------------
    # Determine symbol universe
    # ------------------------------------------------------------------
    if symbol:
        symbols = [s.upper() for s in symbol]
    else:
        # Fallback: load symbols from YAML config if provided – else fail
        if config is None:
            typer.echo(
                "❌ Either specify --symbol OR provide --config with universe list.", err=True
            )
            raise typer.Exit(1)

        try:
            cfg = load_config(config)
        except ConfigVersionError as exc:
            typer.echo(f"❌ Configuration error: {exc}", err=True)
            raise typer.Exit(1) from exc
        symbols = [s.upper() for s in cfg.symbols]

    # ------------------------------------------------------------------
    # Detect gaps & execute ingestion per gap (synchronously)
    # ------------------------------------------------------------------
    parquet_root = Path("data/output")  # writer.write_parquet default in ingest
    detector = GapDetectorService(parquet_root)

    event_bus = InMemoryEventPublisher()

    total_gaps = 0

    for sym in symbols:
        typer.echo(f"🔍 Scanning {sym} for missing partitions…")
        missing_days = detector.find_missing_days(sym, start_date, end_date)
        BACKFILL_GAPS_FOUND_TOTAL.labels(symbol=sym).inc(len(missing_days))
        total_gaps += len(missing_days)

        for gap_day in missing_days:
            typer.echo(f"🚀 Back-filling {sym} {gap_day}…")
            with BACKFILL_GAP_LATENCY_SECONDS.labels(symbol=sym).time():
                started = dt.datetime.utcnow()
                try:
                    # Re-use ingestion command implementation
                    # Fix: For single day backfill, start=gap_day, end=gap_day+1
                    next_day = gap_day + dt.timedelta(days=1)
                    _ingest_impl(
                        config=str(config) if config else None,
                        symbols=sym,
                        start=gap_day.isoformat(),
                        end=next_day.isoformat(),  # Use next day for end to satisfy start < end validation
                        provider=provider,
                        # leave other overrides None to use config/defaults
                    )
                    duration = (dt.datetime.utcnow() - started).total_seconds()
                    asyncio.run(event_bus.publish(BackfillJobCompleted(sym, gap_day, duration)))
                except Exception as exc:  # noqa: BLE001 – surface any ingestion error
                    asyncio.run(event_bus.publish(BackfillJobFailed(sym, gap_day, str(exc))))
                    typer.echo(f"❌ Back-fill failed for {sym} {gap_day}: {exc}", err=True)

    typer.echo(
        f"✅ Back-fill finished – {total_gaps} gap(s) processed (*detected*, not necessarily filled)."
    )
