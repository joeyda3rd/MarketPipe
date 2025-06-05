"""Data ingestion stubs."""

from datetime import date


def ingest(start: str | None = None, end: str | None = None) -> None:
    """Placeholder for daily or historical ingestion."""
    if start and end:
        print(f"Backfilling data from {start} to {end} ...")
    else:
        today = date.today().isoformat()
        print(f"Ingesting data for {today} ...")
