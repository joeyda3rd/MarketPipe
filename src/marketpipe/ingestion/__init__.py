"""Data ingestion stubs."""

from .coordinator import IngestionCoordinator


def ingest(config: str) -> None:
    """Run the ingestion pipeline from a YAML config."""
    coord = IngestionCoordinator(config)
    summary = coord.run()
    print(
        f"Ingested {summary['symbols']} symbols, {summary['rows']} rows, "
        f"wrote {summary['files']} parquet files."
    )
