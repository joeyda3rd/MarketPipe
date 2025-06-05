"""Command line interface for MarketPipe."""

import typer
from . import ingestion, aggregation, validation

app = typer.Typer(add_completion=False, help="MarketPipe ETL commands")


@app.command()
def ingest(start: str | None = typer.Option(None, help="Start date YYYY-MM-DD"),
           end: str | None = typer.Option(None, help="End date YYYY-MM-DD")):
    """Ingest daily or historical OHLCV data."""
    ingestion.ingest(start=start, end=end)


@app.command()
def aggregate():
    """Aggregate raw data into coarser time frames."""
    aggregation.aggregate()


@app.command()
def validate():
    """Validate aggregated data against a reference API."""
    validation.validate()


if __name__ == "__main__":
    app()
