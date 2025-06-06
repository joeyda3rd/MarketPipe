"""Command line interface for MarketPipe."""

import typer
from . import ingestion, aggregation, validation

app = typer.Typer(add_completion=False, help="MarketPipe ETL commands")


@app.command()
def ingest(config: str = typer.Option(..., "--config", help="Path to YAML config")):
    """Run the ingestion pipeline."""
    ingestion.ingest(config)


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
