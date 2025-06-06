"""Module entrypoint for ``python -m marketpipe.ingestion``."""

from __future__ import annotations

import typer

from . import ingest


app = typer.Typer(add_completion=False)


@app.command()
def run(config: str = typer.Option(..., "--config", help="Path to YAML config")):
    ingest(config)


if __name__ == "__main__":  # pragma: no cover - thin wrapper
    app()

