"""Command line interface for MarketPipe."""

import typer
from . import ingestion, aggregation, validation
from .metrics_server import run as metrics_server_run

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


@app.command()
def metrics(port: int = typer.Option(8000, "--port", "-p", help="Port to run metrics server on")):
    """Start the Prometheus metrics server."""
    import os
    import tempfile
    
    # Set up multiprocess metrics directory if not already set
    if 'PROMETHEUS_MULTIPROC_DIR' not in os.environ:
        multiproc_dir = os.path.join(tempfile.gettempdir(), 'prometheus_multiproc')
        os.makedirs(multiproc_dir, exist_ok=True)
        os.environ['PROMETHEUS_MULTIPROC_DIR'] = multiproc_dir
        print(f"📊 Multiprocess metrics enabled: {multiproc_dir}")
    
    print(f"Starting metrics server on http://localhost:{port}/metrics")
    print("Press Ctrl+C to stop the server")
    try:
        metrics_server_run(port=port)
        # Keep the server running
        import time
        while True:
            time.sleep(1)
    except OSError as e:
        if e.errno == 98:  # Address already in use
            print(f"\n❌ Error: Port {port} is already in use!")
            print(f"To find what's using the port: lsof -i :{port}")
            print(f"To kill the process: kill <PID>")
            print(f"Or try a different port: python -m marketpipe metrics --port <other_port>")
            raise typer.Exit(1)
        else:
            raise  # Re-raise if it's a different OSError
    except KeyboardInterrupt:
        print("\nMetrics server stopped")


if __name__ == "__main__":
    app()
