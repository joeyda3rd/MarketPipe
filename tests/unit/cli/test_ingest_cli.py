# SPDX-License-Identifier: Apache-2.0
"""Tests for the CLI ingest command."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from marketpipe.cli import app

runner = CliRunner()


def test_ingest_cli_smoke():
    """Test that the ingest CLI command can be invoked without errors."""
    with patch.dict("os.environ", {"ALPACA_KEY": "test-key", "ALPACA_SECRET": "test-secret"}):
        with (
            patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build,
            patch("marketpipe.cli.ohlcv_ingest._check_boundaries"),
        ):
            # Mock the services
            mock_job_service = AsyncMock()
            mock_coordinator_service = AsyncMock()

            # Mock create_job to return a job ID
            mock_job_service.create_job.return_value = "job-123"

            # Mock execute_job to return a result dict
            mock_coordinator_service.execute_job.return_value = {
                "symbols_processed": 1,
                "total_bars": 100,
                "symbols_failed": 0,
                "processing_time_seconds": 5.0,
            }

            mock_build.return_value = (mock_job_service, mock_coordinator_service)

            # Run the CLI command using new command name
            result = runner.invoke(
                app,
                [
                    "ingest-ohlcv",
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2025-01-01",
                    "--end",
                    "2025-01-02",
                ],
            )

            # Verify the command succeeded
            assert result.exit_code == 0
            assert "job-123" in result.stdout
            assert "Job completed successfully" in result.stdout
            assert "Symbols processed: 1" in result.stdout


def test_ingest_cli_with_multiple_symbols():
    """Test the ingest CLI with multiple symbols."""
    with patch.dict("os.environ", {"ALPACA_KEY": "test-key", "ALPACA_SECRET": "test-secret"}):
        with (
            patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build,
            patch("marketpipe.cli.ohlcv_ingest._check_boundaries"),
        ):
            # Mock the services
            mock_job_service = AsyncMock()
            mock_coordinator_service = AsyncMock()

            # Mock create_job to return a job ID
            mock_job_service.create_job.return_value = "job-456"

            # Mock execute_job to return a result dict
            mock_coordinator_service.execute_job.return_value = {
                "symbols_processed": 3,
                "total_bars": 300,
                "symbols_failed": 0,
                "processing_time_seconds": 15.0,
            }

            mock_build.return_value = (mock_job_service, mock_coordinator_service)

            # Run the CLI command with multiple symbols using new command name
            result = runner.invoke(
                app,
                [
                    "ingest-ohlcv",
                    "--symbols",
                    "AAPL,GOOGL,MSFT",
                    "--start",
                    "2025-01-01",
                    "--end",
                    "2025-01-02",
                    "--batch-size",
                    "500",
                    "--workers",
                    "2",
                ],
            )

            # Verify the command succeeded
            assert result.exit_code == 0
            assert "job-456" in result.stdout
            assert "Symbols processed: 3" in result.stdout


def test_ingest_cli_handles_service_errors():
    """Test that the CLI handles service errors gracefully."""
    with patch.dict("os.environ", {"ALPACA_KEY": "test-key", "ALPACA_SECRET": "test-secret"}):
        with patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build:
            # Mock the services to raise an error
            mock_job_service = AsyncMock()
            mock_coordinator_service = AsyncMock()

            mock_job_service.create_job.side_effect = ValueError("Invalid symbols")

            mock_build.return_value = (mock_job_service, mock_coordinator_service)

            # Run the CLI command using new command name
            result = runner.invoke(
                app,
                [
                    "ingest-ohlcv",
                    "--symbols",
                    "INVALID",
                    "--start",
                    "2025-01-01",
                    "--end",
                    "2025-01-02",
                ],
            )

            # Verify the command failed gracefully
            assert result.exit_code == 1
            assert "Ingestion failed: Invalid symbols" in result.stdout


def test_ingest_cli_handles_missing_credentials():
    """Test that the CLI handles missing credentials gracefully."""
    with patch.dict("os.environ", {}, clear=True):
        # Run the CLI command without credentials using new command name
        result = runner.invoke(
            app,
            [
                "ingest-ohlcv",
                "--symbols",
                "AAPL",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
            ],
        )

        # Verify the command failed gracefully
        assert result.exit_code == 1
        # The CLI can show one of several messages depending on environment
        assert (
            "validation error for ClientConfig" in result.stdout
            or "Provider 'alpaca' not found" in result.stdout
            or "Alpaca credentials not found" in result.stdout
            or "Ingestion failed: 'api_key'" in result.stdout
        )


def test_ingest_cli_wires_polygon_provider_config():
    """Ensure polygon provider wiring forwards env-derived configuration."""

    with patch.dict("os.environ", {"POLYGON_API_KEY": "poly-key"}, clear=True):
        with (
            patch("marketpipe.cli.ohlcv_ingest._check_boundaries"),
            patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build,
        ):
            mock_job_service = AsyncMock()
            mock_coordinator_service = AsyncMock()
            mock_job_service.create_job.return_value = "poly-job"
            mock_coordinator_service.execute_job.return_value = {
                "symbols_processed": 1,
                "total_bars": 100,
                "symbols_failed": 0,
                "processing_time_seconds": 5.0,
            }

            captured: dict[str, dict[str, object]] = {}

            def _stub(provider_config, output_path):
                captured["config"] = provider_config
                return mock_job_service, mock_coordinator_service

            mock_build.side_effect = _stub

            result = runner.invoke(
                app,
                [
                    "ingest-ohlcv",
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2025-01-01",
                    "--end",
                    "2025-01-02",
                    "--provider",
                    "polygon",
                ],
            )

            assert result.exit_code == 0
            assert "poly-job" in result.stdout
            assert "Feed type: delayed" in result.stdout

            provider_config = captured["config"]
            assert provider_config["provider"] == "polygon"
            assert provider_config["api_key"] == "poly-key"
            assert "feed_type" not in provider_config
            assert provider_config["base_url"] == "https://api.polygon.io"


def test_ingest_cli_polygon_requires_api_key():
    """Polygon provider requires POLYGON_API_KEY in environment."""

    with patch.dict("os.environ", {}, clear=True):
        result = runner.invoke(
            app,
            [
                "ingest-ohlcv",
                "--symbols",
                "AAPL",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--provider",
                "polygon",
            ],
        )

        assert result.exit_code == 1
        assert "POLYGON_API_KEY" in result.stdout
