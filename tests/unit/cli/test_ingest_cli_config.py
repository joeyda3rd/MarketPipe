# SPDX-License-Identifier: Apache-2.0
"""Tests for CLI ingestion configuration functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch, AsyncMock

from typer.testing import CliRunner

from marketpipe.cli import app

runner = CliRunner()


class TestIngestCliConfig:
    """Test cases for CLI configuration functionality."""

    def test_config_file_loading(self):
        """Test that config file is loaded correctly."""
        yaml_content = """
symbols: [AAPL, MSFT]
start: 2025-01-01
end: 2025-01-07
batch_size: 1500
workers: 8
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            with patch("marketpipe.cli._build_ingestion_services") as mock_build:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                mock_build.return_value = (mock_job_service, mock_coordinator_service)

                # Mock async functions
                mock_job_service.create_job.return_value = "test-job-123"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 2,
                    "total_bars": 1000,
                    "files_created": 2,
                    "processing_time_seconds": 1.5,
                }

                # Run the CLI command
                result = runner.invoke(app, ["ingest", "--config", temp_path])

                # Check that the command succeeded
                assert result.exit_code == 0
                assert "Loading configuration from:" in result.stdout
                assert "Creating ingestion job for 2 symbols" in result.stdout
                assert "Batch size: 1500" in result.stdout
                assert "Workers: 8" in result.stdout
        finally:
            Path(temp_path).unlink()

    def test_config_override_with_flags(self):
        """Test that CLI flags override config file values."""
        yaml_content = """
symbols: [AAPL]
start: 2025-01-01
end: 2025-01-07
batch_size: 1000
workers: 4
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            with patch("marketpipe.cli._build_ingestion_services") as mock_build:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                mock_build.return_value = (mock_job_service, mock_coordinator_service)

                # Mock async functions
                mock_job_service.create_job.return_value = "test-job-123"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 1,
                    "total_bars": 500,
                    "files_created": 1,
                    "processing_time_seconds": 0.8,
                }

                # Run the CLI command with overrides
                result = runner.invoke(
                    app,
                    [
                        "ingest",
                        "--config",
                        temp_path,
                        "--batch-size",
                        "2000",
                        "--workers",
                        "12",
                        "--symbols",
                        "NVDA,GOOGL",
                    ],
                )

                # Check that the command succeeded
                assert result.exit_code == 0
                assert "Batch size: 2000" in result.stdout  # Override worked
                assert "Workers: 12" in result.stdout  # Override worked
                assert (
                    "Creating ingestion job for 2 symbols" in result.stdout
                )  # Symbol override worked
        finally:
            Path(temp_path).unlink()

    def test_direct_flags_without_config(self):
        """Test using direct CLI flags without config file."""
        with patch("marketpipe.cli._build_ingestion_services") as mock_build:
            # Mock the services
            mock_job_service = AsyncMock()
            mock_coordinator_service = AsyncMock()
            mock_build.return_value = (mock_job_service, mock_coordinator_service)

            # Mock async functions
            mock_job_service.create_job.return_value = "test-job-456"
            mock_coordinator_service.execute_job.return_value = {
                "symbols_processed": 3,
                "total_bars": 1500,
                "files_created": 3,
                "processing_time_seconds": 2.1,
            }

            # Run the CLI command with direct flags
            result = runner.invoke(
                app,
                [
                    "ingest",
                    "--symbols",
                    "AAPL,MSFT,NVDA",
                    "--start",
                    "2025-02-01",
                    "--end",
                    "2025-02-07",
                    "--batch-size",
                    "750",
                    "--workers",
                    "6",
                ],
            )

            # Check that the command succeeded
            assert result.exit_code == 0
            assert (
                "Creating ingestion job for 3 symbols from 2025-02-01 to 2025-02-07"
                in result.stdout
            )
            assert "Batch size: 750" in result.stdout
            assert "Workers: 6" in result.stdout

    def test_missing_required_flags_error(self):
        """Test that missing required flags produces proper error."""
        result = runner.invoke(app, ["ingest"])

        assert result.exit_code == 1
        assert (
            "Either provide --config file OR all of --symbols, --start, and --end"
            in result.stdout
        )
        assert "Examples:" in result.stdout

    def test_partial_flags_error(self):
        """Test that providing only some required flags produces error."""
        result = runner.invoke(
            app,
            [
                "ingest",
                "--symbols",
                "AAPL,MSFT",
                # Missing --start and --end
            ],
        )

        assert result.exit_code == 1
        assert (
            "Either provide --config file OR all of --symbols, --start, and --end"
            in result.stdout
        )

    def test_kebab_case_config_loading(self):
        """Test that kebab-case config fields are handled correctly."""
        yaml_content = """
symbols: [TESLA]
start: 2025-03-01
end: 2025-03-07
batch-size: 800
feed-type: sip
output-path: ./test_output
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            with patch("marketpipe.cli._build_ingestion_services") as mock_build:
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()
                mock_build.return_value = (mock_job_service, mock_coordinator_service)

                # Mock async functions
                mock_job_service.create_job.return_value = "test-job-789"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 1,
                    "total_bars": 400,
                    "files_created": 1,
                    "processing_time_seconds": 0.5,
                }

                # Run the CLI command
                result = runner.invoke(app, ["ingest", "--config", temp_path])

                # Check that the command succeeded and kebab-case was parsed
                assert result.exit_code == 0
                assert "Batch size: 800" in result.stdout
                assert "Provider: alpaca (sip)" in result.stdout
                assert "Output: ./test_output" in result.stdout
        finally:
            Path(temp_path).unlink()

    def test_invalid_config_file_error(self):
        """Test that invalid config file produces proper error."""
        result = runner.invoke(app, ["ingest", "--config", "/nonexistent/config.yaml"])

        assert result.exit_code == 2  # Typer returns 2 for file not found
        assert "not exist" in result.stdout  # The message is split across lines

    def test_invalid_yaml_syntax_error(self):
        """Test that invalid YAML syntax produces proper error."""
        invalid_yaml = """
symbols: [AAPL, MSFT
start: 2025-01-01
end: 2025-01-07
"""  # Missing closing bracket

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_yaml)
            temp_path = f.name

        try:
            result = runner.invoke(app, ["ingest", "--config", temp_path])

            assert result.exit_code == 1
            assert "Configuration error:" in result.stdout
        finally:
            Path(temp_path).unlink()


# Import Mock at the module level to avoid import issues
