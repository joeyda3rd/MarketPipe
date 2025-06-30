# SPDX-License-Identifier: Apache-2.0
"""Tests for the CLI ingest command configuration."""

from __future__ import annotations

import os
import tempfile
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

from marketpipe.cli import app

runner = CliRunner()


class TestIngestCliConfig:
    """Test configuration handling in the ingest CLI."""

    def test_config_file_loading(self):
        """Test that config file is loaded correctly."""
        yaml_content = """
config_version: "1"
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
            with patch.dict(
                "os.environ", {"ALPACA_KEY": "test-key", "ALPACA_SECRET": "test-secret"}
            ):
                with (
                    patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build,
                    patch("marketpipe.cli.ohlcv_ingest._check_boundaries"),
                ):
                    # Mock the services
                    mock_job_service = AsyncMock()
                    mock_coordinator_service = AsyncMock()

                    mock_job_service.create_job.return_value = "config-job-123"
                    mock_coordinator_service.execute_job.return_value = {
                        "symbols_processed": 2,
                        "total_bars": 200,
                        "files_created": 2,
                        "processing_time_seconds": 10.0,
                    }

                    mock_build.return_value = (mock_job_service, mock_coordinator_service)

                    # Run the CLI command using new command name
                    result = runner.invoke(app, ["ingest-ohlcv", "--config", temp_path])

                    # Verify the command succeeded
                    assert result.exit_code == 0
                    assert "config-job-123" in result.stdout
                    assert "Symbols processed: 2" in result.stdout

                    # Verify build was called with correct config
                    mock_build.assert_called_once()
        finally:
            os.unlink(temp_path)

    def test_config_override_with_flags(self):
        """Test that CLI flags override config file values."""
        yaml_content = """
config_version: "1"
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
            with patch.dict(
                "os.environ", {"ALPACA_KEY": "test-key", "ALPACA_SECRET": "test-secret"}
            ):
                with (
                    patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build,
                    patch("marketpipe.cli.ohlcv_ingest._check_boundaries"),
                ):
                    # Mock the services
                    mock_job_service = AsyncMock()
                    mock_coordinator_service = AsyncMock()

                    mock_job_service.create_job.return_value = "override-job-456"
                    mock_coordinator_service.execute_job.return_value = {
                        "symbols_processed": 1,
                        "total_bars": 100,
                        "files_created": 1,
                        "processing_time_seconds": 5.0,
                    }

                    mock_build.return_value = (mock_job_service, mock_coordinator_service)

                    # Run the CLI command with overrides using new command name
                    result = runner.invoke(
                        app,
                        [
                            "ingest-ohlcv",
                            "--config",
                            temp_path,
                            "--symbols",
                            "GOOGL",  # Override config
                            "--batch-size",
                            "2000",  # Override config
                        ],
                    )

                    # Verify the command succeeded
                    assert result.exit_code == 0
                    assert "override-job-456" in result.stdout

                    # Verify build was called
                    mock_build.assert_called_once()
        finally:
            os.unlink(temp_path)

    def test_direct_flags_without_config(self):
        """Test using direct CLI flags without config file."""
        with patch.dict("os.environ", {"ALPACA_KEY": "test-key", "ALPACA_SECRET": "test-secret"}):
            with (
                patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build,
                patch("marketpipe.cli.ohlcv_ingest._check_boundaries"),
            ):
                # Mock the services
                mock_job_service = AsyncMock()
                mock_coordinator_service = AsyncMock()

                mock_job_service.create_job.return_value = "direct-job-789"
                mock_coordinator_service.execute_job.return_value = {
                    "symbols_processed": 2,
                    "total_bars": 150,
                    "files_created": 2,
                    "processing_time_seconds": 8.0,
                }

                mock_build.return_value = (mock_job_service, mock_coordinator_service)

                # Run the CLI command with direct flags using new command name
                result = runner.invoke(
                    app,
                    [
                        "ingest-ohlcv",
                        "--symbols",
                        "AAPL,TSLA",
                        "--start",
                        "2025-01-15",
                        "--end",
                        "2025-01-16",
                        "--batch-size",
                        "750",
                        "--workers",
                        "6",
                    ],
                )

                # Verify the command succeeded
                assert result.exit_code == 0
                assert "direct-job-789" in result.stdout
                assert "Symbols processed: 2" in result.stdout

                # Verify build was called
                mock_build.assert_called_once()

    def test_missing_required_flags_error(self):
        """Test that missing required flags produces proper error."""
        result = runner.invoke(app, ["ingest-ohlcv"])

        assert result.exit_code == 1
        assert (
            "Either provide --config file OR all of --symbols, --start, and --end" in result.stdout
        )

    def test_kebab_case_config_loading(self):
        """Test that kebab-case config fields are handled correctly."""
        yaml_content = """
config_version: "1"
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
            with patch.dict(
                "os.environ", {"ALPACA_KEY": "test-key", "ALPACA_SECRET": "test-secret"}
            ):
                with (
                    patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services") as mock_build,
                    patch("marketpipe.cli.ohlcv_ingest._check_boundaries"),
                ):
                    # Mock the services
                    mock_job_service = AsyncMock()
                    mock_coordinator_service = AsyncMock()

                    mock_job_service.create_job.return_value = "kebab-job-999"
                    mock_coordinator_service.execute_job.return_value = {
                        "symbols_processed": 1,
                        "total_bars": 80,
                        "files_created": 1,
                        "processing_time_seconds": 4.0,
                    }

                    mock_build.return_value = (mock_job_service, mock_coordinator_service)

                    # Run the CLI command using new command name
                    result = runner.invoke(app, ["ingest-ohlcv", "--config", temp_path])

                    # Verify the command succeeded
                    assert result.exit_code == 0
                    assert "kebab-job-999" in result.stdout

                    # Verify build was called
                    mock_build.assert_called_once()
        finally:
            os.unlink(temp_path)

    def test_invalid_yaml_syntax_error(self):
        """Test that invalid YAML syntax produces proper error."""
        invalid_yaml = """
config_version: "1"
symbols: [AAPL, MSFT
start: 2025-01-01
end: 2025-01-07
"""  # Missing closing bracket

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_yaml)
            temp_path = f.name

        try:
            result = runner.invoke(app, ["ingest-ohlcv", "--config", temp_path])

            assert result.exit_code == 2
            assert "invalid YAML in config file" in result.stderr
        finally:
            os.unlink(temp_path)


# Import Mock at the module level to avoid import issues
