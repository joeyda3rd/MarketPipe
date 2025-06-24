"""Integration tests for CLI boundary check functionality."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from marketpipe.cli import app


class TestIngestCLIBoundaryIntegration:
    """Test CLI integration with boundary checks."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    def test_cli_requires_start_end_without_config(self):
        """Test that CLI requires --start and --end when not using config file."""
        # Test with missing start and end
        result = self.runner.invoke(
            app, ["ohlcv", "ingest", "--symbols", "AAPL", "--provider", "fake"]
        )

        # Should fail with error about missing required fields
        assert result.exit_code == 1
        assert (
            "Either provide --config file OR all of --symbols, --start, and --end" in result.stdout
        )

    def test_cli_requires_symbols_without_config(self):
        """Test that CLI requires --symbols when not using config file."""
        result = self.runner.invoke(
            app,
            [
                "ohlcv",
                "ingest",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--provider",
                "fake",
            ],
        )

        # Should fail with error about missing symbols
        assert result.exit_code == 1
        assert (
            "Either provide --config file OR all of --symbols, --start, and --end" in result.stdout
        )

    @patch("marketpipe.cli.ohlcv_ingest._check_boundaries")
    @patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services")
    @patch("marketpipe.cli.ohlcv_ingest.load_config")
    @patch("marketpipe.cli.ohlcv_ingest.asyncio.run")
    def test_boundary_check_called_after_ingestion(
        self,
        mock_asyncio_run,
        mock_load_config,
        mock_build_services,
        mock_check_boundaries,
        tmp_path,
    ):
        """Test that boundary check is called after successful ingestion."""
        # Mock configuration
        mock_config = MagicMock()
        mock_config.symbols = ["AAPL"]
        mock_config.start.isoformat.return_value = "2024-01-01"
        mock_config.end.isoformat.return_value = "2024-01-02"
        mock_config.provider = "fake"
        mock_config.output_path = str(tmp_path)
        mock_config.workers = 1
        mock_config.batch_size = 100
        mock_config.feed_type = "test"

        # Make merge_overrides return the same config object
        mock_config.merge_overrides.return_value = mock_config
        mock_load_config.return_value = mock_config

        # Mock services
        mock_job_service = MagicMock()
        mock_coordinator_service = MagicMock()
        mock_build_services.return_value = (mock_job_service, mock_coordinator_service)

        # Mock successful ingestion result
        mock_result = {
            "symbols_processed": 1,
            "total_bars": 100,
            "processing_time_seconds": 5.0,
            "symbols_failed": 0,
        }
        mock_asyncio_run.return_value = ("job_123", mock_result)

        # Create test config file
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            """
symbols: [AAPL]
start: "2024-01-01"
end: "2024-01-02"
provider: fake
output_path: test_output
"""
        )

        self.runner.invoke(app, ["ohlcv", "ingest", "--config", str(config_file)])

        # Verify boundary check was called
        mock_check_boundaries.assert_called_once_with(
            path=str(tmp_path),
            symbol="AAPL",
            start="2024-01-01",
            end="2024-01-02",
            provider="fake",
        )

    @patch("marketpipe.cli.ohlcv_ingest._check_boundaries")
    @patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services")
    @patch("marketpipe.cli.ohlcv_ingest.asyncio.run")
    def test_boundary_check_failure_exits_cli(
        self, mock_asyncio_run, mock_build_services, mock_check_boundaries, tmp_path
    ):
        """Test that boundary check failure causes CLI to exit with code 1."""
        # Mock services
        mock_job_service = MagicMock()
        mock_coordinator_service = MagicMock()
        mock_build_services.return_value = (mock_job_service, mock_coordinator_service)

        # Mock successful ingestion result
        mock_result = {
            "symbols_processed": 1,
            "total_bars": 100,
            "processing_time_seconds": 5.0,
            "symbols_failed": 0,
        }
        mock_asyncio_run.return_value = ("job_123", mock_result)

        # Mock boundary check to call sys.exit(1)
        def mock_boundary_check_fail(*args, **kwargs):

            sys.exit(1)

        mock_check_boundaries.side_effect = mock_boundary_check_fail

        result = self.runner.invoke(
            app,
            [
                "ohlcv",
                "ingest",
                "--symbols",
                "AAPL",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--provider",
                "fake",
                "--output",
                str(tmp_path),
            ],
        )

        # CLI should exit with code 1 due to boundary check failure
        assert result.exit_code == 1

    def test_help_shows_updated_descriptions(self):
        """Test that help text shows updated descriptions for required fields."""
        result = self.runner.invoke(app, ["ohlcv", "ingest", "--help"])

        assert result.exit_code == 0
        assert "(required without --config)" in result.stdout
        assert "--start" in result.stdout
        assert "--end" in result.stdout
        assert "--symbols" in result.stdout

    @patch("marketpipe.cli.ohlcv_ingest._check_boundaries")
    @patch("marketpipe.cli.ohlcv_ingest._build_ingestion_services")
    @patch("marketpipe.cli.ohlcv_ingest.asyncio.run")
    def test_multiple_symbols_boundary_check(
        self, mock_asyncio_run, mock_build_services, mock_check_boundaries, tmp_path
    ):
        """Test that boundary check is called for each symbol."""
        # Mock services
        mock_job_service = MagicMock()
        mock_coordinator_service = MagicMock()
        mock_build_services.return_value = (mock_job_service, mock_coordinator_service)

        # Mock successful ingestion result
        mock_result = {
            "symbols_processed": 2,
            "total_bars": 200,
            "processing_time_seconds": 10.0,
            "symbols_failed": 0,
        }
        mock_asyncio_run.return_value = ("job_123", mock_result)

        self.runner.invoke(
            app,
            [
                "ohlcv",
                "ingest",
                "--symbols",
                "AAPL,GOOGL",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--provider",
                "fake",
                "--output",
                str(tmp_path),
            ],
        )

        # Verify boundary check was called for each symbol
        assert mock_check_boundaries.call_count == 2

        # Check that both symbols were processed
        call_args_list = mock_check_boundaries.call_args_list
        symbols_checked = [call[1]["symbol"] for call in call_args_list]
        assert "AAPL" in symbols_checked
        assert "GOOGL" in symbols_checked

    def test_invalid_date_format_handling(self):
        """Test that invalid date formats are handled gracefully."""
        result = self.runner.invoke(
            app,
            [
                "ohlcv",
                "ingest",
                "--symbols",
                "AAPL",
                "--start",
                "invalid-date",
                "--end",
                "2024-01-02",
                "--provider",
                "fake",
            ],
        )

        # Should fail with date parsing error
        assert result.exit_code == 1
        # The exact error message depends on datetime.fromisoformat behavior
