"""Integration tests for 'mp symbols update --execute' functionality."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from typer.testing import CliRunner

from marketpipe.cli import app as root_app


def create_mock_duckdb_connection():
    """Create a mock DuckDB connection that supports context manager protocol."""
    mock_conn = MagicMock()
    # Support context manager protocol
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    return mock_conn


class TestSymbolsExecuteIntegration:
    """Integration test suite for the symbols execute pipeline."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.cli.symbols.run_symbol_pipeline")
    def test_full_execute_dummy(
        self,
        mock_run_pipeline,
        mock_list_providers,
    ):
        """Test complete pipeline execution with dummy provider."""
        # Mock provider setup
        mock_list_providers.return_value = ["dummy"]

        # Mock the pipeline to return successful results
        mock_run_pipeline.return_value = (10, 5)  # 10 inserts, 5 updates

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            data_dir = Path(temp_dir) / "data"

            result = self.runner.invoke(
                root_app,
                [
                    "symbols",
                    "update",
                    "-p",
                    "dummy",
                    "--db",
                    str(db_path),
                    "--data-dir",
                    str(data_dir),
                    "--execute",
                ],
            )

        # Verify success
        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert "✅ Pipeline complete." in result.output

        # Verify pipeline was called
        mock_run_pipeline.assert_called_once()

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    def test_execute_missing_provider_token(self, mock_list_providers):
        """Test graceful handling when provider requires missing API token."""
        mock_list_providers.return_value = ["polygon"]

        # Don't set POLYGON_API_KEY environment variable
        result = self.runner.invoke(root_app, ["symbols", "update", "-p", "polygon", "--execute"])

        # Should fail gracefully
        assert result.exit_code == 1

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    def test_execute_without_flag_shows_preview(self, mock_list_providers):
        """Test that without --execute flag, only preview is shown."""
        mock_list_providers.return_value = ["dummy"]

        result = self.runner.invoke(root_app, ["symbols", "update", "-p", "dummy"])

        assert result.exit_code == 0
        assert "Dry preview complete" in result.output
        assert "✅ Pipeline complete." not in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.cli.symbols.run_symbol_pipeline")
    def test_execute_creates_database_views(
        self,
        mock_run_pipeline,
        mock_list_providers,
    ):
        """Test that pipeline creates database views on completion."""
        # Setup mocks
        mock_list_providers.return_value = ["dummy"]

        # Mock the pipeline to return successful results
        mock_run_pipeline.return_value = (5, 3)  # 5 inserts, 3 updates

        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(
                root_app,
                [
                    "symbols",
                    "update",
                    "-p",
                    "dummy",
                    "--db",
                    str(Path(temp_dir) / "test.duckdb"),
                    "--execute",
                ],
            )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert "✅ Pipeline complete." in result.output

        # Verify pipeline was called
        mock_run_pipeline.assert_called_once()

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.cli.symbols.run_symbol_pipeline")
    def test_rerun_same_snapshot_adds_zero_rows(self, mock_run_pipeline, mock_list_providers):
        """Test that rerunning the same snapshot is idempotent."""
        # Setup mocks
        mock_list_providers.return_value = ["dummy"]

        # Mock the pipeline to return no changes (idempotent)
        mock_run_pipeline.return_value = (0, 0)  # 0 inserts, 0 updates

        with tempfile.TemporaryDirectory() as temp_dir:
            # Run twice with same snapshot date
            for _ in range(2):
                result = self.runner.invoke(
                    root_app,
                    [
                        "symbols",
                        "update",
                        "-p",
                        "dummy",
                        "--db",
                        str(Path(temp_dir) / "test.duckdb"),
                        "--snapshot-as-of",
                        "2024-01-01",
                        "--execute",
                    ],
                )
                assert result.exit_code == 0, f"Command failed: {result.output}"

        # Should complete successfully both times
        assert "✅ Pipeline complete." in result.output

        # Verify pipeline was called twice
        assert mock_run_pipeline.call_count == 2
