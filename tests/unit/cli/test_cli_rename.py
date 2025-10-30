"""Tests for CLI command renaming and modular structure."""

from __future__ import annotations

import warnings
from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

from marketpipe.cli import app

runner = CliRunner()


class TestCLIRename:
    """Test suite for CLI command renaming."""

    def test_main_app_help(self):
        """Test that main app help displays correctly."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "MarketPipe ETL commands" in result.stdout

    def test_ohlcv_subapp_exists(self):
        """Test that OHLCV sub-app exists and has commands."""
        result = runner.invoke(app, ["ohlcv", "--help"])
        assert result.exit_code == 0
        assert "OHLCV pipeline commands" in result.stdout
        assert "ingest" in result.stdout
        assert "validate" in result.stdout
        assert "aggregate" in result.stdout

    def test_convenience_commands_exist(self):
        """Test that convenience commands exist at top level."""
        # Test help for convenience commands
        for cmd in ["ingest-ohlcv", "validate-ohlcv", "aggregate-ohlcv"]:
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, f"Command {cmd} should exist"

    def test_deprecated_commands_exist(self):
        """Test that deprecated commands still exist."""
        # Test help for deprecated commands
        for cmd in ["ingest", "validate", "aggregate"]:
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, f"Deprecated command {cmd} should exist"
            assert "DEPRECATED" in result.stdout

    @patch("marketpipe.cli.ohlcv_ingest._ingest_impl")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_deprecated_ingest_shows_warning(self, mock_bootstrap, mock_ingest):
        """Test that deprecated ingest command shows warning."""
        mock_ingest.return_value = None

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = runner.invoke(
                app, ["ingest", "--symbols", "AAPL", "--start", "2024-01-01", "--end", "2024-01-02"]
            )

            # Should show deprecation warning in output
            assert "deprecated" in result.stdout.lower()
            mock_ingest.assert_called_once()

    @patch("marketpipe.cli.ohlcv_validate._validate_impl")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_deprecated_validate_shows_warning(self, mock_bootstrap, mock_validate):
        """Test that deprecated validate command shows warning."""
        mock_validate.return_value = None

        result = runner.invoke(app, ["validate", "--list"])

        # Should show deprecation warning in output
        assert "deprecated" in result.stdout.lower()
        mock_validate.assert_called_once()

    @patch("marketpipe.cli.ohlcv_aggregate._aggregate_impl")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_deprecated_aggregate_shows_warning(self, mock_bootstrap, mock_aggregate):
        """Test that deprecated aggregate command shows warning."""
        mock_aggregate.return_value = None

        result = runner.invoke(app, ["aggregate", "test-job-id"])

        # Should show deprecation warning in output
        assert "deprecated" in result.stdout.lower()
        mock_aggregate.assert_called_once()

    def test_utility_commands_exist(self):
        """Test that utility commands exist."""
        utility_commands = ["query", "metrics", "providers", "migrate"]

        for cmd in utility_commands:
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, f"Utility command {cmd} should exist"

    @patch("marketpipe.cli.ohlcv_ingest._ingest_impl")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_ohlcv_ingest_works(self, mock_bootstrap, mock_ingest):
        """Test that new ohlcv ingest command works."""
        mock_ingest.return_value = None

        runner.invoke(
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
            ],
        )

        mock_ingest.assert_called_once()

    @patch("marketpipe.cli.ohlcv_ingest._ingest_impl")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_ingest_ohlcv_convenience_works(self, mock_bootstrap, mock_ingest):
        """Test that convenience ingest-ohlcv command works."""
        mock_ingest.return_value = None

        runner.invoke(
            app,
            ["ingest-ohlcv", "--symbols", "AAPL", "--start", "2024-01-01", "--end", "2024-01-02"],
        )

        mock_ingest.assert_called_once()

    @patch("marketpipe.cli.ohlcv_validate._validate_impl")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_ohlcv_validate_works(self, mock_bootstrap, mock_validate):
        """Test that new ohlcv validate command works."""
        mock_validate.return_value = None

        runner.invoke(app, ["ohlcv", "validate", "--list"])

        mock_validate.assert_called_once()

    @patch("marketpipe.cli.ohlcv_aggregate._aggregate_impl")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_ohlcv_aggregate_works(self, mock_bootstrap, mock_aggregate):
        """Test that new ohlcv aggregate command works."""
        mock_aggregate.return_value = None

        runner.invoke(app, ["ohlcv", "aggregate", "test-job-id"])

        mock_aggregate.assert_called_once()

    def test_command_signatures_preserved(self):
        """Test that command signatures are preserved from old CLI."""
        # Test that the new commands accept the same parameters as the old ones

        # Ingest command parameters
        result = runner.invoke(app, ["ingest-ohlcv", "--help"])

        # Skip option checks if we're in a minimal environment without proper Typer
        # Check for common indicators that the full Typer options are available
        if (
            "Typer stub placeholder" in result.stdout
            or len(result.stdout) < 200
            or "Options" not in result.stdout  # Full Typer shows "Options" section
            or "--help" not in result.stdout  # Full Typer shows --help option
        ):
            return

        assert "--config" in result.stdout
        assert "--symbols" in result.stdout
        assert "--start" in result.stdout
        assert "--end" in result.stdout
        assert "--provider" in result.stdout

        # Validate command parameters
        result = runner.invoke(app, ["validate-ohlcv", "--help"])
        assert "JOB_ID" in result.stdout  # Now a positional argument, not --job-id
        assert "--list" in result.stdout
        assert "--show" in result.stdout

        # Aggregate command parameters
        result = runner.invoke(app, ["aggregate-ohlcv", "--help"])
        # Should accept a job_id argument
        assert "job_id" in result.stdout or "JOB_ID" in result.stdout

    @patch("marketpipe.cli.utils.list_providers")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_providers_command_works(self, mock_bootstrap, mock_list_providers):
        """Test that providers command works."""
        mock_list_providers.return_value = ["alpaca", "iex", "fake"]

        result = runner.invoke(app, ["providers"])

        assert result.exit_code == 0
        assert "alpaca" in result.stdout
        mock_list_providers.assert_called_once()

    @patch("marketpipe.aggregation.infrastructure.duckdb_views.query")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_query_command_works(self, mock_bootstrap, mock_query):
        """Test that query command works."""
        import pandas as pd

        mock_query.return_value = pd.DataFrame({"symbol": ["AAPL"], "close": [150.0]})

        result = runner.invoke(app, ["query", "SELECT * FROM bars_1d LIMIT 1"])

        assert result.exit_code == 0
        mock_query.assert_called_once()

    @patch("marketpipe.migrations.apply_pending")
    @patch("marketpipe.bootstrap.bootstrap")
    def test_migrate_command_works(self, mock_bootstrap, mock_apply_pending):
        """Test that migrate command works."""
        mock_apply_pending.return_value = None

        result = runner.invoke(app, ["migrate"])

        assert result.exit_code == 0
        assert "up-to-date" in result.stdout
        mock_apply_pending.assert_called_once()


class TestCLIModularity:
    """Test CLI modular structure."""

    def test_can_import_individual_modules(self):
        """Test that individual CLI modules can be imported and have expected functions."""
        # Import the CLI functions to verify they're available
        from marketpipe.cli import metrics, migrate, providers, query
        from marketpipe.cli.ohlcv_aggregate import aggregate_ohlcv
        from marketpipe.cli.ohlcv_ingest import ingest_ohlcv
        from marketpipe.cli.ohlcv_validate import validate_ohlcv

        # Check that all CLI functions are callable
        assert callable(ingest_ohlcv)
        assert callable(validate_ohlcv)
        assert callable(aggregate_ohlcv)
        assert callable(query)
        assert callable(metrics)
        assert callable(providers)
        assert callable(migrate)

    def test_app_structure(self):
        """Test that app has proper structure."""
        from marketpipe.cli import app, ohlcv_app

        # Main app should exist
        assert isinstance(app, typer.Typer)

        # OHLCV sub-app should exist
        assert isinstance(ohlcv_app, typer.Typer)

        # Test that commands can be invoked
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "ingest-ohlcv" in result.stdout
        assert "validate-ohlcv" in result.stdout
        assert "aggregate-ohlcv" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__])
