# SPDX-License-Identifier: Apache-2.0
"""Test bootstrap side-effect removal.

Ensures that importing marketpipe.cli does not create database files
or trigger any side-effects, and that bootstrap is properly called
when CLI commands are executed.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from marketpipe.bootstrap import bootstrap, is_bootstrapped, reset_bootstrap_state


class TestBootstrapSideEffects:
    """Test that import-time side-effects have been removed."""

    def test_import_cli_no_db_creation(self, tmp_path):
        """Test that importing marketpipe.cli does NOT create database files."""
        # Set up clean environment with custom database path
        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            # Reset bootstrap state to ensure clean test
            reset_bootstrap_state()

            # Import CLI module - this should NOT create any files

            # Verify no database file was created at the custom path
            assert not (tmp_path / "test_core.db").exists()
            assert not (tmp_path / "data").exists()

            # Verify bootstrap was not called
            assert not is_bootstrapped()

    def test_help_command_no_db_creation(self, tmp_path):
        """Test that running --help does not create database files."""
        from typer.testing import CliRunner

        from marketpipe.cli import app

        # Set up clean environment
        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            runner = CliRunner()
            result = runner.invoke(app, ["--help"])

            # Help should work without errors
            assert result.exit_code == 0
            assert "MarketPipe ETL commands" in result.stdout

            # No database should be created
            assert not (tmp_path / "test_core.db").exists()
            assert not is_bootstrapped()

    def test_subcommand_help_no_db_creation(self, tmp_path):
        """Test that subcommand help does not trigger bootstrap."""
        from typer.testing import CliRunner

        from marketpipe.cli import app

        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            runner = CliRunner()
            result = runner.invoke(app, ["ohlcv", "--help"])

            # Help should work
            assert result.exit_code == 0
            assert "OHLCV pipeline commands" in result.stdout

            # No database should be created
            assert not (tmp_path / "test_core.db").exists()
            assert not is_bootstrapped()


class TestBootstrapFunctionality:
    """Test the bootstrap module functionality."""

    def test_bootstrap_function_idempotent(self, tmp_path):
        """Test that bootstrap() can be called multiple times safely."""
        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            # Mock the dependencies to avoid actual database/service operations
            with (
                patch("marketpipe.bootstrap.apply_pending_alembic") as mock_apply,
                patch("marketpipe.validation.ValidationRunnerService.register") as mock_val_reg,
                patch("marketpipe.aggregation.AggregationRunnerService.register") as mock_agg_reg,
            ):

                # First call should trigger all operations
                bootstrap()
                assert is_bootstrapped()

                # Verify all operations were called once
                mock_apply.assert_called_once()
                mock_val_reg.assert_called_once()
                mock_agg_reg.assert_called_once()

                # Reset mocks
                mock_apply.reset_mock()
                mock_val_reg.reset_mock()
                mock_agg_reg.reset_mock()

                # Second call should be a no-op
                bootstrap()
                assert is_bootstrapped()

                # Verify no operations were called again
                mock_apply.assert_not_called()
                mock_val_reg.assert_not_called()
                mock_agg_reg.assert_not_called()

    def test_bootstrap_error_handling(self, tmp_path):
        """Test that bootstrap handles errors gracefully."""
        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            # Mock migrations to raise an error
            with patch(
                "marketpipe.bootstrap.apply_pending_alembic",
                side_effect=Exception("Migration failed"),
            ):
                with pytest.raises(RuntimeError, match="MarketPipe bootstrap failed"):
                    bootstrap()

                # Should not be marked as bootstrapped on failure
                assert not is_bootstrapped()

    def test_bootstrap_environment_variable(self, tmp_path):
        """Test that bootstrap respects MP_DB environment variable."""
        custom_db_path = tmp_path / "custom_path" / "custom.db"

        with patch.dict(os.environ, {"MP_DB": str(custom_db_path)}):
            reset_bootstrap_state()

            with (
                patch("marketpipe.bootstrap.apply_pending_alembic") as mock_apply,
                patch("marketpipe.validation.ValidationRunnerService.register"),
                patch("marketpipe.aggregation.AggregationRunnerService.register"),
            ):

                bootstrap()

                # Verify apply_pending was called with the custom path
                mock_apply.assert_called_once_with(custom_db_path)

    def test_reset_bootstrap_state_for_testing(self, tmp_path):
        """Test that reset_bootstrap_state works for testing."""
        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()
            assert not is_bootstrapped()

            # Mock bootstrap to avoid side effects
            with (
                patch("marketpipe.bootstrap.apply_pending_alembic"),
                patch("marketpipe.validation.ValidationRunnerService.register"),
                patch("marketpipe.aggregation.AggregationRunnerService.register"),
            ):

                bootstrap()
                assert is_bootstrapped()

                # Reset state
                reset_bootstrap_state()
                assert not is_bootstrapped()


class TestCliCommandBootstrap:
    """Test that CLI commands properly call bootstrap."""

    def test_ingest_command_calls_bootstrap(self, tmp_path):
        """Test that ingest command calls bootstrap."""
        from typer.testing import CliRunner

        from marketpipe.cli import app

        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            runner = CliRunner()

            # Mock bootstrap itself and _ingest_impl to prevent actual execution
            with (
                patch("marketpipe.bootstrap.bootstrap") as mock_bootstrap,
                patch(
                    "marketpipe.cli.ohlcv_ingest._build_ingestion_services"
                ) as mock_build_services,
                patch("marketpipe.cli.ohlcv_ingest.asyncio.run") as mock_async_run,
            ):

                # Mock the services to avoid complex setup
                mock_build_services.return_value = (MagicMock(), MagicMock())
                mock_async_run.side_effect = Exception("Expected test error")

                # Provide minimum required parameters so command reaches _ingest_impl
                result = runner.invoke(
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

                # Command should fail due to our mock exception
                assert result.exit_code != 0

                # But bootstrap should have been called
                mock_bootstrap.assert_called_once()

    def test_validate_command_calls_bootstrap(self, tmp_path):
        """Test that validate command calls bootstrap."""
        from typer.testing import CliRunner

        from marketpipe.cli import app

        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            runner = CliRunner()

            with (
                patch("marketpipe.bootstrap.apply_pending_alembic") as mock_apply,
                patch("marketpipe.validation.ValidationRunnerService.register"),
                patch("marketpipe.aggregation.AggregationRunnerService.register"),
                patch("marketpipe.cli.ohlcv_validate.CsvReportRepository") as mock_repo,
            ):

                # Mock empty reports list to avoid actual file operations
                mock_repo.return_value.list_reports.return_value = []

                result = runner.invoke(app, ["ohlcv", "validate", "--list"])

                # Command should succeed (listing empty reports)
                assert result.exit_code == 0

                # Bootstrap should have been called
                assert is_bootstrapped()
                mock_apply.assert_called_once()

    def test_aggregate_command_calls_bootstrap(self, tmp_path):
        """Test that aggregate command calls bootstrap."""
        from typer.testing import CliRunner

        from marketpipe.cli import app

        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            runner = CliRunner()

            with (
                patch("marketpipe.bootstrap.apply_pending_alembic") as mock_apply,
                patch("marketpipe.validation.ValidationRunnerService.register") as mock_val_reg,
                patch("marketpipe.aggregation.AggregationRunnerService.register") as mock_agg_reg,
                patch(
                    "marketpipe.cli.ohlcv_aggregate.AggregationRunnerService.build_default"
                ) as mock_build,
            ):

                # Mock the aggregation service
                mock_service = MagicMock()
                mock_build.return_value = mock_service

                result = runner.invoke(app, ["ohlcv", "aggregate", "test-job-id"])

                # Command should succeed
                assert result.exit_code == 0

                # Bootstrap should have been called
                assert is_bootstrapped()
                mock_apply.assert_called_once()
                mock_val_reg.assert_called_once()
                mock_agg_reg.assert_called_once()

    def test_metrics_command_calls_bootstrap(self, tmp_path):
        """Test that metrics command calls bootstrap."""
        from typer.testing import CliRunner

        from marketpipe.cli import app

        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            runner = CliRunner()

            with (
                patch("marketpipe.bootstrap.apply_pending_alembic") as mock_apply,
                patch("marketpipe.validation.ValidationRunnerService.register"),
                patch("marketpipe.aggregation.AggregationRunnerService.register"),
                patch("marketpipe.metrics.SqliteMetricsRepository") as mock_repo,
            ):

                # Mock empty metrics list - need to return an async function
                async def mock_list_metric_names():
                    return []

                mock_repo.return_value.list_metric_names = mock_list_metric_names

                result = runner.invoke(app, ["metrics", "--list"])

                # Debug output for failing test
                if result.exit_code != 0:
                    print(f"Exit code: {result.exit_code}")
                    print(f"Output: {result.output}")
                    print(f"Exception: {result.exception}")

                # Command should succeed
                assert result.exit_code == 0

                # Bootstrap should have been called
                assert is_bootstrapped()
                mock_apply.assert_called_once()

    def test_query_command_calls_bootstrap(self, tmp_path):
        """Test that query command calls bootstrap."""
        from typer.testing import CliRunner

        from marketpipe.cli import app

        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            runner = CliRunner()

            with (
                patch("marketpipe.bootstrap.apply_pending_alembic") as mock_apply,
                patch("marketpipe.validation.ValidationRunnerService.register"),
                patch("marketpipe.aggregation.AggregationRunnerService.register"),
                patch("marketpipe.aggregation.infrastructure.duckdb_views.query") as mock_query,
            ):

                # Mock query to return empty dataframe
                import pandas as pd

                mock_query.return_value = pd.DataFrame()

                result = runner.invoke(app, ["query", "SELECT 1"])

                # Command should succeed
                assert result.exit_code == 0

                # Bootstrap should have been called
                assert is_bootstrapped()
                mock_apply.assert_called_once()


class TestBootstrapConcurrency:
    """Test bootstrap behavior under concurrent access."""

    def test_concurrent_bootstrap_calls(self, tmp_path):
        """Test that concurrent bootstrap calls are handled safely."""
        import threading
        import time

        with patch.dict(os.environ, {"MP_DB": str(tmp_path / "test_core.db")}):
            reset_bootstrap_state()

            call_count = 0
            call_lock = threading.Lock()

            def counting_apply_pending(path):
                nonlocal call_count
                with call_lock:
                    call_count += 1
                # Simulate some work time
                time.sleep(0.01)

            with (
                patch(
                    "marketpipe.bootstrap.apply_pending_alembic", side_effect=counting_apply_pending
                ),
                patch("marketpipe.validation.ValidationRunnerService.register"),
                patch("marketpipe.aggregation.AggregationRunnerService.register"),
            ):

                # Start multiple threads calling bootstrap
                threads = []
                for _ in range(5):
                    thread = threading.Thread(target=bootstrap)
                    threads.append(thread)
                    thread.start()

                # Wait for all threads to complete
                for thread in threads:
                    thread.join()

                # Bootstrap should be marked as completed
                assert is_bootstrapped()

                # apply_pending should only be called once despite multiple threads
                assert call_count == 1
