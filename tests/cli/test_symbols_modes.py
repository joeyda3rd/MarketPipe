"""Tests for symbols CLI modes: --dry-run, --diff-only, --backfill"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from marketpipe.cli import app as root_app
from typer.testing import CliRunner


def create_mock_duckdb_connection():
    """Create a mock DuckDB connection that supports context manager protocol."""
    mock_conn = MagicMock()
    # Support context manager protocol
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)

    # Mock SQL execution methods to return proper results
    mock_result = Mock()
    mock_result.fetchone.return_value = [0]  # Default count result
    mock_conn.sql.return_value = mock_result
    mock_conn.execute.return_value = mock_result

    # Mock the specific queries the pipeline uses
    def mock_sql_query(query_str):
        query_lower = query_str.lower()
        if "max(as_of) from symbols_snapshot" in query_lower:
            # Return today's date for snapshot query
            result = Mock()
            result.fetchone.return_value = [date.today()]
            return result
        elif "count(*) from diff_" in query_lower:
            # Return 0 for diff table counts
            result = Mock()
            result.fetchone.return_value = [0]
            return result
        elif "count(*) from symbols_master" in query_lower:
            # Return 0 for master table count
            result = Mock()
            result.fetchone.return_value = [0]
            return result
        else:
            # Default mock result for any other SQL
            result = Mock()
            result.fetchone.return_value = [0]
            return result

    mock_conn.sql.side_effect = mock_sql_query
    mock_conn.execute.side_effect = mock_sql_query

    # Mock additional methods that might be called
    mock_conn.register = Mock()
    mock_conn.unregister = Mock()
    mock_conn.close = Mock()

    return mock_conn


class TestSymbolsModes:
    """Test suite for symbols CLI modes functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

        # Reset any lingering mocks to prevent test interference
        import gc

        # Force garbage collection to clear any lingering mocks
        gc.collect()

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.cli.symbols.run_symbol_pipeline")
    def test_dry_run_with_execute_precedence(
        self,
        mock_run_pipeline,
        mock_list_providers,
    ):
        """Test that --execute takes precedence over --dry-run and executes with proper mocking."""
        mock_list_providers.return_value = ["dummy"]

        # Mock the pipeline to return successful results
        mock_run_pipeline.return_value = (1, 0)  # 1 insert, 0 updates

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            data_dir = Path(temp_dir) / "data"

            # Test --execute precedence over --dry-run
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
                    "--dry-run",
                    "--execute",  # --execute takes precedence
                ],
            )

            # Verify success and precedence warning
            assert result.exit_code == 0, f"Command failed: {result.output}"
            assert "--execute takes precedence" in result.output
            assert "✅ Pipeline complete." in result.output

            # Verify pipeline was called (not dry-run mode)
            mock_run_pipeline.assert_called_once()

    def test_diff_only_error_combo(self):
        """Test that --dry-run --diff-only combination produces error."""
        result = self.runner.invoke(
            root_app, ["symbols", "update", "-p", "dummy", "--dry-run", "--diff-only", "--execute"]
        )

        assert result.exit_code == 1
        assert "`--diff-only` implies DB writes; cannot combine with --dry-run." in result.output

    def test_backfill_diff_only_error_combo(self):
        """Test that --backfill with --diff-only combination produces error."""
        result = self.runner.invoke(
            root_app,
            [
                "symbols",
                "update",
                "-p",
                "dummy",
                "--backfill",
                "2025-06-17",
                "--diff-only",
                "--execute",
            ],
        )

        assert result.exit_code == 1
        assert "Back-fill requires provider fetch -> cannot use --diff-only." in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.cli.symbols.run_symbol_pipeline")
    def test_backfill_runs_multiple_days(
        self,
        mock_run_pipeline,
        mock_list_providers,
    ):
        """Test that backfill runs for multiple days and creates Parquet partitions."""
        mock_list_providers.return_value = ["dummy"]

        # Mock the pipeline to return successful results
        mock_run_pipeline.return_value = (2, 1)  # 2 inserts, 1 update per day

        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "data"

            result = self.runner.invoke(
                root_app,
                [
                    "symbols",
                    "update",
                    "-p",
                    "dummy",
                    "--data-dir",
                    str(data_dir),
                    "--backfill",
                    "2025-06-17",
                    "--snapshot-as-of",
                    "2025-06-19",
                    "--execute",
                ],
            )

            # Verify success
            assert result.exit_code == 0, f"Command failed: {result.output}"

            # Verify three days were processed (17, 18, 19)
            assert "2025-06-17" in result.output
            assert "2025-06-18" in result.output
            assert "2025-06-19" in result.output
            assert "✅ Pipeline complete." in result.output

            # Verify pipeline was called 3 times (once for each date)
            assert mock_run_pipeline.call_count == 3

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.run_scd_update")  # Mock SCD execution
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.refresh")  # Mock view refresh
    @patch("duckdb.connect")
    def test_diff_only_path(self, mock_duckdb, mock_refresh, mock_scd, mock_list_providers):
        """Test that diff-only mode skips provider fetch and assumes symbols_snapshot exists."""
        mock_list_providers.return_value = ["dummy"]

        # Mock DuckDB connection with pre-existing symbols_snapshot
        mock_conn = create_mock_duckdb_connection()

        # Override the default to return 1 for symbols_snapshot count (table exists with data)
        def mock_sql_with_data(query_str):
            if "count(*) from symbols_snapshot" in query_str.lower():
                result = Mock()
                result.fetchone.return_value = [1]  # Table exists with data
                return result
            else:
                return create_mock_duckdb_connection().sql(query_str)

        mock_conn.execute.side_effect = mock_sql_with_data
        mock_duckdb.return_value = mock_conn

        # Mock pipeline components
        mock_scd.return_value = None
        mock_refresh.return_value = None

        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "data"

            # Patch fetch_providers to ensure it's not called
            with patch(
                "marketpipe.ingestion.pipeline.symbol_pipeline.fetch_providers"
            ) as mock_fetch:
                result = self.runner.invoke(
                    root_app,
                    [
                        "symbols",
                        "update",
                        "-p",
                        "dummy",
                        "--data-dir",
                        str(data_dir),
                        "--diff-only",
                        "--execute",
                    ],
                )

                # Verify success
                assert result.exit_code == 0

                # Verify provider fetch was NOT called
                mock_fetch.assert_not_called()

                # Verify SCD and refresh were called
                mock_scd.assert_called_once()
                mock_refresh.assert_called_once()

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.get_provider")
    @patch(
        "marketpipe.ingestion.pipeline.symbol_pipeline.normalize_stage"
    )  # Mock the SQL execution
    @patch("duckdb.connect")
    def test_combined_backfill_dry_run(
        self, mock_duckdb, mock_normalize, mock_get_provider, mock_list_providers
    ):
        """Test that backfill + dry-run prints summary for each date with no writes."""
        mock_list_providers.return_value = ["dummy"]

        # Mock provider instance
        mock_provider = Mock()

        async def mock_fetch_symbols():
            record = Mock()
            record.meta = {}
            record.model_dump = lambda: {
                "symbol": "TEST",
                "name": "Test Co",
                "meta": {"provider": "dummy"},
            }
            return [record]

        mock_provider.fetch_symbols = mock_fetch_symbols
        mock_get_provider.return_value = mock_provider

        # Mock in-memory connection for dry-run
        mock_conn = create_mock_duckdb_connection()
        mock_duckdb.return_value = mock_conn

        mock_normalize.return_value = None

        # Use ONLY --dry-run with --backfill (not --execute)
        result = self.runner.invoke(
            root_app,
            [
                "symbols",
                "update",
                "-p",
                "dummy",
                "--backfill",
                "2025-06-17",
                "--snapshot-as-of",
                "2025-06-18",
                "--dry-run",
                # Removed --execute to get actual dry-run behavior
            ],
        )

        # Should get dry preview, not actual execution
        assert result.exit_code == 0
        assert "Dry preview complete" in result.output
        assert "Re-run with --execute" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.get_provider")
    def test_pipeline_failure_propagates_exit_code(self, mock_get_provider, mock_list_providers):
        """Test that pipeline failures result in non-zero exit codes."""
        mock_list_providers.return_value = ["dummy"]

        # Mock provider to raise an exception
        mock_provider = Mock()

        async def failing_fetch():
            raise RuntimeError("Provider fetch failed")

        mock_provider.fetch_symbols = failing_fetch
        mock_get_provider.return_value = mock_provider

        result = self.runner.invoke(root_app, ["symbols", "update", "-p", "dummy", "--execute"])

        # Verify failure
        assert result.exit_code == 1
        assert "❌ Pipeline failed:" in result.output
        assert "Provider fetch failed" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    def test_invalid_date_format_error(self, mock_list_providers):
        """Test that invalid date formats produce helpful error messages."""
        mock_list_providers.return_value = ["dummy"]

        result = self.runner.invoke(
            root_app,
            ["symbols", "update", "-p", "dummy", "--backfill", "invalid-date", "--execute"],
        )

        assert result.exit_code == 1
        assert "Invalid date format for --backfill: invalid-date. Use YYYY-MM-DD." in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.get_provider")
    @patch(
        "marketpipe.ingestion.pipeline.symbol_pipeline.normalize_stage"
    )  # Mock the SQL execution
    @patch("duckdb.connect")
    def test_performance_backfill_three_days(
        self, mock_duckdb, mock_normalize, mock_get_provider, mock_list_providers
    ):
        """Test that backfill of three dummy days completes in reasonable time."""
        import time

        mock_list_providers.return_value = ["dummy"]

        # Mock provider instance
        mock_provider = Mock()

        async def mock_fetch_symbols():
            # Simulate minimal processing time
            return [
                Mock(
                    meta={},
                    model_dump=lambda: {
                        "symbol": "TEST",
                        "name": "Test",
                        "meta": {"provider": "dummy"},
                    },
                )
            ]

        mock_provider.fetch_symbols = mock_fetch_symbols
        mock_get_provider.return_value = mock_provider

        # Mock connection
        mock_conn = create_mock_duckdb_connection()
        mock_duckdb.return_value = mock_conn

        mock_normalize.return_value = None

        start_time = time.time()

        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(
                root_app,
                [
                    "symbols",
                    "update",
                    "-p",
                    "dummy",
                    "--data-dir",
                    str(temp_dir),
                    "--backfill",
                    "2025-06-17",
                    "--snapshot-as-of",
                    "2025-06-19",
                    "--execute",
                ],
            )

        duration = time.time() - start_time

        # Should complete reasonably fast and show expected dates
        assert result.exit_code == 0
        assert duration < 10.0  # Should complete in under 10 seconds
        assert "2025-06-17" in result.output
        assert "2025-06-18" in result.output
        assert "2025-06-19" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.get_provider")
    @patch(
        "marketpipe.ingestion.pipeline.symbol_pipeline.normalize_stage"
    )  # Mock the SQL execution
    @patch("duckdb.connect")
    def test_dry_run_without_execute_shows_preview(
        self, mock_duckdb, mock_normalize, mock_get_provider, mock_list_providers
    ):
        """Test that pure --dry-run (without --execute) shows preview only."""
        mock_list_providers.return_value = ["dummy"]

        # Mock provider instance
        mock_provider = Mock()

        async def mock_fetch_symbols():
            record = Mock()
            record.meta = {}
            record.model_dump = lambda: {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "meta": {"provider": "dummy"},
            }
            return [record]

        mock_provider.fetch_symbols = mock_fetch_symbols
        mock_get_provider.return_value = mock_provider

        # Mock connection
        mock_conn = create_mock_duckdb_connection()
        mock_duckdb.return_value = mock_conn

        mock_normalize.return_value = None

        result = self.runner.invoke(
            root_app,
            [
                "symbols",
                "update",
                "-p",
                "dummy",
                "--dry-run",
                # No --execute flag
            ],
        )

        # Should show preview and exit without execution
        assert result.exit_code == 0
        assert "Dry preview complete" in result.output
        assert "Re-run with --execute" in result.output

        # Should not call any providers since --execute was not used
        mock_get_provider.assert_not_called()
        mock_normalize.assert_not_called()

    # NEW TESTS FOR ENHANCED VALIDATION

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    def test_backfill_date_after_snapshot_error(self, mock_list_providers):
        """Test that backfill date after snapshot date produces error."""
        mock_list_providers.return_value = ["dummy"]

        result = self.runner.invoke(
            root_app,
            [
                "symbols",
                "update",
                "-p",
                "dummy",
                "--backfill",
                "2025-06-20",
                "--snapshot-as-of",
                "2025-06-19",  # Earlier than backfill
                "--execute",
            ],
        )

        assert result.exit_code == 1
        assert "Backfill date 2025-06-20 cannot be after snapshot date 2025-06-19" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("typer.confirm")
    def test_large_backfill_warning_and_confirmation(self, mock_confirm, mock_list_providers):
        """Test that large backfills show warnings and ask for confirmation."""
        mock_list_providers.return_value = ["dummy"]
        mock_confirm.return_value = False  # User cancels

        result = self.runner.invoke(
            root_app,
            [
                "symbols",
                "update",
                "-p",
                "dummy",
                "--backfill",
                "2020-01-01",  # 5+ years ago
                "--snapshot-as-of",
                "2025-06-19",
                "--execute",
            ],
        )

        assert result.exit_code == 0  # Cancelled by user, not error
        assert "Large backfill detected" in result.output
        assert "years" in result.output
        assert "Backfill cancelled by user" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    def test_large_backfill_warning_shows_for_over_365_days(self, mock_list_providers):
        """Test that backfills over 365 days show warning."""
        mock_list_providers.return_value = ["dummy"]

        # Use exactly 400 days to trigger warning but not confirmation
        result = self.runner.invoke(
            root_app,
            [
                "symbols",
                "update",
                "-p",
                "dummy",
                "--backfill",
                "2024-01-01",
                "--snapshot-as-of",
                "2025-02-04",  # ~400 days
                # Don't use --execute to avoid actual pipeline run
            ],
        )

        assert result.exit_code == 0
        assert "Large backfill detected" in result.output
        assert "1.1 years" in result.output or "400 days" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("duckdb.connect")
    def test_diff_only_missing_table_error(self, mock_duckdb, mock_list_providers):
        """Test that diff-only mode fails when symbols_snapshot table doesn't exist."""
        mock_list_providers.return_value = ["dummy"]

        # Mock DuckDB to raise table not found error
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("Table with name symbols_snapshot does not exist")
        mock_duckdb.return_value = mock_conn

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"

            result = self.runner.invoke(
                root_app,
                [
                    "symbols",
                    "update",
                    "-p",
                    "dummy",
                    "--db",
                    str(db_path),
                    "--diff-only",
                    "--execute",
                ],
            )

            assert result.exit_code == 1
            assert "--diff-only requires existing symbols_snapshot table" in result.output
            assert "Run without --diff-only first" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("duckdb.connect")
    def test_diff_only_empty_table_error(self, mock_duckdb, mock_list_providers):
        """Test that diff-only mode fails when symbols_snapshot table is empty."""
        mock_list_providers.return_value = ["dummy"]

        # Mock DuckDB to return 0 count for symbols_snapshot
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_result = Mock()
        mock_result.fetchone.return_value = [0]  # Empty table
        mock_conn.execute.return_value = mock_result
        mock_duckdb.return_value = mock_conn

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"

            result = self.runner.invoke(
                root_app,
                [
                    "symbols",
                    "update",
                    "-p",
                    "dummy",
                    "--db",
                    str(db_path),
                    "--diff-only",
                    "--execute",
                ],
            )

            assert result.exit_code == 1
            assert "--diff-only requires existing symbols_snapshot table with data" in result.output

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    def test_snapshot_date_format_validation(self, mock_list_providers):
        """Test that invalid snapshot date format produces error."""
        mock_list_providers.return_value = ["dummy"]

        result = self.runner.invoke(
            root_app,
            ["symbols", "update", "-p", "dummy", "--snapshot-as-of", "invalid-date", "--execute"],
        )

        assert result.exit_code == 1
        assert (
            "Invalid date format for --snapshot-as-of: invalid-date. Use YYYY-MM-DD."
            in result.output
        )

    @patch("marketpipe.ingestion.symbol_providers.list_providers")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.get_provider")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.normalize_stage")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.run_scd_update")
    @patch("marketpipe.ingestion.pipeline.symbol_pipeline.refresh")
    @patch("duckdb.connect")
    def test_progress_feedback_verbose_mode(
        self,
        mock_duckdb,
        mock_refresh,
        mock_scd,
        mock_normalize,
        mock_get_provider,
        mock_list_providers,
    ):
        """Test that verbose mode shows detailed progress for each date."""
        mock_list_providers.return_value = ["dummy"]

        # Mock provider instance
        mock_provider = Mock()

        async def mock_fetch_symbols():
            return [
                Mock(
                    meta={},
                    model_dump=lambda: {
                        "symbol": "TEST",
                        "name": "Test",
                        "meta": {"provider": "dummy"},
                    },
                )
            ]

        mock_provider.fetch_symbols = mock_fetch_symbols
        mock_get_provider.return_value = mock_provider

        # Mock connection
        mock_conn = create_mock_duckdb_connection()
        mock_duckdb.return_value = mock_conn

        # Mock pipeline components
        mock_normalize.return_value = None
        mock_scd.return_value = None
        mock_refresh.return_value = None

        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(
                root_app,
                [
                    "symbols",
                    "update",
                    "-p",
                    "dummy",
                    "--data-dir",
                    str(temp_dir),
                    "--backfill",
                    "2025-06-17",
                    "--snapshot-as-of",
                    "2025-06-19",
                    "--verbose",  # Enable verbose mode
                    "--execute",
                ],
            )

            assert result.exit_code == 0

            # Check that all dates are shown in output (verbose mode)
            assert "[1/3] 2025-06-17:" in result.output
            assert "[2/3] 2025-06-18:" in result.output
            assert "[3/3] 2025-06-19:" in result.output
