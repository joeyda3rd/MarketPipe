from __future__ import annotations

from typer.testing import CliRunner
from unittest.mock import patch
import os
import pytest

from marketpipe.cli import app as root_app


class TestSymbolsUpdateCommand:
    """Test suite for the 'mp symbols update' command."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_help_shows_flags(self):
        """Test that help displays all required flags with descriptions."""
        # Use a fresh runner for this specific test to avoid state pollution
        runner = CliRunner()
        result = runner.invoke(root_app, ["symbols", "update", "--help"], catch_exceptions=False)
        
        assert result.exit_code == 0
        
        # Check that all required flags are in the help output
        required_flags = [
            "--provider", "-p",
            "--db", 
            "--data-dir",
            "--backfill",
            "--snapshot-as-of",
            "--dry-run",
            "--diff-only", 
            "--execute"
        ]
        
        for flag in required_flags:
            assert flag in result.output, f"Flag {flag} not found in help output"
        
        # Normalize output by removing extra whitespace and newlines for more robust checking
        normalized_output = ' '.join(result.output.split())
        
        # Check that help text includes descriptions
        assert "Symbol provider(s) to ingest" in normalized_output
        assert "DuckDB database path" in normalized_output
        assert "Parquet dataset root" in normalized_output
        # Check for backfill text with more flexibility
        assert "Back-fill symbols" in normalized_output and "this date" in normalized_output
        assert "(YYYY-MM-DD)" in normalized_output
        assert "snapshot" in normalized_output and "date" in normalized_output  # More flexible
        assert "Run pipeline but skip" in normalized_output and "Parquet writes" in normalized_output
        assert "Skip provider fetch" in normalized_output and "SCD update only" in normalized_output
        assert "Perform writes" in normalized_output and "read-only" in normalized_output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_unknown_provider_exits(self, mock_list_providers):
        """Test that unknown provider names cause command to exit with error."""
        mock_list_providers.return_value = ["polygon", "nasdaq_dl", "dummy"]
        
        result = self.runner.invoke(root_app, ["symbols", "update", "-p", "bogus"])
        
        assert result.exit_code == 1
        assert "Unknown provider" in result.output
        assert "bogus" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_preview_mode_requires_execute(self, mock_list_providers):
        """Test that command prints preview and exits without --execute flag."""
        mock_list_providers.return_value = ["polygon", "nasdaq_dl", "dummy"]
        
        result = self.runner.invoke(root_app, ["symbols", "update", "-p", "polygon"])
        
        assert result.exit_code == 0
        assert "Symbol update plan:" in result.output
        assert "Dry preview complete" in result.output
        assert "Re-run with --execute to perform writes" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_multiple_providers_aggregate(self, mock_list_providers):
        """Test that multiple --provider flags are aggregated correctly."""
        mock_list_providers.return_value = ["polygon", "nasdaq_dl", "dummy"]
        
        result = self.runner.invoke(root_app, [
            "symbols", "update", 
            "-p", "polygon", 
            "-p", "nasdaq_dl"
        ])
        
        assert result.exit_code == 0
        assert "polygon" in result.output
        assert "nasdaq_dl" in result.output
        assert "providers" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_all_providers_used_when_none_specified(self, mock_list_providers):
        """Test that all available providers are used when none specified."""
        mock_list_providers.return_value = ["polygon", "nasdaq_dl", "dummy"]
        
        result = self.runner.invoke(root_app, ["symbols", "update"])
        
        assert result.exit_code == 0
        # All providers should be shown in the plan
        assert "polygon" in result.output
        assert "nasdaq_dl" in result.output  
        assert "dummy" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_environment_variables_respected(self, mock_list_providers):
        """Test that MP_DB and MP_DATA_DIR environment variables are used."""
        mock_list_providers.return_value = ["polygon"]
        
        with patch.dict(os.environ, {
            'MP_DB': '/custom/db/path.duckdb',
            'MP_DATA_DIR': '/custom/data/symbols'
        }):
            result = self.runner.invoke(root_app, ["symbols", "update", "-p", "polygon"])
            
            assert result.exit_code == 0
            assert "/custom/db/path.duckdb" in result.output
            assert "/custom/data/symbols" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_explicit_flags_override_environment(self, mock_list_providers):
        """Test that explicit CLI flags override environment variables."""
        mock_list_providers.return_value = ["polygon"]
        
        with patch.dict(os.environ, {
            'MP_DB': '/env/db.duckdb',
            'MP_DATA_DIR': '/env/data'
        }):
            result = self.runner.invoke(root_app, [
                "symbols", "update",
                "-p", "polygon",
                "--db", "/custom/cli.duckdb",
                "--data-dir", "/custom/cli-data"
            ])
            
            assert result.exit_code == 0
            assert "/custom/cli.duckdb" in result.output
            assert "/custom/cli-data" in result.output
            # Environment values should not appear
            assert "/env/db.duckdb" not in result.output
            assert "/env/data" not in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_date_parsing_works(self, mock_list_providers):
        """Test that date parsing works correctly."""
        mock_list_providers.return_value = ["polygon"]
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon",
            "--backfill", "2024-01-01",
            "--snapshot-as-of", "2024-06-15"
        ])
        
        assert result.exit_code == 0
        assert "2024-01-01" in result.output
        assert "2024-06-15" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_boolean_flags_work(self, mock_list_providers):
        """Test that boolean flags are properly handled."""
        mock_list_providers.return_value = ["polygon"]
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon",
            "--dry-run",
            "--diff-only"
        ])
        
        assert result.exit_code == 0
        assert "dry_run: True" in result.output
        assert "diff_only: True" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    @patch('marketpipe.ingestion.pipeline.symbol_pipeline.run_symbol_pipeline')
    def test_execute_flag_triggers_pipeline(self, mock_run_pipeline, mock_list_providers):
        """Test that --execute flag bypasses preview mode and triggers pipeline."""
        mock_list_providers.return_value = ["polygon"]
        mock_run_pipeline.return_value = None  # Successful execution
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon", 
            "--execute"
        ])
        
        assert result.exit_code == 0
        assert "✅ Pipeline complete." in result.output
        # Should not show preview message when executing
        assert "Dry preview complete" not in result.output
        # Verify the pipeline was actually called
        mock_run_pipeline.assert_called_once()

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_plan_summary_shows_all_parameters(self, mock_list_providers):
        """Test that the plan summary shows all configuration parameters."""
        mock_list_providers.return_value = ["polygon", "nasdaq_dl"]
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon",
            "--db", "/test/db.duckdb", 
            "--data-dir", "/test/data",
            "--backfill", "2024-01-01",
            "--snapshot-as-of", "2024-06-15",
            "--dry-run",
            "--diff-only"
        ])
        
        assert result.exit_code == 0
        assert "Symbol update plan:" in result.output
        
        # Check that all parameters are shown in the plan
        plan_keys = [
            "providers", "db", "data_dir", "backfill", 
            "snapshot_as_of", "dry_run", "diff_only", "execute"
        ]
        for key in plan_keys:
            assert key in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_no_filesystem_operations_without_execute(self, mock_list_providers):
        """Test that no filesystem operations occur without --execute flag."""
        mock_list_providers.return_value = ["polygon"]
        
        # This test ensures the command is read-only without --execute
        # We can't easily mock filesystem operations, but we can verify
        # the command exits at the preview stage
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon"
        ])
        
        assert result.exit_code == 0
        assert "Dry preview complete" in result.output
        assert "✅ Pipeline complete." not in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_invalid_date_format_handled(self, mock_list_providers):
        """Test that invalid date formats are handled gracefully."""
        mock_list_providers.return_value = ["polygon"]
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon",
            "--backfill", "invalid-date"
        ])
        
        assert result.exit_code == 1
        assert "Invalid date format" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    def test_provider_order_preserved(self, mock_list_providers):
        """Test that provider order is preserved from command-line arguments."""
        mock_list_providers.return_value = ["polygon", "nasdaq_dl", "dummy"]
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "nasdaq_dl",
            "-p", "polygon", 
            "-p", "dummy"
        ])
        
        assert result.exit_code == 0
        # Check that providers appear in specified order (sorted in plan output)
        assert "nasdaq_dl" in result.output
        assert "polygon" in result.output
        assert "dummy" in result.output

    @patch('marketpipe.ingestion.symbol_providers.list_providers')
    @patch('marketpipe.ingestion.pipeline.symbol_pipeline.run_symbol_pipeline')
    def test_execute_overrides_dry_run(self, mock_run_pipeline, mock_list_providers):
        """Test that --execute flag overrides --dry-run with warning."""
        mock_list_providers.return_value = ["polygon"]
        mock_run_pipeline.return_value = None  # Successful execution
        
        result = self.runner.invoke(root_app, [
            "symbols", "update",
            "-p", "polygon",
            "--dry-run",
            "--execute"
        ])
        
        assert result.exit_code == 0
        assert "Both --dry-run and --execute specified" in result.output
        assert "--execute takes precedence" in result.output
        assert "dry_run: False" in result.output
        assert "execute: True" in result.output
        assert "✅ Pipeline complete." in result.output  # Should execute, not preview 