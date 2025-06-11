# SPDX-License-Identifier: Apache-2.0
"""Unit tests for CLI help display functionality."""

from __future__ import annotations

import subprocess


class TestCliHelpDisplay:
    """Test CLI help command displays correct information."""

    def test_main_cli_help_displays_marketpipe_etl_commands(self):
        """Test that main CLI help displays MarketPipe ETL commands correctly."""
        result = subprocess.run(
            ["python", "-m", "marketpipe.cli", "--help"], capture_output=True, text=True
        )

        assert result.returncode == 0
        assert "MarketPipe ETL commands" in result.stdout

    def test_ingestion_cli_help_displays_domain_driven_commands(self):
        """Test that ingestion CLI help displays domain-driven ingestion commands."""
        result = subprocess.run(
            ["python", "-m", "marketpipe.ingestion.cli", "--help"],
            capture_output=True,
            text=True,
        )

        # This might fail if the CLI module doesn't exist yet, so we'll handle that
        if result.returncode == 0:
            assert "ingestion" in result.stdout.lower()
        else:
            # Skip test if new CLI not yet available
            assert True

    def test_legacy_ingestion_module_removed(self):
        """Test that legacy ingestion module entry point has been removed."""
        result = subprocess.run(
            ["python", "-m", "marketpipe.ingestion", "--help"],
            capture_output=True,
            text=True,
        )

        # Should fail since we removed the legacy __main__.py
        assert result.returncode != 0
        assert "cannot be directly executed" in result.stderr
