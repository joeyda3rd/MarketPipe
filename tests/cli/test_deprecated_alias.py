# SPDX-License-Identifier: Apache-2.0
"""Test that deprecated command aliases show appropriate warnings."""

from __future__ import annotations

import subprocess
import sys
import tempfile

from typer.testing import CliRunner

from marketpipe.cli import app


def test_deprecated_ingest_command_warning():
    """Test that 'mp ingest' shows deprecation warning."""
    runner = CliRunner()

    # Just test help to avoid execution side effects
    result = runner.invoke(app, ["ingest", "--help"])

    # Should contain deprecation warning in help text
    assert (
        "[DEPRECATED]" in result.stdout or "deprecated" in result.stdout.lower()
    ), f"Expected deprecation warning not found. Stdout: {result.stdout}"

    # Should suggest the new command
    assert (
        "ingest-ohlcv" in result.stdout or "ohlcv ingest" in result.stdout
    ), f"Expected suggestion for new command not found in: {result.stdout}"


def test_deprecated_validate_command_warning():
    """Test that 'mp validate' shows deprecation warning."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory():
        result = runner.invoke(app, ["validate", "--help"])

        # Check that help text indicates deprecation
        assert (
            "[DEPRECATED]" in result.stdout or "deprecated" in result.stdout.lower()
        ), f"Expected deprecation notice in help text: {result.stdout}"


def test_deprecated_aggregate_command_warning():
    """Test that 'mp aggregate' shows deprecation warning."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory():
        result = runner.invoke(app, ["aggregate", "--help"])

        # Check that help text indicates deprecation
        assert (
            "[DEPRECATED]" in result.stdout or "deprecated" in result.stdout.lower()
        ), f"Expected deprecation notice in help text: {result.stdout}"


def test_new_commands_work():
    """Test that new command structure works without deprecation warnings."""
    runner = CliRunner()

    # Test new hyphenated commands
    new_commands = [
        ["ingest-ohlcv", "--help"],
        ["validate-ohlcv", "--help"],
        ["aggregate-ohlcv", "--help"],
    ]

    for cmd in new_commands:
        result = runner.invoke(app, cmd)
        assert result.exit_code == 0, f"Command {' '.join(cmd)} failed: {result.stdout}"

        # Should NOT contain deprecation warnings
        assert (
            "deprecated" not in result.stdout.lower()
        ), f"New command {' '.join(cmd)} should not show deprecation warning: {result.stdout}"


def test_ohlcv_subcommands_work():
    """Test that OHLCV subcommands work without deprecation warnings."""
    runner = CliRunner()

    # Test OHLCV subcommands
    ohlcv_commands = [
        ["ohlcv", "ingest", "--help"],
        ["ohlcv", "validate", "--help"],
        ["ohlcv", "aggregate", "--help"],
    ]

    for cmd in ohlcv_commands:
        result = runner.invoke(app, cmd)
        assert result.exit_code == 0, f"Command {' '.join(cmd)} failed: {result.stdout}"

        # Should NOT contain deprecation warnings
        assert (
            "deprecated" not in result.stdout.lower()
        ), f"OHLCV subcommand {' '.join(cmd)} should not show deprecation warning: {result.stdout}"


def test_subprocess_deprecated_warning():
    """Test deprecation warning via subprocess help command."""
    # Run deprecated command help via subprocess
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ingest", "--help"], capture_output=True, text=True
    )

    # Should succeed
    assert result.returncode == 0, f"Help command failed: {result.stderr}"

    # Check stdout for deprecation warning
    assert (
        "[DEPRECATED]" in result.stdout or "deprecated" in result.stdout.lower()
    ), f"Expected deprecation warning not found. Stdout: {result.stdout}"
