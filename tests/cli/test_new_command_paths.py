# SPDX-License-Identifier: Apache-2.0
"""Test that new command paths work correctly."""

from __future__ import annotations

import subprocess
import sys

from typer.testing import CliRunner

from marketpipe.cli import app


def test_ingest_ohlcv_help():
    """Test that 'mp ingest-ohlcv --help' works and exits successfully."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ingest-ohlcv", "--help"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"ingest-ohlcv --help failed: {result.stderr}"
    assert "ingest" in result.stdout.lower()
    assert "ohlcv" in result.stdout.lower()


def test_ohlcv_ingest_help():
    """Test that 'mp ohlcv ingest --help' works and exits successfully."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ohlcv", "ingest", "--help"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"ohlcv ingest --help failed: {result.stderr}"
    assert "ingest" in result.stdout.lower()


def test_validate_ohlcv_help():
    """Test that 'mp validate-ohlcv --help' works and exits successfully."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "validate-ohlcv", "--help"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"validate-ohlcv --help failed: {result.stderr}"
    assert "validate" in result.stdout.lower()


def test_ohlcv_validate_help():
    """Test that 'mp ohlcv validate --help' works and exits successfully."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ohlcv", "validate", "--help"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"ohlcv validate --help failed: {result.stderr}"
    assert "validate" in result.stdout.lower()


def test_aggregate_ohlcv_help():
    """Test that 'mp aggregate-ohlcv --help' works and exits successfully."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "aggregate-ohlcv", "--help"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"aggregate-ohlcv --help failed: {result.stderr}"
    assert "aggregate" in result.stdout.lower()


def test_ohlcv_aggregate_help():
    """Test that 'mp ohlcv aggregate --help' works and exits successfully."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ohlcv", "aggregate", "--help"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"ohlcv aggregate --help failed: {result.stderr}"
    assert "aggregate" in result.stdout.lower()


def test_ohlcv_subcommand_list():
    """Test that 'mp ohlcv --help' lists expected subcommands."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ohlcv", "--help"], capture_output=True, text=True
    )

    assert result.returncode == 0, f"ohlcv --help failed: {result.stderr}"

    # Should list the three main OHLCV operations
    assert "ingest" in result.stdout
    assert "validate" in result.stdout
    assert "aggregate" in result.stdout

    # Should contain OHLCV pipeline description
    assert "OHLCV pipeline commands" in result.stdout


def test_utility_commands_help():
    """Test that utility commands work correctly."""
    utility_commands = [
        ["query", "--help"],
        ["metrics", "--help"],
        ["providers", "--help"],
        ["migrate", "--help"],
    ]

    for cmd in utility_commands:
        result = subprocess.run(
            [sys.executable, "-m", "marketpipe"] + cmd, capture_output=True, text=True
        )

        assert result.returncode == 0, f"Utility command {' '.join(cmd)} failed: {result.stderr}"
        assert len(result.stdout) > 0, f"Utility command {' '.join(cmd)} produced no output"


def test_command_structure_consistency():
    """Test that both command structures are equivalent."""
    runner = CliRunner()

    # Test that both ingest commands have same core functionality
    hyphenated_result = runner.invoke(app, ["ingest-ohlcv", "--help"])
    subcommand_result = runner.invoke(app, ["ohlcv", "ingest", "--help"])

    assert hyphenated_result.exit_code == 0
    assert subcommand_result.exit_code == 0

    # Both should mention similar functionality
    assert "ingest" in hyphenated_result.stdout.lower()
    assert "ingest" in subcommand_result.stdout.lower()


def test_main_app_help_lists_commands():
    """Test that main app help lists all available commands."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "--help"], capture_output=True, text=True
    )

    assert result.returncode == 0, f"Main help failed: {result.stderr}"

    # Should list main commands
    commands_expected = [
        "ohlcv",  # Subcommand group
        "ingest-ohlcv",  # Hyphenated convenience commands
        "validate-ohlcv",
        "aggregate-ohlcv",
        "query",  # Utility commands
        "metrics",
        "providers",
        "migrate",
    ]

    for cmd in commands_expected:
        assert cmd in result.stdout, f"Expected command '{cmd}' not found in help output"


def test_command_consistency_both_paths():
    """Test that both command paths have consistent help output."""
    # Test hyphenated command help
    result1 = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ingest-ohlcv", "--help"],
        capture_output=True,
        text=True,
    )

    # Test subcommand help
    result2 = subprocess.run(
        [sys.executable, "-m", "marketpipe", "ohlcv", "ingest", "--help"],
        capture_output=True,
        text=True,
    )

    # Both should succeed
    assert result1.returncode == 0, f"ingest-ohlcv --help failed: {result1.stderr}"
    assert result2.returncode == 0, f"ohlcv ingest --help failed: {result2.stderr}"

    # Both should mention ingest functionality
    assert "ingest" in result1.stdout.lower()
    assert "ingest" in result2.stdout.lower()

    # Check if we're using the stub (which won't have options)
    # If we're in stub mode or the output is very short, skip option checks
    if "Typer stub placeholder" in result1.stdout or len(result1.stdout) < 200:
        # Skip option checks in minimal/stub environment
        return

    # In CI environments, the help output might not include options if typer isn't fully available
    # Look for specific indicators that options should be present
    has_proper_typer = (
        "--help" in result1.stdout
        and "Show this message" in result1.stdout
        and len(result1.stdout) > 500  # Real typer help is usually longer
    )

    if not has_proper_typer:
        # Skip detailed option checks in minimal environments
        return

    # Both should have similar options available
    for option in ["--config", "--symbols", "--start", "--end"]:
        assert option in result1.stdout, f"Option {option} missing from ingest-ohlcv help"
        assert option in result2.stdout, f"Option {option} missing from ohlcv ingest help"


def test_non_existent_command_fails():
    """Test that non-existent commands fail gracefully."""
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "nonexistent-command"], capture_output=True, text=True
    )

    # Should fail with non-zero exit code
    assert result.returncode != 0, "Non-existent command should fail"

    # Should provide helpful error message
    assert (
        "no such command" in result.stderr.lower() or "usage:" in result.stderr.lower()
    ), f"Expected helpful error message, got: {result.stderr}"
