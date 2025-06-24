# SPDX-License-Identifier: Apache-2.0
"""Test that help commands don't create side effects."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile


def test_help_no_side_effects():
    """Test that `python -m marketpipe --help` doesn't create files or directories."""
    with tempfile.TemporaryDirectory() as td:
        # Change to temporary directory
        original_cwd = os.getcwd()
        try:
            os.chdir(td)

            # Run help command
            result = subprocess.run(
                [sys.executable, "-m", "marketpipe", "--help"], capture_output=True, text=True
            )

            # Should exit successfully
            assert result.returncode == 0, f"Help command failed: {result.stderr}"

            # Should contain expected help text
            assert "MarketPipe ETL commands" in result.stdout

            # Should not create any data directories or files
            assert not pathlib.Path("data").exists(), "Help command created data directory"
            assert not pathlib.Path("data/db").exists(), "Help command created database directory"
            assert not pathlib.Path(
                "data/db/core.db"
            ).exists(), "Help command created database file"

            # Should not create any other common directories
            temp_path = pathlib.Path(td)
            created_files = list(temp_path.rglob("*"))
            assert (
                len(created_files) == 0
            ), f"Help command created unexpected files: {created_files}"

        finally:
            os.chdir(original_cwd)


def test_ohlcv_help_no_side_effects():
    """Test that OHLCV subcommand help doesn't create side effects."""
    with tempfile.TemporaryDirectory() as td:
        original_cwd = os.getcwd()
        try:
            os.chdir(td)

            # Run OHLCV help command
            result = subprocess.run(
                [sys.executable, "-m", "marketpipe", "ohlcv", "--help"],
                capture_output=True,
                text=True,
            )

            # Should exit successfully
            assert result.returncode == 0, f"OHLCV help command failed: {result.stderr}"

            # Should contain expected help text
            assert "OHLCV pipeline commands" in result.stdout

            # Should not create any files
            assert not pathlib.Path("data").exists()
            assert not pathlib.Path("data/db").exists()

            temp_path = pathlib.Path(td)
            created_files = list(temp_path.rglob("*"))
            assert len(created_files) == 0, f"OHLCV help command created files: {created_files}"

        finally:
            os.chdir(original_cwd)


def test_individual_command_help_no_side_effects():
    """Test that individual command help doesn't create side effects."""
    commands_to_test = [
        ["ingest-ohlcv", "--help"],
        ["validate-ohlcv", "--help"],
        ["aggregate-ohlcv", "--help"],
        ["ohlcv", "ingest", "--help"],
        ["ohlcv", "validate", "--help"],
        ["ohlcv", "aggregate", "--help"],
        ["query", "--help"],
        ["metrics", "--help"],
        ["providers", "--help"],
    ]

    for command_args in commands_to_test:
        with tempfile.TemporaryDirectory() as td:
            original_cwd = os.getcwd()
            try:
                os.chdir(td)

                # Run command help
                full_command = [sys.executable, "-m", "marketpipe"] + command_args
                result = subprocess.run(full_command, capture_output=True, text=True)

                # Should exit successfully
                assert (
                    result.returncode == 0
                ), f"Help command {' '.join(command_args)} failed: {result.stderr}"

                # Should not create any files
                temp_path = pathlib.Path(td)
                created_files = list(temp_path.rglob("*"))
                assert (
                    len(created_files) == 0
                ), f"Help command {' '.join(command_args)} created files: {created_files}"

            finally:
                os.chdir(original_cwd)


def test_import_cli_no_side_effects():
    """Test that importing CLI module doesn't create side effects."""
    with tempfile.TemporaryDirectory() as td:
        original_cwd = os.getcwd()
        try:
            os.chdir(td)

            # Import the CLI module

            # Should not create any files just from import
            assert not pathlib.Path("data").exists()
            assert not pathlib.Path("data/db").exists()

            temp_path = pathlib.Path(td)
            created_files = list(temp_path.rglob("*"))
            assert len(created_files) == 0, f"CLI import created files: {created_files}"

        finally:
            os.chdir(original_cwd)
