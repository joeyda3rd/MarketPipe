"""Test for the main module entry point."""

import subprocess
import sys
from unittest.mock import patch


def test_main_module_entry_point():
    """Test that the main module can be executed via python -m."""
    # Test that the module can be executed as a script
    result = subprocess.run(
        [sys.executable, "-m", "marketpipe", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )

    # Should execute successfully (exit code 0) and show help
    assert result.returncode == 0
    assert "Usage:" in result.stdout or "usage:" in result.stdout.lower()


def test_main_module_cli_execution():
    """Test that the CLI app is available for execution."""
    # Test that the app can be imported and is callable
    with patch("marketpipe.cli.app") as mock_app:
        mock_app.return_value = None

        # Import and execute directly
        from marketpipe.__main__ import app

        app()

        # Verify the CLI was invoked
        mock_app.assert_called_once()
