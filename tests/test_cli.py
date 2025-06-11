# SPDX-License-Identifier: Apache-2.0
"""Legacy CLI tests - maintaining backward compatibility."""

import subprocess


def test_legacy_cli_help_displays_marketpipe_commands():
    """Test that legacy CLI help displays MarketPipe commands correctly."""
    result = subprocess.run([
        'python', '-m', 'marketpipe.cli', '--help'
    ], capture_output=True, text=True)
    assert result.returncode == 0
    assert 'MarketPipe ETL commands' in result.stdout
