"""Tests for CLI command renaming and deprecation warnings."""

import pytest
from typer.testing import CliRunner
from marketpipe.cli import app


@pytest.fixture
def runner():
    """Fixture providing CliRunner."""
    return CliRunner()


def test_deprecated_commands_show_deprecated_in_help(runner):
    """Test that deprecated commands show [DEPRECATED] in their help text."""
    # Test ingest help shows deprecation
    result = runner.invoke(app, ['ingest', '--help'])
    assert result.exit_code == 0
    assert "[DEPRECATED]" in result.stdout
    assert "ingest-ohlcv" in result.stdout
    
    # Test validate help shows deprecation
    result = runner.invoke(app, ['validate', '--help'])
    assert result.exit_code == 0
    assert "[DEPRECATED]" in result.stdout
    assert "validate-ohlcv" in result.stdout
    
    # Test aggregate help shows deprecation
    result = runner.invoke(app, ['aggregate', '--help'])
    assert result.exit_code == 0
    assert "[DEPRECATED]" in result.stdout
    assert "aggregate-ohlcv" in result.stdout


def test_new_ingest_ohlcv_command_exists(runner):
    """Test that new 'ingest-ohlcv' convenience command exists."""
    result = runner.invoke(app, ['ingest-ohlcv', '--help'])
    
    # Should show help without error
    assert result.exit_code == 0
    assert "Start a new OHLCV ingestion" in result.stdout


def test_new_validate_ohlcv_command_exists(runner):
    """Test that new 'validate-ohlcv' convenience command exists."""
    result = runner.invoke(app, ['validate-ohlcv', '--help'])
    
    # Should show help without error
    assert result.exit_code == 0
    assert "Validate ingested OHLCV data" in result.stdout


def test_new_aggregate_ohlcv_command_exists(runner):
    """Test that new 'aggregate-ohlcv' convenience command exists."""
    result = runner.invoke(app, ['aggregate-ohlcv', '--help'])
    
    # Should show help without error
    assert result.exit_code == 0
    assert "Run OHLCV aggregation manually" in result.stdout


def test_ohlcv_subapp_commands_exist(runner):
    """Test that OHLCV sub-app commands exist."""
    # Test the sub-app exists
    result = runner.invoke(app, ['ohlcv', '--help'])
    assert result.exit_code == 0
    assert "OHLCV pipeline commands" in result.stdout
    
    # Test individual commands in sub-app
    result = runner.invoke(app, ['ohlcv', 'ingest', '--help'])
    assert result.exit_code == 0
    assert "Start a new OHLCV ingestion" in result.stdout
    
    result = runner.invoke(app, ['ohlcv', 'validate', '--help'])
    assert result.exit_code == 0
    assert "Validate ingested OHLCV data" in result.stdout
    
    result = runner.invoke(app, ['ohlcv', 'aggregate', '--help'])
    assert result.exit_code == 0
    assert "Run OHLCV aggregation manually" in result.stdout


def test_main_help_shows_all_commands(runner):
    """Test that main help shows both new and deprecated commands."""
    result = runner.invoke(app, ['--help'])
    
    assert result.exit_code == 0
    # Should show the new OHLCV sub-app
    assert "ohlcv" in result.stdout
    
    # Should show convenience commands
    assert "ingest-ohlcv" in result.stdout
    assert "validate-ohlcv" in result.stdout  
    assert "aggregate-ohlcv" in result.stdout
    
    # Should still show deprecated commands
    assert "ingest" in result.stdout
    assert "validate" in result.stdout
    assert "aggregate" in result.stdout


def test_all_command_signatures_match(runner):
    """Test that new commands have the same signature as old commands."""
    # Test ingest commands have same options
    old_help = runner.invoke(app, ['ingest', '--help'])
    new_help = runner.invoke(app, ['ingest-ohlcv', '--help'])
    sub_help = runner.invoke(app, ['ohlcv', 'ingest', '--help'])
    
    # All should have common options
    for option in ['--config', '--symbols', '--start', '--end', '--batch-size']:
        assert option in old_help.stdout
        assert option in new_help.stdout
        assert option in sub_help.stdout
    
    # Test validate commands have same options
    old_help = runner.invoke(app, ['validate', '--help'])
    new_help = runner.invoke(app, ['validate-ohlcv', '--help'])
    sub_help = runner.invoke(app, ['ohlcv', 'validate', '--help'])
    
    for option in ['--job-id', '--list', '--show']:
        assert option in old_help.stdout
        assert option in new_help.stdout
        assert option in sub_help.stdout 