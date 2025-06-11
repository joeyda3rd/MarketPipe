# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the query CLI command."""

from __future__ import annotations

import pandas as pd
import pytest
from typer.testing import CliRunner
from unittest.mock import patch

from marketpipe.cli import app


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_query_data():
    """Mock data returned by DuckDB query."""
    return pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "ts_ns": [1704067800000000000, 1704067800000000000 + 300000000000],
            "open": [100.0, 200.0],
            "high": [102.0, 202.0],
            "low": [99.0, 199.0],
            "close": [101.5, 201.5],
            "volume": [1000, 2000],
        }
    )


def test_query_command_basic_success(runner, mock_query_data):
    """Test basic successful query execution."""
    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = mock_query_data

        result = runner.invoke(app, ["query", "SELECT * FROM bars_5m LIMIT 2"])

        assert result.exit_code == 0
        assert "AAPL" in result.stdout
        assert "MSFT" in result.stdout
        mock_query.assert_called_once_with("SELECT * FROM bars_5m LIMIT 2")


def test_query_command_csv_output(runner, mock_query_data):
    """Test CSV output format."""
    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = mock_query_data

        result = runner.invoke(app, ["query", "SELECT * FROM bars_5m", "--csv"])

        assert result.exit_code == 0
        # Check CSV format
        lines = result.stdout.strip().split("\n")
        assert "symbol,ts_ns,open,high,low,close,volume" in lines[0]
        assert "AAPL" in lines[1]
        assert "MSFT" in lines[2]
        mock_query.assert_called_once()


def test_query_command_with_limit(runner):
    """Test query with custom limit."""
    large_df = pd.DataFrame(
        {"symbol": [f"SYM{i}" for i in range(100)], "value": list(range(100))}
    )

    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = large_df

        result = runner.invoke(app, ["query", "SELECT * FROM bars_5m", "--limit", "10"])

        assert result.exit_code == 0
        assert "Showing first 10 of 100 rows" in result.stdout
        assert "... 90 more rows" in result.stdout
        mock_query.assert_called_once()


def test_query_command_empty_result(runner):
    """Test query that returns no results."""
    empty_df = pd.DataFrame()

    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = empty_df

        result = runner.invoke(
            app, ["query", "SELECT * FROM bars_5m WHERE symbol='NONEXISTENT'"]
        )

        assert result.exit_code == 0
        assert "Query returned no results" in result.stdout
        mock_query.assert_called_once()


def test_query_command_sql_error(runner):
    """Test handling of SQL errors."""
    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.side_effect = RuntimeError("SQL execution failed: table not found")

        result = runner.invoke(app, ["query", "SELECT * FROM nonexistent_table"])

        assert result.exit_code == 1
        assert "Query failed: SQL execution failed: table not found" in result.stdout
        mock_query.assert_called_once()


def test_query_command_import_error(runner):
    """Test handling of import errors."""
    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.side_effect = ImportError("DuckDB not available")

        result = runner.invoke(app, ["query", "SELECT COUNT(*) FROM bars_5m"])

        assert result.exit_code == 1
        assert "Query failed" in result.stdout
        mock_query.assert_called_once()


def test_query_command_markdown_format(runner, mock_query_data):
    """Test markdown table format output."""
    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = mock_query_data

        result = runner.invoke(app, ["query", "SELECT symbol, close FROM bars_5m"])

        assert result.exit_code == 0
        # Should try to format as markdown (or fallback to string)
        assert "AAPL" in result.stdout
        assert "101.5" in result.stdout
        mock_query.assert_called_once()


def test_query_command_markdown_fallback(runner, mock_query_data):
    """Test fallback when markdown formatting fails."""
    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = mock_query_data

        # Mock the to_markdown method to raise ImportError
        with patch.object(
            pd.DataFrame,
            "to_markdown",
            side_effect=ImportError("tabulate not available"),
        ):
            result = runner.invoke(app, ["query", "SELECT symbol FROM bars_5m"])

            assert result.exit_code == 0
            # Should fall back to string representation
            assert "AAPL" in result.stdout
            mock_query.assert_called_once()


def test_query_command_complex_sql(runner, mock_query_data):
    """Test complex SQL queries."""
    aggregated_df = pd.DataFrame(
        {"symbol": ["AAPL"], "count": [10], "avg_close": [150.5]}
    )

    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = aggregated_df

        complex_sql = """
        SELECT symbol, COUNT(*) as count, AVG(close) as avg_close 
        FROM bars_5m 
        WHERE symbol='AAPL' 
        GROUP BY symbol
        """

        result = runner.invoke(app, ["query", complex_sql])

        assert result.exit_code == 0
        assert "AAPL" in result.stdout
        assert "150.5" in result.stdout
        mock_query.assert_called_once_with(complex_sql)


def test_query_command_help(runner):
    """Test query command help display."""
    result = runner.invoke(app, ["query", "--help"])

    assert result.exit_code == 0
    assert "Run an ad-hoc query on aggregated data" in result.stdout
    assert "Available views: bars_5m, bars_15m, bars_1h, bars_1d" in result.stdout
    assert "Examples:" in result.stdout
    assert "SELECT * FROM bars_5m WHERE symbol='AAPL'" in result.stdout


def test_query_command_various_sql_patterns(runner):
    """Test various SQL query patterns."""
    test_cases = [
        ("SELECT COUNT(*) FROM bars_5m", pd.DataFrame({"count": [42]})),
        (
            "SELECT DISTINCT symbol FROM bars_1d",
            pd.DataFrame({"symbol": ["AAPL", "MSFT"]}),
        ),
        (
            "SELECT MAX(high), MIN(low) FROM bars_1h",
            pd.DataFrame({"max": [200.0], "min": [50.0]}),
        ),
    ]

    for sql, expected_df in test_cases:
        with patch(
            "marketpipe.aggregation.infrastructure.duckdb_views.query"
        ) as mock_query:
            mock_query.return_value = expected_df

            result = runner.invoke(app, ["query", sql])

            assert result.exit_code == 0
            mock_query.assert_called_once_with(sql)


def test_query_command_with_special_characters(runner):
    """Test queries with special characters and escaping."""
    result_df = pd.DataFrame({"result": ["success"]})

    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = result_df

        # SQL with quotes and special characters
        sql = "SELECT symbol FROM bars_5m WHERE symbol LIKE '%APP%'"

        result = runner.invoke(app, ["query", sql])

        assert result.exit_code == 0
        mock_query.assert_called_once_with(sql)


def test_query_command_limit_behavior(runner):
    """Test different limit behaviors."""
    # Test with data under limit
    small_df = pd.DataFrame({"symbol": ["AAPL", "MSFT"]})

    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = small_df

        result = runner.invoke(app, ["query", "SELECT * FROM bars_5m", "--limit", "50"])

        assert result.exit_code == 0
        assert "Showing first" not in result.stdout  # Should not show limit message
        assert "more rows" not in result.stdout
        mock_query.assert_called_once()


def test_query_command_csv_with_special_values(runner):
    """Test CSV output with special values like NaN."""
    special_df = pd.DataFrame(
        {"symbol": ["AAPL", "MSFT"], "value": [100.5, None], "flag": [True, False]}
    )

    with patch(
        "marketpipe.aggregation.infrastructure.duckdb_views.query"
    ) as mock_query:
        mock_query.return_value = special_df

        result = runner.invoke(app, ["query", "SELECT * FROM bars_5m", "--csv"])

        assert result.exit_code == 0
        assert "symbol,value,flag" in result.stdout
        assert "True" in result.stdout
        assert "False" in result.stdout
        mock_query.assert_called_once()
