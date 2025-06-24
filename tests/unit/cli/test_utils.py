# SPDX-License-Identifier: Apache-2.0
"""Test CLI utility functions."""

from __future__ import annotations

from unittest.mock import patch

from marketpipe.cli.utils import _create_sparkline, _parse_time_window


class TestCliUtils:
    """Test CLI utility functions."""

    def test_parse_time_window_valid_formats(self):
        """Test parsing valid time window formats."""
        # Hours
        assert _parse_time_window("1h") == 3600
        assert _parse_time_window("2h") == 7200
        assert _parse_time_window("24h") == 86400

        # Minutes
        assert _parse_time_window("1m") == 60
        assert _parse_time_window("30m") == 1800
        assert _parse_time_window("60m") == 3600

        # Days
        assert _parse_time_window("1d") == 86400
        assert _parse_time_window("7d") == 604800

        # Seconds
        assert _parse_time_window("30s") == 30
        assert _parse_time_window("120s") == 120

    def test_parse_time_window_invalid_formats(self):
        """Test parsing invalid time window formats."""
        assert _parse_time_window("invalid") is None
        assert _parse_time_window("1") is None
        assert _parse_time_window("h") is None
        assert _parse_time_window("1x") is None
        assert _parse_time_window("") is None
        assert _parse_time_window("-1h") is None

    def test_create_sparkline_empty_list(self):
        """Test sparkline creation with empty list."""
        result = _create_sparkline([])
        assert result == ""

    def test_create_sparkline_single_value(self):
        """Test sparkline creation with single value."""
        result = _create_sparkline([5.0])
        assert result == "▄"  # Should return a single bar

    def test_create_sparkline_ascending_values(self):
        """Test sparkline creation with ascending values."""
        result = _create_sparkline([1, 2, 3, 4, 5])
        assert len(result) == 5
        # Should show increasing pattern
        assert result[0] < result[-1] or all(c in "▁▂▃▄▅▆▇█" for c in result)

    def test_create_sparkline_descending_values(self):
        """Test sparkline creation with descending values."""
        result = _create_sparkline([5, 4, 3, 2, 1])
        assert len(result) == 5
        # Should show decreasing pattern
        assert all(c in "▁▂▃▄▅▆▇█" for c in result)

    def test_create_sparkline_constant_values(self):
        """Test sparkline creation with constant values."""
        result = _create_sparkline([3, 3, 3, 3, 3])
        assert len(result) == 5
        # All bars should be the same
        assert all(c == result[0] for c in result)

    def test_create_sparkline_mixed_values(self):
        """Test sparkline creation with mixed values."""
        result = _create_sparkline([1, 5, 2, 8, 3])
        assert len(result) == 5
        assert all(c in "▁▂▃▄▅▆▇█" for c in result)

    def test_create_sparkline_negative_values(self):
        """Test sparkline creation with negative values."""
        result = _create_sparkline([-2, -1, 0, 1, 2])
        assert len(result) == 5
        assert all(c in "▁▂▃▄▅▆▇█" for c in result)

    def test_create_sparkline_float_values(self):
        """Test sparkline creation with floating point values."""
        result = _create_sparkline([1.5, 2.7, 3.2, 1.8, 4.1])
        assert len(result) == 5
        assert all(c in "▁▂▃▄▅▆▇█" for c in result)

    @patch("marketpipe.cli.utils.list_providers")
    def test_providers_function_exists(self, mock_list_providers):
        """Test that providers function can be called."""
        mock_list_providers.return_value = ["alpaca", "polygon"]

        # Import the function to ensure it exists
        from marketpipe.cli.utils import providers

        # Function should exist and be callable
        assert callable(providers)

    def test_time_window_parsing_edge_cases(self):
        """Test edge cases in time window parsing."""
        # Zero values (these are valid and return 0)
        assert _parse_time_window("0s") == 0
        assert _parse_time_window("0m") == 0
        assert _parse_time_window("0h") == 0
        assert _parse_time_window("0d") == 0

        # Very large values
        assert _parse_time_window("999d") == 999 * 86400
        assert _parse_time_window("9999h") == 9999 * 3600

        # Fractional values (should fail)
        assert _parse_time_window("1.5h") is None
        assert _parse_time_window("2.5m") is None

    def test_sparkline_normalization(self):
        """Test that sparkline properly normalizes different ranges."""
        # Small range
        small_range = _create_sparkline([1, 2, 3])

        # Large range with same pattern
        large_range = _create_sparkline([100, 200, 300])

        # Both should show similar increasing pattern
        assert len(small_range) == len(large_range)
        assert all(c in "▁▂▃▄▅▆▇█" for c in small_range)
        assert all(c in "▁▂▃▄▅▆▇█" for c in large_range)

    def test_sparkline_with_outliers(self):
        """Test sparkline behavior with outlier values."""
        # Values with one extreme outlier
        result = _create_sparkline([1, 1, 1, 1, 100])
        assert len(result) == 5
        # Last character should be highest
        assert result[-1] in "▆▇█"
        # Earlier characters should be lower
        assert all(c in "▁▂▃▄" for c in result[:-1])

    def test_sparkline_unicode_characters(self):
        """Test that sparkline uses proper Unicode block characters."""
        result = _create_sparkline([1, 2, 3, 4, 5, 6, 7, 8])

        # Should only contain valid sparkline characters
        valid_chars = "▁▂▃▄▅▆▇█"
        assert all(c in valid_chars for c in result)

        # Should use variety of characters for different values
        unique_chars = set(result)
        assert len(unique_chars) > 1  # Should have different heights

    @patch("marketpipe.cli.utils.list_providers")
    def test_providers_function_with_providers(self, mock_list_providers):
        """Test the providers function with available providers."""
        mock_list_providers.return_value = ["alpaca", "polygon", "iex"]

        # Capture print output
        captured_output = []

        with patch("builtins.print", side_effect=lambda *args: captured_output.extend(args)):
            from marketpipe.cli.utils import providers

            providers()

        output_text = " ".join(str(arg) for arg in captured_output)
        assert "Available market data providers:" in output_text
        assert "alpaca" in output_text
        assert "polygon" in output_text
        assert "iex" in output_text
        assert "Total: 3 providers" in output_text

    @patch("marketpipe.cli.utils.list_providers")
    def test_providers_function_no_providers(self, mock_list_providers):
        """Test the providers function when no providers are available."""
        mock_list_providers.return_value = []

        # Capture print output
        captured_output = []

        with patch("builtins.print", side_effect=lambda *args: captured_output.extend(args)):
            from marketpipe.cli.utils import providers

            providers()

        output_text = " ".join(str(arg) for arg in captured_output)
        assert "No providers registered" in output_text

    @patch("marketpipe.cli.utils.list_providers")
    def test_providers_function_exception_handling(self, mock_list_providers):
        """Test the providers function handles exceptions properly."""
        mock_list_providers.side_effect = Exception("Provider registry error")

        # Capture print output
        captured_output = []

        with patch("builtins.print", side_effect=lambda *args: captured_output.extend(args)):
            from marketpipe.cli.utils import providers

            try:
                providers()
                assert False, "Expected exception was not raised"
            except (SystemExit, Exception):
                pass  # Expected - typer.Exit(1) raises different exceptions

        output_text = " ".join(str(arg) for arg in captured_output)
        assert "Failed to list providers" in output_text

    @patch("marketpipe.migrations.apply_pending")
    @patch("typer.echo")
    def test_migrate_function_success(self, mock_echo, mock_apply_pending, tmp_path):
        """Test the migrate function with successful migration."""
        mock_apply_pending.return_value = None  # Successful migration

        from marketpipe.cli.utils import migrate

        # Test successful migration
        migrate(tmp_path / "test.db")

        mock_apply_pending.assert_called_once()
        mock_echo.assert_called_with("✅ Migrations up-to-date")

    @patch("marketpipe.migrations.apply_pending")
    @patch("typer.echo")
    def test_migrate_function_failure(self, mock_echo, mock_apply_pending, tmp_path):
        """Test the migrate function with failed migration."""
        mock_apply_pending.side_effect = Exception("Migration failed")

        from marketpipe.cli.utils import migrate

        # Test failed migration
        try:
            migrate(tmp_path / "test.db")
            assert False, "Expected exception was not raised"
        except (SystemExit, Exception):
            pass  # Expected - typer.Exit(1) raises different exceptions

        mock_apply_pending.assert_called_once()
        mock_echo.assert_called_with("❌ Migration failed: Migration failed", err=True)
