# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ingestion verification service."""

from __future__ import annotations

from datetime import date
from unittest.mock import Mock, patch

import pytest

from marketpipe.ingestion.infrastructure.verification import (
    IngestionVerificationService,
    VerificationResult,
    VerificationSummary,
)


@pytest.fixture
def verification_service():
    """Create verification service for testing."""
    return IngestionVerificationService(tolerance_days=1)


@pytest.fixture
def mock_output_path(tmp_path):
    """Create a mock output path structure."""
    output_path = tmp_path / "test_output"
    output_path.mkdir()

    # Create the expected directory structure
    symbol_path = output_path / "frame=1m" / "symbol=TSLA" / "date=2024-06-20"
    symbol_path.mkdir(parents=True)

    return output_path


class TestIngestionVerificationService:
    """Test cases for IngestionVerificationService."""

    def test_verify_ingestion_success(self, verification_service, mock_output_path):
        """Test successful verification with matching date ranges."""
        requested_start = date(2024, 6, 20)
        requested_end = date(2024, 6, 21)

        # Mock successful DuckDB query
        with patch.object(verification_service, "_query_symbol_bounds") as mock_query:
            mock_query.return_value = (requested_start, requested_end, 1000)

            summary = verification_service.verify_ingestion(
                symbols=["TSLA"],
                requested_start=requested_start,
                requested_end=requested_end,
                provider="alpaca",
                output_path=mock_output_path,
            )

        assert summary.all_passed is True
        assert len(summary.failed_symbols) == 0
        assert summary.total_bars == 1000
        assert len(summary.results) == 1

        result = summary.results[0]
        assert result.passed is True
        assert result.symbol == "TSLA"
        assert result.provider == "alpaca"
        assert result.total_bars == 1000
        assert result.error_message is None

    def test_verify_ingestion_date_mismatch(self, verification_service, mock_output_path):
        """Test verification failure when dates don't match."""
        requested_start = date(2024, 6, 20)
        requested_end = date(2024, 6, 21)
        actual_start = date(2020, 7, 27)  # Much older data
        actual_end = date(2020, 8, 3)

        # Mock DuckDB query returning old data
        with patch.object(verification_service, "_query_symbol_bounds") as mock_query:
            mock_query.return_value = (actual_start, actual_end, 500)

            summary = verification_service.verify_ingestion(
                symbols=["TSLA"],
                requested_start=requested_start,
                requested_end=requested_end,
                provider="alpaca",
                output_path=mock_output_path,
            )

        assert summary.all_passed is False
        assert summary.failed_symbols == ["TSLA"]
        assert summary.total_bars == 500

        result = summary.results[0]
        assert result.passed is False
        assert result.symbol == "TSLA"
        assert result.provider == "alpaca"
        assert result.actual_start == actual_start
        assert result.actual_end == actual_end
        assert "Alpaca returned data from 2020-07-27 to 2020-08-03" in result.error_message
        assert "Try provider=" in result.error_message

    def test_verify_ingestion_no_data(self, verification_service, mock_output_path):
        """Test verification when no data is found."""
        requested_start = date(2024, 6, 20)
        requested_end = date(2024, 6, 21)

        # Mock DuckDB query returning no data
        with patch.object(verification_service, "_query_symbol_bounds") as mock_query:
            mock_query.return_value = (None, None, 0)

            summary = verification_service.verify_ingestion(
                symbols=["TSLA"],
                requested_start=requested_start,
                requested_end=requested_end,
                provider="alpaca",
                output_path=mock_output_path,
            )

        assert summary.all_passed is False
        assert summary.failed_symbols == ["TSLA"]
        assert summary.total_bars == 0

        result = summary.results[0]
        assert result.passed is False
        assert result.total_bars == 0
        assert "No data found for TSLA" in result.error_message

    def test_verify_ingestion_multiple_symbols(self, verification_service, mock_output_path):
        """Test verification with multiple symbols - mixed results."""
        requested_start = date(2024, 6, 20)
        requested_end = date(2024, 6, 21)

        def mock_query_side_effect(symbol, output_path):
            if symbol == "AAPL":
                return (requested_start, requested_end, 1000)  # Good data
            elif symbol == "TSLA":
                return (date(2020, 7, 27), date(2020, 8, 3), 500)  # Old data
            else:
                return (None, None, 0)  # No data

        with patch.object(verification_service, "_query_symbol_bounds") as mock_query:
            mock_query.side_effect = mock_query_side_effect

            summary = verification_service.verify_ingestion(
                symbols=["AAPL", "TSLA", "GOOGL"],
                requested_start=requested_start,
                requested_end=requested_end,
                provider="alpaca",
                output_path=mock_output_path,
            )

        assert summary.all_passed is False
        assert set(summary.failed_symbols) == {"TSLA", "GOOGL"}
        assert summary.total_bars == 1500

        # Check individual results
        aapl_result = next(r for r in summary.results if r.symbol == "AAPL")
        assert aapl_result.passed is True

        tsla_result = next(r for r in summary.results if r.symbol == "TSLA")
        assert tsla_result.passed is False
        assert "outside the requested range" in tsla_result.error_message

        googl_result = next(r for r in summary.results if r.symbol == "GOOGL")
        assert googl_result.passed is False
        assert "No data found" in googl_result.error_message

    def test_check_date_boundaries_within_tolerance(self, verification_service):
        """Test date boundary checking with tolerance."""
        requested_start = date(2024, 6, 20)
        requested_end = date(2024, 6, 21)

        # Within tolerance (1 day)
        actual_start = date(2024, 6, 21)  # 1 day after
        actual_end = date(2024, 6, 20)  # 1 day before

        result = verification_service._check_date_boundaries(
            requested_start, requested_end, actual_start, actual_end
        )

        assert result is True

    def test_check_date_boundaries_outside_tolerance(self, verification_service):
        """Test date boundary checking outside tolerance."""
        requested_start = date(2024, 6, 20)
        requested_end = date(2024, 6, 21)

        # Outside tolerance (more than 1 day)
        actual_start = date(2020, 7, 27)  # Years before
        actual_end = date(2020, 8, 3)

        result = verification_service._check_date_boundaries(
            requested_start, requested_end, actual_start, actual_end
        )

        assert result is False

    def test_query_symbol_bounds_with_mocked_duckdb(self, verification_service, mock_output_path):
        """Test DuckDB query for symbol bounds."""
        with patch("duckdb.connect") as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn

            # Mock successful query result
            mock_conn.execute.return_value.fetchone.return_value = (
                date(2024, 6, 20),
                date(2024, 6, 21),
                1000,
            )

            start, end, count = verification_service._query_symbol_bounds("TSLA", mock_output_path)

            assert start == date(2024, 6, 20)
            assert end == date(2024, 6, 21)
            assert count == 1000

            # Verify connection was closed
            mock_conn.close.assert_called_once()

    def test_query_symbol_bounds_duckdb_error(self, verification_service, mock_output_path):
        """Test DuckDB query error handling."""
        with patch("duckdb.connect") as mock_connect:
            mock_connect.side_effect = Exception("DuckDB connection failed")

            start, end, count = verification_service._query_symbol_bounds("TSLA", mock_output_path)

            assert start is None
            assert end is None
            assert count == 0

    def test_print_verification_summary_success(self, verification_service, capsys):
        """Test printing successful verification summary."""
        summary = VerificationSummary(
            results=[
                VerificationResult(
                    symbol="AAPL",
                    provider="alpaca",
                    requested_start=date(2024, 6, 20),
                    requested_end=date(2024, 6, 21),
                    actual_start=date(2024, 6, 20),
                    actual_end=date(2024, 6, 21),
                    total_bars=1000,
                    passed=True,
                )
            ],
            all_passed=True,
            failed_symbols=[],
            total_bars=1000,
        )

        verification_service.print_verification_summary(summary)

        captured = capsys.readouterr()
        assert "✅ All symbols passed verification" in captured.out
        assert "Total bars ingested: 1,000" in captured.out
        assert "✅ AAPL: 1,000 bars" in captured.out

    def test_print_verification_summary_failure(self, verification_service, capsys):
        """Test printing failed verification summary."""
        summary = VerificationSummary(
            results=[
                VerificationResult(
                    symbol="TSLA",
                    provider="alpaca",
                    requested_start=date(2024, 6, 20),
                    requested_end=date(2024, 6, 21),
                    actual_start=date(2020, 7, 27),
                    actual_end=date(2020, 8, 3),
                    total_bars=500,
                    passed=False,
                    error_message="Provider returned old data",
                )
            ],
            all_passed=False,
            failed_symbols=["TSLA"],
            total_bars=500,
        )

        verification_service.print_verification_summary(summary)

        captured = capsys.readouterr()
        assert "❌ Verification failed for 1 symbols" in captured.out
        assert "❌ TSLA: 500 bars" in captured.out
        assert "Provider returned old data" in captured.out
