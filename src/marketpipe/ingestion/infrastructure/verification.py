# SPDX-License-Identifier: Apache-2.0
"""Post-ingestion data verification service."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb

from marketpipe.providers import ProviderFeatureMatrix


@dataclass
class VerificationResult:
    """Result of post-ingestion verification."""

    symbol: str
    provider: str
    requested_start: date
    requested_end: date
    actual_start: Optional[date]
    actual_end: Optional[date]
    total_bars: int
    passed: bool
    error_message: Optional[str] = None
    suggested_providers: Optional[list[str]] = None


@dataclass
class VerificationSummary:
    """Summary of verification results for multiple symbols."""

    results: list[VerificationResult]
    all_passed: bool
    failed_symbols: list[str]
    total_bars: int


class IngestionVerificationService:
    """Service for verifying ingested data meets requirements."""

    def __init__(self, tolerance_days: int = 1):
        """Initialize verification service.

        Args:
            tolerance_days: Number of days tolerance for boundary mismatches
        """
        self.tolerance_days = tolerance_days
        self.log = logging.getLogger(self.__class__.__name__)

    def verify_ingestion(
        self,
        symbols: list[str],
        requested_start: date,
        requested_end: date,
        provider: str,
        output_path: Path,
    ) -> VerificationSummary:
        """Verify that ingested data meets expectations.

        Args:
            symbols: List of symbols that were requested
            requested_start: Start date that was requested
            requested_end: End date that was requested
            provider: Provider that was used
            output_path: Path where data was written

        Returns:
            VerificationSummary with results for all symbols

        Raises:
            VerificationError: If verification fails
        """
        results = []

        for symbol in symbols:
            result = self._verify_symbol_data(
                symbol, requested_start, requested_end, provider, output_path
            )
            results.append(result)

        # Calculate summary
        failed_symbols = [r.symbol for r in results if not r.passed]
        all_passed = len(failed_symbols) == 0
        total_bars = sum(r.total_bars for r in results)

        summary = VerificationSummary(
            results=results,
            all_passed=all_passed,
            failed_symbols=failed_symbols,
            total_bars=total_bars,
        )

        return summary

    def _verify_symbol_data(
        self,
        symbol: str,
        requested_start: date,
        requested_end: date,
        provider: str,
        output_path: Path,
    ) -> VerificationResult:
        """Verify data for a single symbol."""
        try:
            # Query the written Parquet files
            actual_start, actual_end, total_bars = self._query_symbol_bounds(symbol, output_path)

            if total_bars == 0:
                return VerificationResult(
                    symbol=symbol,
                    provider=provider,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    actual_start=None,
                    actual_end=None,
                    total_bars=0,
                    passed=False,
                    error_message=f"No data found for {symbol}",
                )

            # Check if actual boundaries are within tolerance
            passed = self._check_date_boundaries(
                requested_start, requested_end, actual_start, actual_end
            )

            error_message = None
            suggested_providers = None

            if not passed:
                # Generate error message with provider suggestions
                if actual_start is not None and actual_end is not None:
                    error_message = ProviderFeatureMatrix.get_suggestion_message(
                        provider, actual_start, actual_end, requested_start, requested_end
                    )
                else:
                    error_message = (
                        "Insufficient data to verify boundaries; try a different provider or range."
                    )
                suggested_providers = ProviderFeatureMatrix.suggest_alternatives(
                    provider, requested_start, requested_end
                )

            return VerificationResult(
                symbol=symbol,
                provider=provider,
                requested_start=requested_start,
                requested_end=requested_end,
                actual_start=actual_start,
                actual_end=actual_end,
                total_bars=total_bars,
                passed=passed,
                error_message=error_message,
                suggested_providers=suggested_providers,
            )

        except Exception as e:
            self.log.error(f"Verification failed for {symbol}: {e}")
            return VerificationResult(
                symbol=symbol,
                provider=provider,
                requested_start=requested_start,
                requested_end=requested_end,
                actual_start=None,
                actual_end=None,
                total_bars=0,
                passed=False,
                error_message=f"Verification error: {e}",
            )

    def _query_symbol_bounds(
        self, symbol: str, output_path: Path
    ) -> tuple[Optional[date], Optional[date], int]:
        """Query the min/max dates and bar count for a symbol."""

        # Construct glob pattern for symbol data
        symbol_pattern = str(output_path / f"frame=1m/symbol={symbol}/**/*.parquet")

        try:
            conn = duckdb.connect()

            # Query for date bounds and count
            query = f"""
            SELECT
                MIN(date) AS min_date,
                MAX(date) AS max_date,
                COUNT(*) AS total_bars
            FROM read_parquet('{symbol_pattern}')
            WHERE symbol = ?
            """

            result = conn.execute(query, [symbol]).fetchone()
            conn.close()

            if result and result[2] > 0:  # Check if we have bars
                min_date = result[0]
                max_date = result[1]
                total_bars = result[2]

                # Convert to Python date objects if they're strings
                if isinstance(min_date, str):
                    min_date = datetime.fromisoformat(min_date).date()
                if isinstance(max_date, str):
                    max_date = datetime.fromisoformat(max_date).date()

                return min_date, max_date, total_bars
            else:
                return None, None, 0

        except Exception as e:
            self.log.warning(f"Failed to query symbol {symbol}: {e}")
            return None, None, 0

    def _check_date_boundaries(
        self,
        requested_start: date,
        requested_end: date,
        actual_start: Optional[date],
        actual_end: Optional[date],
    ) -> bool:
        """Check if actual date boundaries are within tolerance of requested."""
        if actual_start is None or actual_end is None:
            return False

        # Define tolerance window
        tolerance = timedelta(days=self.tolerance_days)

        # Check start boundary (allow some tolerance for market holidays)
        start_tolerance_begin = requested_start - tolerance
        start_tolerance_end = requested_start + tolerance
        start_ok = start_tolerance_begin <= actual_start <= start_tolerance_end

        # Check end boundary
        end_tolerance_begin = requested_end - tolerance
        end_tolerance_end = requested_end + tolerance
        end_ok = end_tolerance_begin <= actual_end <= end_tolerance_end

        return start_ok and end_ok

    def print_verification_summary(self, summary: VerificationSummary) -> None:
        """Print a human-readable verification summary."""
        if summary.all_passed:
            print("✅ All symbols passed verification")
            print(f"📊 Total bars ingested: {summary.total_bars:,}")
        else:
            print(f"❌ Verification failed for {len(summary.failed_symbols)} symbols")
            print(f"📊 Total bars ingested: {summary.total_bars:,}")

        print("\n📋 Verification Details:")
        for result in summary.results:
            status = "✅" if result.passed else "❌"
            print(f"  {status} {result.symbol}: {result.total_bars:,} bars")

            if result.actual_start and result.actual_end:
                print(f"      Actual range: {result.actual_start} to {result.actual_end}")
            else:
                print("      No data found")

            if not result.passed and result.error_message:
                print(f"      {result.error_message}")


class VerificationError(Exception):
    """Exception raised when ingestion verification fails."""

    def __init__(self, message: str, summary: VerificationSummary):
        super().__init__(message)
        self.summary = summary
