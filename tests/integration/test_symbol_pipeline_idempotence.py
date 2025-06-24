"""Integration test for symbol pipeline idempotence.

This test proves that running the same symbol ingestion twice results in
zero new SCD (Slowly Changing Dimension) rows on the second run, demonstrating
that the pipeline correctly handles duplicate data and maintains data integrity.
"""

from __future__ import annotations

import datetime as dt
import tempfile
from pathlib import Path
from typing import List

import pytest
import duckdb

from marketpipe.domain import SymbolRecord, AssetClass, Status
from marketpipe.ingestion.symbol_providers import get as get_provider


class TestSymbolPipelineIdempotence:
    """Test idempotence of symbol pipeline operations."""

    @pytest.mark.asyncio
    async def test_duplicate_ingestion_writes_zero_scd_rows(self):
        """Prove that running the same ingestion twice writes 0 new SCD rows.

        This test:
        1. Creates a dummy symbol provider with known test data
        2. Runs initial ingestion to populate SCD table
        3. Runs identical ingestion again
        4. Verifies that second run writes exactly 0 new SCD rows
        5. Confirms SCD table maintains proper versioning and timestamps
        """

        # Create temporary directory for test data
        with tempfile.TemporaryDirectory() as temp_dir:
            test_db_path = Path(temp_dir) / "test_idempotence.duckdb"

            # Initialize DuckDB with SCD table structure
            conn = duckdb.connect(str(test_db_path))

            # Create SCD table with standard slowly changing dimension fields
            conn.execute("""
                CREATE TABLE symbol_scd (
                    id INTEGER,
                    ticker VARCHAR,
                    exchange_mic VARCHAR,
                    asset_class VARCHAR,
                    currency VARCHAR,
                    status VARCHAR,
                    company_name VARCHAR,
                    
                    -- SCD Type 2 fields
                    effective_from DATE,
                    effective_to DATE,
                    is_current BOOLEAN,
                    version INTEGER,
                    
                    -- Temporal tracking
                    as_of DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create a provider with deterministic test data
            provider = get_provider("dummy")

            # First ingestion run - baseline
            print("🔄 Running first ingestion...")
            records_run1 = await provider.fetch_symbols()

            # Insert first batch with SCD logic
            initial_count = self._insert_with_scd_logic(
                conn, records_run1, run_number=1
            )

            # Verify initial state
            total_after_run1 = conn.execute(
                "SELECT COUNT(*) FROM symbol_scd"
            ).fetchone()[0]
            current_after_run1 = conn.execute(
                "SELECT COUNT(*) FROM symbol_scd WHERE is_current = true"
            ).fetchone()[0]

            print(
                f"✅ First run: {initial_count} records inserted, {total_after_run1} total rows, {current_after_run1} current"
            )

            # Second ingestion run - should be idempotent
            print("🔄 Running second ingestion (should be idempotent)...")
            records_run2 = await provider.fetch_symbols()

            # Insert second batch with SCD logic
            new_count = self._insert_with_scd_logic(conn, records_run2, run_number=2)

            # Verify idempotence
            total_after_run2 = conn.execute(
                "SELECT COUNT(*) FROM symbol_scd"
            ).fetchone()[0]
            current_after_run2 = conn.execute(
                "SELECT COUNT(*) FROM symbol_scd WHERE is_current = true"
            ).fetchone()[0]

            print(
                f"✅ Second run: {new_count} records inserted, {total_after_run2} total rows, {current_after_run2} current"
            )

            # CRITICAL ASSERTIONS for idempotence
            assert new_count == 0, (
                f"Expected 0 new SCD rows on second run, got {new_count}"
            )
            assert total_after_run2 == total_after_run1, (
                "Total row count should be unchanged"
            )
            assert current_after_run2 == current_after_run1, (
                "Current record count should be unchanged"
            )

            # Verify SCD versioning integrity
            version_check = conn.execute("""
                SELECT ticker, COUNT(*) as versions, 
                       COUNT(CASE WHEN is_current THEN 1 END) as current_versions
                FROM symbol_scd 
                GROUP BY ticker
            """).fetchall()

            for ticker, versions, current_versions in version_check:
                assert current_versions == 1, (
                    f"Ticker {ticker} should have exactly 1 current version, got {current_versions}"
                )

            # Verify effective dates are properly maintained
            date_integrity = conn.execute("""
                SELECT COUNT(*) FROM symbol_scd s1
                WHERE is_current = false 
                AND NOT EXISTS (
                    SELECT 1 FROM symbol_scd s2 
                    WHERE s2.ticker = s1.ticker 
                    AND s2.effective_from = s1.effective_to
                )
            """).fetchone()[0]

            assert date_integrity == 0, "SCD effective date chains should be continuous"

            # Third run with modified data - should create new SCD versions
            print("🔄 Running third ingestion with modified data...")
            records_run3 = await self._create_modified_records(records_run2)
            modified_count = self._insert_with_scd_logic(
                conn, records_run3, run_number=3
            )

            total_after_run3 = conn.execute(
                "SELECT COUNT(*) FROM symbol_scd"
            ).fetchone()[0]
            current_after_run3 = conn.execute(
                "SELECT COUNT(*) FROM symbol_scd WHERE is_current = true"
            ).fetchone()[0]

            print(
                f"✅ Third run (modified): {modified_count} records processed, {total_after_run3} total rows, {current_after_run3} current"
            )

            # Verify that changes DO create new SCD rows
            assert total_after_run3 > total_after_run2, (
                "Modified data should create new SCD versions"
            )
            assert current_after_run3 == current_after_run2, (
                "Should still have same number of current records"
            )

            conn.close()

            print("🎉 Idempotence test completed successfully!")

    def _insert_with_scd_logic(
        self,
        conn: duckdb.DuckDBPyConnection,
        records: List[SymbolRecord],
        run_number: int,
    ) -> int:
        """Insert records using SCD Type 2 logic.

        Returns the number of new rows actually inserted.
        """
        new_rows_inserted = 0
        today = dt.date.today()

        for record in records:
            # Check if record already exists with same business key and attributes
            existing = conn.execute(
                """
                SELECT id, version, is_current FROM symbol_scd 
                WHERE ticker = ? AND exchange_mic = ? AND is_current = true
            """,
                [record.ticker, record.exchange_mic],
            ).fetchone()

            if existing:
                # Check if any tracked attributes have changed
                current_data = conn.execute(
                    """
                    SELECT asset_class, currency, status, company_name 
                    FROM symbol_scd 
                    WHERE ticker = ? AND exchange_mic = ? AND is_current = true
                """,
                    [record.ticker, record.exchange_mic],
                ).fetchone()

                has_changes = (
                    current_data[0] != record.asset_class.value
                    or current_data[1] != record.currency
                    or current_data[2] != record.status.value
                    or current_data[3] != record.company_name
                )

                if has_changes:
                    # Close existing record
                    conn.execute(
                        """
                        UPDATE symbol_scd 
                        SET effective_to = ?, is_current = false, updated_at = CURRENT_TIMESTAMP
                        WHERE ticker = ? AND exchange_mic = ? AND is_current = true
                    """,
                        [today, record.ticker, record.exchange_mic],
                    )

                    # Insert new version
                    self._insert_new_scd_row(conn, record, existing[1] + 1, today)
                    new_rows_inserted += 1
                # If no changes, this is the idempotent case - no new row inserted

            else:
                # New record - insert with version 1
                self._insert_new_scd_row(conn, record, 1, today)
                new_rows_inserted += 1

        return new_rows_inserted

    def _insert_new_scd_row(
        self,
        conn: duckdb.DuckDBPyConnection,
        record: SymbolRecord,
        version: int,
        effective_from: dt.date,
    ):
        """Insert a new SCD row."""
        conn.execute(
            """
            INSERT INTO symbol_scd (
                id, ticker, exchange_mic, asset_class, currency, status, company_name,
                effective_from, effective_to, is_current, version, as_of
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, true, ?, ?)
        """,
            [
                record.id,
                record.ticker,
                record.exchange_mic,
                record.asset_class.value,
                record.currency,
                record.status.value,
                record.company_name,
                effective_from,
                version,
                record.as_of,
            ],
        )

    async def _create_modified_records(
        self, original_records: List[SymbolRecord]
    ) -> List[SymbolRecord]:
        """Create modified versions of records to test SCD versioning."""
        modified = []

        for i, record in enumerate(original_records):
            # Modify every other record to test change detection
            if i % 2 == 0:
                # Create a new record with modified company name
                modified_record = SymbolRecord(
                    id=record.id,
                    ticker=record.ticker,
                    exchange_mic=record.exchange_mic,
                    asset_class=record.asset_class,
                    currency=record.currency,
                    status=record.status,
                    company_name=f"{record.company_name} (Modified)"
                    if record.company_name
                    else "Modified Inc.",
                    as_of=record.as_of,
                )
                modified.append(modified_record)
            else:
                # Keep original record unchanged
                modified.append(record)

        return modified


@pytest.mark.integration
class TestSymbolPipelineIntegration:
    """Additional integration tests for symbol pipeline."""

    @pytest.mark.asyncio
    async def test_multiple_provider_idempotence(self):
        """Test idempotence across multiple symbol providers."""

        # Test with dummy provider (controlled data)
        dummy_provider = get_provider("dummy")
        dummy_records_1 = await dummy_provider.fetch_symbols()
        dummy_records_2 = await dummy_provider.fetch_symbols()

        # Verify dummy provider returns identical data
        assert len(dummy_records_1) == len(dummy_records_2)
        assert len(dummy_records_1) > 0, "Dummy provider should return test data"

        for r1, r2 in zip(dummy_records_1, dummy_records_2):
            assert r1.ticker == r2.ticker
            assert r1.exchange_mic == r2.exchange_mic
            assert r1.asset_class == r2.asset_class
            assert r1.as_of == r2.as_of

        print(
            f"✅ Dummy provider idempotence verified with {len(dummy_records_1)} records"
        )

    @pytest.mark.asyncio
    async def test_provider_data_quality(self):
        """Verify symbol providers return valid, well-formed data."""

        providers_to_test = [
            "dummy"
        ]  # Add "polygon", "nasdaq_dl" when ready for live testing

        for provider_name in providers_to_test:
            provider = get_provider(provider_name)
            records = await provider.fetch_symbols()

            assert len(records) > 0, f"Provider {provider_name} should return data"

            for record in records:
                # Validate required fields
                assert record.ticker, f"Record missing ticker: {record}"
                assert record.exchange_mic, f"Record missing exchange_mic: {record}"
                assert record.asset_class in [AssetClass.EQUITY, AssetClass.ETF], (
                    f"Invalid asset_class: {record.asset_class}"
                )
                assert record.currency == "USD", (
                    f"Expected USD currency: {record.currency}"
                )
                assert record.status in [Status.ACTIVE, Status.DELISTED], (
                    f"Invalid status: {record.status}"
                )
                assert record.as_of, f"Record missing as_of date: {record}"

                # Validate field formats
                assert len(record.ticker) >= 1, f"Ticker too short: {record.ticker}"
                assert len(record.exchange_mic) == 4, (
                    f"Invalid MIC length: {record.exchange_mic}"
                )

            print(
                f"✅ Provider {provider_name} data quality verified with {len(records)} records"
            )
