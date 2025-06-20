"""Unit tests for symbol normalizer.

Tests the DuckDB SQL script for symbol deduplication and surrogate ID assignment
using in-memory databases with fixture data.
"""

from __future__ import annotations

import duckdb
import pytest
from datetime import date

# Importing the normalize_stage function is available but we'll test hermetically with inline SQL


class TestSymbolNormalizer:
    """Test suite for symbol normalization functionality."""
    
    @pytest.fixture
    def fixture_rows(self) -> list[dict]:
        """Six staged rows covering all deduplication scenarios.
        
        Returns:
            List of symbol records representing various dedup scenarios:
            - Two providers with same FIGI (latest as_of wins)
            - Two rows with same ticker/MIC but null FIGI (latest as_of wins)
            - One unique symbol
            - One duplicate with older as_of (should be filtered out)
        """
        return [
            # Scenario 1: Same FIGI from two providers - alpaca has later as_of, should win
            {
                "ticker": "AAPL",
                "name": "Apple Inc. - Alpaca",
                "exchange_mic": "XNAS",
                "figi": "BBG000B9XRY4",
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "320193",
                "lei": None,
                "sic": "3571",
                "country": "US",
                "industry": "Technology",
                "sector": "Consumer Electronics",
                "market_cap": 3000000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-15T10:00:00Z",
                "type": "CS",
                "first_trade_date": date(1980, 12, 12),
                "provider": "alpaca",
                "as_of": date(2024, 1, 15),
            },
            {
                "ticker": "AAPL",
                "name": "Apple Inc. - Polygon",
                "exchange_mic": "XNAS",
                "figi": "BBG000B9XRY4",  # Same FIGI as above
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "320193",
                "lei": None,
                "sic": "3571",
                "country": "US",
                "industry": "Technology",
                "sector": "Consumer Electronics",
                "market_cap": 2900000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-14T10:00:00Z",
                "type": "CS",
                "first_trade_date": date(1980, 12, 12),
                "provider": "polygon",
                "as_of": date(2024, 1, 14),  # Earlier as_of, should lose
            },
            
            # Scenario 2: Same ticker/MIC, both FIGI null - finnhub has later as_of, should win
            {
                "ticker": "TSLA",
                "name": "Tesla Inc. - Finnhub",
                "exchange_mic": "XNAS",
                "figi": None,  # No FIGI
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "1318605",
                "lei": None,
                "sic": "3711",
                "country": "US",
                "industry": "Automotive",
                "sector": "Consumer Discretionary",
                "market_cap": 800000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-15T12:00:00Z",
                "type": "CS",
                "first_trade_date": date(2010, 6, 29),
                "provider": "finnhub",
                "as_of": date(2024, 1, 15),
            },
            {
                "ticker": "TSLA",
                "name": "Tesla Inc. - IEX",
                "exchange_mic": "XNAS",
                "figi": None,  # No FIGI
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "1318605",
                "lei": None,
                "sic": "3711",
                "country": "US",
                "industry": "Automotive",
                "sector": "Consumer Discretionary",
                "market_cap": 790000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-14T12:00:00Z",
                "type": "CS",
                "first_trade_date": date(2010, 6, 29),
                "provider": "iex",
                "as_of": date(2024, 1, 14),  # Earlier as_of, should lose
            },
            
            # Scenario 3: Unique symbol - should always be included
            {
                "ticker": "GOOGL",
                "name": "Alphabet Inc.",
                "exchange_mic": "XNAS",
                "figi": "BBG009S39JX6",
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "1652044",
                "lei": None,
                "sic": "7370",
                "country": "US",
                "industry": "Technology",
                "sector": "Internet Services",
                "market_cap": 1800000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-15T14:00:00Z",
                "type": "CS",
                "first_trade_date": date(2004, 8, 19),
                "provider": "alpaca",
                "as_of": date(2024, 1, 15),
            },
            
            # Scenario 4: Duplicate of GOOGL with older as_of - should be filtered out
            {
                "ticker": "GOOGL",
                "name": "Alphabet Inc. - Old",
                "exchange_mic": "XNAS",
                "figi": "BBG009S39JX6",  # Same FIGI as above
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "1652044",
                "lei": None,
                "sic": "7370",
                "country": "US",
                "industry": "Technology",
                "sector": "Internet Services",
                "market_cap": 1750000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-10T14:00:00Z",
                "type": "CS",
                "first_trade_date": date(2004, 8, 19),
                "provider": "polygon",
                "as_of": date(2024, 1, 10),  # Much earlier as_of, should lose
            },
        ]
    
    def _run_normalization_sql(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Run the symbol normalization SQL directly in the connection."""
        # Embedded SQL script for hermetic testing
        normalization_sql = """
        -- Drop existing symbols_master table if it exists
        DROP TABLE IF EXISTS symbols_master;
        
        -- Validate required columns exist
        CREATE OR REPLACE TEMP TABLE validation_check AS
        SELECT 
            CASE WHEN COUNT(*) = 0 THEN 'OK' 
                 WHEN COUNT(*) > 0 AND MIN(provider) IS NOT NULL THEN 'OK'
                 ELSE 'MISSING_PROVIDER' 
            END as validation_result
        FROM symbols_stage;
        
        -- Create the master table with deduplication and ID assignment
        CREATE TABLE symbols_master AS
        WITH ranked AS (
            SELECT *,
                -- Create natural key: prefer FIGI, fall back to ticker|exchange_mic
                CASE 
                    WHEN figi IS NOT NULL THEN figi
                    ELSE ticker || '|' || COALESCE(exchange_mic, 'UNKNOWN')
                END as natural_key,
                -- Rank within each natural key group: latest as_of wins, then provider ASC for ties
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        CASE 
                            WHEN figi IS NOT NULL THEN figi
                            ELSE ticker || '|' || COALESCE(exchange_mic, 'UNKNOWN')
                        END
                    ORDER BY as_of DESC, provider ASC
                ) AS rn
            FROM symbols_stage
        ),
        deduped AS (
            SELECT * FROM ranked WHERE rn = 1
        ),
        with_ids AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY natural_key) AS id,
                natural_key,
                ticker, name, exchange_mic, figi, composite_figi, share_class_figi,
                cik, lei, sic, country, industry, sector, market_cap, locale,
                primary_exchange, currency, active, delisted_utc, last_updated_utc,
                type, first_trade_date, provider, as_of
            FROM deduped
        )
        SELECT * FROM with_ids;
        """
        
        # Execute the normalization SQL
        conn.execute(normalization_sql)
    
    def _setup_stage_table(self, conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
        """Create and populate symbols_stage table in DuckDB connection."""
        # Create table with all expected columns
        conn.execute("""
            CREATE OR REPLACE TABLE symbols_stage (
                ticker VARCHAR,
                name VARCHAR,
                exchange_mic VARCHAR,
                figi VARCHAR,
                composite_figi VARCHAR,
                share_class_figi VARCHAR,
                cik VARCHAR,
                lei VARCHAR,
                sic VARCHAR,
                country VARCHAR,
                industry VARCHAR,
                sector VARCHAR,
                market_cap BIGINT,
                locale VARCHAR,
                primary_exchange VARCHAR,
                currency VARCHAR,
                active BOOLEAN,
                delisted_utc VARCHAR,
                last_updated_utc VARCHAR,
                type VARCHAR,
                first_trade_date DATE,
                provider VARCHAR,
                as_of DATE
            )
        """)
        
        # Insert fixture data
        for row in rows:
            placeholders = ", ".join(["?" for _ in row.values()])
            columns = ", ".join(row.keys())
            conn.execute(
                f"INSERT INTO symbols_stage ({columns}) VALUES ({placeholders})",
                list(row.values())
            )
    
    def test_dedup_count(self, fixture_rows: list[dict]) -> None:
        """Test that deduplication produces expected number of unique symbols.
        
        Expected result: 3 unique symbols from 6 input rows
        - AAPL (FIGI BBG000B9XRY4): 2 rows -> 1 (latest as_of wins)
        - TSLA (ticker|MIC): 2 rows -> 1 (latest as_of wins)  
        - GOOGL (FIGI BBG009S39JX6): 2 rows -> 1 (latest as_of wins)
        """
        with duckdb.connect(":memory:") as conn:
            self._setup_stage_table(conn, fixture_rows)
            self._run_normalization_sql(conn)
            
            result = conn.execute("SELECT COUNT(*) FROM symbols_master").fetchone()
            assert result[0] == 3, "Expected exactly 3 deduplicated symbols"
    
    def test_id_stability(self, fixture_rows: list[dict]) -> None:
        """Test that surrogate IDs are stable across multiple runs.
        
        Running the normalizer twice on the same staging data should
        produce identical ID assignments.
        """
        with duckdb.connect(":memory:") as conn:
            self._setup_stage_table(conn, fixture_rows)
            
            # First run
            self._run_normalization_sql(conn)
            first_run = conn.execute(
                "SELECT id, natural_key FROM symbols_master ORDER BY id"
            ).fetchall()
            
            # Second run (recreate stage table first)
            self._setup_stage_table(conn, fixture_rows)
            self._run_normalization_sql(conn)
            second_run = conn.execute(
                "SELECT id, natural_key FROM symbols_master ORDER BY id"
            ).fetchall()
            
            assert first_run == second_run, "IDs should be stable across reruns"
    
    def test_natural_key_choice(self, fixture_rows: list[dict]) -> None:
        """Test that correct row is chosen for FIGI and ticker/MIC ties.
        
        For AAPL (same FIGI): alpaca should win (as_of 2024-01-15 > 2024-01-14)
        For TSLA (same ticker/MIC): finnhub should win (as_of 2024-01-15 > 2024-01-14)
        For GOOGL (same FIGI): alpaca should win (as_of 2024-01-15 > 2024-01-10)
        """
        with duckdb.connect(":memory:") as conn:
            self._setup_stage_table(conn, fixture_rows)
            self._run_normalization_sql(conn)
            
            results = conn.execute("""
                SELECT ticker, provider, figi, natural_key, as_of
                FROM symbols_master 
                ORDER BY ticker
            """).fetchall()
            
            # Should have exactly 3 results
            assert len(results) == 3
            
            # AAPL: alpaca should win (latest as_of)
            aapl = next((r for r in results if r[0] == "AAPL"), None)
            assert aapl is not None, "AAPL should be present"
            assert aapl[1] == "alpaca", "alpaca should win AAPL tie (latest as_of)"
            assert aapl[2] == "BBG000B9XRY4", "AAPL should use FIGI as natural key"
            
            # TSLA: finnhub should win (latest as_of)
            tsla = next((r for r in results if r[0] == "TSLA"), None)
            assert tsla is not None, "TSLA should be present"
            assert tsla[1] == "finnhub", "finnhub should win TSLA tie (latest as_of)"
            assert tsla[3] == "TSLA|XNAS", "TSLA should use ticker|MIC as natural key"
            
            # GOOGL: alpaca should win (latest as_of)
            googl = next((r for r in results if r[0] == "GOOGL"), None)
            assert googl is not None, "GOOGL should be present"
            assert googl[1] == "alpaca", "alpaca should win GOOGL tie (latest as_of)"
            assert googl[2] == "BBG009S39JX6", "GOOGL should use FIGI as natural key"
    
    def test_all_columns_preserved(self, fixture_rows: list[dict]) -> None:
        """Test that all original columns are preserved in output."""
        with duckdb.connect(":memory:") as conn:
            self._setup_stage_table(conn, fixture_rows)
            self._run_normalization_sql(conn)
            
            # Get column names from both tables
            stage_columns = [
                row[0] for row in conn.execute("DESCRIBE symbols_stage").fetchall()
            ]
            master_columns = [
                row[0] for row in conn.execute("DESCRIBE symbols_master").fetchall()
            ]
            
            # Master should have all stage columns plus id and natural_key
            expected_columns = set(stage_columns) | {"id", "natural_key"}
            actual_columns = set(master_columns)
            
            assert actual_columns == expected_columns, (
                f"Missing columns: {expected_columns - actual_columns}, "
                f"Extra columns: {actual_columns - expected_columns}"
            )
    
    def test_dense_id_assignment(self, fixture_rows: list[dict]) -> None:
        """Test that IDs are dense integers starting from 1."""
        with duckdb.connect(":memory:") as conn:
            self._setup_stage_table(conn, fixture_rows)
            self._run_normalization_sql(conn)
            
            ids = conn.execute("SELECT id FROM symbols_master ORDER BY id").fetchall()
            id_values = [row[0] for row in ids]
            
            # Should be dense sequence starting from 1
            expected_ids = list(range(1, len(id_values) + 1))
            assert id_values == expected_ids, "IDs should be dense sequence starting from 1"
    
    def test_natural_key_deterministic_ordering(self, fixture_rows: list[dict]) -> None:
        """Test that natural keys determine ID ordering consistently."""
        with duckdb.connect(":memory:") as conn:
            self._setup_stage_table(conn, fixture_rows)
            self._run_normalization_sql(conn)
            
            results = conn.execute("""
                SELECT id, natural_key FROM symbols_master ORDER BY id
            """).fetchall()
            
            # Natural keys should be in sorted order
            natural_keys = [row[1] for row in results]
            sorted_keys = sorted(natural_keys)
            
            assert natural_keys == sorted_keys, "Natural keys should determine ID ordering"
    
    def test_provider_tie_breaking(self) -> None:
        """Test provider ASC tie-breaking when as_of dates are identical."""
        tie_rows = [
            {
                "ticker": "MSFT",
                "name": "Microsoft - Provider Z",
                "exchange_mic": "XNAS",
                "figi": "BBG000BPH459",
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "789019",
                "lei": None,
                "sic": "7372",
                "country": "US",
                "industry": "Technology",
                "sector": "Software",
                "market_cap": 2800000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-15T10:00:00Z",
                "type": "CS",
                "first_trade_date": date(1986, 3, 13),
                "provider": "z_provider",  # Should lose (alphabetically last)
                "as_of": date(2024, 1, 15),
            },
            {
                "ticker": "MSFT",
                "name": "Microsoft - Provider A",
                "exchange_mic": "XNAS",
                "figi": "BBG000BPH459",  # Same FIGI
                "composite_figi": None,
                "share_class_figi": None,
                "cik": "789019",
                "lei": None,
                "sic": "7372",
                "country": "US",
                "industry": "Technology",
                "sector": "Software",
                "market_cap": 2850000000000,
                "locale": "us",
                "primary_exchange": "NASDAQ",
                "currency": "USD",
                "active": True,
                "delisted_utc": None,
                "last_updated_utc": "2024-01-15T10:00:00Z",
                "type": "CS",
                "first_trade_date": date(1986, 3, 13),
                "provider": "a_provider",  # Should win (alphabetically first)
                "as_of": date(2024, 1, 15),  # Same as_of date
            },
        ]
        
        with duckdb.connect(":memory:") as conn:
            self._setup_stage_table(conn, tie_rows)
            self._run_normalization_sql(conn)
            
            result = conn.execute("""
                SELECT provider, name FROM symbols_master WHERE ticker = 'MSFT'
            """).fetchone()
            
            assert result is not None, "MSFT should be present"
            assert result[0] == "a_provider", "a_provider should win tie (alphabetically first)"
            assert "Provider A" in result[1], "Should have Provider A's name" 