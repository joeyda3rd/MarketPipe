-- Symbol Views SQL Script
-- Creates read-only views over the SCD-2 symbols_master Parquet dataset
-- Assumes symbols_master table already exists and has SCD-2 structure with:
-- - id: Surrogate key
-- - valid_from: Start date of validity period
-- - valid_to: End date of validity period (NULL for current/active records)
-- - as_of: Original data snapshot date
-- Plus all symbol attribute columns

----------------------------------------
-- View: v_symbol_history
-- Purpose: Expose ALL SCD-2 rows for point-in-time analysis
-- Returns: Every row in symbols_master (complete history)
-- Use cases: Backtesting, audit trails, point-in-time joins
----------------------------------------
CREATE OR REPLACE VIEW v_symbol_history AS
SELECT *
FROM symbols_master;

----------------------------------------
-- View: v_symbol_latest
-- Purpose: Expose ONLY the current/active version of each symbol
-- Returns: Exactly one row per symbol ID where valid_to IS NULL
-- Use cases: Real-time pipelines, UI dropdowns, current symbol lists
-- Safety: Uses row_number() + valid_to filter to prevent double-active intervals
----------------------------------------
CREATE OR REPLACE VIEW v_symbol_latest AS
SELECT t.*
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY id
               ORDER BY valid_from DESC
           ) AS rn
    FROM symbols_master
) AS t
WHERE rn = 1 AND t.valid_to IS NULL;
