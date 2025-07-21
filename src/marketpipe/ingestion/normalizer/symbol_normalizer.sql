-- Symbol Normalizer SQL Script
-- Takes symbols_stage table and produces symbols_master with deduped rows and surrogate IDs
-- Assumes symbols_stage already exists in current DuckDB connection
--
-- Natural Key Strategy:
-- 1. FIGI preferred (globally unique financial instrument identifier)
-- 2. Fallback to ticker|exchange_mic for securities without FIGI
-- 3. Future: Consider override flag if FIGI quality issues discovered

-- Step 1: Drop existing symbols_master table if it exists
DROP TABLE IF EXISTS symbols_master;

-- Step 2: Validate required columns exist
-- This will fail fast if provider column is missing
CREATE TEMP TABLE validation_check AS
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'OK'
         WHEN COUNT(*) > 0 AND MIN(provider) IS NOT NULL THEN 'OK'
         ELSE 'MISSING_PROVIDER'
    END as validation_result
FROM symbols_stage;

-- Step 3: Create the master table with deduplication and ID assignment
CREATE TABLE symbols_master AS
WITH ranked AS (
    SELECT
        *,
        -- Natural key determination: prefer FIGI, fallback to ticker|exchange_mic
        COALESCE(figi, CONCAT(ticker, '|', exchange_mic)) AS natural_key,
        -- Rank for deduplication within each natural key group
        -- Order by as_of DESC (latest first), then provider ASC (deterministic tie-break)
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(figi, CONCAT(ticker, '|', exchange_mic))
            ORDER BY as_of DESC, provider ASC
        ) AS rn
    FROM symbols_stage
),

-- Keep only the top-ranked row from each natural key group
deduped AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
),

-- Assign stable surrogate IDs based on natural key ordering
-- IDs are dense integers, stable across reruns for same natural_key set
assigned AS (
    SELECT
        -- Assign surrogate ID using row_number over ordered natural keys
        ROW_NUMBER() OVER (ORDER BY natural_key) AS id,
        -- Preserve all original columns
        ticker,
        name,
        exchange_mic,
        figi,
        composite_figi,
        share_class_figi,
        cik,
        lei,
        sic,
        country,
        industry,
        sector,
        market_cap,
        locale,
        primary_exchange,
        currency,
        active,
        delisted_utc,
        last_updated_utc,
        type,
        first_trade_date,
        provider,
        as_of,
        natural_key
    FROM deduped
)

-- Final selection with all columns including surrogate ID
SELECT *
FROM assigned;
