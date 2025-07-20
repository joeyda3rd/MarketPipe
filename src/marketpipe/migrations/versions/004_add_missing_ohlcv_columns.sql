-- Add missing columns to ohlcv_bars table
-- Adds trading_date, trade_count, and vwap columns that are expected by repositories

-- Add trading_date column if it doesn't exist
-- First check if column exists using a different approach
BEGIN;

-- Create a new table with the desired schema if we can't detect columns easily
-- This approach avoids the column existence check complexity

-- Add trading_date column (check if exists first by using IF NOT EXISTS equivalent)
-- In SQLite, we use a different approach for conditional column addition
PRAGMA table_info(ohlcv_bars);

-- Since SQLite doesn't have IF NOT EXISTS for ALTER TABLE, we use a workaround
-- First create a temp table with new schema, copy data, then replace

CREATE TABLE ohlcv_bars_new (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    open_price TEXT NOT NULL,
    high_price TEXT NOT NULL,
    low_price TEXT NOT NULL,
    close_price TEXT NOT NULL,
    volume INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    trading_date TEXT,           -- NEW COLUMN
    trade_count INTEGER,         -- NEW COLUMN
    vwap TEXT                    -- NEW COLUMN
);

-- Copy existing data from original schema
INSERT INTO ohlcv_bars_new (
    id, symbol, timestamp_ns, open_price, high_price, low_price, close_price, volume, created_at
)
SELECT
    id, symbol, timestamp_ns, open_price, high_price, low_price, close_price, volume, created_at
FROM ohlcv_bars;

-- Drop old table and rename new one
DROP TABLE ohlcv_bars;
ALTER TABLE ohlcv_bars_new RENAME TO ohlcv_bars;

-- Recreate the index that was lost when we recreated the table
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_timestamp
ON ohlcv_bars(symbol, timestamp_ns);

COMMIT;

-- Update existing rows to populate trading_date from timestamp_ns
-- Convert nanoseconds to date in YYYY-MM-DD format
UPDATE ohlcv_bars
SET trading_date = date(timestamp_ns / 1000000000, 'unixepoch')
WHERE trading_date IS NULL;

-- Create index on trading_date for efficient queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_trading_date
ON ohlcv_bars(trading_date);

-- Create composite index for symbol and trading_date queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_trading_date
ON ohlcv_bars(symbol, trading_date);
