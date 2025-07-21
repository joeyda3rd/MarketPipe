-- Provider and feed labels migration
-- Adds provider and feed columns to metrics table for better granularity
-- Simplified approach to avoid column conflicts

-- Recreate the metrics table to add the new columns
-- This handles both fresh and existing databases safely

BEGIN;

-- Create new table with extended schema
CREATE TABLE metrics_temp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    name TEXT NOT NULL,
    value REAL NOT NULL,
    provider TEXT DEFAULT 'unknown',
    feed TEXT DEFAULT 'unknown',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copy data from existing table (only the columns that exist in the original schema)
INSERT INTO metrics_temp (ts, name, value, created_at)
SELECT
    ts,
    name,
    value,
    COALESCE(created_at, CURRENT_TIMESTAMP)
FROM metrics;

-- Replace the old table
DROP TABLE metrics;
ALTER TABLE metrics_temp RENAME TO metrics;

COMMIT;

-- Create composite index for efficient querying by provider and feed
CREATE INDEX IF NOT EXISTS idx_metrics_provider_feed ON metrics(provider, feed);

-- Create composite index for name, provider, feed queries
CREATE INDEX IF NOT EXISTS idx_metrics_name_provider_feed ON metrics(name, provider, feed);

-- Recreate the original indexes with correct names from migration 002
CREATE INDEX IF NOT EXISTS idx_metrics_name_ts ON metrics(name, ts);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name);
