-- Migration 005: SQLite ingestion jobs schema
-- Creates ingestion_jobs table for SQLite with JSON payload support

-- Drop existing table if it exists (clean slate approach)
DROP TABLE IF EXISTS ingestion_jobs;

-- Create ingestion jobs table with JSON payload support (SQLite compatible)
CREATE TABLE ingestion_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    day DATE NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'CANCELLED')),
    payload TEXT,  -- JSON as TEXT in SQLite
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint per requirements
    UNIQUE(symbol, day)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_state
ON ingestion_jobs(state);

CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_symbol_day
ON ingestion_jobs(symbol, day);

CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_updated_at
ON ingestion_jobs(updated_at);

-- SQLite trigger for auto-updating updated_at column
CREATE TRIGGER IF NOT EXISTS trigger_ingestion_jobs_updated_at
    AFTER UPDATE ON ingestion_jobs
    FOR EACH ROW
    BEGIN
        UPDATE ingestion_jobs
        SET updated_at = CURRENT_TIMESTAMP
        WHERE id = NEW.id;
    END;
