-- Migration 005: Postgres ingestion jobs schema
-- Creates ingestion_jobs table for PostgreSQL with rich DDD model support

-- Ingestion jobs table with JSONB payload support
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    day DATE NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING',
    payload JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

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

-- Index for JSONB payload queries
-- TODO: Monitor pg_stat_user_indexes for GIN index bloat in production
CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_payload_gin
ON ingestion_jobs USING GIN(payload);

-- Check constraint for valid states
ALTER TABLE ingestion_jobs
ADD CONSTRAINT check_ingestion_jobs_state
CHECK (state IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'CANCELLED'));

-- Auto-update trigger for updated_at column
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_ingestion_jobs_updated_at
    BEFORE UPDATE ON ingestion_jobs
    FOR EACH ROW
    EXECUTE FUNCTION touch_updated_at();
