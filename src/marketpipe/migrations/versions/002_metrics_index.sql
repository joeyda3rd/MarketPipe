-- Metrics optimization migration
-- Adds optimized index for metrics queries

-- Drop the existing separate indexes and create a composite one
DROP INDEX IF EXISTS idx_metrics_ts_name;
DROP INDEX IF EXISTS idx_metrics_name;

-- Create optimized composite index for time-based metric queries
CREATE INDEX IF NOT EXISTS idx_metrics_name_ts ON metrics(name, ts); 