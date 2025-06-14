-- Provider and feed labels migration
-- Adds provider and feed columns to metrics table for better granularity

-- Add provider column with default value
ALTER TABLE metrics ADD COLUMN provider TEXT DEFAULT 'unknown';

-- Add feed column with default value  
ALTER TABLE metrics ADD COLUMN feed TEXT DEFAULT 'unknown';

-- Back-fill existing rows with default values (for clarity, though DEFAULT handles this)
UPDATE metrics SET provider = 'unknown' WHERE provider IS NULL;
UPDATE metrics SET feed = 'unknown' WHERE feed IS NULL;

-- Create composite index for efficient querying by provider and feed
CREATE INDEX IF NOT EXISTS idx_metrics_provider_feed ON metrics(provider, feed);

-- Create composite index for name, provider, feed queries
CREATE INDEX IF NOT EXISTS idx_metrics_name_provider_feed ON metrics(name, provider, feed); 