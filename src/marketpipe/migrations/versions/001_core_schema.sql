-- Initial schema migration for MarketPipe core tables
-- Creates tables used by all repository implementations

-- Symbol bars aggregates table
CREATE TABLE IF NOT EXISTS symbol_bars_aggregates (
    symbol TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    is_complete BOOLEAN NOT NULL DEFAULT 0,
    collection_started BOOLEAN NOT NULL DEFAULT 0,
    bar_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trading_date)
);

-- OHLCV bars table
CREATE TABLE IF NOT EXISTS ohlcv_bars (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    open_price TEXT NOT NULL,
    high_price TEXT NOT NULL,
    low_price TEXT NOT NULL,
    close_price TEXT NOT NULL,
    volume INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp_ns)
);

-- Checkpoints table
CREATE TABLE IF NOT EXISTS checkpoints (
    symbol TEXT PRIMARY KEY,
    checkpoint_data TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metrics table
CREATE TABLE IF NOT EXISTS metrics (
    ts INTEGER NOT NULL,
    name TEXT NOT NULL,
    value REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Basic indexes for performance
CREATE INDEX IF NOT EXISTS idx_symbol_bars_date 
ON symbol_bars_aggregates(trading_date);

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_timestamp 
ON ohlcv_bars(symbol, timestamp_ns);

CREATE INDEX IF NOT EXISTS idx_metrics_ts_name 
ON metrics(ts, name);

CREATE INDEX IF NOT EXISTS idx_metrics_name 
ON metrics(name); 