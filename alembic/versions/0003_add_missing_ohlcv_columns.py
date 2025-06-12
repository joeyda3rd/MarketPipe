"""add_missing_ohlcv_columns

Revision ID: 0003
Revises: 0002
Create Date: 2025-06-11 22:26:13.946793

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0003'
down_revision: Union[str, None] = '0002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add missing columns to ohlcv_bars table."""
    # First check if we need to migrate by seeing if columns exist
    # This is a SQLite-compatible approach using table recreation
    
    # Create new table with desired schema
    op.execute("""
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
            vwap TEXT,                   -- NEW COLUMN
            UNIQUE(symbol, timestamp_ns)
        )
    """)
    
    # Copy existing data from original table
    op.execute("""
        INSERT INTO ohlcv_bars_new (
            id, symbol, timestamp_ns, open_price, high_price, low_price, 
            close_price, volume, created_at
        )
        SELECT 
            id, symbol, timestamp_ns, open_price, high_price, low_price, 
            close_price, volume, created_at
        FROM ohlcv_bars
    """)
    
    # Drop old table and rename new one
    op.execute("DROP TABLE ohlcv_bars")
    op.execute("ALTER TABLE ohlcv_bars_new RENAME TO ohlcv_bars")
    
    # Recreate indexes
    op.execute("CREATE INDEX idx_ohlcv_symbol_timestamp ON ohlcv_bars(symbol, timestamp_ns)")
    
    # Update existing rows to populate trading_date from timestamp_ns
    op.execute("""
        UPDATE ohlcv_bars 
        SET trading_date = date(timestamp_ns / 1000000000, 'unixepoch')
        WHERE trading_date IS NULL
    """)
    
    # Create new indexes
    op.execute("CREATE INDEX idx_ohlcv_trading_date ON ohlcv_bars(trading_date)")
    op.execute("CREATE INDEX idx_ohlcv_symbol_trading_date ON ohlcv_bars(symbol, trading_date)")


def downgrade() -> None:
    """Remove added columns from ohlcv_bars table."""
    # Recreate table without the new columns
    op.execute("""
        CREATE TABLE ohlcv_bars_old (
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
        )
    """)
    
    # Copy data back (excluding new columns)
    op.execute("""
        INSERT INTO ohlcv_bars_old (
            id, symbol, timestamp_ns, open_price, high_price, low_price, 
            close_price, volume, created_at
        )
        SELECT 
            id, symbol, timestamp_ns, open_price, high_price, low_price, 
            close_price, volume, created_at
        FROM ohlcv_bars
    """)
    
    # Drop new table and rename old one
    op.execute("DROP TABLE ohlcv_bars")
    op.execute("ALTER TABLE ohlcv_bars_old RENAME TO ohlcv_bars")
    
    # Recreate original index
    op.execute("CREATE INDEX idx_ohlcv_symbol_timestamp ON ohlcv_bars(symbol, timestamp_ns)")
