"""initial_schema

Revision ID: 0001
Revises: 
Create Date: 2025-06-11 22:25:05.620787

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create initial schema tables."""
    # Symbol bars aggregates table
    op.execute("""
        CREATE TABLE symbol_bars_aggregates (
            symbol TEXT NOT NULL,
            trading_date TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            is_complete BOOLEAN NOT NULL DEFAULT 0,
            collection_started BOOLEAN NOT NULL DEFAULT 0,
            bar_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, trading_date)
        )
    """)

    # OHLCV bars table
    op.execute("""
        CREATE TABLE ohlcv_bars (
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

    # Checkpoints table
    op.execute("""
        CREATE TABLE checkpoints (
            symbol TEXT PRIMARY KEY,
            checkpoint_data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Metrics table
    op.execute("""
        CREATE TABLE metrics (
            ts INTEGER NOT NULL,
            name TEXT NOT NULL,
            value REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Basic indexes for performance
    op.execute("CREATE INDEX idx_symbol_bars_date ON symbol_bars_aggregates(trading_date)")
    op.execute("CREATE INDEX idx_ohlcv_symbol_timestamp ON ohlcv_bars(symbol, timestamp_ns)")
    op.execute("CREATE INDEX idx_metrics_ts_name ON metrics(ts, name)")
    op.execute("CREATE INDEX idx_metrics_name ON metrics(name)")


def downgrade() -> None:
    """Drop all initial schema tables."""
    op.execute("DROP TABLE IF EXISTS metrics")
    op.execute("DROP TABLE IF EXISTS checkpoints")
    op.execute("DROP TABLE IF EXISTS ohlcv_bars")
    op.execute("DROP TABLE IF EXISTS symbol_bars_aggregates")
