"""initial_schema

Revision ID: 0001
Revises:
Create Date: 2025-06-11 22:25:05.620787

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    return table_name in inspector.get_table_names()


def _index_exists(index_name: str) -> bool:
    """Check if an index exists in the database."""
    conn = op.get_bind()
    # For SQLite, query sqlite_master table
    result = conn.execute(
        sa.text("SELECT name FROM sqlite_master WHERE type='index' AND name=:index_name"),
        {"index_name": index_name},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    """Create initial schema tables (idempotent)."""
    # Symbol bars aggregates table
    if not _table_exists("symbol_bars_aggregates"):
        op.execute(
            """
            CREATE TABLE symbol_bars_aggregates (
                symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                is_complete BOOLEAN NOT NULL DEFAULT FALSE,
                collection_started BOOLEAN NOT NULL DEFAULT FALSE,
                bar_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, trading_date)
            )
        """
        )

    # OHLCV bars table
    if not _table_exists("ohlcv_bars"):
        op.execute(
            """
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
        """
        )

    # Checkpoints table
    if not _table_exists("checkpoints"):
        op.execute(
            """
            CREATE TABLE checkpoints (
                symbol TEXT PRIMARY KEY,
                checkpoint_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

    # Metrics table
    if not _table_exists("metrics"):
        op.execute(
            """
            CREATE TABLE metrics (
                ts INTEGER NOT NULL,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

    # Basic indexes for performance (only create if they don't exist)
    if not _index_exists("idx_symbol_bars_date"):
        op.execute("CREATE INDEX idx_symbol_bars_date ON symbol_bars_aggregates(trading_date)")

    if not _index_exists("idx_ohlcv_symbol_timestamp"):
        op.execute("CREATE INDEX idx_ohlcv_symbol_timestamp ON ohlcv_bars(symbol, timestamp_ns)")

    if not _index_exists("idx_metrics_ts_name"):
        op.execute("CREATE INDEX idx_metrics_ts_name ON metrics(ts, name)")

    if not _index_exists("idx_metrics_name"):
        op.execute("CREATE INDEX idx_metrics_name ON metrics(name)")


def downgrade() -> None:
    """Drop all initial schema tables."""
    op.execute("DROP TABLE IF EXISTS metrics")
    op.execute("DROP TABLE IF EXISTS checkpoints")
    op.execute("DROP TABLE IF EXISTS ohlcv_bars")
    op.execute("DROP TABLE IF EXISTS symbol_bars_aggregates")
