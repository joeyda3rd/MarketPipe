"""add_provider_feed_labels

Revision ID: 0004
Revises: 0003
Create Date: 2024-12-19 15:30:00.000000

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name)]
    return column_name in columns


def _index_exists(index_name: str) -> bool:
    """Check if an index exists in the database."""
    conn = op.get_bind()
    result = conn.execute(
        sa.text("SELECT name FROM sqlite_master WHERE type='index' AND name=:index_name"),
        {"index_name": index_name},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    """Add provider and feed columns to metrics table for better granularity (idempotent)."""
    # Add provider column (only if it doesn't exist)
    if not _column_exists("metrics", "provider"):
        op.execute("ALTER TABLE metrics ADD COLUMN provider TEXT DEFAULT 'unknown'")

    # Add feed column (only if it doesn't exist)
    if not _column_exists("metrics", "feed"):
        op.execute("ALTER TABLE metrics ADD COLUMN feed TEXT DEFAULT 'unknown'")

    # Create composite index for efficient querying by provider and feed (idempotent)
    if not _index_exists("idx_metrics_provider_feed"):
        op.execute("CREATE INDEX idx_metrics_provider_feed ON metrics(provider, feed)")

    # Create composite index for name, provider, feed queries (idempotent)
    if not _index_exists("idx_metrics_name_provider_feed"):
        op.execute("CREATE INDEX idx_metrics_name_provider_feed ON metrics(name, provider, feed)")


def downgrade() -> None:
    """Remove provider and feed columns from metrics table."""
    # Drop indexes first
    op.execute("DROP INDEX IF EXISTS idx_metrics_provider_feed")
    op.execute("DROP INDEX IF EXISTS idx_metrics_name_provider_feed")

    # Drop columns (SQLite doesn't support DROP COLUMN directly, so we recreate the table)
    op.execute(
        """
        CREATE TABLE metrics_backup AS
        SELECT ts, name, value, created_at
        FROM metrics
    """
    )

    op.execute("DROP TABLE metrics")

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

    op.execute("INSERT INTO metrics SELECT * FROM metrics_backup")
    op.execute("DROP TABLE metrics_backup")

    # Recreate original index
    op.execute("CREATE INDEX idx_metrics_name_ts ON metrics(name, ts)")
