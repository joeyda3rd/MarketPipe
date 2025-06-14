"""add_provider_feed_labels

Revision ID: 0004
Revises: 0003
Create Date: 2024-12-19 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0004'
down_revision: Union[str, None] = '0003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add provider and feed columns to metrics table for better granularity."""
    # Add provider column
    op.execute("ALTER TABLE metrics ADD COLUMN provider TEXT DEFAULT 'unknown'")
    
    # Add feed column  
    op.execute("ALTER TABLE metrics ADD COLUMN feed TEXT DEFAULT 'unknown'")
    
    # Create composite index for efficient querying by provider and feed
    op.execute("CREATE INDEX idx_metrics_provider_feed ON metrics(provider, feed)")
    
    # Create composite index for name, provider, feed queries
    op.execute("CREATE INDEX idx_metrics_name_provider_feed ON metrics(name, provider, feed)")


def downgrade() -> None:
    """Remove provider and feed columns from metrics table."""
    # Drop indexes first
    op.execute("DROP INDEX IF EXISTS idx_metrics_provider_feed")
    op.execute("DROP INDEX IF EXISTS idx_metrics_name_provider_feed")
    
    # Drop columns (SQLite doesn't support DROP COLUMN directly, so we recreate the table)
    op.execute("""
        CREATE TABLE metrics_backup AS 
        SELECT ts, name, value, created_at 
        FROM metrics
    """)
    
    op.execute("DROP TABLE metrics")
    
    op.execute("""
        CREATE TABLE metrics (
            ts INTEGER NOT NULL,
            name TEXT NOT NULL,
            value REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    op.execute("INSERT INTO metrics SELECT * FROM metrics_backup")
    op.execute("DROP TABLE metrics_backup")
    
    # Recreate original index
    op.execute("CREATE INDEX idx_metrics_name_ts ON metrics(name, ts)") 