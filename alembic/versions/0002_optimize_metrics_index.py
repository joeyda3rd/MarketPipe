"""optimize_metrics_index

Revision ID: 0002
Revises: 0001
Create Date: 2025-06-11 22:25:39.165124

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _index_exists(index_name: str) -> bool:
    """Inspect indexes without issuing SQL for a different database dialect."""
    inspector = sa.inspect(op.get_bind())
    return any(
        index["name"] == index_name
        for table in inspector.get_table_names()
        for index in inspector.get_indexes(table)
    )


def upgrade() -> None:
    """Optimize metrics table indexes (idempotent)."""
    # Drop the existing separate indexes and create a composite one
    op.execute("DROP INDEX IF EXISTS idx_metrics_ts_name")
    op.execute("DROP INDEX IF EXISTS idx_metrics_name")

    # Create optimized composite index for time-based metric queries (only if it doesn't exist)
    if not _index_exists("idx_metrics_name_ts"):
        op.execute("CREATE INDEX idx_metrics_name_ts ON metrics(name, ts)")


def downgrade() -> None:
    """Revert metrics index optimization."""
    # Drop the composite index
    op.execute("DROP INDEX IF EXISTS idx_metrics_name_ts")

    # Recreate the original separate indexes
    op.execute("CREATE INDEX idx_metrics_ts_name ON metrics(ts, name)")
    op.execute("CREATE INDEX idx_metrics_name ON metrics(name)")
