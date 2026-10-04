"""Widen timestamps in PostgreSQL databases already at the previous migration head.

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Preserve existing rows while enabling 64-bit nanosecond timestamps."""
    if op.get_bind().dialect.name == "postgresql":
        op.execute("ALTER TABLE ohlcv_bars ALTER COLUMN timestamp_ns TYPE BIGINT")


def downgrade() -> None:
    """Keep the widened column so a rollback cannot truncate persisted timestamps."""
