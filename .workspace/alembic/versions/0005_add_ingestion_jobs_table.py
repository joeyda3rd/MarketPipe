"""Add ingestion jobs table for async PostgreSQL repository

Revision ID: 0005
Revises: 0004
Create Date: 2024-12-19 10:30:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create ingestion_jobs table with database-agnostic design."""
    # Determine if we're using PostgreSQL
    conn = op.get_bind()
    is_postgresql = conn.dialect.name == "postgresql"

    # Create ingestion_jobs table
    payload_column = (
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True)
        if is_postgresql
        else sa.Column("payload", sa.JSON(), nullable=True)
    )

    op.create_table(
        "ingestion_jobs",
        sa.Column("id", sa.Integer(), nullable=False, primary_key=True),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("state", sa.String(20), nullable=False),
        payload_column,
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("retry_after", sa.TIMESTAMP(timezone=True), nullable=True),
        # Define unique constraint inline for SQLite compatibility
        sa.UniqueConstraint("symbol", "day", name="uq_ingestion_jobs_symbol_day"),
    )

    # Create indexes for efficient querying
    op.create_index("idx_ingestion_jobs_state", "ingestion_jobs", ["state"])
    op.create_index("idx_ingestion_jobs_created_at", "ingestion_jobs", ["created_at"])
    op.create_index("idx_ingestion_jobs_day", "ingestion_jobs", ["day"])

    # PostgreSQL-specific optimizations
    if is_postgresql:
        # Create GIN index on JSONB payload for efficient JSON queries
        op.create_index(
            "idx_ingestion_jobs_payload_gin", "ingestion_jobs", ["payload"], postgresql_using="gin"
        )

        # Create trigger function for automatic updated_at timestamp
        op.execute(
            """
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """
        )

        # Create trigger to automatically update updated_at column
        op.execute(
            """
            CREATE TRIGGER update_ingestion_jobs_updated_at
                BEFORE UPDATE ON ingestion_jobs
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
        """
        )


def downgrade() -> None:
    """Drop ingestion_jobs table and related objects."""
    # Determine if we're using PostgreSQL
    conn = op.get_bind()
    is_postgresql = conn.dialect.name == "postgresql"

    if is_postgresql:
        # Drop trigger first
        op.execute("DROP TRIGGER IF EXISTS update_ingestion_jobs_updated_at ON ingestion_jobs;")

        # Drop trigger function
        op.execute("DROP FUNCTION IF EXISTS update_updated_at_column();")

        # Drop PostgreSQL-specific index
        op.drop_index("idx_ingestion_jobs_payload_gin", table_name="ingestion_jobs")

    # Drop indexes (they'll be dropped with the table, but being explicit)
    op.drop_index("idx_ingestion_jobs_day", table_name="ingestion_jobs")
    op.drop_index("idx_ingestion_jobs_created_at", table_name="ingestion_jobs")
    op.drop_index("idx_ingestion_jobs_state", table_name="ingestion_jobs")

    # Drop table (this will also drop the unique constraint defined inline)
    op.drop_table("ingestion_jobs")
