"""Tests for Postgres-specific Alembic database migrations."""

from __future__ import annotations

import os

import pytest

# Mark all tests in this file as requiring Postgres
pytestmark = pytest.mark.postgres


@pytest.fixture
def postgres_url():
    """Get Postgres database URL from environment."""
    url = os.getenv("DATABASE_URL")
    if not url or not url.startswith("postgresql"):
        pytest.skip("Postgres not available (DATABASE_URL not set or not postgres)")
    return url


class TestPostgresMigrations:
    """Test Postgres-specific Alembic migration functionality."""

    def test_postgres_migration_from_scratch(self, postgres_url):
        """Test Postgres migration from scratch."""
        # Import asyncpg to ensure it's available
        pytest.importorskip("asyncpg")

        from alembic import command
        from alembic.config import Config

        # Create alembic config with Postgres URL
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", postgres_url)

        # Apply migrations
        command.upgrade(alembic_cfg, "head")

        # Verify current migration version using sync driver
        from sqlalchemy import create_engine

        from alembic.runtime.migration import MigrationContext

        # Convert asyncpg URL to psycopg2 URL for sync operations
        sync_url = postgres_url.replace("+asyncpg", "")

        # Skip if psycopg2 is not available
        try:
            import psycopg2
        except ImportError:
            pytest.skip("psycopg2 not available for sync operations")

        engine = create_engine(sync_url)
        with engine.connect() as conn:
            context = MigrationContext.configure(conn)
            current_rev = context.get_current_revision()
            assert current_rev == "0003"  # Should be at latest migration

    def test_postgres_specific_features(self, postgres_url):
        """Test Postgres-specific SQL features work correctly."""
        # Import required dependencies
        pytest.importorskip("asyncpg")
        pytest.importorskip("sqlalchemy")

        from sqlalchemy import create_engine, text

        # Convert asyncpg URL to psycopg2 URL for sync operations
        sync_url = postgres_url.replace("+asyncpg", "")

        # Skip if psycopg2 is not available
        try:
            import psycopg2
        except ImportError:
            pytest.skip("psycopg2 not available for sync operations")

        engine = create_engine(sync_url)

        # Test some Postgres-specific functionality
        with engine.connect() as conn:
            # Test table exists
            result = conn.execute(
                text(
                    """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'symbol_bars_aggregates'
            """
                )
            )
            tables = result.fetchall()
            assert len(tables) == 1

            # Test index exists
            result = conn.execute(
                text(
                    """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                AND tablename = 'metrics'
                AND indexname = 'idx_metrics_name_ts'
            """
                )
            )
            indexes = result.fetchall()
            assert len(indexes) == 1

    def test_postgres_concurrent_migrations(self, postgres_url):
        """Test that Postgres handles concurrent migration attempts gracefully."""
        pytest.importorskip("asyncpg")

        import threading
        import time

        from alembic import command
        from alembic.config import Config

        # Create alembic config
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", postgres_url)

        # First ensure we're at head
        command.upgrade(alembic_cfg, "head")

        # Try to run migrations concurrently (should be idempotent)
        results = []

        def run_migration():
            try:
                command.upgrade(alembic_cfg, "head")
                results.append("success")
            except Exception as e:
                results.append(f"error: {e}")

        threads = []
        for _ in range(3):
            t = threading.Thread(target=run_migration)
            threads.append(t)
            t.start()
            time.sleep(0.1)  # Stagger starts slightly

        for t in threads:
            t.join()

        # All should succeed (migrations are idempotent)
        assert all(r == "success" for r in results), f"Results: {results}"

    @pytest.mark.skipif(
        not os.getenv("DATABASE_URL", "").startswith("postgresql"),
        reason="Test requires Postgres DATABASE_URL",
    )
    def test_database_url_is_postgres(self):
        """Verify we're actually testing against Postgres in CI."""
        db_url = os.getenv("DATABASE_URL", "")
        assert "postgresql" in db_url, f"Expected Postgres URL, got: {db_url}"

        # Test connection
        pytest.importorskip("asyncpg")

        from sqlalchemy import create_engine

        # Convert asyncpg URL to psycopg2 URL for sync operations
        sync_url = db_url.replace("+asyncpg", "")

        # Skip if psycopg2 is not available
        try:
            import psycopg2
        except ImportError:
            pytest.skip("psycopg2 not available for sync operations")

        engine = create_engine(sync_url)

        with engine.connect() as conn:
            from sqlalchemy import text

            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            assert "PostgreSQL" in version
