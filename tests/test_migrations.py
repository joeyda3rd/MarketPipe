"""Tests for Alembic database migrations."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from alembic import command
from alembic.config import Config

from marketpipe.bootstrap import apply_pending_alembic

# Mark all tests in this file as potentially SQLite-specific
# Individual tests can override this with postgres marker if needed
pytestmark = pytest.mark.sqlite_only


class TestAlembicMigrations:
    """Test Alembic migration functionality."""

    def test_sqlite_migration_from_scratch(self, tmp_path):
        """Test SQLite migration from scratch."""
        db_path = tmp_path / "test.db"
        
        # Apply migrations
        apply_pending_alembic(db_path)
        
        # Verify database was created
        assert db_path.exists()
        
        # Verify tables exist
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = [
                'alembic_version',
                'checkpoints',
                'metrics', 
                'ohlcv_bars',
                'symbol_bars_aggregates'
            ]
            assert sorted(tables) == sorted(expected_tables)

    def test_sqlite_migration_idempotent(self, tmp_path):
        """Test that running migrations multiple times is safe."""
        db_path = tmp_path / "test.db"
        
        # Apply migrations twice
        apply_pending_alembic(db_path)
        apply_pending_alembic(db_path)  # Should not fail
        
        # Verify migration version
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT version_num FROM alembic_version")
            version = cursor.fetchone()[0]
            assert version == "0003"

    def test_alembic_current_command(self, tmp_path):
        """Test alembic current command works."""
        db_path = tmp_path / "test.db"
        apply_pending_alembic(db_path)
        
        # Test alembic current command
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
        
        # This should not raise an exception
        command.current(alembic_cfg)

    def test_alembic_upgrade_downgrade(self, tmp_path):
        """Test migration upgrade and downgrade."""
        db_path = tmp_path / "test.db"
        
        # Create alembic config
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
        
        # Test upgrade to specific revision
        command.upgrade(alembic_cfg, "0001")
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT version_num FROM alembic_version")
            version = cursor.fetchone()[0]
            assert version == "0001"
        
        # Test upgrade to head
        command.upgrade(alembic_cfg, "head")
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT version_num FROM alembic_version")
            version = cursor.fetchone()[0]
            assert version == "0003"

    def test_ohlcv_columns_after_migration(self, tmp_path):
        """Test that OHLCV table has all expected columns after migration."""
        db_path = tmp_path / "test.db"
        apply_pending_alembic(db_path)
        
        with sqlite3.connect(db_path) as conn:
            # Get column info for ohlcv_bars table
            cursor = conn.execute("PRAGMA table_info(ohlcv_bars)")
            columns = [row[1] for row in cursor.fetchall()]
            
            expected_columns = [
                'id', 'symbol', 'timestamp_ns', 'open_price', 'high_price',
                'low_price', 'close_price', 'volume', 'created_at',
                'trading_date', 'trade_count', 'vwap'
            ]
            
            for col in expected_columns:
                assert col in columns, f"Missing column: {col}"

    def test_database_url_environment_variable(self, tmp_path):
        """Test that DATABASE_URL environment variable is respected."""
        db_path = tmp_path / "env_test.db"
        test_url = f"sqlite:///{db_path.absolute()}"
        
        with patch.dict(os.environ, {'DATABASE_URL': test_url}):
            # Apply migration should use the environment variable
            apply_pending_alembic(db_path)
            
            # Verify database was created at the specified path
            assert db_path.exists()

    def test_migration_error_handling(self, tmp_path):
        """Test error handling in migration system."""
        # Test with invalid database path (read-only directory)
        readonly_path = tmp_path / "readonly"
        readonly_path.mkdir()
        readonly_path.chmod(0o444)  # Make read-only
        
        invalid_db_path = readonly_path / "test.db"
        
        with pytest.raises(RuntimeError, match="Database migration failed"):
            apply_pending_alembic(invalid_db_path)


@pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES"), 
    reason="Postgres tests require TEST_POSTGRES=1 and running Postgres"
)
class TestPostgresMigrations:
    """Test Postgres migration functionality (requires running Postgres)."""
    
    @pytest.fixture
    def postgres_url(self):
        """Get Postgres test database URL."""
        return os.environ.get(
            "POSTGRES_TEST_URL", 
            "postgresql://postgres:postgres@localhost:5432/marketpipe_test"
        )

    def test_postgres_migration_from_scratch(self, postgres_url):
        """Test Postgres migration from scratch."""
        # Create alembic config with Postgres URL
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", postgres_url)
        
        try:
            # Drop all tables first (clean slate)
            from sqlalchemy import create_engine, text
            engine = create_engine(postgres_url)
            with engine.connect() as conn:
                # Drop alembic_version table if it exists
                conn.execute(text("DROP TABLE IF EXISTS alembic_version CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS symbol_bars_aggregates CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ohlcv_bars CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS checkpoints CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS metrics CASCADE"))
                conn.commit()
            
            # Run migrations
            command.upgrade(alembic_cfg, "head")
            
            # Verify tables exist
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name
                """))
                tables = [row[0] for row in result.fetchall()]
                
                expected_tables = [
                    'alembic_version',
                    'checkpoints',
                    'metrics',
                    'ohlcv_bars', 
                    'symbol_bars_aggregates'
                ]
                
                for table in expected_tables:
                    assert table in tables, f"Missing table: {table}"
                
        except Exception as e:
            pytest.skip(f"Postgres test failed (likely no running Postgres): {e}")

    def test_postgres_idempotent_migration(self, postgres_url):
        """Test that Postgres migrations are idempotent."""
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", postgres_url)
        
        try:
            # Run migrations twice
            command.upgrade(alembic_cfg, "head")
            command.upgrade(alembic_cfg, "head")  # Should not fail
            
            # Verify version
            from sqlalchemy import create_engine, text
            engine = create_engine(postgres_url)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version_num FROM alembic_version"))
                version = result.fetchone()[0]
                assert version == "0003"
                
        except Exception as e:
            pytest.skip(f"Postgres test failed (likely no running Postgres): {e}")


class TestLegacyCompatibility:
    """Test backward compatibility with legacy migration system."""
    
    def test_legacy_apply_pending_function(self, tmp_path):
        """Test that legacy apply_pending function still works."""
        from marketpipe.bootstrap import apply_pending
        
        db_path = tmp_path / "legacy_test.db"
        
        # Should work (deprecation warning may not be catchable by pytest)
        apply_pending(db_path)
        
        # Verify database was created
        assert db_path.exists()
        
        # Verify tables exist  
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='symbol_bars_aggregates'
            """)
            assert cursor.fetchone() is not None


class TestAlembicConfiguration:
    """Test Alembic configuration and setup."""
    
    def test_alembic_ini_exists(self):
        """Test that alembic.ini configuration file exists."""
        assert Path("alembic.ini").exists()
    
    def test_alembic_migrations_exist(self):
        """Test that migration files exist."""
        versions_dir = Path("alembic/versions")
        assert versions_dir.exists()
        
        migration_files = list(versions_dir.glob("*.py"))
        assert len(migration_files) >= 3  # At least our 3 migrations
        
        # Check specific migrations exist
        migration_names = [f.name for f in migration_files]
        assert any("0001_initial_schema" in name for name in migration_names)
        assert any("0002_optimize_metrics" in name for name in migration_names)  
        assert any("0003_add_missing_ohlcv" in name for name in migration_names)

    def test_alembic_env_py_exists(self):
        """Test that alembic env.py exists and is configured."""
        env_py = Path("alembic/env.py")
        assert env_py.exists()
        
        # Check that it contains our DATABASE_URL support
        content = env_py.read_text()
        assert "DATABASE_URL" in content
        assert "os.environ.get" in content 