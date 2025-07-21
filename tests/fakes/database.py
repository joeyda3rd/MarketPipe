# SPDX-License-Identifier: Apache-2.0
"""Fake database implementations for testing with real SQLite behavior."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Optional

import pytest

# Import from the legacy bootstrap module, not the new bootstrap package
import marketpipe.bootstrap as legacy_bootstrap


class FakeDatabase:
    """Test database using real SQLite with isolation.

    Provides a real SQLite database for tests that need database behavior
    without mocking, while ensuring isolation between test runs.
    """

    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            # Use in-memory database by default for speed
            self.db_path = ":memory:"
            self._temp_file = None
        elif str(db_path) == ":memory:":
            self.db_path = ":memory:"
            self._temp_file = None
        else:
            self.db_path = str(db_path)
            self._temp_file = None

        self._connection_string: Optional[str] = None
        self._is_setup = False

    @classmethod
    def create_temp_file(cls) -> FakeDatabase:
        """Create database backed by temporary file."""
        temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        temp_file.close()

        instance = cls(Path(temp_file.name))
        instance._temp_file = temp_file.name
        return instance

    async def setup_schema(self):
        """Apply real migrations to create test schema."""
        if self._is_setup:
            return

        # Apply real migrations using the bootstrap module
        if self.db_path != ":memory:":
            legacy_bootstrap.apply_pending_alembic(self.db_path)
        else:
            # For in-memory databases, we need to use a temporary file
            # because Alembic requires a file path
            if not self._temp_file:
                self._temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
                legacy_bootstrap.apply_pending_alembic(self._temp_file)

        self._is_setup = True

    async def cleanup(self):
        """Clean up test data while preserving schema."""
        if not self._is_setup:
            return

        # For file-based databases, we can truncate tables
        # For in-memory databases, we rely on the database being recreated
        if self.db_path != ":memory:" and Path(self.db_path).exists():
            # Here you could add table truncation logic if needed
            # For now, we rely on test isolation through separate database instances
            pass

    def get_connection_string(self) -> str:
        """Get connection string for services."""
        if self._connection_string is None:
            if self._temp_file:
                self._connection_string = f"sqlite:///{self._temp_file}"
            elif self.db_path == ":memory:":
                self._connection_string = "sqlite:///:memory:"
            else:
                self._connection_string = f"sqlite:///{self.db_path}"

        return self._connection_string

    def get_file_path(self) -> str:
        """Get file path for services that need it."""
        if self._temp_file:
            return self._temp_file
        elif self.db_path != ":memory:":
            return str(self.db_path)
        else:
            # Create a temp file for services that require a file path
            if not hasattr(self, "_file_path_backup"):
                self._file_path_backup = tempfile.NamedTemporaryFile(
                    suffix=".db", delete=False
                ).name
            return self._file_path_backup

    async def seed_test_data(self, dataset: str):
        """Load predefined test datasets."""
        # This can be extended to load different test data scenarios
        if dataset == "sample_ohlcv":
            await self._seed_sample_ohlcv_data()
        elif dataset == "ingestion_jobs":
            await self._seed_ingestion_jobs()
        # Add more datasets as needed

    async def _seed_sample_ohlcv_data(self):
        """Load sample OHLCV data for testing."""
        # Implementation would insert sample OHLCV bars
        # This is a placeholder for actual data seeding
        pass

    async def _seed_ingestion_jobs(self):
        """Load sample ingestion jobs for testing."""
        # Implementation would insert sample ingestion jobs
        # This is a placeholder for actual data seeding
        pass

    def __del__(self):
        """Clean up temporary files."""
        if self._temp_file and os.path.exists(self._temp_file):
            try:
                os.unlink(self._temp_file)
            except OSError:
                pass  # File might already be deleted

        if hasattr(self, "_file_path_backup") and os.path.exists(self._file_path_backup):
            try:
                os.unlink(self._file_path_backup)
            except OSError:
                pass


# Pytest fixtures for easy use in tests


@pytest.fixture
async def test_database():
    """Pytest fixture providing isolated test database."""
    db = FakeDatabase()
    await db.setup_schema()
    yield db
    await db.cleanup()


@pytest.fixture
async def test_database_file():
    """Pytest fixture providing file-backed test database."""
    db = FakeDatabase.create_temp_file()
    await db.setup_schema()
    yield db
    await db.cleanup()


@pytest.fixture
def database_path(tmp_path):
    """Pytest fixture providing temporary database path."""
    return tmp_path / "test.db"


class DatabaseTestCase:
    """Base test class providing database setup."""

    async def setup_method(self):
        """Set up test database."""
        self.database = FakeDatabase()
        await self.database.setup_schema()

    async def teardown_method(self):
        """Clean up test database."""
        await self.database.cleanup()


# Context manager for temporary database environment variables


class DatabaseEnvironment:
    """Context manager for setting database environment variables."""

    def __init__(self, database: FakeDatabase):
        self.database = database
        self.original_env = {}

    def __enter__(self):
        # Store original environment variables
        self.original_env = {key: os.environ.get(key) for key in ["MP_DB", "DATABASE_URL"]}

        # Set test database environment
        db_path = self.database.get_file_path()
        os.environ["MP_DB"] = db_path
        os.environ["DATABASE_URL"] = self.database.get_connection_string()

        return self.database

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original environment variables
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# Helper functions for common test scenarios


async def create_test_database_with_data(dataset: str = "sample_ohlcv") -> FakeDatabase:
    """Create test database with pre-loaded data."""
    db = FakeDatabase()
    await db.setup_schema()
    await db.seed_test_data(dataset)
    return db


def with_test_database(test_func):
    """Decorator for test functions that need a database."""

    async def wrapper(*args, **kwargs):
        db = FakeDatabase()
        await db.setup_schema()
        try:
            with DatabaseEnvironment(db):
                return await test_func(db, *args, **kwargs)
        finally:
            await db.cleanup()

    return wrapper


# Example usage patterns for documentation


class ExampleTestPatterns:
    """Example patterns for using FakeDatabase in tests."""

    async def test_with_fixture(self, test_database):
        """Example using pytest fixture."""
        # test_database is automatically set up and torn down
        test_database.get_connection_string()
        # Use real database operations...

    async def test_with_context_manager(self):
        """Example using context manager."""
        db = FakeDatabase()
        await db.setup_schema()

        with DatabaseEnvironment(db):
            # Environment variables are set for bootstrap/services
            from marketpipe.bootstrap import bootstrap

            bootstrap()  # Uses real database

    async def test_with_decorator(self):
        """Example using decorator."""

        @with_test_database
        async def actual_test(database):
            # Database is set up and environment is configured
            pass

        await actual_test()
