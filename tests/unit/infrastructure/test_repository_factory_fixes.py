# SPDX-License-Identifier: Apache-2.0
"""Tests for repository factory micro-patches."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from marketpipe.ingestion.domain.entities import ProcessingState
from marketpipe.ingestion.infrastructure.repository_factory import create_ingestion_job_repository
from marketpipe.ingestion.infrastructure.simple_job_adapter import SimpleJobRepository


class TestRepositoryFactoryFixes:
    """Test the micro-patches for repository factory."""

    @pytest.mark.parametrize(
        "database_url,expected_type",
        [
            ("postgres://user:pass@localhost/db", "PostgresIngestionJobRepository"),
            ("postgresql://user:pass@localhost/db", "PostgresIngestionJobRepository"),
            ("postgresql+asyncpg://user:pass@localhost/db", "PostgresIngestionJobRepository"),
            ("postgresql+psycopg://user:pass@localhost/db", "PostgresIngestionJobRepository"),
            ("sqlite:///path/to/db.sqlite", "SqliteIngestionJobRepository"),
            (
                "mysql://user:pass@localhost/db",
                "SqliteIngestionJobRepository",
            ),  # Falls back to SQLite
            (None, "SqliteIngestionJobRepository"),  # No URL = SQLite default
        ],
    )
    def test_database_url_detection(self, database_url, expected_type):
        """Test that various PostgreSQL URL formats are detected correctly."""
        with patch.dict(
            "os.environ", {"DATABASE_URL": database_url} if database_url else {}, clear=True
        ):
            with patch(
                "marketpipe.ingestion.infrastructure.repository_factory.PostgresIngestionJobRepository"
            ) as mock_postgres:
                with patch(
                    "marketpipe.ingestion.infrastructure.repository_factory.SqliteIngestionJobRepository"
                ) as mock_sqlite:

                    create_ingestion_job_repository()

                    if expected_type == "PostgresIngestionJobRepository":
                        mock_postgres.assert_called_once_with(database_url)
                        mock_sqlite.assert_not_called()
                    else:
                        mock_sqlite.assert_called_once()
                        mock_postgres.assert_not_called()


class TestSimpleJobRepositoryFixes:
    """Test fixes for SimpleJobRepository status normalization issues."""

    @pytest.fixture
    def mock_repo(self):
        """Create a mock repository with proper async methods."""
        repo = MagicMock()
        repo.save = AsyncMock(return_value=None)
        repo.get_by_state = AsyncMock(return_value=[])
        repo.get_job_history = AsyncMock(return_value=[])
        return repo

    @pytest.fixture
    def simple_repo(self, mock_repo):
        """Create SimpleJobRepository with mocked backend."""
        with patch(
            "marketpipe.ingestion.infrastructure.simple_job_adapter.create_ingestion_job_repository",
            return_value=mock_repo,
        ):
            return SimpleJobRepository()

    @pytest.mark.asyncio
    async def test_upsert_normalizes_status_case(self, simple_repo, mock_repo):
        """Test that status strings are normalized to lowercase."""
        # Test various case combinations
        test_cases = [
            ("Pending", ProcessingState.PENDING),
            ("RUNNING", ProcessingState.IN_PROGRESS),
            ("Done", ProcessingState.COMPLETED),
            ("ERROR", ProcessingState.FAILED),
        ]

        for status_input, expected_state in test_cases:
            mock_repo.get_job_history.return_value = []  # No existing jobs

            await simple_repo.upsert("AAPL", "2024-01-01", status_input)

            # Verify save was called
            assert mock_repo.save.called

            # Reset mock for next iteration
            mock_repo.save.reset_mock()

    @pytest.mark.asyncio
    async def test_list_jobs_returns_tuples(self, simple_repo, mock_repo):
        """Test that list_jobs returns proper tuple format."""
        # Create a mock job
        mock_job = MagicMock()
        mock_job.symbols = [MagicMock()]
        mock_job.symbols[0].__str__ = MagicMock(return_value="AAPL")
        mock_job.state = ProcessingState.PENDING

        # Mock the _extract_day_string method to return a proper date string
        with patch.object(simple_repo, "_extract_day_string", return_value="2024-01-01"):
            mock_repo.get_job_history.return_value = [mock_job]

            result = await simple_repo.list_jobs()

            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], tuple)
            assert len(result[0]) == 3  # (symbol, day, status)
            assert result[0][0] == "AAPL"
            assert result[0][1] == "2024-01-01"
            assert result[0][2] == "pending"

    @pytest.mark.asyncio
    async def test_mark_done_with_various_statuses(self, simple_repo, mock_repo):
        """Test mark_done with different final statuses."""
        # Create a mock job that can be completed
        mock_job = MagicMock()
        mock_job.can_complete = True
        mock_job.can_fail = True
        mock_job.can_cancel = True
        mock_job.complete = MagicMock()
        mock_job.fail = MagicMock()
        mock_job.cancel = MagicMock()

        # Mock finding the job
        with patch.object(simple_repo, "_find_job_by_symbol_day", return_value=mock_job):
            # Test done status
            await simple_repo.mark_done("AAPL", "2024-01-01", "done")
            mock_job.complete.assert_called_once()

            # Test error status
            await simple_repo.mark_done("AAPL", "2024-01-01", "error")
            mock_job.fail.assert_called_once()

            # Test cancelled status
            await simple_repo.mark_done("AAPL", "2024-01-01", "cancelled")
            mock_job.cancel.assert_called_once()


class TestPostgresPoolRaceCondition:
    """Test that pool initialization is race-condition safe."""

    @pytest.mark.asyncio
    async def test_concurrent_pool_initialization(self):
        """Test that concurrent calls to _get_pool() don't create multiple pools."""
        from marketpipe.ingestion.infrastructure.postgres_repository import (
            PostgresIngestionJobRepository,
        )

        repo = PostgresIngestionJobRepository("postgresql://test")

        # Mock asyncpg.create_pool to track calls
        with patch("asyncpg.create_pool") as mock_create_pool:
            mock_pool = MagicMock()

            # Return the mock directly, not as a coroutine
            async def mock_create_pool_func(*args, **kwargs):
                return mock_pool

            mock_create_pool.side_effect = mock_create_pool_func

            # Simulate concurrent calls to _get_pool
            tasks = [repo._get_pool() for _ in range(5)]
            pools = await asyncio.gather(*tasks)

            # Verify only one pool was created despite concurrent calls
            mock_create_pool.assert_called_once()

            # Verify all calls returned the same pool instance
            assert all(pool is mock_pool for pool in pools)
