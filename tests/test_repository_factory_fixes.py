# SPDX-License-Identifier: Apache-2.0
"""Tests for repository factory micro-patches."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from marketpipe.ingestion.infrastructure.repository_factory import (
    create_ingestion_job_repository,
)
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

                    repo = create_ingestion_job_repository()

                    if expected_type == "PostgresIngestionJobRepository":
                        mock_postgres.assert_called_once_with(database_url)
                        mock_sqlite.assert_not_called()
                    else:
                        mock_sqlite.assert_called_once()
                        mock_postgres.assert_not_called()


class TestSimpleJobAdapterFixes:
    """Test the micro-patches for simple job adapter."""

    @pytest.fixture
    def mock_repo(self):
        """Create a mock repository."""
        mock = AsyncMock()
        mock.get_by_state = AsyncMock(return_value=[])
        mock.save = AsyncMock()
        return mock

    @pytest.fixture
    def simple_adapter(self, mock_repo):
        """Create simple adapter with mocked repository."""
        adapter = SimpleJobRepository()
        adapter._repo = mock_repo
        return adapter

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "input_status,expected_normalized",
        [
            ("pending", "pending"),
            ("PENDING", "pending"),
            ("Pending", "pending"),
            ("RUNNING", "running"),
            ("Running", "running"),
            ("DONE", "done"),
            ("Done", "done"),
            ("ERROR", "error"),
            ("Error", "error"),
        ],
    )
    async def test_status_normalization_in_upsert(
        self, simple_adapter, input_status, expected_normalized
    ):
        """Test that status strings are normalized to lowercase."""
        with patch.object(simple_adapter, "_find_job_by_symbol_day", return_value=None):
            with patch.object(simple_adapter, "_create_minimal_job") as mock_create:
                mock_job = AsyncMock()
                mock_create.return_value = mock_job

                await simple_adapter.upsert("AAPL", "2024-01-15", input_status)

                # Verify the job was created (meaning status was valid after normalization)
                mock_create.assert_called_once()
                simple_adapter._repo.save.assert_called_once_with(mock_job)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "input_status,expected_normalized",
        [
            ("done", "done"),
            ("DONE", "done"),
            ("Done", "done"),
            ("ERROR", "error"),
            ("Error", "error"),
            ("CANCELLED", "cancelled"),
            ("Cancelled", "cancelled"),
        ],
    )
    async def test_status_normalization_in_mark_done(
        self, simple_adapter, input_status, expected_normalized
    ):
        """Test that final status strings are normalized to lowercase."""
        mock_job = AsyncMock()
        mock_job.can_complete = True
        mock_job.can_fail = True
        mock_job.can_cancel = True

        with patch.object(simple_adapter, "_find_job_by_symbol_day", return_value=mock_job):
            await simple_adapter.mark_done("AAPL", "2024-01-15", input_status)

            # Verify the job was processed (meaning status was valid after normalization)
            simple_adapter._repo.save.assert_called_once_with(mock_job)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "input_status",
        [
            "pending",
            "PENDING",
            "Pending",
            "running",
            "RUNNING",
            "Running",
            "done",
            "DONE",
            "Done",
        ],
    )
    async def test_status_normalization_in_list_jobs(self, simple_adapter, input_status):
        """Test that status filter is normalized to lowercase."""
        await simple_adapter.list_jobs(status=input_status)

        # Verify the repository method was called (meaning status was valid after normalization)
        simple_adapter._repo.get_by_state.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_status_after_normalization(self, simple_adapter):
        """Test that invalid statuses still raise errors after normalization."""
        with pytest.raises(ValueError, match="Invalid status 'invalid'"):
            await simple_adapter.upsert("AAPL", "2024-01-15", "INVALID")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mixed_case_status",
        [
            "PeNdInG",
            "rUnNiNg",
            "DoNe",
            "ErRoR",
            "cAnCeLlEd",
            "PENDING",
            "RUNNING",
            "DONE",
            "ERROR",
            "CANCELLED",
            "pending",
            "running",
            "done",
            "error",
            "cancelled",
        ],
    )
    async def test_comprehensive_case_insensitive_handling(self, simple_adapter, mixed_case_status):
        """Comprehensive test for case-insensitive status handling across all methods."""
        mock_job = AsyncMock()
        mock_job.can_complete = True
        mock_job.can_fail = True
        mock_job.can_cancel = True

        with patch.object(
            simple_adapter, "_find_job_by_symbol_day", return_value=None
        ) as mock_find:
            with patch.object(simple_adapter, "_create_minimal_job", return_value=mock_job):
                # Test upsert with mixed case
                await simple_adapter.upsert("AAPL", "2024-01-15", mixed_case_status)
                simple_adapter._repo.save.assert_called()

        # Reset mock
        simple_adapter._repo.reset_mock()

        # Test mark_done with mixed case (only for valid final statuses)
        if mixed_case_status.lower() in ["done", "error", "cancelled"]:
            with patch.object(simple_adapter, "_find_job_by_symbol_day", return_value=mock_job):
                await simple_adapter.mark_done("AAPL", "2024-01-15", mixed_case_status)
                simple_adapter._repo.save.assert_called()

        # Reset mock
        simple_adapter._repo.reset_mock()

        # Test list_jobs with mixed case
        await simple_adapter.list_jobs(status=mixed_case_status)
        simple_adapter._repo.get_by_state.assert_called()


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
            mock_pool = AsyncMock()

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
