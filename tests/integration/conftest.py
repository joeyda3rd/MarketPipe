# SPDX-License-Identifier: Apache-2.0
"""Integration test fixtures and configuration."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


def _cleanup_in_progress_jobs():
    """Helper function to clean up IN_PROGRESS jobs."""
    data_dir = Path("data")
    jobs_db = data_dir / "ingestion_jobs.db"

    if jobs_db.exists():
        try:
            conn = sqlite3.connect(str(jobs_db))
            cursor = conn.cursor()

            # Mark all IN_PROGRESS jobs as FAILED to free up slots
            cursor.execute("UPDATE ingestion_jobs SET state = 'FAILED' WHERE state = 'IN_PROGRESS'")
            rows_updated = cursor.rowcount

            conn.commit()
            conn.close()

            return rows_updated
        except Exception as e:
            print(f"\n⚠️  Warning: Failed to clean up stuck jobs: {e}")
            return 0
    return 0


@pytest.fixture(scope="session", autouse=True)
def cleanup_stuck_jobs_session():
    """Clean up any stuck IN_PROGRESS jobs before running integration tests.

    This fixture runs once at the start of the test session to ensure
    we don't hit job limits from previous test runs.
    """
    rows = _cleanup_in_progress_jobs()
    if rows > 0:
        print(f"\n✓ Session start: Cleaned up {rows} stuck IN_PROGRESS jobs")
    yield


@pytest.fixture(scope="function", autouse=True)
def cleanup_stuck_jobs_after_test():
    """Clean up IN_PROGRESS jobs after each integration test.

    This prevents tests that create jobs from blocking subsequent tests.
    Runs after each test function completes.
    """
    yield  # Let the test run first

    # Clean up after the test
    _cleanup_in_progress_jobs()
    # Don't print for every test, only if we actually cleaned something
    # and only in verbose mode
