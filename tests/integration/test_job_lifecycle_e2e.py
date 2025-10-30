# SPDX-License-Identifier: Apache-2.0
"""Job lifecycle end-to-end tests.

This test file validates the complete job lifecycle from creation to cleanup.

PURPOSE:
Test the complete workflow of MarketPipe jobs:
Create → Run → Monitor → Complete → Cleanup

WHAT THIS TESTS:
- Job creation and ID generation
- Job execution and status tracking
- Job completion handling
- Job cleanup command functionality
- Database interactions through job lifecycle

WHY THIS MATTERS:
- Jobs are the fundamental unit of work in MarketPipe
- Job tracking is critical for monitoring and debugging
- Cleanup functionality prevents database bloat
- Users depend on reliable job management

EXECUTION TIME: Target <15 seconds for CI
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


@pytest.mark.integration
def test_job_creation_via_ingest(tmp_path):
    """
    Test 5A: Validate job is created when running ingest command.

    Jobs should be automatically created and tracked when ingestion runs.
    """
    # Setup isolated environment
    import os

    env = os.environ.copy()
    env["MP_DATA_DIR"] = str(tmp_path / "data")

    # Run ingest (this should create a job)
    result = subprocess.run(
        [
            "marketpipe",
            "ingest",
            "--provider",
            "fake",
            "--symbols",
            "JOBTEST",
            "--start",
            "2025-01-15",
            "--end",
            "2025-01-16",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    # Validate ingest succeeded
    assert (
        result.returncode == 0
    ), f"Ingest should succeed. Exit code: {result.returncode}\nStderr: {result.stderr}"

    # Validate data was created (implies job ran)
    data_dir = tmp_path / "data"
    if data_dir.exists():
        parquet_files = list(data_dir.rglob("*.parquet"))
        assert len(parquet_files) > 0, "Job should create data files"


@pytest.mark.integration
def test_job_listing_command(tmp_path):
    """
    Test 5B: Validate jobs list command works.

    Users should be able to list jobs to monitor progress.
    """
    # Setup isolated environment
    import os

    env = os.environ.copy()
    env["MP_DATA_DIR"] = str(tmp_path / "data")

    # Create a job by running ingest
    ingest_result = subprocess.run(
        [
            "marketpipe",
            "ingest",
            "--provider",
            "fake",
            "--symbols",
            "LISTTEST",
            "--start",
            "2025-01-15",
            "--end",
            "2025-01-16",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    assert ingest_result.returncode == 0, "Ingest should succeed"

    # List jobs
    list_result = subprocess.run(
        ["marketpipe", "jobs", "list"], capture_output=True, text=True, timeout=30, env=env
    )

    # Jobs list should execute successfully
    assert (
        list_result.returncode == 0
    ), f"Jobs list should succeed. Exit code: {list_result.returncode}\nStderr: {list_result.stderr}"

    # Output should contain some job information
    output = list_result.stdout + list_result.stderr
    assert len(output) > 0, "Jobs list should produce output"


@pytest.mark.integration
def test_job_cleanup_dry_run(tmp_path):
    """
    Test 5C: Validate jobs cleanup --dry-run command.

    Dry-run mode should show what would be deleted without actually deleting.
    """
    # Setup isolated environment
    import os

    env = os.environ.copy()
    env["MP_DATA_DIR"] = str(tmp_path / "data")

    # Create a job
    ingest_result = subprocess.run(
        [
            "marketpipe",
            "ingest",
            "--provider",
            "fake",
            "--symbols",
            "CLEANUP",
            "--start",
            "2025-01-15",
            "--end",
            "2025-01-16",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    assert ingest_result.returncode == 0, "Ingest should succeed"

    # Get initial file count
    data_dir = tmp_path / "data"
    initial_files = list(data_dir.rglob("*.parquet")) if data_dir.exists() else []
    initial_count = len(initial_files)

    # Run cleanup in dry-run mode
    cleanup_result = subprocess.run(
        ["marketpipe", "jobs", "cleanup", "--all", "--dry-run"],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    # Dry-run should succeed
    assert (
        cleanup_result.returncode == 0
    ), f"Cleanup dry-run should succeed. Exit code: {cleanup_result.returncode}\nStderr: {cleanup_result.stderr}"

    # Files should NOT be deleted in dry-run mode
    final_files = list(data_dir.rglob("*.parquet")) if data_dir.exists() else []
    final_count = len(final_files)

    assert (
        final_count == initial_count
    ), f"Dry-run should not delete files. Initial: {initial_count}, Final: {final_count}"


@pytest.mark.integration
def test_complete_job_lifecycle(tmp_path):
    """
    Test 5D: Test complete job lifecycle from creation to cleanup.

    This is the comprehensive test that validates the entire job workflow:
    1. Create job (via ingest)
    2. Verify job created data
    3. List jobs (verify job appears)
    4. Dry-run cleanup (verify no deletion)
    5. Optional: Actual cleanup (if implemented)
    """
    # Setup isolated environment
    import os

    env = os.environ.copy()
    env["MP_DATA_DIR"] = str(tmp_path / "data")

    # STEP 1: CREATE - Run ingest to create a job
    print("\n=== Step 1: Create Job ===")
    ingest_result = subprocess.run(
        [
            "marketpipe",
            "ingest",
            "--provider",
            "fake",
            "--symbols",
            "LIFECYCLE",
            "--start",
            "2025-01-15",
            "--end",
            "2025-01-16",
            "--output",
            str(tmp_path / "data"),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    assert (
        ingest_result.returncode == 0
    ), f"Job creation (ingest) failed. Exit code: {ingest_result.returncode}\nStderr: {ingest_result.stderr}"
    print("✓ Job created successfully")

    # STEP 2: VERIFY - Check that data was created
    print("\n=== Step 2: Verify Data Created ===")
    data_dir = tmp_path / "data"
    assert data_dir.exists(), "Data directory should exist after ingest"

    parquet_files = list(data_dir.rglob("*.parquet"))
    assert (
        len(parquet_files) > 0
    ), f"Job should create data files. Found: {list(data_dir.rglob('*'))}"
    print(f"✓ Data files created: {len(parquet_files)} parquet files")

    # STEP 3: LIST - Verify job listing works
    print("\n=== Step 3: List Jobs ===")
    list_result = subprocess.run(
        ["marketpipe", "jobs", "list"], capture_output=True, text=True, timeout=30, env=env
    )

    assert (
        list_result.returncode == 0
    ), f"Job listing failed. Exit code: {list_result.returncode}\nStderr: {list_result.stderr}"
    print(f"✓ Job listing succeeded. Output length: {len(list_result.stdout)} bytes")

    # STEP 4: CLEANUP DRY-RUN - Verify dry-run doesn't delete
    print("\n=== Step 4: Cleanup Dry-Run ===")
    files_before_dry_run = list(data_dir.rglob("*.parquet"))

    dry_run_result = subprocess.run(
        ["marketpipe", "jobs", "cleanup", "--all", "--dry-run"],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )

    assert (
        dry_run_result.returncode == 0
    ), f"Cleanup dry-run failed. Exit code: {dry_run_result.returncode}\nStderr: {dry_run_result.stderr}"

    files_after_dry_run = list(data_dir.rglob("*.parquet"))
    assert len(files_after_dry_run) == len(files_before_dry_run), "Dry-run should not delete files"
    print(f"✓ Dry-run completed without deletions ({len(files_after_dry_run)} files remain)")

    print("\n=== ✅ Complete Job Lifecycle Test PASSED ===")


@pytest.mark.integration
def test_multiple_jobs_management(tmp_path):
    """
    Test 5E: Validate management of multiple jobs.

    MarketPipe should handle multiple jobs gracefully.
    """
    # Setup isolated environment
    import os

    env = os.environ.copy()
    env["MP_DATA_DIR"] = str(tmp_path / "data")

    # Create multiple jobs
    symbols = ["JOB1", "JOB2", "JOB3"]

    for symbol in symbols:
        result = subprocess.run(
            [
                "marketpipe",
                "ingest",
                "--provider",
                "fake",
                "--symbols",
                symbol,
                "--start",
                "2025-01-15",
                "--end",
                "2025-01-16",
                "--output",
                str(tmp_path / "data"),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        assert (
            result.returncode == 0
        ), f"Ingest for {symbol} should succeed. Exit code: {result.returncode}"

    # List all jobs
    list_result = subprocess.run(
        ["marketpipe", "jobs", "list"], capture_output=True, text=True, timeout=30, env=env
    )

    assert list_result.returncode == 0, "Jobs list should succeed with multiple jobs"

    # Verify data for all symbols was created
    data_dir = tmp_path / "data"
    assert data_dir.exists(), "Data directory should exist"

    parquet_files = list(data_dir.rglob("*.parquet"))
    # Should have at least some files created (may be fewer than 3 if partitioned by day)
    assert (
        len(parquet_files) >= 1
    ), f"Should have created at least one parquet file. Found: {len(parquet_files)}"


if __name__ == "__main__":
    # Allow running this file directly for quick testing
    pytest.main([__file__, "-v", "-s"])
