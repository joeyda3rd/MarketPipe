# SPDX-License-Identifier: Apache-2.0
"""Integration fixtures must not modify databases in the invoking directory."""

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def test_integration_tests_isolate_default_paths_and_preserve_existing_jobs(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    jobs_db = data_dir / "ingestion_jobs.db"
    with sqlite3.connect(jobs_db) as connection:
        connection.execute("CREATE TABLE ingestion_jobs (state TEXT)")
        connection.execute("INSERT INTO ingestion_jobs VALUES ('IN_PROGRESS')")

    integration_fixtures = Path(__file__).parents[1] / "integration" / "conftest.py"
    (tmp_path / "conftest.py").write_text(integration_fixtures.read_text())
    (tmp_path / "test_isolated_paths.py").write_text(
        "from pathlib import Path\n"
        "def test_first():\n"
        "    assert not Path('data/ingestion_jobs.db').exists()\n"
        "    Path('created_by_first_test').touch()\n"
        "def test_second():\n"
        "    assert not Path('created_by_first_test').exists()\n"
        "    assert not Path('data/ingestion_jobs.db').exists()\n"
    )

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "test_isolated_paths.py"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    with sqlite3.connect(jobs_db) as connection:
        assert connection.execute("SELECT state FROM ingestion_jobs").fetchone() == ("IN_PROGRESS",)
