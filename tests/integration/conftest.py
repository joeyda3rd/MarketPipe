# SPDX-License-Identifier: Apache-2.0
"""Integration tests keep default database and output paths in temporary storage."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def isolated_working_directory(tmp_path, monkeypatch):
    """Give each test its own relative paths instead of changing developer data."""
    monkeypatch.chdir(tmp_path)
