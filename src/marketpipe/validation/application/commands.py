"""Validation application commands."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidateJobCommand:
    """Command to validate an ingestion job."""
    job_id: str 