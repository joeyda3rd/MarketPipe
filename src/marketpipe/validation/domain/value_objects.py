# SPDX-License-Identifier: Apache-2.0
"""Validation domain value objects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BarError:
    """Represents a validation error for a specific bar."""

    ts_ns: int
    reason: str


@dataclass(frozen=True)
class ValidationResult:
    """Result of validating a collection of bars for a symbol."""

    symbol: str
    total: int
    errors: list[BarError]

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return not self.errors
