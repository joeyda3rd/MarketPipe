# SPDX-License-Identifier: Apache-2.0
"""Domain event handlers for business logic.

This module contains pure domain event handlers that implement business rules
and domain logic without any infrastructure concerns.
"""

from __future__ import annotations

# Currently no pure domain event handlers are needed.
# All logging and metrics handlers have been moved to infrastructure layer.
# Future domain event handlers should contain only business logic.

# Explicitly type __all__ for mypy
__all__: list[str] = []
