# SPDX-License-Identifier: Apache-2.0
"""Infrastructure layer for messaging and event bus implementations.

This package contains concrete implementations for event buses and messaging
infrastructure, keeping these concerns separate from the domain layer.
"""

from __future__ import annotations

__all__ = ["in_memory_bus"]

from . import in_memory_bus 