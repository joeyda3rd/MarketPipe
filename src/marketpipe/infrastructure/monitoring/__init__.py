# SPDX-License-Identifier: Apache-2.0
"""Infrastructure layer for monitoring and metrics collection.

This package contains concrete implementations for metrics collection
and monitoring infrastructure, keeping these concerns separate from
the domain layer.
"""

from __future__ import annotations

__all__ = ["event_handlers"]

from . import event_handlers
