# SPDX-License-Identifier: Apache-2.0
"""DEPRECATED: Legacy metrics event handlers module.

This module has been deprecated and moved to the infrastructure layer.
Use marketpipe.infrastructure.monitoring.event_handlers instead.

This file provides backward compatibility by forwarding to the new module.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "marketpipe.metrics_event_handlers is deprecated. "
    "Use marketpipe.infrastructure.monitoring.event_handlers instead.",
    DeprecationWarning,
    stacklevel=2
)

# Forward to new implementation for backward compatibility
from marketpipe.infrastructure.monitoring.event_handlers import register as setup_metrics_event_handlers

# Legacy function alias for backward compatibility
def setup_metrics_event_handlers_deprecated() -> None:
    """DEPRECATED: Use marketpipe.infrastructure.monitoring.event_handlers.register() instead."""
    warnings.warn(
        "setup_metrics_event_handlers() is deprecated. "
        "Use bootstrap() which now automatically registers monitoring handlers.",
        DeprecationWarning,
        stacklevel=2
    )
    setup_metrics_event_handlers()

# NOTE: Auto-registration removed - now handled by bootstrap.py
