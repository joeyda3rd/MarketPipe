# SPDX-License-Identifier: Apache-2.0
"""Bootstrap module for MarketPipe initialization.

This module handles database migrations and service registration that previously
ran at import time. Bootstrap is now lazily invoked by CLI commands to avoid
side-effects when importing the CLI module for help text or testing.
"""

from __future__ import annotations

import os
from pathlib import Path
import logging
import threading

# Global flag to ensure bootstrap only runs once per process
_BOOTSTRAPPED = False
_BOOTSTRAP_LOCK = threading.Lock()

logger = logging.getLogger(__name__)


def bootstrap() -> None:
    """Initialize MarketPipe with database migrations and service registration.
    
    This function is idempotent and safe to call multiple times. It will only
    perform initialization once per process.
    
    Performs:
    1. Database migrations on the core database
    2. Service registrations for validation and aggregation services
    """
    global _BOOTSTRAPPED
    
    # Use single lock for thread safety - all checks and modifications inside lock
    with _BOOTSTRAP_LOCK:
        # Check if already bootstrapped inside the lock
        if _BOOTSTRAPPED:
            logger.debug("Bootstrap already completed, skipping")
            return
        
        logger.info("Starting MarketPipe bootstrap initialization...")
        
        try:
            # Apply database migrations
            from marketpipe.migrations import apply_pending
            
            db_path = Path(os.getenv("MP_DB", "data/db/core.db"))
            logger.debug(f"Applying database migrations to: {db_path}")
            apply_pending(db_path)
            
            # Register validation service
            logger.debug("Registering validation service")
            from marketpipe.validation import ValidationRunnerService
            ValidationRunnerService.register()
            
            # Register aggregation service
            logger.debug("Registering aggregation service")
            from marketpipe.aggregation import AggregationRunnerService
            AggregationRunnerService.register()
            
            # Mark as bootstrapped
            _BOOTSTRAPPED = True
            logger.info("MarketPipe bootstrap completed successfully")
            
        except Exception as e:
            logger.error(f"Bootstrap failed: {e}")
            raise RuntimeError(f"MarketPipe bootstrap failed: {e}") from e


def is_bootstrapped() -> bool:
    """Check if bootstrap has been completed.
    
    Returns:
        True if bootstrap() has been called successfully, False otherwise.
    """
    return _BOOTSTRAPPED


def reset_bootstrap_state() -> None:
    """Reset bootstrap state for testing purposes.
    
    This should only be used in tests to reset the global state.
    """
    global _BOOTSTRAPPED
    with _BOOTSTRAP_LOCK:
        _BOOTSTRAPPED = False
    logger.debug("Bootstrap state reset for testing") 