# SPDX-License-Identifier: Apache-2.0
"""Bootstrap module for MarketPipe initialization.

This module handles database migrations and service registration that previously
ran at import time. Bootstrap is now lazily invoked by CLI commands to avoid
side-effects when importing the CLI module for help text or testing.
"""

from __future__ import annotations

__all__ = ["apply_pending_alembic", "apply_pending", "bootstrap", "is_bootstrapped", "reset_bootstrap_state", "get_event_bus"]

import os
from pathlib import Path
import logging
import threading
from typing import TYPE_CHECKING

from alembic import command
from alembic.config import Config

if TYPE_CHECKING:
    from marketpipe.domain.events import IEventBus

# Global flag to ensure bootstrap only runs once per process
_BOOTSTRAPPED = False
_BOOTSTRAP_LOCK = threading.Lock()

# Global event bus instance
_EVENT_BUS: "IEventBus" | None = None

logger = logging.getLogger(__name__)


def apply_pending_alembic(db_path: Path) -> None:
    """Apply pending Alembic migrations to the database.
    
    Args:
        db_path: Path to SQLite database file
    """
    # Ensure database directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Locate the Alembic configuration file relative to the project root
    # to ensure migrations work even if the current working directory is
    # changed by tests or CLI invocations.
    project_root = Path(__file__).resolve().parent.parent.parent  # src/marketpipe → project root
    alembic_ini = project_root / "alembic.ini"

    if not alembic_ini.exists():
        raise FileNotFoundError(
            "Alembic configuration file 'alembic.ini' not found at expected "
            f"location: {alembic_ini}"
        )

    # Set up Alembic configuration using the resolved path
    alembic_cfg = Config(str(alembic_ini))
    
    # Convert path to SQLite URL format (always use 3 slashes for SQLite)
    database_url = f"sqlite:///{db_path.absolute()}"
    
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    
    try:
        # Run migrations to head
        command.upgrade(alembic_cfg, "head")
        logger.info("Alembic migrations completed successfully")
    except Exception as e:
        logger.error(f"Alembic migration failed: {e}")
        raise RuntimeError(f"Database migration failed: {e}") from e


# Legacy function for backward compatibility
def apply_pending(db_path: Path) -> None:
    """Legacy migration function - now uses Alembic.
    
    This function is kept for backward compatibility and now delegates to Alembic.
    """
    logger.warning("apply_pending() is deprecated, using Alembic migrations")
    apply_pending_alembic(db_path)


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
            # Apply database migrations using Alembic
            db_path = Path(os.getenv("MP_DB", "data/db/core.db"))
            logger.debug(f"Applying database migrations to: {db_path}")
            apply_pending_alembic(db_path)
            
            # Register validation service
            logger.debug("Registering validation service")
            from marketpipe.validation import ValidationRunnerService
            ValidationRunnerService.register()
            
            # Register aggregation service
            logger.debug("Registering aggregation service")
            from marketpipe.aggregation import AggregationRunnerService
            AggregationRunnerService.register()
            
            # Register monitoring event handlers
            logger.debug("Registering monitoring event handlers")
            from marketpipe.infrastructure.monitoring.event_handlers import register
            register()
            
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


def get_event_bus() -> "IEventBus":
    """Get the global event bus instance.
    
    Returns a singleton instance of the event bus that can be used throughout
    the application. The event bus is created lazily on first access.
    
    Returns:
        IEventBus: The global event bus instance
    """
    global _EVENT_BUS
    if _EVENT_BUS is None:
        from marketpipe.infrastructure.messaging.in_memory_bus import InMemoryEventBus
        _EVENT_BUS = InMemoryEventBus()
    return _EVENT_BUS