# SPDX-License-Identifier: Apache-2.0
"""Bootstrap package for MarketPipe initialization with dependency injection."""

from __future__ import annotations

import os

# Import legacy functions from the original bootstrap module
import sys

# Export main interfaces
from .interfaces import (  # Concrete implementations
    AlembicMigrationService,
    BootstrapResult,
    EnvironmentProvider,
    IEnvironmentProvider,
    IMigrationService,
    IServiceRegistry,
    MarketPipeServiceRegistry,
)

# Export orchestrator
from .orchestrator import BootstrapOrchestrator, get_global_orchestrator, set_global_orchestrator

sys.path.insert(0, os.path.dirname(__file__))
try:
    # Import bootstrap.py as a module, not the package
    import importlib.util

    bootstrap_module_path = os.path.join(os.path.dirname(__file__), "..", "bootstrap.py")
    spec = importlib.util.spec_from_file_location("legacy_bootstrap", bootstrap_module_path)
    legacy_bootstrap = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(legacy_bootstrap)
    apply_pending_alembic = legacy_bootstrap.apply_pending_alembic
    reset_bootstrap_state = legacy_bootstrap.reset_bootstrap_state
    is_bootstrapped = legacy_bootstrap.is_bootstrapped
    bootstrap = legacy_bootstrap.bootstrap
    get_event_bus = getattr(legacy_bootstrap, "get_event_bus", lambda: None)
except Exception:
    # Fallback - define a simple version
    def apply_pending_alembic(db_path):
        from pathlib import Path

        from alembic import command
        from alembic.config import Config

        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Get alembic directory
        project_root = Path(__file__).parent.parent.parent.parent
        alembic_dir = project_root / "alembic"
        alembic_ini_path = project_root / "alembic.ini"

        if alembic_ini_path.exists():
            alembic_cfg = Config(str(alembic_ini_path))
            alembic_cfg.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
            command.upgrade(alembic_cfg, "head")

    # Simple fallback implementations
    _bootstrapped = False

    def reset_bootstrap_state():
        global _bootstrapped
        _bootstrapped = False

    def is_bootstrapped():
        return _bootstrapped

    def bootstrap():
        """Simple fallback bootstrap implementation."""
        global _bootstrapped
        _bootstrapped = True

    def get_event_bus():
        """Simple fallback get_event_bus implementation."""
        return None


__all__ = [
    # Interfaces
    "BootstrapResult",
    "IMigrationService",
    "IServiceRegistry",
    "IEnvironmentProvider",
    # Concrete implementations
    "AlembicMigrationService",
    "MarketPipeServiceRegistry",
    "EnvironmentProvider",
    # Orchestrator
    "BootstrapOrchestrator",
    "get_global_orchestrator",
    "set_global_orchestrator",
    # Legacy functions
    "apply_pending_alembic",
    "reset_bootstrap_state",
    "is_bootstrapped",
    "bootstrap",
    "get_event_bus",
]
