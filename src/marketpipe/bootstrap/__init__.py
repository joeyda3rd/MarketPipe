# SPDX-License-Identifier: Apache-2.0
"""Bootstrap package for MarketPipe initialization with dependency injection."""

from __future__ import annotations

# Export main interfaces
from .interfaces import (
    BootstrapResult,
    IEnvironmentProvider,
    IMigrationService,
    IServiceRegistry,
    # Concrete implementations
    AlembicMigrationService,
    EnvironmentProvider,
    MarketPipeServiceRegistry,
)

# Export orchestrator
from .orchestrator import (
    BootstrapOrchestrator,
    get_global_orchestrator,
    set_global_orchestrator,
)

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
] 