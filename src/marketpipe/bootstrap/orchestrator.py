# SPDX-License-Identifier: Apache-2.0
"""Bootstrap orchestrator with dependency injection."""

from __future__ import annotations
from typing import Optional

import logging
import threading

from .interfaces import BootstrapResult, IEnvironmentProvider, IMigrationService, IServiceRegistry

logger = logging.getLogger(__name__)


class BootstrapOrchestrator:
    """Orchestrates MarketPipe bootstrap process with dependency injection.

    This class replaces the monolithic bootstrap() function with a configurable
    orchestrator that accepts injected dependencies, making it much easier to test.
    """

    def __init__(
        self,
        migration_service: IMigrationService,
        service_registry: IServiceRegistry,
        environment_provider: IEnvironmentProvider,
    ):
        """Initialize bootstrap orchestrator.

        Args:
            migration_service: Service for applying database migrations
            service_registry: Service for registering application services
            environment_provider: Provider for environment configuration
        """
        self.migration_service = migration_service
        self.service_registry = service_registry
        self.environment_provider = environment_provider

        # Thread-safe bootstrap state
        self._bootstrapped = False
        self._bootstrap_lock = threading.Lock()

    def bootstrap(self) -> BootstrapResult:
        """Execute bootstrap process.

        This method is idempotent and thread-safe. It will only perform
        initialization once per orchestrator instance.

        Returns:
            BootstrapResult: Result of bootstrap operation including success status
                           and list of registered services
        """
        with self._bootstrap_lock:
            # Check if already bootstrapped
            if self._bootstrapped:
                logger.debug("Bootstrap already completed, skipping")
                return BootstrapResult(success=True, was_already_bootstrapped=True)

            logger.info("Starting MarketPipe bootstrap initialization...")

            try:
                services_registered = []

                # Step 1: Apply database migrations
                logger.debug("Applying database migrations")
                db_path = self.environment_provider.get_database_path()
                logger.debug(f"Database path: {db_path}")
                self.migration_service.apply_migrations(db_path)
                services_registered.append("database_migrations")

                # Step 2: Register validation service
                logger.debug("Registering validation service")
                self.service_registry.register_validation_service()
                services_registered.append("validation_service")

                # Step 3: Register aggregation service
                logger.debug("Registering aggregation service")
                self.service_registry.register_aggregation_service()
                services_registered.append("aggregation_service")

                # Step 4: Register monitoring handlers
                logger.debug("Registering monitoring event handlers")
                self.service_registry.register_monitoring_handlers()
                services_registered.append("monitoring_handlers")

                # Step 5: Register logging handlers
                logger.debug("Registering logging event handlers")
                self.service_registry.register_logging_handlers()
                services_registered.append("logging_handlers")

                # Mark as bootstrapped
                self._bootstrapped = True
                logger.info("MarketPipe bootstrap completed successfully")

                return BootstrapResult(success=True, services_registered=services_registered)

            except Exception as e:
                logger.error(f"Bootstrap failed: {e}")
                error_msg = f"MarketPipe bootstrap failed: {e}"

                return BootstrapResult(success=False, error_message=error_msg)

    def is_bootstrapped(self) -> bool:
        """Check if bootstrap has been completed.

        Returns:
            True if bootstrap() has been called successfully, False otherwise.
        """
        with self._bootstrap_lock:
            return self._bootstrapped

    def reset_bootstrap_state(self) -> None:
        """Reset bootstrap state for testing purposes.

        This should only be used in tests to reset the state.
        """
        with self._bootstrap_lock:
            self._bootstrapped = False
        logger.debug("Bootstrap state reset for testing")


# Global orchestrator instance for backward compatibility
_global_orchestrator: Optional[BootstrapOrchestrator] = None
_global_lock = threading.Lock()


def get_global_orchestrator() -> BootstrapOrchestrator:
    """Get or create the global bootstrap orchestrator instance.

    Returns:
        BootstrapOrchestrator: Global singleton instance
    """
    global _global_orchestrator

    if _global_orchestrator is None:
        with _global_lock:
            if _global_orchestrator is None:
                # Create with default implementations
                from .interfaces import (
                    AlembicMigrationService,
                    EnvironmentProvider,
                    MarketPipeServiceRegistry,
                )

                _global_orchestrator = BootstrapOrchestrator(
                    migration_service=AlembicMigrationService(),
                    service_registry=MarketPipeServiceRegistry(),
                    environment_provider=EnvironmentProvider(),
                )

    return _global_orchestrator


def set_global_orchestrator(orchestrator: BootstrapOrchestrator) -> None:
    """Set the global orchestrator instance (for testing).

    Args:
        orchestrator: Orchestrator instance to use globally
    """
    global _global_orchestrator
    with _global_lock:
        _global_orchestrator = orchestrator
