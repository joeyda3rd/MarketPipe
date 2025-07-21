# SPDX-License-Identifier: Apache-2.0
"""Fake bootstrap services for testing."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock

from marketpipe.bootstrap.interfaces import (
    IEnvironmentProvider,
    IMigrationService,
    IServiceRegistry,
)


class FakeMigrationService(IMigrationService):
    """Fake migration service that records operations instead of performing them."""

    def __init__(self):
        self.migrations_applied: List[Path] = []
        self.should_fail = False
        self.failure_message = "Simulated migration failure"

    def apply_migrations(self, db_path: Path) -> None:
        """Record migration attempt."""
        if self.should_fail:
            raise RuntimeError(self.failure_message)

        self.migrations_applied.append(db_path)

    def configure_failure(self, should_fail: bool = True, message: str = "Simulated migration failure"):
        """Configure migration to fail."""
        self.should_fail = should_fail
        self.failure_message = message

    def get_migrations_applied(self) -> List[Path]:
        """Get list of database paths that migrations were applied to."""
        return self.migrations_applied.copy()

    def clear_history(self):
        """Clear migration history."""
        self.migrations_applied.clear()


class FakeServiceRegistry(IServiceRegistry):
    """Fake service registry that records registrations instead of performing them."""

    def __init__(self):
        self.services_registered: List[str] = []
        self.failing_services: set[str] = set()
        self.failure_messages: Dict[str, str] = {}

    def register_validation_service(self) -> None:
        """Record validation service registration."""
        service_name = "validation_service"
        if service_name in self.failing_services:
            raise RuntimeError(self.failure_messages.get(service_name, "Validation service registration failed"))
        self.services_registered.append(service_name)

    def register_aggregation_service(self) -> None:
        """Record aggregation service registration."""
        service_name = "aggregation_service"
        if service_name in self.failing_services:
            raise RuntimeError(self.failure_messages.get(service_name, "Aggregation service registration failed"))
        self.services_registered.append(service_name)

    def register_monitoring_handlers(self) -> None:
        """Record monitoring handlers registration."""
        service_name = "monitoring_handlers"
        if service_name in self.failing_services:
            raise RuntimeError(self.failure_messages.get(service_name, "Monitoring handlers registration failed"))
        self.services_registered.append(service_name)

    def register_logging_handlers(self) -> None:
        """Record logging handlers registration."""
        service_name = "logging_handlers"
        if service_name in self.failing_services:
            raise RuntimeError(self.failure_messages.get(service_name, "Logging handlers registration failed"))
        self.services_registered.append(service_name)

    def configure_service_failure(self, service_name: str, should_fail: bool = True, message: str = None):
        """Configure specific service to fail registration."""
        if should_fail:
            self.failing_services.add(service_name)
            if message:
                self.failure_messages[service_name] = message
        else:
            self.failing_services.discard(service_name)
            self.failure_messages.pop(service_name, None)

    def get_registered_services(self) -> List[str]:
        """Get list of services that were registered."""
        return self.services_registered.copy()

    def clear_history(self):
        """Clear registration history."""
        self.services_registered.clear()


class FakeEnvironmentProvider(IEnvironmentProvider):
    """Fake environment provider with configurable values."""

    def __init__(self, database_path: Path = None, config_values: Dict[str, Any] = None):
        self.database_path = database_path or Path("test_db.db")
        self.config_values = config_values or {}

    def get_database_path(self) -> Path:
        """Get configured database path."""
        return self.database_path

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return self.config_values.get(key, default)

    def set_database_path(self, path: Path):
        """Configure database path."""
        self.database_path = path

    def set_config_value(self, key: str, value: Any):
        """Set configuration value."""
        self.config_values[key] = value

    def clear_config(self):
        """Clear all configuration values."""
        self.config_values.clear()


# Helper functions for creating test orchestrators

def create_fake_bootstrap_orchestrator(
    database_path: Path = None,
    migration_should_fail: bool = False,
    failing_services: List[str] = None
):
    """Create a BootstrapOrchestrator with fake services for testing.

    Args:
        database_path: Path to use for database (defaults to test_db.db)
        migration_should_fail: Whether migration should fail
        failing_services: List of service names that should fail registration

    Returns:
        BootstrapOrchestrator configured with fake services
    """
    from marketpipe.bootstrap import BootstrapOrchestrator

    # Create fake services
    migration_service = FakeMigrationService()
    service_registry = FakeServiceRegistry()
    environment_provider = FakeEnvironmentProvider(database_path)

    # Configure failures if requested
    if migration_should_fail:
        migration_service.configure_failure()

    if failing_services:
        for service_name in failing_services:
            service_registry.configure_service_failure(service_name)

    return BootstrapOrchestrator(
        migration_service=migration_service,
        service_registry=service_registry,
        environment_provider=environment_provider,
    )


def get_fake_services_from_orchestrator(orchestrator):
    """Extract fake services from orchestrator for verification.

    Args:
        orchestrator: BootstrapOrchestrator with fake services

    Returns:
        Dict containing the fake services keyed by type
    """
    return {
        'migration_service': orchestrator.migration_service,
        'service_registry': orchestrator.service_registry,
        'environment_provider': orchestrator.environment_provider,
    }
