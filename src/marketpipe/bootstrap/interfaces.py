# SPDX-License-Identifier: Apache-2.0
"""Bootstrap service interfaces for dependency injection."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class BootstrapResult:
    """Result of bootstrap operation."""

    success: bool
    was_already_bootstrapped: bool = False
    error_message: str | None = None
    services_registered: list[str] = None

    def __post_init__(self):
        if self.services_registered is None:
            self.services_registered = []


class IMigrationService(ABC):
    """Interface for database migration operations."""

    @abstractmethod
    def apply_migrations(self, db_path: Path) -> None:
        """Apply pending database migrations.

        Args:
            db_path: Path to database file

        Raises:
            RuntimeError: If migration fails
        """
        pass


class IServiceRegistry(ABC):
    """Interface for service registration operations."""

    @abstractmethod
    def register_validation_service(self) -> None:
        """Register validation service."""
        pass

    @abstractmethod
    def register_aggregation_service(self) -> None:
        """Register aggregation service."""
        pass

    @abstractmethod
    def register_monitoring_handlers(self) -> None:
        """Register monitoring event handlers."""
        pass

    @abstractmethod
    def register_logging_handlers(self) -> None:
        """Register logging event handlers."""
        pass


class IEnvironmentProvider(ABC):
    """Interface for environment configuration access."""

    @abstractmethod
    def get_database_path(self) -> Path:
        """Get configured database path."""
        pass

    @abstractmethod
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        pass


# Concrete implementations


class AlembicMigrationService(IMigrationService):
    """Alembic-based migration service."""

    def apply_migrations(self, db_path: Path) -> None:
        """Apply Alembic migrations."""
        from marketpipe.bootstrap import apply_pending_alembic

        apply_pending_alembic(db_path)


class MarketPipeServiceRegistry(IServiceRegistry):
    """MarketPipe service registry implementation."""

    def register_validation_service(self) -> None:
        """Register validation service."""
        from marketpipe.validation import ValidationRunnerService

        ValidationRunnerService.register()

    def register_aggregation_service(self) -> None:
        """Register aggregation service."""
        from marketpipe.aggregation import AggregationRunnerService

        AggregationRunnerService.register()

    def register_monitoring_handlers(self) -> None:
        """Register monitoring event handlers."""
        from marketpipe.infrastructure.monitoring.event_handlers import register

        register()

    def register_logging_handlers(self) -> None:
        """Register logging event handlers."""
        from marketpipe.infrastructure.monitoring.domain_event_handlers import (
            register_logging_handlers,
        )

        register_logging_handlers()


class EnvironmentProvider(IEnvironmentProvider):
    """Environment-based configuration provider."""

    def __init__(self):
        import os

        self._env = os.environ

    def get_database_path(self) -> Path:
        """Get database path from MP_DB environment variable."""
        return Path(self._env.get("MP_DB", "data/db/core.db"))

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get configuration value from environment."""
        return self._env.get(key, default)
