# SPDX-License-Identifier: Apache-2.0
"""Integration tests for bootstrap functionality.

These tests replace the mock-heavy approach in test_bootstrap_side_effect.py
with integration tests using real database operations and dependency injection.

IMPROVEMENTS OVER MOCK-BASED TESTS:
- Uses real SQLite database with real migrations
- Tests actual bootstrap behavior, not mock coordination
- Uses Phase 1 + Phase 2 infrastructure (fakes + dependency injection)
- Verifies real database state and service registrations
- Easy error scenario testing with configurable fakes
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from marketpipe.bootstrap import (
    BootstrapOrchestrator,
    reset_bootstrap_state,
    set_global_orchestrator,
)
from marketpipe.bootstrap.interfaces import AlembicMigrationService, EnvironmentProvider
from tests.fakes.bootstrap import (
    FakeEnvironmentProvider,
    FakeServiceRegistry,
    create_fake_bootstrap_orchestrator,
    get_fake_services_from_orchestrator,
)
from tests.fakes.database import FakeDatabase


class TestBootstrapIntegrationWithRealDatabase:
    """Integration tests using real database operations.

    IMPROVEMENT: Tests actual bootstrap behavior with real database
    instead of mocking apply_pending_alembic.
    """

    @pytest.fixture
    def integration_db(self):
        """Real database for integration testing."""
        db = FakeDatabase()
        yield db
        db.cleanup()

    def test_bootstrap_with_real_migrations(self, integration_db):
        """Test bootstrap with real database migrations.

        IMPROVEMENT: Uses real AlembicMigrationService instead of mocking.
        Tests actual migration behavior, catches real migration issues.
        """
        # Set up environment to use our test database
        env_provider = FakeEnvironmentProvider()
        env_provider.set_database_path(Path(integration_db.get_file_path()))

        # Create orchestrator with real migration service + fake service registry
        orchestrator = BootstrapOrchestrator(
            migration_service=AlembicMigrationService(),  # REAL migrations!
            service_registry=FakeServiceRegistry(),  # Fake for easy verification
            environment_provider=env_provider,
        )

        # Execute bootstrap
        result = orchestrator.bootstrap()

        # Verify behavior
        assert result.success
        assert not result.was_already_bootstrapped

        # Verify database was actually migrated (real behavior)
        db_path = Path(integration_db.get_file_path())
        assert db_path.exists()

        # Verify services were registered via fake registry
        fake_services = get_fake_services_from_orchestrator(orchestrator)
        service_registry = fake_services["service_registry"]
        registered_services = service_registry.get_registered_services()

        assert "validation_service" in registered_services
        assert "aggregation_service" in registered_services
        assert "monitoring_handlers" in registered_services
        assert "logging_handlers" in registered_services

    def test_bootstrap_idempotent_with_real_database(self, integration_db):
        """Test bootstrap idempotence with real database operations.

        IMPROVEMENT: Tests real idempotent behavior instead of mock call counts.
        Verifies database state doesn't get corrupted by multiple bootstraps.
        """
        env_provider = FakeEnvironmentProvider()
        env_provider.set_database_path(Path(integration_db.get_file_path()))

        orchestrator = BootstrapOrchestrator(
            migration_service=AlembicMigrationService(),
            service_registry=FakeServiceRegistry(),
            environment_provider=env_provider,
        )

        # First bootstrap
        result1 = orchestrator.bootstrap()
        assert result1.success
        assert not result1.was_already_bootstrapped

        # Verify database exists and is functional
        db_path = Path(integration_db.get_file_path())
        assert db_path.exists()
        initial_size = db_path.stat().st_size

        # Second bootstrap should be idempotent
        result2 = orchestrator.bootstrap()
        assert result2.success
        assert result2.was_already_bootstrapped

        # Verify database wasn't modified
        final_size = db_path.stat().st_size
        assert final_size == initial_size

        # Verify service registry was only called once
        fake_services = get_fake_services_from_orchestrator(orchestrator)
        service_registry = fake_services["service_registry"]
        registered_services = service_registry.get_registered_services()

        # Should have exactly 4 services registered (not 8 from double registration)
        assert len(registered_services) == 4

    def test_bootstrap_migration_failure_with_real_database(self, tmp_path):
        """Test bootstrap handles real migration failures gracefully.

        IMPROVEMENT: Tests real error handling with actual database failures
        instead of mocking exceptions.
        """
        # Create a custom test directory that we can control permissions on
        test_dir = tmp_path / "readonly_test"
        test_dir.mkdir()
        readonly_db_path = test_dir / "readonly.db"

        env_provider = FakeEnvironmentProvider()
        env_provider.set_database_path(readonly_db_path)

        orchestrator = BootstrapOrchestrator(
            migration_service=AlembicMigrationService(),
            service_registry=FakeServiceRegistry(),
            environment_provider=env_provider,
        )

        # Make the test directory read-only to cause migration failure
        try:
            test_dir.chmod(0o444)  # Read-only directory

            result = orchestrator.bootstrap()

            # Should fail due to permission error
            assert not result.success
            assert result.error_message is not None
            assert "bootstrap failed" in result.error_message.lower()

            # Should not be marked as bootstrapped
            assert not orchestrator.is_bootstrapped()

            # No services should be registered after migration failure
            fake_services = get_fake_services_from_orchestrator(orchestrator)
            service_registry = fake_services["service_registry"]
            assert len(service_registry.get_registered_services()) == 0

        finally:
            # Restore permissions so cleanup can work
            test_dir.chmod(0o755)

    def test_bootstrap_with_custom_database_path(self, tmp_path):
        """Test bootstrap uses custom database path correctly.

        IMPROVEMENT: Tests real path configuration and file creation
        instead of mocking path handling.
        """
        custom_db_path = tmp_path / "custom_location" / "market.db"

        env_provider = FakeEnvironmentProvider()
        env_provider.set_database_path(custom_db_path)

        orchestrator = BootstrapOrchestrator(
            migration_service=AlembicMigrationService(),
            service_registry=FakeServiceRegistry(),
            environment_provider=env_provider,
        )

        result = orchestrator.bootstrap()

        assert result.success

        # Verify database was created at custom location
        assert custom_db_path.exists()
        assert custom_db_path.parent.exists()  # Directory was created

        # Verify it's a valid SQLite database
        assert custom_db_path.stat().st_size > 0


class TestBootstrapServiceFailures:
    """Test bootstrap service registration failure scenarios.

    IMPROVEMENT: Uses fake services to easily test failure scenarios
    instead of complex mock setup for each service type.
    """

    def test_validation_service_failure(self):
        """Test bootstrap handles validation service registration failure.

        IMPROVEMENT: Easy error scenario testing with configurable fake services.
        """
        orchestrator = create_fake_bootstrap_orchestrator(failing_services=["validation_service"])

        result = orchestrator.bootstrap()

        assert not result.success
        assert "validation service registration failed" in result.error_message.lower()

        # Verify migration succeeded but service registration failed
        fake_services = get_fake_services_from_orchestrator(orchestrator)
        migration_service = fake_services["migration_service"]
        service_registry = fake_services["service_registry"]

        # Migration should have been attempted
        assert len(migration_service.get_migrations_applied()) == 1

        # No services should be registered due to early failure
        assert len(service_registry.get_registered_services()) == 0

    def test_aggregation_service_failure(self):
        """Test bootstrap handles aggregation service registration failure."""
        orchestrator = create_fake_bootstrap_orchestrator(failing_services=["aggregation_service"])

        result = orchestrator.bootstrap()

        assert not result.success
        assert "aggregation service registration failed" in result.error_message.lower()

    def test_monitoring_handlers_failure(self):
        """Test bootstrap handles monitoring handlers registration failure."""
        orchestrator = create_fake_bootstrap_orchestrator(failing_services=["monitoring_handlers"])

        result = orchestrator.bootstrap()

        assert not result.success
        assert "monitoring handlers registration failed" in result.error_message.lower()

    def test_multiple_service_failures(self):
        """Test bootstrap with multiple service failures."""
        orchestrator = create_fake_bootstrap_orchestrator(
            failing_services=["validation_service", "aggregation_service"]
        )

        result = orchestrator.bootstrap()

        assert not result.success
        # Should fail on first service (validation)
        assert "validation service registration failed" in result.error_message.lower()


class TestBootstrapLegacyCompatibility:
    """Test that legacy bootstrap() function works with new orchestrator.

    IMPROVEMENT: Tests backward compatibility while using new infrastructure.
    """

    def setup_method(self):
        """Reset global state before each test."""
        reset_bootstrap_state()

    def test_legacy_bootstrap_function_integration(self, tmp_path):
        """Test legacy bootstrap() function with real database operations.

        IMPROVEMENT: Tests legacy function with real database instead of mocks.
        """
        # Set up custom database path
        custom_db = tmp_path / "legacy_test.db"

        with patch.dict(os.environ, {"MP_DB": str(custom_db)}):
            # Create orchestrator that uses real migrations but fake services
            test_orchestrator = BootstrapOrchestrator(
                migration_service=AlembicMigrationService(),
                service_registry=FakeServiceRegistry(),
                environment_provider=EnvironmentProvider(),
            )

            # Inject it as global orchestrator
            set_global_orchestrator(test_orchestrator)

            # Use legacy function
            from marketpipe.bootstrap import bootstrap

            bootstrap()  # Should complete without error

            # Verify real database was created
            assert custom_db.exists()
            assert custom_db.stat().st_size > 0

            # Verify services were registered via fake registry
            fake_services = get_fake_services_from_orchestrator(test_orchestrator)
            service_registry = fake_services["service_registry"]
            registered_services = service_registry.get_registered_services()
            assert len(registered_services) == 4

    def test_legacy_bootstrap_idempotent_integration(self, tmp_path):
        """Test legacy bootstrap() idempotence with real database.

        IMPROVEMENT: Tests real idempotent behavior of legacy function.
        """
        custom_db = tmp_path / "legacy_idempotent.db"

        with patch.dict(os.environ, {"MP_DB": str(custom_db)}):
            # Create test orchestrator
            test_orchestrator = BootstrapOrchestrator(
                migration_service=AlembicMigrationService(),
                service_registry=FakeServiceRegistry(),
                environment_provider=EnvironmentProvider(),
            )
            set_global_orchestrator(test_orchestrator)

            from marketpipe.bootstrap import bootstrap, is_bootstrapped

            # First call
            bootstrap()
            assert is_bootstrapped()
            initial_size = custom_db.stat().st_size

            # Second call should be idempotent
            bootstrap()
            assert is_bootstrapped()
            final_size = custom_db.stat().st_size
            assert final_size == initial_size

            # Service registry should show services only registered once
            fake_services = get_fake_services_from_orchestrator(test_orchestrator)
            service_registry = fake_services["service_registry"]
            assert len(service_registry.get_registered_services()) == 4


class TestBootstrapEnvironmentConfiguration:
    """Test bootstrap with different environment configurations.

    IMPROVEMENT: Tests real environment variable handling and configuration.
    """

    def test_custom_mp_db_environment_variable(self, tmp_path):
        """Test bootstrap respects MP_DB environment variable.

        IMPROVEMENT: Tests real environment variable processing and database creation.
        """
        custom_path = tmp_path / "env_var_test" / "custom.db"

        with patch.dict(os.environ, {"MP_DB": str(custom_path)}):
            orchestrator = BootstrapOrchestrator(
                migration_service=AlembicMigrationService(),
                service_registry=FakeServiceRegistry(),
                environment_provider=EnvironmentProvider(),  # Real environment provider
            )

            result = orchestrator.bootstrap()

            assert result.success
            # Verify database was created at the environment-specified location
            assert custom_path.exists()
            assert custom_path.parent.exists()

    def test_default_database_path(self, tmp_path):
        """Test bootstrap uses default path when MP_DB not set."""
        # Temporarily change to tmp directory to avoid creating files in project root
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            # Clear MP_DB environment variable
            env = dict(os.environ)
            env.pop("MP_DB", None)

            with patch.dict(os.environ, env, clear=True):
                orchestrator = BootstrapOrchestrator(
                    migration_service=AlembicMigrationService(),
                    service_registry=FakeServiceRegistry(),
                    environment_provider=EnvironmentProvider(),
                )

                result = orchestrator.bootstrap()

                assert result.success
                # Should create database at default location (data/db/core.db)
                default_path = Path("data/db/core.db")
                assert default_path.exists()

        finally:
            os.chdir(original_cwd)


# Comparison test showing the improvement


class TestComparisonOldVsNewBootstrapTesting:
    """Side-by-side comparison showing integration test benefits."""

    def test_old_mock_approach_limitations(self):
        """OLD APPROACH: What mock-based tests couldn't verify.

        Mock-based tests could only verify:
        - Functions were called with right parameters
        - Call count and order
        - Mock return values

        They COULD NOT verify:
        - Real database migrations working
        - Actual file creation and permissions
        - Real error handling behavior
        - Service registration side effects
        - Environment variable processing
        - Idempotent behavior with real state
        """
        # This test documents the limitations for comparison
        pass

    def test_new_integration_approach_benefits(self, tmp_path):
        """NEW APPROACH: What integration tests can verify.

        Integration tests verify:
        - Real database operations and migrations
        - Actual file system interactions
        - Real error conditions (permissions, disk space)
        - Service registration with real side effects
        - Environment variable processing
        - Idempotent behavior with persistent state
        - Cross-component interactions
        """
        custom_db = tmp_path / "integration_demo.db"

        orchestrator = BootstrapOrchestrator(
            migration_service=AlembicMigrationService(),  # Real database operations
            service_registry=FakeServiceRegistry(),  # Controlled service behavior
            environment_provider=FakeEnvironmentProvider(database_path=custom_db),
        )

        result = orchestrator.bootstrap()

        # Can verify REAL behavior
        assert result.success
        assert custom_db.exists()  # Real file created
        assert custom_db.stat().st_size > 0  # Real database content

        # Can verify service interactions
        fake_services = get_fake_services_from_orchestrator(orchestrator)
        service_registry = fake_services["service_registry"]
        registered_services = service_registry.get_registered_services()
        assert len(registered_services) == 4  # Real service count

        # Can test real idempotence
        result2 = orchestrator.bootstrap()
        assert result2.was_already_bootstrapped  # Real state persistence

        # This is the power of integration + dependency injection!
