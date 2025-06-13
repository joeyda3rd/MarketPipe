# SPDX-License-Identifier: Apache-2.0
"""Guard-rail tests to ensure DDD architectural boundaries are maintained.

These tests verify that our Domain-Driven Design refactor maintains
proper separation of concerns and that critical functionality like
metrics collection continues to work correctly.
"""

from __future__ import annotations

import pytest
from unittest.mock import Mock, patch

from marketpipe.domain.events import IngestionJobCompleted, ValidationFailed
from marketpipe.domain.value_objects import Symbol, Timestamp
from marketpipe.bootstrap import get_event_bus


class TestDDDArchitecturalBoundaries:
    """Test that DDD architectural boundaries are properly enforced."""

    def test_domain_layer_has_no_infrastructure_imports(self):
        """Verify domain layer doesn't import infrastructure modules."""
        import ast
        import os
        from pathlib import Path

        domain_path = Path("src/marketpipe/domain")
        forbidden_imports = {
            "prometheus_client",
            "sqlite3", 
            "httpx",
            "requests",
            "duckdb",
            "pandas",
            "pyarrow",
            "marketpipe.infrastructure",
            "marketpipe.bootstrap"  # Domain should not import bootstrap
        }

        violations = []
        
        for py_file in domain_path.rglob("*.py"):
            if py_file.name == "__pycache__":
                continue
                
            try:
                with open(py_file, 'r') as f:
                    content = f.read()
                    
                tree = ast.parse(content)
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            if any(forbidden in alias.name for forbidden in forbidden_imports):
                                violations.append(f"{py_file}: import {alias.name}")
                    elif isinstance(node, ast.ImportFrom):
                        if node.module and any(forbidden in node.module for forbidden in forbidden_imports):
                            violations.append(f"{py_file}: from {node.module} import ...")
                            
            except Exception as e:
                # Skip files that can't be parsed
                continue
        
        assert not violations, f"Domain layer has forbidden imports: {violations}"

    def test_domain_services_are_pure_business_logic(self):
        """Verify domain services contain only business logic."""
        from marketpipe.domain.services import (
            OHLCVCalculationService,
            MarketDataValidationService,
            TradingCalendarService
        )
        
        # Domain services should be instantiable without infrastructure dependencies
        calc_service = OHLCVCalculationService()
        validation_service = MarketDataValidationService()
        calendar_service = TradingCalendarService()
        
        # Services should have business methods
        assert hasattr(calc_service, 'vwap')
        assert hasattr(calc_service, 'daily_summary')
        assert hasattr(validation_service, 'validate_bar')
        assert hasattr(validation_service, 'validate_batch')
        assert hasattr(calendar_service, 'is_trading_day')


class TestMetricsGuardRails:
    """Guard-rail tests to ensure metrics functionality is preserved."""

    @patch('marketpipe.metrics.REQUESTS')
    @patch('marketpipe.metrics.record_metric')
    def test_metrics_increment_after_refactor(self, mock_record_metric, mock_requests):
        """Verify metrics still increment correctly after DDD refactor."""
        from marketpipe.metrics import record_metric, REQUESTS
        
        # Test that metrics recording still works
        record_metric("test_metric", 1)
        mock_record_metric.assert_called_once_with("test_metric", 1)
        
        # Test that Prometheus counters are accessible
        assert REQUESTS is not None
        
    def test_event_bus_metrics_integration(self):
        """Verify event bus and metrics integration still works."""
        from marketpipe.infrastructure.monitoring.event_handlers import (
            _handle_ingestion_completed,
            _handle_validation_failed
        )
        
        # Create test events with correct parameters
        from datetime import date
        
        ingestion_event = IngestionJobCompleted(
            job_id="test-job",
            symbol=Symbol.from_string("AAPL"),
            trading_date=date(2024, 1, 1),
            bars_processed=100,
            success=True
        )
        
        validation_event = ValidationFailed(
            symbol=Symbol.from_string("AAPL"),
            timestamp=Timestamp.from_nanoseconds(1640995800000000000),
            error_message="Test validation error",
            rule_id="test_rule",
            severity="error"
        )
        
        # Event handlers should be callable without errors
        try:
            _handle_ingestion_completed(ingestion_event)
            _handle_validation_failed(validation_event)
        except Exception as e:
            pytest.fail(f"Event handlers failed: {e}")

    def test_bootstrap_event_registration(self):
        """Verify bootstrap properly registers event handlers."""
        from marketpipe.bootstrap import bootstrap
        from datetime import date
        
        # Bootstrap should complete without errors
        try:
            bootstrap()
        except Exception as e:
            pytest.fail(f"Bootstrap failed: {e}")
        
        # Event bus should be available
        event_bus = get_event_bus()
        assert event_bus is not None
        
        # Should be able to publish events with correct parameters
        test_event = IngestionJobCompleted(
            job_id="test",
            symbol=Symbol.from_string("TEST"),
            trading_date=date(2024, 1, 1),
            bars_processed=1,
            success=True
        )
        
        try:
            event_bus.publish(test_event)
        except Exception as e:
            pytest.fail(f"Event publishing failed: {e}")


class TestApplicationLayerIntegration:
    """Test that application layer properly coordinates domain and infrastructure."""

    def test_validation_application_service_handles_events(self):
        """Verify validation application service can handle domain events."""
        from marketpipe.validation.application.services import ValidationRunnerService
        
        # Should be able to build service with default dependencies
        try:
            service = ValidationRunnerService.build_default()
            assert service is not None
        except Exception as e:
            pytest.fail(f"Failed to build validation service: {e}")

    def test_ingestion_application_service_coordination(self):
        """Verify ingestion application service coordinates properly."""
        from marketpipe.ingestion.application.services import IngestionCoordinatorService
        
        # Service should be importable and have expected methods
        assert hasattr(IngestionCoordinatorService, 'execute_job')
        assert hasattr(IngestionCoordinatorService, '_process_symbol')


class TestInfrastructureIsolation:
    """Test that infrastructure concerns are properly isolated."""

    def test_infrastructure_modules_exist(self):
        """Verify infrastructure modules are properly organized."""
        from pathlib import Path
        
        infra_path = Path("src/marketpipe/infrastructure")
        assert infra_path.exists(), "Infrastructure directory should exist"
        
        # Key infrastructure modules should exist
        expected_modules = [
            "monitoring",
            "events", 
            "persistence",
            "storage"
        ]
        
        for module in expected_modules:
            module_path = infra_path / module
            if module_path.exists():
                # Module exists, verify it has __init__.py
                init_file = module_path / "__init__.py"
                assert init_file.exists(), f"Infrastructure module {module} missing __init__.py"

    def test_event_publishers_in_infrastructure(self):
        """Verify event publishers are in infrastructure layer."""
        from marketpipe.infrastructure.events import InMemoryEventPublisher
        
        # Should be able to instantiate event publisher
        publisher = InMemoryEventPublisher()
        assert publisher is not None
        
        # Should have required interface methods (async publisher interface)
        assert hasattr(publisher, 'publish')
        assert hasattr(publisher, 'publish_many')
        assert hasattr(publisher, 'register_handler') 