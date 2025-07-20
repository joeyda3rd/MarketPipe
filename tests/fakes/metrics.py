# SPDX-License-Identifier: Apache-2.0
"""Fake metrics implementations for testing."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional
from unittest.mock import Mock, patch

import pytest


class FakeMetricsCollector:
    """In-memory metrics collection for tests.
    
    Replaces prometheus_client with controllable fake that can:
    - Record metric observations for verification
    - Support counters, histograms, and gauges
    - Provide query interface for test assertions
    - Mock prometheus behavior without external dependencies
    """
    
    def __init__(self):
        # Store metric values with labels
        self.counters: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.histograms: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
        self.gauges: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        
        # Track metric metadata
        self.counter_metadata: Dict[str, Dict[str, Any]] = {}
        self.histogram_metadata: Dict[str, Dict[str, Any]] = {}
        self.gauge_metadata: Dict[str, Dict[str, Any]] = {}
        
        # Track all observations for verification
        self.observations: List[Dict[str, Any]] = []
        
    def increment_counter(self, name: str, labels: Dict[str, str] = None, value: float = 1.0):
        """Record counter increment."""
        label_key = self._label_key(labels)
        self.counters[name][label_key] += value
        
        # Record observation
        self.observations.append({
            "type": "counter",
            "name": name,
            "labels": labels or {},
            "value": value,
            "operation": "increment"
        })
        
    def observe_histogram(self, name: str, labels: Dict[str, str] = None, value: float = 0.0):
        """Record histogram observation."""
        label_key = self._label_key(labels)
        self.histograms[name][label_key].append(value)
        
        # Record observation
        self.observations.append({
            "type": "histogram",
            "name": name,
            "labels": labels or {},
            "value": value,
            "operation": "observe"
        })
        
    def set_gauge(self, name: str, labels: Dict[str, str] = None, value: float = 0.0):
        """Set gauge value."""
        label_key = self._label_key(labels)
        self.gauges[name][label_key] = value
        
        # Record observation
        self.observations.append({
            "type": "gauge",
            "name": name,
            "labels": labels or {},
            "value": value,
            "operation": "set"
        })
        
    def increment_gauge(self, name: str, labels: Dict[str, str] = None, value: float = 1.0):
        """Increment gauge value."""
        label_key = self._label_key(labels)
        self.gauges[name][label_key] += value
        
        # Record observation
        self.observations.append({
            "type": "gauge",
            "name": name,
            "labels": labels or {},
            "value": value,
            "operation": "increment"
        })
        
    def decrement_gauge(self, name: str, labels: Dict[str, str] = None, value: float = 1.0):
        """Decrement gauge value."""
        label_key = self._label_key(labels)
        self.gauges[name][label_key] -= value
        
        # Record observation
        self.observations.append({
            "type": "gauge",
            "name": name,
            "labels": labels or {},
            "value": -value,
            "operation": "decrement"
        })
        
    def get_counter_value(self, name: str, labels: Dict[str, str] = None) -> float:
        """Get counter value for test verification."""
        label_key = self._label_key(labels)
        return self.counters[name].get(label_key, 0.0)
        
    def get_histogram_values(self, name: str, labels: Dict[str, str] = None) -> List[float]:
        """Get histogram observations for test verification."""
        label_key = self._label_key(labels)
        return self.histograms[name].get(label_key, []).copy()
        
    def get_gauge_value(self, name: str, labels: Dict[str, str] = None) -> float:
        """Get gauge value for test verification.""" 
        label_key = self._label_key(labels)
        return self.gauges[name].get(label_key, 0.0)
        
    def get_histogram_stats(self, name: str, labels: Dict[str, str] = None) -> Dict[str, float]:
        """Get histogram statistics."""
        values = self.get_histogram_values(name, labels)
        if not values:
            return {"count": 0, "sum": 0.0, "min": 0.0, "max": 0.0, "avg": 0.0}
            
        return {
            "count": len(values),
            "sum": sum(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values)
        }
        
    def get_all_observations(self) -> List[Dict[str, Any]]:
        """Get all metric observations for test verification."""
        return self.observations.copy()
        
    def get_observations_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Get observations for specific metric name."""
        return [obs for obs in self.observations if obs["name"] == name]
        
    def get_observations_by_type(self, metric_type: str) -> List[Dict[str, Any]]:
        """Get observations for specific metric type."""
        return [obs for obs in self.observations if obs["type"] == metric_type]
        
    def clear_all(self):
        """Clear all metrics and observations."""
        self.counters.clear()
        self.histograms.clear()
        self.gauges.clear()
        self.observations.clear()
        
    def clear_metric(self, name: str):
        """Clear specific metric."""
        self.counters.pop(name, None)
        self.histograms.pop(name, None)
        self.gauges.pop(name, None)
        
        # Remove observations for this metric
        self.observations = [
            obs for obs in self.observations 
            if obs["name"] != name
        ]
        
    def _label_key(self, labels: Dict[str, str] = None) -> str:
        """Convert labels dict to string key."""
        if not labels:
            return ""
        return ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        
    def assert_counter_incremented(self, name: str, labels: Dict[str, str] = None, expected_count: int = 1):
        """Assert that counter was incremented specific number of times."""
        observations = [
            obs for obs in self.observations
            if (obs["name"] == name and obs["type"] == "counter" and 
                obs["operation"] == "increment" and obs["labels"] == (labels or {}))
        ]
        actual_count = len(observations)
        assert actual_count == expected_count, f"Expected {expected_count} increments for {name}, got {actual_count}"
        
    def assert_histogram_observed(self, name: str, labels: Dict[str, str] = None, min_observations: int = 1):
        """Assert that histogram received minimum number of observations."""
        observations = [
            obs for obs in self.observations
            if (obs["name"] == name and obs["type"] == "histogram" and
                obs["operation"] == "observe" and obs["labels"] == (labels or {}))
        ]
        actual_count = len(observations)
        assert actual_count >= min_observations, f"Expected >= {min_observations} observations for {name}, got {actual_count}"
        
    def assert_gauge_set(self, name: str, labels: Dict[str, str] = None, expected_value: float = None):
        """Assert that gauge was set to expected value."""
        if expected_value is not None:
            actual_value = self.get_gauge_value(name, labels)
            assert actual_value == expected_value, f"Expected gauge {name} = {expected_value}, got {actual_value}"
            
        # Also check that at least one set operation occurred
        observations = [
            obs for obs in self.observations
            if (obs["name"] == name and obs["type"] == "gauge" and
                obs["labels"] == (labels or {}))
        ]
        assert len(observations) > 0, f"No gauge operations found for {name}"


class PrometheusTestDouble:
    """Test double for prometheus_client module.
    
    Provides mock implementations of prometheus_client classes that
    integrate with FakeMetricsCollector.
    """
    
    def __init__(self, collector: FakeMetricsCollector):
        self.collector = collector
        
    def create_counter_mock(self, name: str, description: str, labelnames: List[str] = None):
        """Create mock Counter that integrates with collector."""
        counter_mock = Mock()
        
        def labels(**kwargs):
            label_mock = Mock()
            label_mock.inc = lambda value=1.0: self.collector.increment_counter(name, kwargs, value)
            return label_mock
            
        def inc(value=1.0):
            self.collector.increment_counter(name, {}, value)
            
        counter_mock.labels = labels
        counter_mock.inc = inc
        
        return counter_mock
        
    def create_histogram_mock(self, name: str, description: str, labelnames: List[str] = None):
        """Create mock Histogram that integrates with collector."""
        histogram_mock = Mock()
        
        def labels(**kwargs):
            label_mock = Mock()
            label_mock.observe = lambda value: self.collector.observe_histogram(name, kwargs, value)
            return label_mock
            
        def observe(value):
            self.collector.observe_histogram(name, {}, value)
            
        histogram_mock.labels = labels
        histogram_mock.observe = observe
        
        return histogram_mock
        
    def create_gauge_mock(self, name: str, description: str, labelnames: List[str] = None):
        """Create mock Gauge that integrates with collector."""
        gauge_mock = Mock()
        
        def labels(**kwargs):
            label_mock = Mock()
            label_mock.set = lambda value: self.collector.set_gauge(name, kwargs, value)
            label_mock.inc = lambda value=1.0: self.collector.increment_gauge(name, kwargs, value)
            label_mock.dec = lambda value=1.0: self.collector.decrement_gauge(name, kwargs, value)
            return label_mock
            
        def set_gauge(value):
            self.collector.set_gauge(name, {}, value)
        def inc_gauge(value=1.0):
            self.collector.increment_gauge(name, {}, value)  
        def dec_gauge(value=1.0):
            self.collector.decrement_gauge(name, {}, value)
            
        gauge_mock.labels = labels
        gauge_mock.set = set_gauge
        gauge_mock.inc = inc_gauge
        gauge_mock.dec = dec_gauge
        
        return gauge_mock


@pytest.fixture
def fake_metrics():
    """Pytest fixture providing FakeMetricsCollector."""
    return FakeMetricsCollector()


@pytest.fixture
def prometheus_test_double(fake_metrics):
    """Pytest fixture providing PrometheusTestDouble."""
    return PrometheusTestDouble(fake_metrics)


def patch_prometheus_metrics(collector: FakeMetricsCollector):
    """Context manager to patch prometheus_client with fake collector."""
    
    class PrometheusClientPatcher:
        def __init__(self, collector):
            self.collector = collector
            self.test_double = PrometheusTestDouble(collector)
            
        def __enter__(self):
            # Patch the main prometheus_client classes
            counter_patcher = patch('prometheus_client.Counter', side_effect=self._create_counter)
            histogram_patcher = patch('prometheus_client.Histogram', side_effect=self._create_histogram)
            gauge_patcher = patch('prometheus_client.Gauge', side_effect=self._create_gauge)
            
            self.counter_patcher = counter_patcher
            self.histogram_patcher = histogram_patcher
            self.gauge_patcher = gauge_patcher
            
            counter_patcher.__enter__()
            histogram_patcher.__enter__()
            gauge_patcher.__enter__()
            
            return self.collector
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.counter_patcher.__exit__(exc_type, exc_val, exc_tb)
            self.histogram_patcher.__exit__(exc_type, exc_val, exc_tb)
            self.gauge_patcher.__exit__(exc_type, exc_val, exc_tb)
            
        def _create_counter(self, name, description, labelnames=None):
            return self.test_double.create_counter_mock(name, description, labelnames)
            
        def _create_histogram(self, name, description, labelnames=None, buckets=None):
            return self.test_double.create_histogram_mock(name, description, labelnames)
            
        def _create_gauge(self, name, description, labelnames=None):
            return self.test_double.create_gauge_mock(name, description, labelnames)
    
    return PrometheusClientPatcher(collector)


# Helper functions for common testing patterns

def assert_metrics_collected(collector: FakeMetricsCollector, expected_metrics: Dict[str, Any]):
    """Assert that expected metrics were collected."""
    for metric_name, expected_data in expected_metrics.items():
        metric_type = expected_data["type"]
        labels = expected_data.get("labels", {})
        
        if metric_type == "counter":
            expected_value = expected_data.get("value", 1.0)
            actual_value = collector.get_counter_value(metric_name, labels)
            assert actual_value >= expected_value, f"Counter {metric_name} expected >= {expected_value}, got {actual_value}"
            
        elif metric_type == "histogram":
            min_observations = expected_data.get("min_observations", 1)
            values = collector.get_histogram_values(metric_name, labels)
            assert len(values) >= min_observations, f"Histogram {metric_name} expected >= {min_observations} observations, got {len(values)}"
            
        elif metric_type == "gauge":
            if "value" in expected_data:
                expected_value = expected_data["value"]
                actual_value = collector.get_gauge_value(metric_name, labels)
                assert actual_value == expected_value, f"Gauge {metric_name} expected {expected_value}, got {actual_value}"


# Example usage patterns

class ExampleMetricsTestPatterns:
    """Example patterns for using FakeMetricsCollector in tests."""
    
    def test_with_fixture(self, fake_metrics):
        """Example using pytest fixture."""
        # Code under test would increment metrics
        fake_metrics.increment_counter("requests_total", {"endpoint": "/api"})
        
        # Test can verify metrics were collected
        assert fake_metrics.get_counter_value("requests_total", {"endpoint": "/api"}) == 1.0
        
    def test_with_prometheus_patching(self, fake_metrics):
        """Example using prometheus_client patching."""
        with patch_prometheus_metrics(fake_metrics):
            # Code under test imports and uses prometheus_client
            from prometheus_client import Counter
            requests_counter = Counter("requests_total", "Total requests", ["endpoint"])
            requests_counter.labels(endpoint="/api").inc()
            
            # Verify metrics were collected
            assert fake_metrics.get_counter_value("requests_total", {"endpoint": "/api"}) == 1.0
            
    def test_metric_assertions(self, fake_metrics):
        """Example using built-in assertion helpers."""
        fake_metrics.increment_counter("requests_total", {"method": "GET"})
        fake_metrics.observe_histogram("request_duration", {"method": "GET"}, 0.5)
        fake_metrics.set_gauge("active_connections", {}, 10)
        
        # Use assertion helpers
        fake_metrics.assert_counter_incremented("requests_total", {"method": "GET"})
        fake_metrics.assert_histogram_observed("request_duration", {"method": "GET"})
        fake_metrics.assert_gauge_set("active_connections", {}, 10) 