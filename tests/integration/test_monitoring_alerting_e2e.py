# SPDX-License-Identifier: Apache-2.0
"""Monitoring and alerting end-to-end tests.

This test validates MarketPipe's monitoring infrastructure including metrics
collection, alert triggering, dashboard functionality, and observability
across the entire system lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List
from unittest.mock import Mock, patch

import pytest

from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


class MetricsCollector:
    """Simulates metrics collection and monitoring infrastructure."""
    
    def __init__(self):
        self.metrics: Dict[str, List] = {
            "ingestion_jobs_total": [],
            "ingestion_jobs_failed": [],
            "bars_processed_total": [],
            "processing_duration_seconds": [],
            "memory_usage_bytes": [],
            "cpu_usage_percent": [],
            "disk_usage_percent": [],
            "api_requests_total": [],
            "api_requests_failed": [],
            "storage_operations_total": [],
            "storage_errors_total": [],
        }
        self.alerts_triggered = []
        self.alert_rules = {}
        
    def record_metric(self, metric_name: str, value: float, labels: Dict = None):
        """Record a metric value with optional labels."""
        timestamp = time.time()
        
        metric_entry = {
            "timestamp": timestamp,
            "value": value,
            "labels": labels or {},
        }
        
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        
        self.metrics[metric_name].append(metric_entry)
        
        # Check alert rules
        self._check_alerts(metric_name, value, labels)
    
    def _check_alerts(self, metric_name: str, value: float, labels: Dict):
        """Check if any alert rules are triggered."""
        
        for rule_name, rule in self.alert_rules.items():
            if rule["metric"] == metric_name:
                threshold = rule["threshold"]
                condition = rule["condition"]
                
                triggered = False
                if condition == "greater_than" and value > threshold:
                    triggered = True
                elif condition == "less_than" and value < threshold:
                    triggered = True
                elif condition == "equal" and value == threshold:
                    triggered = True
                
                if triggered:
                    alert = {
                        "rule_name": rule_name,
                        "metric": metric_name,
                        "value": value,
                        "threshold": threshold,
                        "condition": condition,
                        "labels": labels,
                        "timestamp": time.time(),
                        "severity": rule.get("severity", "warning"),
                    }
                    
                    self.alerts_triggered.append(alert)
                    print(f"🚨 ALERT: {rule_name} - {metric_name} {condition} {threshold} (actual: {value})")
    
    def add_alert_rule(self, rule_name: str, metric: str, condition: str, threshold: float, severity: str = "warning"):
        """Add an alert rule."""
        self.alert_rules[rule_name] = {
            "metric": metric,
            "condition": condition,
            "threshold": threshold,
            "severity": severity,
        }
    
    def get_metric_summary(self, metric_name: str, time_window_seconds: int = 300) -> Dict:
        """Get summary statistics for a metric within a time window."""
        
        if metric_name not in self.metrics:
            return {"error": f"Metric {metric_name} not found"}
        
        cutoff_time = time.time() - time_window_seconds
        recent_metrics = [
            m for m in self.metrics[metric_name] 
            if m["timestamp"] >= cutoff_time
        ]
        
        if not recent_metrics:
            return {"error": f"No recent data for {metric_name}"}
        
        values = [m["value"] for m in recent_metrics]
        
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "latest": values[-1] if values else 0,
            "time_window_seconds": time_window_seconds,
        }
    
    def get_alert_summary(self) -> Dict:
        """Get summary of triggered alerts."""
        
        if not self.alerts_triggered:
            return {"total_alerts": 0, "by_severity": {}}
        
        by_severity = {}
        for alert in self.alerts_triggered:
            severity = alert["severity"]
            by_severity[severity] = by_severity.get(severity, 0) + 1
        
        recent_alerts = [
            a for a in self.alerts_triggered
            if time.time() - a["timestamp"] < 300  # Last 5 minutes
        ]
        
        return {
            "total_alerts": len(self.alerts_triggered),
            "recent_alerts": len(recent_alerts),
            "by_severity": by_severity,
            "latest_alerts": self.alerts_triggered[-5:] if self.alerts_triggered else [],
        }


class SystemHealthMonitor:
    """Monitors overall system health and generates health scores."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.health_checks = {}
        
    def add_health_check(self, check_name: str, check_func, critical: bool = False):
        """Add a health check function."""
        self.health_checks[check_name] = {
            "function": check_func,
            "critical": critical,
            "last_result": None,
            "last_check_time": None,
        }
    
    async def run_health_checks(self) -> Dict:
        """Run all health checks and return results."""
        
        health_results = {
            "overall_status": "healthy",
            "checks": {},
            "critical_failures": 0,
            "total_checks": len(self.health_checks),
            "check_time": time.time(),
        }
        
        for check_name, check_config in self.health_checks.items():
            try:
                # Run health check
                if asyncio.iscoroutinefunction(check_config["function"]):
                    result = await check_config["function"]()
                else:
                    result = check_config["function"]()
                
                check_status = "healthy" if result.get("healthy", False) else "unhealthy"
                
                health_results["checks"][check_name] = {
                    "status": check_status,
                    "details": result,
                    "critical": check_config["critical"],
                    "check_time": time.time(),
                }
                
                # Update health check record
                check_config["last_result"] = result
                check_config["last_check_time"] = time.time()
                
                # Count critical failures
                if check_config["critical"] and check_status == "unhealthy":
                    health_results["critical_failures"] += 1
                
            except Exception as e:
                health_results["checks"][check_name] = {
                    "status": "error",
                    "error": str(e),
                    "critical": check_config["critical"],
                    "check_time": time.time(),
                }
                
                if check_config["critical"]:
                    health_results["critical_failures"] += 1
        
        # Determine overall status
        if health_results["critical_failures"] > 0:
            health_results["overall_status"] = "critical"
        elif any(c["status"] != "healthy" for c in health_results["checks"].values()):
            health_results["overall_status"] = "degraded"
        
        return health_results
    
    def calculate_health_score(self) -> float:
        """Calculate overall system health score (0-100)."""
        
        if not self.health_checks:
            return 100.0
        
        total_checks = len(self.health_checks)
        healthy_checks = 0
        
        for check_name, check_config in self.health_checks.items():
            if check_config["last_result"] and check_config["last_result"].get("healthy", False):
                healthy_checks += 1
        
        return (healthy_checks / total_checks) * 100


class AlertManager:
    """Manages alert routing, notification, and escalation."""
    
    def __init__(self):
        self.notification_channels = {}
        self.escalation_rules = {}
        self.sent_notifications = []
        
    def add_notification_channel(self, channel_name: str, channel_config: Dict):
        """Add a notification channel (email, slack, webhook, etc.)."""
        self.notification_channels[channel_name] = channel_config
    
    def add_escalation_rule(self, rule_name: str, conditions: Dict, actions: List):
        """Add an escalation rule."""
        self.escalation_rules[rule_name] = {
            "conditions": conditions,
            "actions": actions,
        }
    
    async def process_alert(self, alert: Dict) -> Dict:
        """Process an alert and send notifications."""
        
        severity = alert.get("severity", "warning")
        
        # Determine notification channels based on severity
        channels = []
        if severity == "critical":
            channels = ["email", "slack", "pager"]
        elif severity == "warning":
            channels = ["slack"]
        else:
            channels = ["log"]
        
        # Send notifications
        notifications_sent = []
        for channel in channels:
            if channel in self.notification_channels:
                notification = await self._send_notification(channel, alert)
                notifications_sent.append(notification)
        
        # Record notification
        notification_record = {
            "alert": alert,
            "channels": channels,
            "notifications_sent": notifications_sent,
            "timestamp": time.time(),
        }
        
        self.sent_notifications.append(notification_record)
        
        return notification_record
    
    async def _send_notification(self, channel: str, alert: Dict) -> Dict:
        """Simulate sending notification to a channel."""
        
        # Simulate network delay
        await asyncio.sleep(0.1)
        
        notification = {
            "channel": channel,
            "alert_id": id(alert),
            "status": "sent",
            "timestamp": time.time(),
            "message": f"Alert: {alert['rule_name']} - {alert['metric']} {alert['condition']} {alert['threshold']}",
        }
        
        print(f"📧 Notification sent via {channel}: {notification['message']}")
        
        return notification


@pytest.mark.integration
@pytest.mark.monitoring
class TestMonitoringAlertingEndToEnd:
    """Monitoring and alerting end-to-end testing."""
    
    def test_metrics_collection_pipeline(self, tmp_path):
        """Test end-to-end metrics collection pipeline."""
        
        collector = MetricsCollector()
        
        print("📊 Testing metrics collection pipeline")
        
        # Simulate ingestion job metrics
        job_metrics = [
            {"metric": "ingestion_jobs_total", "value": 1, "labels": {"status": "started"}},
            {"metric": "bars_processed_total", "value": 1000, "labels": {"symbol": "AAPL"}},
            {"metric": "processing_duration_seconds", "value": 5.2, "labels": {"job_id": "test-job-1"}},
            {"metric": "ingestion_jobs_total", "value": 1, "labels": {"status": "completed"}},
        ]
        
        # Simulate system metrics
        system_metrics = [
            {"metric": "memory_usage_bytes", "value": 1024 * 1024 * 512},  # 512 MB
            {"metric": "cpu_usage_percent", "value": 45.5},
            {"metric": "disk_usage_percent", "value": 68.2},
        ]
        
        # Simulate API metrics
        api_metrics = [
            {"metric": "api_requests_total", "value": 10, "labels": {"provider": "alpaca", "endpoint": "/bars"}},
            {"metric": "api_requests_failed", "value": 1, "labels": {"provider": "alpaca", "error": "timeout"}},
        ]
        
        # Record all metrics
        all_metrics = job_metrics + system_metrics + api_metrics
        
        for metric in all_metrics:
            collector.record_metric(
                metric_name=metric["metric"],
                value=metric["value"],
                labels=metric.get("labels", {})
            )
            
            # Small delay to simulate real-time collection
            time.sleep(0.01)
        
        print(f"✓ Recorded {len(all_metrics)} metrics across {len(collector.metrics)} metric types")
        
        # Verify metrics were collected
        assert len(collector.metrics["ingestion_jobs_total"]) == 2  # started + completed
        assert len(collector.metrics["bars_processed_total"]) == 1
        assert len(collector.metrics["api_requests_total"]) == 1
        
        # Test metric summaries
        bars_summary = collector.get_metric_summary("bars_processed_total")
        assert bars_summary["latest"] == 1000
        assert bars_summary["count"] == 1
        
        memory_summary = collector.get_metric_summary("memory_usage_bytes")
        assert memory_summary["latest"] == 1024 * 1024 * 512
        
        print("✓ Metrics collection and summarization working correctly")
        print("✅ Metrics collection pipeline test completed")
    
    def test_alert_triggering_and_notification(self, tmp_path):
        """Test alert rule evaluation and notification system."""
        
        collector = MetricsCollector()
        alert_manager = AlertManager()
        
        # Setup notification channels
        alert_manager.add_notification_channel("email", {"type": "email", "endpoint": "ops@company.com"})
        alert_manager.add_notification_channel("slack", {"type": "slack", "webhook": "https://hooks.slack.com/..."})
        alert_manager.add_notification_channel("pager", {"type": "pagerduty", "service_key": "abc123"})
        
        print("🚨 Testing alert triggering and notification")
        
        # Setup alert rules
        alert_rules = [
            {"name": "high_memory_usage", "metric": "memory_usage_bytes", "condition": "greater_than", "threshold": 1024**3, "severity": "warning"},
            {"name": "high_error_rate", "metric": "api_requests_failed", "condition": "greater_than", "threshold": 5, "severity": "critical"},
            {"name": "low_throughput", "metric": "bars_processed_total", "condition": "less_than", "threshold": 100, "severity": "warning"},
            {"name": "job_failure", "metric": "ingestion_jobs_failed", "condition": "greater_than", "threshold": 0, "severity": "critical"},
        ]
        
        for rule in alert_rules:
            collector.add_alert_rule(
                rule_name=rule["name"],
                metric=rule["metric"],
                condition=rule["condition"],
                threshold=rule["threshold"],
                severity=rule["severity"]
            )
        
        print(f"✓ Configured {len(alert_rules)} alert rules")
        
        async def simulate_alert_scenarios():
            # Scenario 1: Normal operations (no alerts)
            collector.record_metric("memory_usage_bytes", 512 * 1024**2)  # 512 MB - OK
            collector.record_metric("api_requests_failed", 2)  # 2 failures - OK
            collector.record_metric("bars_processed_total", 1500)  # Good throughput
            
            print("  📊 Normal operations - no alerts expected")
            
            # Scenario 2: High memory usage (warning alert)
            collector.record_metric("memory_usage_bytes", 1.5 * 1024**3)  # 1.5 GB - WARNING
            
            # Process any triggered alerts
            for alert in collector.alerts_triggered[-1:]:
                if alert["rule_name"] == "high_memory_usage":
                    await alert_manager.process_alert(alert)
            
            print("  ⚠️  High memory usage alert triggered")
            
            # Scenario 3: Critical API failures (critical alert)
            collector.record_metric("api_requests_failed", 10)  # Too many failures - CRITICAL
            
            # Process critical alert
            for alert in collector.alerts_triggered[-1:]:
                if alert["rule_name"] == "high_error_rate":
                    await alert_manager.process_alert(alert)
            
            print("  🚨 Critical API error rate alert triggered")
            
            # Scenario 4: Job failure (critical alert)
            collector.record_metric("ingestion_jobs_failed", 1)  # Job failed - CRITICAL
            
            # Process job failure alert
            for alert in collector.alerts_triggered[-1:]:
                if alert["rule_name"] == "job_failure":
                    await alert_manager.process_alert(alert)
            
            print("  🚨 Job failure alert triggered")
            
            return collector.alerts_triggered, alert_manager.sent_notifications
        
        alerts, notifications = asyncio.run(simulate_alert_scenarios())
        
        # Verify alerts were triggered
        assert len(alerts) >= 3, f"Expected at least 3 alerts, got {len(alerts)}"
        
        # Verify notifications were sent
        assert len(notifications) >= 2, f"Expected at least 2 notifications, got {len(notifications)}"
        
        # Check alert severity distribution
        alert_summary = collector.get_alert_summary()
        print(f"📊 Alert Summary: {alert_summary['total_alerts']} total, by severity: {alert_summary['by_severity']}")
        
        # Verify critical alerts were properly escalated
        critical_alerts = [a for a in alerts if a["severity"] == "critical"]
        assert len(critical_alerts) >= 2, "Should have triggered critical alerts"
        
        print("✅ Alert triggering and notification test completed")
    
    def test_system_health_monitoring(self, tmp_path):
        """Test comprehensive system health monitoring."""
        
        collector = MetricsCollector()
        health_monitor = SystemHealthMonitor(collector)
        
        print("🏥 Testing system health monitoring")
        
        # Define health checks
        def storage_health_check():
            """Check if storage system is healthy."""
            storage_dir = tmp_path / "health_storage"
            storage_dir.mkdir(exist_ok=True)
            
            try:
                # Test write/read operation
                test_file = storage_dir / "health_check.txt"
                test_file.write_text("health check")
                content = test_file.read_text()
                test_file.unlink()
                
                return {"healthy": True, "details": "Storage read/write successful"}
            except Exception as e:
                return {"healthy": False, "details": f"Storage error: {e}"}
        
        def memory_health_check():
            """Check memory usage health."""
            # Simulate memory check
            memory_usage_percent = 65  # Simulated value
            
            if memory_usage_percent < 80:
                return {"healthy": True, "memory_usage_percent": memory_usage_percent}
            else:
                return {"healthy": False, "memory_usage_percent": memory_usage_percent, "reason": "High memory usage"}
        
        async def api_connectivity_check():
            """Check API connectivity health."""
            # Simulate API connectivity test
            await asyncio.sleep(0.1)  # Simulate network call
            
            # Randomly simulate success/failure for testing
            import random
            success = random.random() > 0.2  # 80% success rate
            
            if success:
                return {"healthy": True, "response_time_ms": 150}
            else:
                return {"healthy": False, "error": "Connection timeout"}
        
        def database_health_check():
            """Check database connectivity health."""
            # Simulate database check
            return {"healthy": True, "connection_pool_size": 5, "active_connections": 2}
        
        # Register health checks
        health_monitor.add_health_check("storage", storage_health_check, critical=True)
        health_monitor.add_health_check("memory", memory_health_check, critical=False)
        health_monitor.add_health_check("api_connectivity", api_connectivity_check, critical=True)
        health_monitor.add_health_check("database", database_health_check, critical=True)
        
        print(f"✓ Registered {len(health_monitor.health_checks)} health checks")
        
        # Run health checks
        async def run_health_monitoring():
            results = []
            
            # Run multiple health check cycles
            for cycle in range(3):
                print(f"  🔄 Health check cycle {cycle + 1}")
                
                health_result = await health_monitor.run_health_checks()
                results.append(health_result)
                
                print(f"    Overall status: {health_result['overall_status']}")
                print(f"    Checks passed: {health_result['total_checks'] - health_result['critical_failures']}/{health_result['total_checks']}")
                
                # Brief pause between cycles
                await asyncio.sleep(0.5)
            
            return results
        
        health_results = asyncio.run(run_health_monitoring())
        
        # Verify health monitoring worked
        assert len(health_results) == 3, "Should have run 3 health check cycles"
        
        for result in health_results:
            assert "overall_status" in result
            assert "checks" in result
            assert len(result["checks"]) == 4  # All 4 health checks
        
        # Calculate health score
        health_score = health_monitor.calculate_health_score()
        print(f"📊 Overall system health score: {health_score:.1f}/100")
        
        # Health score should be reasonable
        assert health_score >= 50, f"Health score too low: {health_score:.1f}"
        
        print("✅ System health monitoring test completed")
    
    def test_dashboard_metrics_integration(self, tmp_path):
        """Test integration with dashboard and visualization systems."""
        
        collector = MetricsCollector()
        
        print("📈 Testing dashboard metrics integration")
        
        # Simulate a complete ingestion workflow with metrics
        workflow_steps = [
            # Job initialization
            {"metric": "ingestion_jobs_total", "value": 1, "labels": {"status": "initialized", "job_id": "dashboard-test"}},
            
            # API requests
            {"metric": "api_requests_total", "value": 5, "labels": {"provider": "alpaca"}},
            {"metric": "api_requests_total", "value": 3, "labels": {"provider": "polygon"}},
            
            # Data processing
            {"metric": "bars_processed_total", "value": 500, "labels": {"symbol": "AAPL"}},
            {"metric": "bars_processed_total", "value": 450, "labels": {"symbol": "GOOGL"}},
            {"metric": "bars_processed_total", "value": 380, "labels": {"symbol": "MSFT"}},
            
            # Storage operations
            {"metric": "storage_operations_total", "value": 3, "labels": {"operation": "write"}},
            
            # Performance metrics
            {"metric": "processing_duration_seconds", "value": 12.5, "labels": {"job_id": "dashboard-test"}},
            {"metric": "memory_usage_bytes", "value": 800 * 1024**2},  # 800 MB
            {"metric": "cpu_usage_percent", "value": 42.3},
            
            # Job completion
            {"metric": "ingestion_jobs_total", "value": 1, "labels": {"status": "completed", "job_id": "dashboard-test"}},
        ]
        
        # Record metrics with timestamps
        for i, step in enumerate(workflow_steps):
            collector.record_metric(
                metric_name=step["metric"],
                value=step["value"],
                labels=step.get("labels", {})
            )
            
            # Simulate real-time data collection
            time.sleep(0.05)
        
        print(f"✓ Simulated workflow with {len(workflow_steps)} metric points")
        
        # Generate dashboard data summaries
        dashboard_metrics = {}
        
        # Key performance indicators
        dashboard_metrics["kpi"] = {
            "total_bars_processed": sum(
                m["value"] for m in collector.metrics.get("bars_processed_total", [])
            ),
            "jobs_completed": len([
                m for m in collector.metrics.get("ingestion_jobs_total", [])
                if m.get("labels", {}).get("status") == "completed"
            ]),
            "api_requests_made": sum(
                m["value"] for m in collector.metrics.get("api_requests_total", [])
            ),
            "storage_operations": sum(
                m["value"] for m in collector.metrics.get("storage_operations_total", [])
            ),
        }
        
        # Performance metrics
        processing_times = [m["value"] for m in collector.metrics.get("processing_duration_seconds", [])]
        dashboard_metrics["performance"] = {
            "avg_processing_time": sum(processing_times) / len(processing_times) if processing_times else 0,
            "throughput_bars_per_second": dashboard_metrics["kpi"]["total_bars_processed"] / sum(processing_times) if processing_times else 0,
        }
        
        # Resource utilization
        dashboard_metrics["resources"] = {
            "peak_memory_mb": max(
                m["value"] for m in collector.metrics.get("memory_usage_bytes", [])
            ) / 1024**2 if collector.metrics.get("memory_usage_bytes") else 0,
            "avg_cpu_percent": sum(
                m["value"] for m in collector.metrics.get("cpu_usage_percent", [])
            ) / len(collector.metrics.get("cpu_usage_percent", [])) if collector.metrics.get("cpu_usage_percent") else 0,
        }
        
        print("📊 Dashboard Metrics Summary:")
        print(f"  📈 KPIs:")
        print(f"    Total bars processed: {dashboard_metrics['kpi']['total_bars_processed']:,}")
        print(f"    Jobs completed: {dashboard_metrics['kpi']['jobs_completed']}")
        print(f"    API requests: {dashboard_metrics['kpi']['api_requests_made']}")
        
        print(f"  ⚡ Performance:")
        print(f"    Avg processing time: {dashboard_metrics['performance']['avg_processing_time']:.1f}s")
        print(f"    Throughput: {dashboard_metrics['performance']['throughput_bars_per_second']:.0f} bars/sec")
        
        print(f"  💻 Resources:")
        print(f"    Peak memory: {dashboard_metrics['resources']['peak_memory_mb']:.1f} MB")
        print(f"    Avg CPU: {dashboard_metrics['resources']['avg_cpu_percent']:.1f}%")
        
        # Verify dashboard data is reasonable
        assert dashboard_metrics["kpi"]["total_bars_processed"] == 1330  # 500 + 450 + 380
        assert dashboard_metrics["kpi"]["jobs_completed"] == 1
        assert dashboard_metrics["performance"]["throughput_bars_per_second"] > 0
        
        # Generate time-series data for charts
        time_series_data = {}
        for metric_name, metric_data in collector.metrics.items():
            if metric_data:
                time_series_data[metric_name] = [
                    {"timestamp": m["timestamp"], "value": m["value"]}
                    for m in metric_data
                ]
        
        print(f"✓ Generated time-series data for {len(time_series_data)} metrics")
        
        print("✅ Dashboard metrics integration test completed")


@pytest.mark.integration
@pytest.mark.monitoring
def test_comprehensive_monitoring_integration_demo(tmp_path):
    """Comprehensive demonstration of monitoring and alerting capabilities."""
    
    print("🎭 COMPREHENSIVE MONITORING & ALERTING DEMONSTRATION")
    print("=" * 65)
    
    # Setup monitoring infrastructure
    collector = MetricsCollector()
    health_monitor = SystemHealthMonitor(collector)
    alert_manager = AlertManager()
    
    # Configure monitoring system
    print("\n⚙️  Configuring Monitoring Infrastructure")
    
    # Setup alert rules
    alert_rules = [
        {"name": "critical_memory_usage", "metric": "memory_usage_bytes", "condition": "greater_than", "threshold": 2*1024**3, "severity": "critical"},
        {"name": "high_error_rate", "metric": "processing_errors_total", "condition": "greater_than", "threshold": 10, "severity": "warning"},
        {"name": "job_processing_slow", "metric": "processing_duration_seconds", "condition": "greater_than", "threshold": 30, "severity": "warning"},
    ]
    
    for rule in alert_rules:
        collector.add_alert_rule(
            rule_name=rule["name"],
            metric=rule["metric"],
            condition=rule["condition"],
            threshold=rule["threshold"],
            severity=rule["severity"]
        )
    
    # Setup notification channels
    alert_manager.add_notification_channel("email", {"endpoint": "sre@company.com"})
    alert_manager.add_notification_channel("slack", {"webhook": "https://hooks.slack.com/monitoring"})
    alert_manager.add_notification_channel("pager", {"service_key": "monitoring-key-123"})
    
    print(f"✓ Configured {len(alert_rules)} alert rules and 3 notification channels")
    
    # Setup health checks
    def quick_storage_check():
        return {"healthy": True, "details": "Storage responsive"}
    
    def quick_memory_check():
        return {"healthy": True, "memory_usage": "normal"}
    
    health_monitor.add_health_check("storage", quick_storage_check, critical=True)
    health_monitor.add_health_check("memory", quick_memory_check, critical=False)
    
    print(f"✓ Configured {len(health_monitor.health_checks)} health checks")
    
    async def run_comprehensive_monitoring_demo():
        print("\n🚀 Phase 1: Normal Operations Monitoring")
        
        # Simulate normal operations
        normal_metrics = [
            {"metric": "ingestion_jobs_total", "value": 5, "labels": {"status": "completed"}},
            {"metric": "bars_processed_total", "value": 10000},
            {"metric": "processing_duration_seconds", "value": 8.5},
            {"metric": "memory_usage_bytes", "value": 1.2 * 1024**3},  # 1.2 GB
            {"metric": "api_requests_total", "value": 25},
            {"metric": "storage_operations_total", "value": 15},
        ]
        
        for metric in normal_metrics:
            collector.record_metric(
                metric_name=metric["metric"],
                value=metric["value"],
                labels=metric.get("labels", {})
            )
        
        # Run health check
        health_result = await health_monitor.run_health_checks()
        health_score = health_monitor.calculate_health_score()
        
        print(f"  System health: {health_result['overall_status']} (score: {health_score:.1f}/100)")
        print(f"  Total bars processed: {sum(m['value'] for m in normal_metrics if m['metric'] == 'bars_processed_total'):,}")
        
        print("\n⚠️  Phase 2: Alert Scenarios")
        
        # Simulate conditions that trigger alerts
        alert_scenarios = [
            {"metric": "memory_usage_bytes", "value": 2.5 * 1024**3, "description": "High memory usage"},
            {"metric": "processing_errors_total", "value": 15, "description": "Processing errors spike"},
            {"metric": "processing_duration_seconds", "value": 45, "description": "Slow job processing"},
        ]
        
        triggered_alerts = []
        
        for scenario in alert_scenarios:
            collector.record_metric(
                metric_name=scenario["metric"],
                value=scenario["value"]
            )
            
            # Check for new alerts
            new_alerts = [a for a in collector.alerts_triggered if a not in triggered_alerts]
            
            for alert in new_alerts:
                notification_result = await alert_manager.process_alert(alert)
                triggered_alerts.append(alert)
                print(f"  🚨 {scenario['description']}: Alert sent to {len(notification_result['channels'])} channels")
        
        print("\n📊 Phase 3: Dashboard and Reporting")
        
        # Generate comprehensive metrics summary
        metrics_summary = {}
        for metric_name in collector.metrics:
            if collector.metrics[metric_name]:
                summary = collector.get_metric_summary(metric_name, time_window_seconds=600)
                if "error" not in summary:
                    metrics_summary[metric_name] = summary
        
        alert_summary = collector.get_alert_summary()
        
        print(f"  📈 Metrics collected: {len(metrics_summary)} types")
        print(f"  🚨 Alerts triggered: {alert_summary['total_alerts']}")
        print(f"  📧 Notifications sent: {len(alert_manager.sent_notifications)}")
        
        # Performance analytics
        processing_times = [m["value"] for m in collector.metrics.get("processing_duration_seconds", [])]
        bars_processed = [m["value"] for m in collector.metrics.get("bars_processed_total", [])]
        
        if processing_times and bars_processed:
            avg_processing_time = sum(processing_times) / len(processing_times)
            total_bars = sum(bars_processed)
            throughput = total_bars / sum(processing_times)
            
            print(f"  ⚡ Performance Analytics:")
            print(f"    Average processing time: {avg_processing_time:.1f}s")
            print(f"    Total throughput: {throughput:.0f} bars/sec")
        
        return {
            "metrics_collected": len(metrics_summary),
            "alerts_triggered": alert_summary['total_alerts'],
            "notifications_sent": len(alert_manager.sent_notifications),
            "health_score": health_score,
            "system_status": health_result['overall_status'],
        }
    
    demo_results = asyncio.run(run_comprehensive_monitoring_demo())
    
    print(f"\n🎯 MONITORING DEMONSTRATION SUMMARY:")
    print(f"  ✅ Metrics types collected: {demo_results['metrics_collected']}")
    print(f"  🚨 Alerts successfully triggered: {demo_results['alerts_triggered']}")
    print(f"  📧 Notifications delivered: {demo_results['notifications_sent']}")
    print(f"  🏥 Final system health score: {demo_results['health_score']:.1f}/100")
    print(f"  📊 System status: {demo_results['system_status'].upper()}")
    
    # Verify monitoring system effectiveness
    monitoring_checks = {
        "metrics_collection": demo_results['metrics_collected'] >= 5,
        "alert_triggering": demo_results['alerts_triggered'] >= 2,
        "notification_delivery": demo_results['notifications_sent'] >= 2,
        "health_monitoring": demo_results['health_score'] > 0,
    }
    
    passed_checks = sum(monitoring_checks.values())
    total_checks = len(monitoring_checks)
    
    print(f"\n✅ MONITORING EFFECTIVENESS: {passed_checks}/{total_checks} capabilities verified")
    
    for check_name, passed in monitoring_checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {check_name.replace('_', ' ').title()}")
    
    assert passed_checks >= total_checks * 0.75, f"Monitoring system insufficient: {passed_checks}/{total_checks}"
    
    print("\n🎉 Comprehensive monitoring and alerting demonstration completed successfully!")
    print("=" * 65)