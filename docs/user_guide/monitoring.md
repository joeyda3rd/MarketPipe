# Monitoring Guide

This guide covers setting up comprehensive monitoring for MarketPipe, including metrics collection, alerting, and observability. Learn to monitor data ingestion, system performance, and data quality.

## Overview

MarketPipe provides multi-layered monitoring capabilities:

- **Prometheus Metrics** - Performance and system metrics
- **Structured Logging** - Detailed operational logs
- **Health Checks** - System and dependency status
- **Data Quality Metrics** - Data validation and quality tracking
- **Grafana Dashboards** - Visual monitoring and alerting

## Quick Setup

### 1. Enable Metrics Collection

```bash
# Start metrics server
marketpipe metrics --port 8000

# View metrics in browser
open http://localhost:8000/metrics

# Check metrics are being collected
curl http://localhost:8000/metrics | grep marketpipe
```

### 2. Basic Health Monitoring

```bash
# Run comprehensive health check
marketpipe health-check --verbose

# Monitor continuously
watch -n 30 'marketpipe health-check --quiet'

# Check specific components
marketpipe health-check --database --providers
```

## Prometheus Metrics

### Metrics Server Setup

#### Basic Setup

```bash
# Start metrics server (development)
marketpipe metrics --port 8000

# Production setup with multiprocess support
export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
mkdir -p $PROMETHEUS_MULTIPROC_DIR
marketpipe metrics --port 8000 --multiprocess-dir $PROMETHEUS_MULTIPROC_DIR
```

#### Configuration

```yaml
# config.yaml
metrics:
  enabled: true
  port: 8000
  path: "/metrics"

  prometheus:
    multiprocess_mode: true
    multiprocess_dir: "/tmp/prometheus_multiproc"

  # Custom metrics configuration
  collection_interval: 60  # seconds
  retention_period: "7d"
```

### Available Metrics

#### Core Pipeline Metrics

```bash
# Request metrics
marketpipe_requests_total{provider="alpaca"}           # Total API requests
marketpipe_errors_total{provider="alpaca",status="429"} # Request errors
marketpipe_request_duration_seconds{provider="alpaca"} # Request latency

# Data processing metrics
marketpipe_ingestion_records_total{symbol="AAPL"}      # Records processed
marketpipe_ingestion_duration_seconds{symbol="AAPL"}   # Processing time
marketpipe_ingestion_backlog{symbol="AAPL"}            # Pending jobs

# Data quality metrics
marketpipe_validation_errors_total{symbol="AAPL",type="schema"} # Validation errors
marketpipe_data_quality_score{symbol="AAPL"}                    # Quality score (0-1)
```

#### System Metrics

```bash
# Resource utilization
marketpipe_memory_usage_bytes                          # Memory usage
marketpipe_disk_usage_bytes{path="/data"}             # Disk usage
marketpipe_cpu_usage_percent                          # CPU utilization

# Database metrics
marketpipe_db_connections_active                       # Active connections
marketpipe_db_query_duration_seconds                   # Query latency
marketpipe_db_errors_total                            # Database errors
```

### Custom Metrics

Add application-specific metrics:

```python
# Custom metrics example
from prometheus_client import Counter, Gauge, Histogram

# Business metrics
SYMBOLS_TRACKED = Gauge('marketpipe_symbols_tracked_total', 'Number of symbols being tracked')
DATA_FRESHNESS = Gauge('marketpipe_data_freshness_seconds', 'Seconds since last data update', ['symbol'])
PROCESSING_LAG = Histogram('marketpipe_processing_lag_seconds', 'Time between data arrival and processing', ['symbol'])

# Usage in application
SYMBOLS_TRACKED.set(len(active_symbols))
DATA_FRESHNESS.labels(symbol='AAPL').set(time.time() - last_update_time)
PROCESSING_LAG.labels(symbol='AAPL').observe(processing_duration)
```

## Grafana Integration

### Dashboard Setup

#### 1. Install Grafana

```bash
# Using Docker
docker run -d -p 3000:3000 --name grafana grafana/grafana-oss

# Using package manager (Ubuntu)
sudo apt-get install -y grafana

# Start Grafana
sudo systemctl start grafana-server
```

#### 2. Configure Prometheus Data Source

1. Open Grafana: http://localhost:3000 (admin/admin)
2. Add data source → Prometheus
3. URL: http://localhost:9090 (or your Prometheus server)
4. Test connection

#### 3. Import MarketPipe Dashboard

```bash
# Download dashboard configuration
curl -O https://raw.githubusercontent.com/yourorg/marketpipe/main/monitoring/grafana_dashboard.json

# Import via Grafana UI: Import → Upload JSON file
```

### Key Dashboard Panels

#### System Overview Panel

```json
{
  "title": "MarketPipe System Overview",
  "panels": [
    {
      "title": "Request Rate",
      "type": "graph",
      "targets": [
        {
          "expr": "rate(marketpipe_requests_total[5m])",
          "legendFormat": "{{provider}} requests/sec"
        }
      ]
    },
    {
      "title": "Error Rate",
      "type": "graph",
      "targets": [
        {
          "expr": "rate(marketpipe_errors_total[5m])",
          "legendFormat": "{{provider}} {{status}} errors/sec"
        }
      ]
    },
    {
      "title": "Data Processing Rate",
      "type": "graph",
      "targets": [
        {
          "expr": "rate(marketpipe_ingestion_records_total[5m])",
          "legendFormat": "{{symbol}} records/sec"
        }
      ]
    }
  ]
}
```

#### Data Quality Panel

```json
{
  "title": "Data Quality Monitoring",
  "panels": [
    {
      "title": "Data Quality Score",
      "type": "gauge",
      "targets": [
        {
          "expr": "marketpipe_data_quality_score",
          "legendFormat": "{{symbol}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "min": 0,
          "max": 1,
          "thresholds": {
            "steps": [
              {"color": "red", "value": 0},
              {"color": "yellow", "value": 0.8},
              {"color": "green", "value": 0.95}
            ]
          }
        }
      }
    },
    {
      "title": "Validation Errors",
      "type": "graph",
      "targets": [
        {
          "expr": "increase(marketpipe_validation_errors_total[1h])",
          "legendFormat": "{{symbol}} {{type}} errors"
        }
      ]
    }
  ]
}
```

## Logging Configuration

### Structured Logging Setup

```yaml
# config.yaml
logging:
  level: "INFO"

  # File logging
  file:
    enabled: true
    path: "logs/marketpipe.log"
    max_size: "100MB"
    backup_count: 5
    rotation: "midnight"

  # Console logging
  console:
    enabled: true
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    colored: true

  # Structured logging (JSON)
  structured:
    enabled: true
    format: "json"
    additional_fields:
      service: "marketpipe"
      environment: "production"
```

### Log Aggregation

#### Using ELK Stack

```yaml
# logstash.conf
input {
  file {
    path => "/var/log/marketpipe/*.log"
    type => "marketpipe"
    codec => "json"
  }
}

filter {
  if [type] == "marketpipe" {
    json {
      source => "message"
    }

    date {
      match => ["timestamp", "ISO8601"]
    }
  }
}

output {
  elasticsearch {
    hosts => ["localhost:9200"]
    index => "marketpipe-%{+YYYY.MM.dd}"
  }
}
```

#### Log Analysis Queries

```bash
# View recent errors
marketpipe logs --level ERROR --since 1h

# Filter by component
marketpipe logs --component ingestion --since 24h

# Symbol-specific logs
marketpipe logs --filter "symbol=AAPL" --since 1d
```

## Alerting

### Prometheus Alerting Rules

```yaml
# alerts.yml
groups:
  - name: marketpipe
    rules:
      - alert: HighErrorRate
        expr: rate(marketpipe_errors_total[5m]) > 0.1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} errors/sec for provider {{ $labels.provider }}"

      - alert: DataIngestionStopped
        expr: increase(marketpipe_ingestion_records_total[10m]) == 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "Data ingestion has stopped"
          description: "No data records processed for symbol {{ $labels.symbol }} in 10 minutes"

      - alert: LowDataQuality
        expr: marketpipe_data_quality_score < 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Data quality below threshold"
          description: "Data quality score {{ $value }} for {{ $labels.symbol }}"

      - alert: HighMemoryUsage
        expr: marketpipe_memory_usage_bytes / (1024*1024*1024) > 4
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High memory usage"
          description: "Memory usage is {{ $value }}GB"
```

### Notification Channels

#### Slack Integration

```yaml
# alertmanager.yml
route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'web.hook'

receivers:
  - name: 'web.hook'
    slack_configs:
      - api_url: 'YOUR_SLACK_WEBHOOK_URL'
        channel: '#alerts'
        title: 'MarketPipe Alert'
        text: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
```

#### Email Alerts

```yaml
receivers:
  - name: 'email-alerts'
    email_configs:
      - to: 'ops-team@company.com'
        from: 'alerts@company.com'
        smarthost: 'smtp.company.com:587'
        subject: 'MarketPipe Alert: {{ .GroupLabels.alertname }}'
        body: |
          {{ range .Alerts }}
          Alert: {{ .Annotations.summary }}
          Description: {{ .Annotations.description }}
          Labels: {{ range .Labels.SortedPairs }}{{ .Name }}={{ .Value }} {{ end }}
          {{ end }}
```

## Health Checks

### System Health Monitoring

```bash
# Comprehensive health check
marketpipe health-check --comprehensive

# Component-specific checks
marketpipe health-check --database --providers --disk-space

# Automated health monitoring
#!/bin/bash
while true; do
  if ! marketpipe health-check --quiet; then
    echo "Health check failed at $(date)"
    # Send alert
    curl -X POST -H 'Content-type: application/json' \
      --data '{"text":"MarketPipe health check failed"}' \
      $SLACK_WEBHOOK_URL
  fi
  sleep 300  # Check every 5 minutes
done
```

### Custom Health Checks

```python
# Custom health check implementation
from marketpipe.health import HealthChecker

class CustomHealthCheck(HealthChecker):
    def check_data_freshness(self):
        """Check if data is fresh (updated within last hour)."""
        latest_data = self.get_latest_data_timestamp()
        age_seconds = time.time() - latest_data

        if age_seconds > 3600:  # 1 hour
            return {
                "status": "unhealthy",
                "message": f"Data is {age_seconds/3600:.1f} hours old"
            }

        return {"status": "healthy", "message": "Data is fresh"}

    def check_symbol_coverage(self):
        """Check that all configured symbols have recent data."""
        configured_symbols = self.get_configured_symbols()
        symbols_with_data = self.get_symbols_with_recent_data()

        missing = set(configured_symbols) - set(symbols_with_data)

        if missing:
            return {
                "status": "unhealthy",
                "message": f"Missing data for symbols: {', '.join(missing)}"
            }

        return {"status": "healthy", "message": "All symbols have data"}
```

## Performance Monitoring

### Key Performance Indicators

#### Ingestion Performance

```bash
# Monitor ingestion rates
marketpipe metrics query "rate(marketpipe_ingestion_records_total[5m])"

# Check processing latency
marketpipe metrics query "histogram_quantile(0.95, rate(marketpipe_ingestion_duration_seconds_bucket[5m]))"

# Monitor backlog
marketpipe metrics query "marketpipe_ingestion_backlog"
```

#### System Performance

```bash
# Memory usage trend
marketpipe metrics query "marketpipe_memory_usage_bytes"

# Disk usage by partition
marketpipe metrics query "marketpipe_disk_usage_bytes"

# Database performance
marketpipe metrics query "histogram_quantile(0.95, rate(marketpipe_db_query_duration_seconds_bucket[5m]))"
```

### Performance Optimization Alerts

```yaml
# Performance alerts
- alert: SlowIngestion
  expr: histogram_quantile(0.95, rate(marketpipe_ingestion_duration_seconds_bucket[5m])) > 30
  for: 5m
  annotations:
    summary: "Slow data ingestion detected"

- alert: HighBacklog
  expr: marketpipe_ingestion_backlog > 100
  for: 2m
  annotations:
    summary: "High ingestion backlog"

- alert: DatabaseSlowness
  expr: histogram_quantile(0.95, rate(marketpipe_db_query_duration_seconds_bucket[5m])) > 1
  for: 5m
  annotations:
    summary: "Slow database queries detected"
```

## Data Quality Monitoring

### Quality Metrics

```bash
# Data quality score by symbol
marketpipe metrics query "marketpipe_data_quality_score"

# Validation errors by type
marketpipe metrics query "rate(marketpipe_validation_errors_total[5m])"

# Data completeness
marketpipe metrics query "marketpipe_data_completeness_ratio"
```

### Quality Alerts

```yaml
- alert: LowDataQuality
  expr: marketpipe_data_quality_score < 0.95
  for: 5m
  annotations:
    summary: "Data quality below threshold for {{ $labels.symbol }}"

- alert: ValidationErrors
  expr: rate(marketpipe_validation_errors_total[5m]) > 0.01
  for: 2m
  annotations:
    summary: "High validation error rate for {{ $labels.symbol }}"

- alert: DataCompleteness
  expr: marketpipe_data_completeness_ratio < 0.98
  for: 10m
  annotations:
    summary: "Data completeness below threshold for {{ $labels.symbol }}"
```

## Production Monitoring Setup

### Docker Compose Monitoring Stack

```yaml
# docker-compose.monitoring.yml
version: '3.8'

services:
  marketpipe:
    image: marketpipe:latest
    ports:
      - "8000:8000"  # Metrics port
    environment:
      - MARKETPIPE_METRICS_ENABLED=true
      - PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - ./alerts.yml:/etc/prometheus/alerts.yml

  grafana:
    image: grafana/grafana-oss:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-storage:/var/lib/grafana
      - ./grafana-dashboards:/var/lib/grafana/dashboards
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123

  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml

volumes:
  grafana-storage:
```

### Kubernetes Monitoring

```yaml
# marketpipe-monitoring.yaml
apiVersion: v1
kind: Service
metadata:
  name: marketpipe-metrics
  labels:
    app: marketpipe
spec:
  ports:
  - port: 8000
    name: metrics
  selector:
    app: marketpipe
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: marketpipe
spec:
  selector:
    matchLabels:
      app: marketpipe
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
```

## Monitoring Best Practices

### Metrics Collection

- **Collect meaningful metrics** - Focus on business and system KPIs
- **Use appropriate metric types** - Counters for rates, gauges for levels
- **Add relevant labels** - Provider, symbol, environment for filtering
- **Avoid high cardinality** - Don't use unique IDs as labels
- **Set retention policies** - Balance storage cost with historical needs

### Alerting Strategy

- **Alert on symptoms, not causes** - Alert when users are impacted
- **Use runbooks** - Document response procedures for each alert
- **Avoid alert fatigue** - Tune thresholds to reduce false positives
- **Test alerts regularly** - Ensure notifications reach the right people
- **Group related alerts** - Avoid notification storms

### Dashboard Design

- **Focus on actionability** - Show what operators need to see
- **Use consistent time ranges** - Align all panels for correlation
- **Add context** - Include SLA targets and normal operating ranges
- **Organize by audience** - Separate dashboards for different roles
- **Keep it simple** - Too much information reduces effectiveness

## Troubleshooting Monitoring

### Common Issues

**Metrics not appearing:**
```bash
# Check metrics server is running
curl http://localhost:8000/metrics

# Verify multiprocess directory
ls -la $PROMETHEUS_MULTIPROC_DIR

# Check metric registration
marketpipe metrics list
```

**High memory usage in metrics:**
```bash
# Clear multiprocess metrics
rm -rf $PROMETHEUS_MULTIPROC_DIR/*

# Restart metrics collection
marketpipe metrics restart --port 8000
```

**Missing data in Grafana:**
```bash
# Test Prometheus data source
curl "http://localhost:9090/api/v1/query?query=marketpipe_requests_total"

# Check time range alignment
# Ensure dashboard time range matches data availability
```

## Next Steps

- **Set up alerting**: Configure Prometheus alerting rules for your environment
- **Create dashboards**: Build Grafana dashboards for your specific needs
- **Integrate with infrastructure**: Connect to existing monitoring systems
- **Document runbooks**: Create response procedures for common alerts

---

*Last updated: 2024-01-20*
