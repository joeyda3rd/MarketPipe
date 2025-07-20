# MarketPipe Monitoring & Metrics Guide

MarketPipe provides comprehensive monitoring capabilities through Prometheus metrics, real-time dashboards, and alerting systems. This guide covers how to set up, configure, and use the monitoring features effectively.

## Quick Start

### Dual Server Setup (Recommended)

Start both metrics collection and dashboard with a single command:

```bash
marketpipe metrics --port 8000
```

This starts **two servers**:
- **Prometheus metrics**: `http://localhost:8000/metrics` (for scraping)
- **Dashboard**: `http://localhost:8001` (human-friendly interface)

### Basic Monitoring Workflow

1. **Start monitoring**: `marketpipe metrics`
2. **Run ingestion**: `marketpipe ingest --config config.yaml`
3. **View dashboard**: Open `http://localhost:8001`
4. **Check metrics**: Visit `http://localhost:8000/metrics`

## Core Commands

### Start Metrics Server

```bash
# Modern async server (recommended)
marketpipe metrics --port 8000

# Custom port
marketpipe metrics --port 9090

# Legacy blocking server
marketpipe metrics --legacy-metrics --port 8000
```

### Configuration Options

```bash
# Enable multiprocess metrics (for production)
export PROMETHEUS_MULTIPROC_DIR="/tmp/marketpipe_metrics"
marketpipe metrics --port 8000

# Custom metrics directory
marketpipe metrics --port 8000 --multiproc-dir /var/lib/marketpipe/metrics
```

## Available Metrics

### Core Ingestion Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `marketpipe_requests_total` | Counter | Total API requests made | `vendor` |
| `marketpipe_errors_total` | Counter | Total API errors | `vendor`, `status_code` |
| `marketpipe_request_duration_seconds` | Histogram | Request latency | `vendor` |
| `marketpipe_ingestion_backlog` | Gauge | Pending ingestion tasks | `symbol` |
| `marketpipe_data_quality_total` | Counter | Data quality issues | `symbol`, `issue_type` |

### System Metrics

- **HTTP Request Performance**: Latency percentiles (50th, 95th, 99th)
- **Error Rates**: By vendor and status code
- **Throughput**: Requests per second, rows processed
- **Queue Depth**: Backlog by symbol
- **Data Quality**: Validation failures and warnings

## Dashboard Features

The built-in dashboard (`http://localhost:8001`) provides:

### Real-time Monitoring
- **Live metrics updates** every 5 seconds
- **Request rate graphs** with vendor breakdown
- **Error rate tracking** by status code
- **Latency percentiles** (p50, p95, p99)

### Data Quality Insights
- **Validation failure rates** by symbol
- **Schema compliance scores**
- **Data completeness metrics**
- **Processing throughput**

### Operational Health
- **Ingestion backlog** per symbol
- **API rate limit status**
- **System resource usage**
- **Error trend analysis**

## Configuration

### Basic Configuration

Add metrics to your configuration file:

```yaml
# config/example_with_metrics.yaml
alpaca:
  key: # From ALPACA_KEY env var
  secret: # From ALPACA_SECRET env var
  base_url: https://data.alpaca.markets/v2
  rate_limit_per_min: 200

symbols:
  - AAPL
  - GOOGL

# Enable metrics
metrics:
  enabled: true
  port: 8000
  multiprocess_dir: "/tmp/prometheus_multiproc"
```

### Advanced Configuration

```yaml
metrics:
  enabled: true
  port: 8000
  dashboard_port: 8001
  multiprocess_dir: "/var/lib/marketpipe/metrics"
  collection_interval: 15  # seconds
  retention_days: 7

  # Export configuration
  export:
    prometheus: true
    json: true
    csv: false

  # Alert thresholds
  alerts:
    error_rate_threshold: 0.05  # 5%
    latency_threshold_ms: 5000
    backlog_threshold: 100
```

## Integration with External Systems

### Prometheus Integration

1. **Configure Prometheus** to scrape MarketPipe:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'marketpipe'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 15s
```

2. **Start MarketPipe metrics**:
```bash
marketpipe metrics --port 8000
```

3. **Verify scraping**: Check Prometheus targets page

### Grafana Dashboard

Use the provided dashboard configuration:

```bash
# Import the dashboard
cp docs/grafana_dashboard.json /var/lib/grafana/dashboards/
```

Or create custom dashboards with queries like:
```promql
# Request rate by vendor
rate(marketpipe_requests_total[5m])

# Error rate percentage
rate(marketpipe_errors_total[5m]) / rate(marketpipe_requests_total[5m]) * 100

# 95th percentile latency
histogram_quantile(0.95, rate(marketpipe_request_duration_seconds_bucket[5m]))
```

## Monitoring Workflows

### Development Monitoring

```bash
# Start with debug logging
marketpipe metrics --port 8000 --verbose

# Run ingestion in another terminal
marketpipe ingest --config config.yaml --verbose

# Monitor in real-time
curl http://localhost:8000/metrics | grep marketpipe
```

### Production Monitoring

```bash
# 1. Setup multiprocess metrics
export PROMETHEUS_MULTIPROC_DIR="/var/lib/marketpipe/metrics"
mkdir -p "$PROMETHEUS_MULTIPROC_DIR"

# 2. Start metrics server
marketpipe metrics --port 8000 &

# 3. Run ingestion with monitoring
marketpipe ingest --config production_config.yaml

# 4. Monitor via dashboard
open http://localhost:8001
```

### Performance Analysis

```bash
# Check specific metrics
curl http://localhost:8000/metrics | grep request_duration

# Export metrics for analysis
curl http://localhost:8000/metrics > metrics_$(date +%Y%m%d_%H%M%S).txt

# Monitor backlog in real-time
watch -n 1 "curl -s http://localhost:8000/metrics | grep backlog"
```

## Alerting

### Built-in Alerts

The dashboard provides visual alerts for:
- **High error rates** (>5%)
- **Excessive latency** (>5 seconds)
- **Large backlogs** (>100 pending tasks)
- **Data quality issues** (validation failures)

### Custom Alerting

Configure external alerting systems with these metric queries:

```promql
# High error rate alert
rate(marketpipe_errors_total[5m]) / rate(marketpipe_requests_total[5m]) > 0.05

# High latency alert
histogram_quantile(0.95, rate(marketpipe_request_duration_seconds_bucket[5m])) > 5

# Backlog alert
marketpipe_ingestion_backlog > 100

# Data quality alert
increase(marketpipe_data_quality_total[5m]) > 10
```

## Troubleshooting

### Common Issues

**Port Already in Use**
```bash
# Find what's using the port
lsof -i :8000

# Kill the process
kill <PID>

# Or use different port
marketpipe metrics --port 8080
```

**Metrics Not Updating**
```bash
# Check multiprocess directory
ls -la $PROMETHEUS_MULTIPROC_DIR

# Clear stale metrics
rm -rf $PROMETHEUS_MULTIPROC_DIR/*

# Restart metrics server
marketpipe metrics --port 8000
```

**Dashboard Not Loading**
```bash
# Check if both servers are running
netstat -tlnp | grep -E ':(8000|8001)'

# Restart with verbose logging
marketpipe metrics --port 8000 --verbose
```

### Performance Optimization

**High Memory Usage**
```bash
# Use separate metrics directory
export PROMETHEUS_MULTIPROC_DIR="/tmp/marketpipe_metrics"

# Rotate metrics regularly
find $PROMETHEUS_MULTIPROC_DIR -name "*.db" -mtime +1 -delete
```

**Slow Dashboard Loading**
```bash
# Reduce collection interval
marketpipe metrics --port 8000 --collection-interval 30

# Limit metric retention
export PROMETHEUS_RETENTION_DAYS=3
```

## Best Practices

### Development
- Always start metrics before ingestion
- Use verbose logging for debugging
- Monitor dashboard during development
- Check error rates after configuration changes

### Production
- Set up proper multiprocess directory
- Configure Prometheus scraping
- Set up alerting rules
- Monitor disk space for metrics storage
- Rotate metrics files regularly

### Performance
- Use appropriate scrape intervals (15-30s)
- Limit metric retention (3-7 days)
- Monitor metrics server resource usage
- Use dedicated metrics instance for high-volume environments

## Examples

### Quick Health Check
```bash
# Start monitoring
marketpipe metrics --port 8000 &

# Run test ingestion
marketpipe ingest --config config.yaml --symbol AAPL --dry-run

# Check if metrics are working
curl http://localhost:8000/metrics | grep marketpipe_requests_total
```

### Production Setup
```bash
# 1. Environment setup
export PROMETHEUS_MULTIPROC_DIR="/var/lib/marketpipe/metrics"
mkdir -p "$PROMETHEUS_MULTIPROC_DIR"

# 2. Start metrics (background)
nohup marketpipe metrics --port 8000 > metrics.log 2>&1 &

# 3. Start ingestion
marketpipe ingest --config production_config.yaml

# 4. Monitor progress
curl http://localhost:8001  # Dashboard
curl http://localhost:8000/metrics  # Raw metrics
```

For more advanced monitoring configurations, see the Grafana dashboard in `docs/grafana_dashboard.json` and environment variable reference in `docs/ENVIRONMENT_VARIABLES.md`.
