# MarketPipe Monitoring

This directory contains monitoring and observability configurations for MarketPipe.

## Grafana Dashboard

### `grafana/marketpipe_dashboard.json`
Complete Grafana dashboard for monitoring MarketPipe ETL operations.

**Features:**
- **Request Rate**: API requests per second by vendor
- **Error Rate**: API errors and failure rates  
- **Latency**: Request duration percentiles (50th, 95th, 99th)
- **Ingestion Backlog**: Pending ingestion jobs by symbol
- **Data Quality**: Validation errors and data quality metrics
- **System Performance**: Memory, CPU, and throughput metrics

## Setup Instructions

### 1. Enable Metrics in MarketPipe

Add metrics configuration to your config file:

```yaml
# config/production_config.yaml
metrics:
  enabled: true
  port: 8000
  multiprocess_dir: "/tmp/prometheus_multiproc"
```

### 2. Start Metrics Server

```bash
# Start the Prometheus metrics endpoint
marketpipe metrics --port 8000
```

The metrics will be available at `http://localhost:8000/metrics`

### 3. Configure Prometheus

Add MarketPipe as a target in your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'marketpipe'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 15s
    metrics_path: /metrics
```

### 4. Import Grafana Dashboard

1. Open Grafana (typically at `http://localhost:3000`)
2. Go to **Dashboards** → **Import**
3. Upload `monitoring/grafana/marketpipe_dashboard.json`
4. Configure data source to point to your Prometheus instance

## Key Metrics

### Application Metrics
- `marketpipe_requests_total` - Total API requests by vendor
- `marketpipe_errors_total` - Total errors by vendor and status code  
- `marketpipe_request_duration_seconds` - Request latency histogram
- `marketpipe_ingestion_backlog` - Pending ingestion jobs by symbol
- `marketpipe_data_quality_total` - Data quality issues by type

### System Metrics (via node_exporter)
- CPU usage, memory consumption
- Disk I/O and network traffic
- System load and process counts

## Alerts and Monitoring

### Recommended Alerts
1. **High Error Rate**: Error rate > 5% for 5 minutes
2. **High Latency**: 95th percentile latency > 10 seconds
3. **Backlog Growth**: Ingestion backlog growing for 15 minutes
4. **Data Quality**: Validation failure rate > 10%
5. **System Resources**: CPU > 80% or Memory > 90%

### Alert Configuration Example (Prometheus)

```yaml
# alerting_rules.yml
groups:
  - name: marketpipe
    rules:
      - alert: HighErrorRate
        expr: rate(marketpipe_errors_total[5m]) / rate(marketpipe_requests_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value | humanizePercentage }} for vendor {{ $labels.vendor }}"

      - alert: HighLatency
        expr: histogram_quantile(0.95, rate(marketpipe_request_duration_seconds_bucket[5m])) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High latency detected"
          description: "95th percentile latency is {{ $value }}s for vendor {{ $labels.vendor }}"
```

## Dashboard Panels

### Request Monitoring
- **Request Rate**: Tracks API requests per second
- **Success Rate**: Percentage of successful requests
- **Error Breakdown**: Errors by status code and vendor

### Performance Monitoring  
- **Response Times**: Latency percentiles over time
- **Throughput**: Data processing rate (rows/second)
- **Queue Depth**: Pending work by symbol

### Data Quality
- **Validation Errors**: Schema and business rule violations
- **Data Completeness**: Missing or invalid data points
- **Processing Status**: Job success/failure rates

## Development vs Production

### Development
```bash
# Quick metrics for development
marketpipe metrics --port 8000
```

### Production
```bash
# Production setup with external metrics directory
export PROMETHEUS_MULTIPROC_DIR=/var/lib/marketpipe/metrics
marketpipe metrics --port 8000
```

## Troubleshooting

### Metrics Not Appearing
1. Check metrics server is running: `curl http://localhost:8000/metrics`
2. Verify Prometheus is scraping: Check Prometheus targets page
3. Check multiprocess directory permissions

### Dashboard Issues
1. Verify Prometheus data source connection in Grafana
2. Check time range - metrics may take a few minutes to appear
3. Verify metric names match between dashboard and actual metrics

### Performance Impact
- Metrics collection adds ~1-2% CPU overhead
- Multiprocess mode required for production (multiple workers)
- Consider metrics retention policies for long-running systems 