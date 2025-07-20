# MarketPipe Docker Deployment

Complete containerized deployment stack for MarketPipe with monitoring.

## Services

### MarketPipe Application
- **Port**: 8000 (metrics endpoint)
- **Volumes**: `./data` for Parquet files, `/tmp/prometheus_multiproc` for metrics
- **Command**: Runs metrics server (`marketpipe metrics --port 8000`)

### Prometheus
- **Port**: 9090 (web UI)
- **Config**: `monitoring/prometheus.yml`
- **Retention**: 200 hours
- **Targets**: MarketPipe metrics on port 8000

### Grafana
- **Port**: 3000 (web UI)
- **Login**: admin/admin
- **Dashboards**: Auto-provisioned from `monitoring/grafana/`
- **Data Source**: Prometheus on port 9090

## Quick Start

```bash
# Start the full monitoring stack
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f marketpipe
docker compose logs -f prometheus
docker compose logs -f grafana
```

## Access URLs

- **MarketPipe Metrics**: http://localhost:8000/metrics
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

## Configuration

### Environment Variables
```bash
# Override in docker-compose.yml or .env file
METRICS_PORT=8000
PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
```

### Data Persistence
- **MarketPipe data**: `./data/` (bind mount)
- **Prometheus data**: `prometheus_data` (named volume)
- **Grafana data**: `grafana_data` (named volume)

### Grafana Dashboard
The MarketPipe dashboard from `monitoring/grafana/marketpipe_dashboard.json` should be imported manually:
1. Go to http://localhost:3000
2. Login with admin/admin
3. Import dashboard from file

## Production Setup

### Security
```yaml
# docker-compose.override.yml for production
services:
  grafana:
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
      - GF_SECURITY_SECRET_KEY=${GRAFANA_SECRET}

  prometheus:
    command:
      # Add authentication, TLS, etc.
```

### Resource Limits
```yaml
# Add to services for production
deploy:
  resources:
    limits:
      memory: 512M
      cpus: '0.5'
```

### External Database
```yaml
# For PostgreSQL instead of SQLite
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: marketpipe
      POSTGRES_USER: marketpipe
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data

  marketpipe:
    environment:
      - DATABASE_URL=postgresql://marketpipe:${POSTGRES_PASSWORD}@postgres:5432/marketpipe
    depends_on:
      - postgres
```

## Development

### Local Development with Docker
```bash
# Build local image
docker compose build

# Run with local code changes
docker compose up --build

# Shell into container
docker compose exec marketpipe bash
```

### Cleanup
```bash
# Stop services
docker compose down

# Remove volumes (deletes data!)
docker compose down -v

# Remove images
docker compose down --rmi all
```

## Troubleshooting

### Common Issues

**MarketPipe not starting**:
```bash
docker compose logs marketpipe
# Check for configuration errors, missing API keys
```

**Prometheus not scraping**:
```bash
# Check targets page: http://localhost:9090/targets
# Verify MarketPipe metrics: curl http://localhost:8000/metrics
```

**Grafana dashboard empty**:
```bash
# Verify Prometheus data source connection
# Import dashboard manually if not auto-provisioned
# Check time range (metrics may take time to appear)
```

### Performance Monitoring
```bash
# Check container resource usage
docker stats

# Monitor logs in real-time
docker compose logs -f --tail=100
```
