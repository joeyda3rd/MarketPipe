# MarketPipe Documentation

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://badge.fury.io/py/marketpipe.svg)](https://badge.fury.io/py/marketpipe)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![CI Status](https://github.com/yourorg/marketpipe/workflows/CI/badge.svg)](https://github.com/yourorg/marketpipe/actions)
[![Coverage](https://codecov.io/gh/yourorg/marketpipe/branch/main/graph/badge.svg)](https://codecov.io/gh/yourorg/marketpipe)

**Modern, Python-native ETL framework for financial market data workflows with Domain-Driven Design**

MarketPipe transforms how you collect, validate, and store financial market data. Built with modern Python patterns, it provides a robust, scalable foundation for financial data workflows with enterprise-grade observability and monitoring.

> **⚠️ Alpha Software**: MarketPipe is currently in alpha. APIs may change and features are being actively developed. Use with caution in production environments.

## Quick Navigation

### 🚀 **New Users**
- **[Getting Started](getting_started.md)** - Installation, first run, and hello-world example
- **[CLI Usage](user_guide/cli_usage.md)** - Command reference and common workflows
- **[Configuration](user_guide/configuration.md)** - Setup and configuration guide
- **[Troubleshooting](user_guide/troubleshooting.md)** - FAQ and common issues

### 🔧 **Operators & DevOps**
- **[Monitoring](user_guide/monitoring.md)** - Metrics, alerting, and observability
- **[CLI Reference](reference/cli/)** - Complete command documentation
- **[Configuration Reference](user_guide/configuration.md)** - All environment variables and settings

### 👩‍💻 **Contributors**
- **[Contributing](developer_guide/contributing.md)** - Development setup and workflow
- **[Architecture](developer_guide/architecture.md)** - System design and patterns
- **[Testing](developer_guide/testing.md)** - Test strategy and guidelines
- **[Release Process](developer_guide/release_process.md)** - How we ship releases

## Features Highlights

### 🏗️ **Modern Architecture**
- Domain-Driven Design with enforced boundaries
- Plugin-based providers for easy integration
- Async/sync dual APIs for maximum flexibility
- Type-safe configuration with validation

### 📊 **Data Providers**
- **Alpaca Markets** - Real-time and historical data
- **IEX Cloud** - Professional-grade financial data
- **Fake Provider** - Generate test data
- **Extensible Plugin System** - Add custom providers

### 🔧 **ETL Pipeline**
- Parallel ingestion across symbols and timeframes
- Schema validation with business rule enforcement
- Incremental loading with checkpoint/resume
- Partitioned Parquet storage for performance

### 📈 **Observability**
- Prometheus metrics with multiprocess support
- Grafana dashboards for real-time monitoring
- Structured logging with configurable levels
- Performance tracking and error alerting

## Quick Start

Install and run your first data ingestion in 2 minutes:

```bash
# Install MarketPipe
pip install marketpipe

# Set up credentials
echo "ALPACA_KEY=your_key" > .env
echo "ALPACA_SECRET=your_secret" >> .env

# Run first ingestion
marketpipe ingest --symbol AAPL --start 2024-01-02 --end 2024-01-02

# View results
marketpipe query --symbol AAPL --limit 10
```

See the [Getting Started Guide](getting_started.md) for detailed setup instructions.

## Support & Community

- **GitHub Issues**: [Report bugs or request features](https://github.com/yourorg/marketpipe/issues)
- **Discussions**: [Community Q&A and ideas](https://github.com/yourorg/marketpipe/discussions)
- **Contributing**: [Join our contributor community](developer_guide/contributing.md)
- **Security**: [Report security issues](../SECURITY.md)

## License

MarketPipe is licensed under the [Apache 2.0 License](../LICENSE).

---

*Last updated: 2024-01-20*
