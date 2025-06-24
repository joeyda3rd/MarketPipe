# Changelog

All notable changes to MarketPipe will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Fix Alpaca ms→ns timestamp bug - remove obsolete 9600-second offset and correct millisecond to nanosecond conversion
- Add comprehensive unit tests for timestamp conversion and boundary check validation
- Implement boundary check validation to ensure ingested data respects requested date ranges

### Added
- New verification scripts for large-scale data validation (SPY full year test)
- Comprehensive timestamp fix tests covering edge cases and boundary conditions

### Changed
- Improved error handling in ingestion pipeline with better boundary validation
- Enhanced code formatting and linting compliance across the codebase 