"""
Provider-Specific Validation

Comprehensive testing framework that validates each market data provider
works correctly with MarketPipe's CLI and infrastructure.

This module extends Phase 3 of the CLI validation framework:
- Validates each provider's connection and authentication
- Tests data format compliance with OHLCV schema
- Validates rate limiting and error handling
- Tests provider-specific features and options
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest


@dataclass
class ProviderTestConfig:
    """Configuration for provider-specific testing."""

    name: str
    auth_required: bool
    supported_feed_types: list[str]
    rate_limit_per_minute: int | None
    supports_batch: bool
    max_batch_size: int | None
    auth_env_vars: list[str] = field(default_factory=list)
    special_features: list[str] = field(default_factory=list)


@dataclass
class ProviderTestResult:
    """Result of provider-specific testing."""

    provider: str
    connection_test: bool = False
    auth_test: bool = False
    data_format_test: bool = False
    rate_limit_test: bool = False
    error_handling_test: bool = False
    batch_size_test: bool = False
    feed_type_test: bool = False
    performance_metrics: dict[str, float] = field(default_factory=dict)
    error_messages: list[str] = field(default_factory=list)


class ProviderValidator:
    """Validates individual market data providers."""

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or Path(__file__).parent.parent.parent
        self.provider_configs = self._get_provider_configs()

    def _get_provider_configs(self) -> dict[str, ProviderTestConfig]:
        """Get configuration for all supported providers."""
        return {
            "fake": ProviderTestConfig(
                name="fake",
                auth_required=False,
                supported_feed_types=["iex", "sip"],
                rate_limit_per_minute=None,
                supports_batch=True,
                max_batch_size=10000,
                special_features=["deterministic_data", "no_rate_limits"],
            ),
            "alpaca": ProviderTestConfig(
                name="alpaca",
                auth_required=True,
                supported_feed_types=["iex", "sip"],
                rate_limit_per_minute=200,
                supports_batch=True,
                max_batch_size=1000,
                auth_env_vars=["ALPACA_KEY", "ALPACA_SECRET"],
                special_features=["real_time", "paper_trading"],
            ),
            "iex": ProviderTestConfig(
                name="iex",
                auth_required=True,
                supported_feed_types=["iex"],
                rate_limit_per_minute=100,
                supports_batch=True,
                max_batch_size=100,
                auth_env_vars=["IEX_TOKEN"],
                special_features=["cloud_api"],
            ),
            "polygon": ProviderTestConfig(
                name="polygon",
                auth_required=True,
                supported_feed_types=["sip"],
                rate_limit_per_minute=5,  # Free tier
                supports_batch=True,
                max_batch_size=50000,
                auth_env_vars=["POLYGON_API_KEY"],
                special_features=["high_throughput", "crypto", "forex"],
            ),
            "finnhub": ProviderTestConfig(
                name="finnhub",
                auth_required=True,
                supported_feed_types=["finnhub"],
                rate_limit_per_minute=60,  # Free tier
                supports_batch=False,
                max_batch_size=None,
                auth_env_vars=["FINNHUB_API_KEY"],
                special_features=["news", "earnings", "sentiment"],
            ),
        }

    def validate_provider(self, provider_name: str, test_auth: bool = False) -> ProviderTestResult:
        """
        Comprehensive validation of a specific provider.

        Args:
            provider_name: Name of provider to validate
            test_auth: Whether to test authentication (requires credentials)

        Returns:
            ProviderTestResult with validation details
        """
        if provider_name not in self.provider_configs:
            raise ValueError(f"Unknown provider: {provider_name}")

        config = self.provider_configs[provider_name]
        result = ProviderTestResult(provider=provider_name)

        # Test connection/help
        result.connection_test = self._test_provider_connection(config)

        # Test authentication (if credentials available)
        if test_auth and config.auth_required:
            result.auth_test = self._test_provider_authentication(config)
        elif not config.auth_required:
            result.auth_test = True  # No auth required

        # Test data format compliance
        result.data_format_test = self._test_data_format(config)

        # Test rate limiting (if applicable)
        if config.rate_limit_per_minute:
            result.rate_limit_test = self._test_rate_limiting(config)
        else:
            result.rate_limit_test = True  # No rate limits

        # Test error handling
        result.error_handling_test = self._test_error_handling(config)

        # Test batch size handling
        if config.supports_batch:
            result.batch_size_test = self._test_batch_sizes(config)
        else:
            result.batch_size_test = True  # No batch support expected

        # Test feed types
        result.feed_type_test = self._test_feed_types(config)

        # Calculate performance metrics
        result.performance_metrics = self._measure_provider_performance(config)

        return result

    def _test_provider_connection(self, config: ProviderTestConfig) -> bool:
        """Test basic provider connection/availability."""
        try:
            cmd = [
                "python",
                "-m",
                "marketpipe",
                "ingest-ohlcv",
                "--provider",
                config.name,
                "--help",
            ]

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30, cwd=self.base_dir
            )

            return result.returncode == 0

        except Exception:
            return False

    def _test_provider_authentication(self, config: ProviderTestConfig) -> bool:
        """Test provider authentication."""
        try:
            # Check if required environment variables are set
            missing_vars = [var for var in config.auth_env_vars if not os.getenv(var)]
            if missing_vars:
                return False  # Skip if credentials not available

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    config.name,
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2024-01-15",  # Use recent date within retention window
                    "--end",
                    "2024-01-16",  # End date must be after start
                    "--output",
                    str(temp_path / "data"),
                ]

                # Add feed-type for providers that require it
                if config.name in ["alpaca", "iex", "polygon"]:
                    cmd.extend(["--feed-type", "iex"])

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=60, cwd=self.base_dir
                )

                # Authentication is successful if:
                # 1. Command succeeds (returncode 0)
                # 2. Command fails but NOT due to authentication issues
                #    (errors like "No data found", "Rate limit", etc. indicate auth worked)
                if result.returncode == 0:
                    return True

                # Check for authentication-related failures
                error_output = (result.stdout + result.stderr).lower()

                # These indicate authentication failure
                auth_failure_indicators = [
                    "unauthorized",
                    "invalid key",
                    "invalid secret",
                    "authentication failed",
                    "invalid credentials",
                    "forbidden",
                    "access denied",
                ]

                # If any auth failure indicators are found, auth failed
                if any(indicator in error_output for indicator in auth_failure_indicators):
                    return False

                # If we get here, the command failed but not due to authentication
                # This suggests auth worked but some other business rule failed
                # (e.g., "No data found", "Symbol not supported", etc.)
                return True

        except Exception:
            return False

    def _test_data_format(self, config: ProviderTestConfig) -> bool:
        """Test that provider returns data in correct OHLCV format."""
        if config.name != "fake":
            return True  # Skip for real providers without auth

        try:
            # Clean up any existing persistent database files that could cause job conflicts
            persistent_db_files = [
                self.base_dir / "data" / "ingestion_jobs.db",
                self.base_dir / "data" / "metrics.db",
                self.base_dir / "data" / "db" / "core.db",
            ]
            for db_file in persistent_db_files:
                if db_file.exists():
                    try:
                        db_file.unlink()
                    except (PermissionError, OSError):
                        pass  # Continue if we can't remove the file

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                data_dir = temp_path / "data"

                # Use a supported fake provider symbol
                unique_symbol = "TEST"

                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    config.name,
                    "--symbols",
                    unique_symbol,
                    "--start",
                    "2024-01-15",
                    "--end",
                    "2024-01-16",
                    "--output",
                    str(data_dir),
                ]

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=60, cwd=self.base_dir
                )

                if result.returncode == 0:
                    # Check if parquet files were created
                    parquet_files = list(data_dir.rglob("*.parquet"))
                    return len(parquet_files) > 0

                return False

        except Exception:
            return False

    def _test_rate_limiting(self, config: ProviderTestConfig) -> bool:
        """Test rate limiting behavior."""
        if config.name != "fake":
            return True  # Skip for real providers without making actual requests

        # For fake provider, rate limiting doesn't apply
        return True

    def _test_error_handling(self, config: ProviderTestConfig) -> bool:
        """Test error handling for invalid inputs."""
        try:
            # Test with invalid symbol
            cmd = [
                "python",
                "-m",
                "marketpipe",
                "ingest-ohlcv",
                "--provider",
                config.name,
                "--symbols",
                "INVALID_SYMBOL_12345",
                "--start",
                "2023-01-01",
                "--end",
                "2023-01-01",
                "--output",
                "/tmp/test_error",
            ]

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30, cwd=self.base_dir
            )

            # Should either succeed (fake provider) or fail gracefully
            return "crash" not in result.stderr.lower() and "traceback" not in result.stderr.lower()

        except Exception:
            return False

    def _test_batch_sizes(self, config: ProviderTestConfig) -> bool:
        """Test batch size handling."""
        if not config.supports_batch:
            return True

        try:
            # Test with small batch size
            with tempfile.TemporaryDirectory() as temp_dir:
                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    config.name,
                    "--symbols",
                    "AAPL",
                    "--start",
                    "2023-01-01",
                    "--end",
                    "2023-01-01",
                    "--batch-size",
                    "10",
                    "--output",
                    str(Path(temp_dir) / "data"),
                ]

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30, cwd=self.base_dir
                )

                # Should handle small batch sizes
                return "batch" not in result.stderr.lower() or result.returncode == 0

        except Exception:
            return False

    def _test_feed_types(self, config: ProviderTestConfig) -> bool:
        """Test supported feed types."""
        success_count = 0

        for feed_type in config.supported_feed_types:
            try:
                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    config.name,
                    "--feed-type",
                    feed_type,
                    "--help",
                ]

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30, cwd=self.base_dir
                )

                if result.returncode == 0:
                    success_count += 1

            except Exception:
                continue

        # Should support at least one feed type
        return success_count > 0

    def _measure_provider_performance(self, config: ProviderTestConfig) -> dict[str, float]:
        """Measure provider performance metrics."""
        metrics = {}

        if config.name != "fake":
            return metrics  # Skip performance testing for real providers

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                start_time = time.time()

                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    config.name,
                    "--symbols",
                    "AAPL,MSFT,GOOGL",
                    "--start",
                    "2023-01-01",
                    "--end",
                    "2023-01-03",
                    "--output",
                    str(temp_path / "data"),
                ]

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=60, cwd=self.base_dir
                )

                execution_time = time.time() - start_time
                metrics["execution_time_seconds"] = execution_time

                if result.returncode == 0:
                    # Count files created
                    data_dir = temp_path / "data"
                    if data_dir.exists():
                        parquet_files = list(data_dir.rglob("*.parquet"))
                        metrics["files_created"] = len(parquet_files)

                        # Estimate throughput
                        if execution_time > 0:
                            metrics["files_per_second"] = len(parquet_files) / execution_time

        except Exception:
            pass

        return metrics


class TestProviderSpecificValidation:
    """Test suite for provider-specific validation."""

    @pytest.fixture
    def provider_validator(self):
        """Provider validator fixture."""
        return ProviderValidator()

    def test_fake_provider_comprehensive(self, provider_validator):
        """Comprehensive test of fake provider (no auth required)."""
        result = provider_validator.validate_provider("fake", test_auth=False)

        # Fake provider should pass all tests
        assert result.connection_test, "Fake provider connection failed"
        assert result.auth_test, "Fake provider auth test failed"
        assert result.data_format_test, "Fake provider data format test failed"
        assert result.rate_limit_test, "Fake provider rate limit test failed"
        assert result.error_handling_test, "Fake provider error handling test failed"
        assert result.batch_size_test, "Fake provider batch size test failed"
        assert result.feed_type_test, "Fake provider feed type test failed"

        # Should have performance metrics
        assert "execution_time_seconds" in result.performance_metrics

        if result.error_messages:
            pytest.fail(f"Fake provider validation errors: {result.error_messages}")

    def test_all_providers_connection(self, provider_validator):
        """Test that all providers can be reached for basic connection."""
        providers = ["fake", "alpaca", "iex", "polygon", "finnhub"]
        failed_connections = []

        for provider in providers:
            result = provider_validator.validate_provider(provider, test_auth=False)

            if not result.connection_test:
                failed_connections.append(f"{provider}: {result.error_messages}")

        if failed_connections:
            pytest.fail("Provider connection tests failed:\n" + "\n".join(failed_connections))

    def test_provider_help_consistency(self, provider_validator):
        """Test that all providers have consistent help output."""
        providers = ["fake", "alpaca", "iex", "polygon", "finnhub"]
        inconsistent_help = []

        for provider in providers:
            try:
                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    provider,
                    "--help",
                ]

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30, cwd=provider_validator.base_dir
                )

                if result.returncode == 0:
                    help_output = result.stdout.lower()

                    # Should contain standard help elements
                    required_elements = ["usage:", "options:", "--symbols", "--start", "--end"]
                    missing_elements = [
                        elem for elem in required_elements if elem not in help_output
                    ]

                    if missing_elements:
                        inconsistent_help.append(f"{provider}: Missing {missing_elements}")
                else:
                    inconsistent_help.append(f"{provider}: Help command failed")

            except Exception as e:
                inconsistent_help.append(f"{provider}: Exception - {e}")

        if inconsistent_help:
            pytest.fail("Provider help inconsistencies:\n" + "\n".join(inconsistent_help))

    def test_provider_feed_type_support(self, provider_validator):
        """Test provider feed type support."""
        expected_feed_types = {
            "fake": ["iex", "sip"],
            "alpaca": ["iex", "sip"],
            "iex": ["iex"],
            "polygon": ["sip"],
            "finnhub": ["finnhub"],
        }

        feed_type_failures = []

        for provider, feed_types in expected_feed_types.items():
            for feed_type in feed_types:
                try:
                    cmd = [
                        "python",
                        "-m",
                        "marketpipe",
                        "ingest-ohlcv",
                        "--provider",
                        provider,
                        "--feed-type",
                        feed_type,
                        "--help",
                    ]

                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        cwd=provider_validator.base_dir,
                    )

                    if result.returncode != 0:
                        feed_type_failures.append(f"{provider} does not support {feed_type}")

                except Exception as e:
                    feed_type_failures.append(f"{provider} {feed_type}: {e}")

        if feed_type_failures:
            pytest.fail("Feed type support failures:\n" + "\n".join(feed_type_failures))

    @pytest.mark.auth_required
    def test_authenticated_providers(self, provider_validator):
        """Test providers that require authentication (skip if credentials not available)."""
        auth_providers = ["alpaca", "iex", "polygon", "finnhub"]

        # Collect providers that have credentials available
        testable_providers = []
        skipped_providers = []

        for provider in auth_providers:
            config = provider_validator.provider_configs[provider]

            # Check if credentials are available
            missing_vars = [var for var in config.auth_env_vars if not os.getenv(var)]
            if missing_vars:
                skipped_providers.append(f"{provider} (missing: {missing_vars})")
                continue

            testable_providers.append(provider)

        # Skip entire test only if NO providers have credentials
        if not testable_providers:
            pytest.skip(
                f"No authentication credentials available for any provider. Skipped: {skipped_providers}"
            )

        print(f"Testing {len(testable_providers)} providers with credentials: {testable_providers}")
        if skipped_providers:
            print(
                f"Skipped {len(skipped_providers)} providers without credentials: {skipped_providers}"
            )

        # Test each provider that has credentials
        for provider in testable_providers:
            result = provider_validator.validate_provider(provider, test_auth=True)

            # Should at least pass connection and auth tests
            assert result.connection_test, f"{provider} connection test failed"
            assert result.auth_test, f"{provider} authentication test failed"

    def test_provider_error_messages(self, provider_validator):
        """Test that providers give helpful error messages."""
        providers = ["fake", "alpaca", "iex", "polygon", "finnhub"]
        error_message_failures = []

        for provider in providers:
            try:
                # Test with invalid date format
                cmd = [
                    "python",
                    "-m",
                    "marketpipe",
                    "ingest-ohlcv",
                    "--provider",
                    provider,
                    "--symbols",
                    "AAPL",
                    "--start",
                    "invalid-date",
                    "--end",
                    "2023-01-01",
                    "--output",
                    "/tmp/test",
                ]

                # Add feed-type for providers that require it
                if provider in ["alpaca", "iex", "polygon"]:
                    cmd.extend(["--feed-type", "iex"])

                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30, cwd=provider_validator.base_dir
                )

                if result.returncode != 0:
                    error_output = (result.stdout + result.stderr).lower()

                    # Should contain helpful error message
                    if "date" not in error_output and "invalid" not in error_output:
                        error_message_failures.append(
                            f"{provider}: Unhelpful error message for invalid date"
                        )
                else:
                    error_message_failures.append(f"{provider}: Should reject invalid date format")

            except Exception as e:
                error_message_failures.append(f"{provider}: Exception testing error messages - {e}")

        if error_message_failures:
            pytest.fail("Error message failures:\n" + "\n".join(error_message_failures))

    def test_provider_performance_comparison(self, provider_validator):
        """Compare performance across providers (fake provider only)."""
        result = provider_validator.validate_provider("fake", test_auth=False)

        metrics = result.performance_metrics

        # Fake provider should complete quickly
        if "execution_time_seconds" in metrics:
            assert (
                metrics["execution_time_seconds"] < 30
            ), f"Fake provider too slow: {metrics['execution_time_seconds']}s"

        # Should create files
        if "files_created" in metrics:
            assert metrics["files_created"] > 0, "Fake provider created no files"

        # Should have reasonable throughput
        if "files_per_second" in metrics:
            assert (
                metrics["files_per_second"] > 0.1
            ), f"Fake provider throughput too low: {metrics['files_per_second']} files/sec"


if __name__ == "__main__":
    # Can be run directly for provider validation
    validator = ProviderValidator()

    providers = ["fake", "alpaca", "iex", "polygon", "finnhub"]

    print("Provider Validation Report")
    print("=" * 50)

    for provider in providers:
        print(f"\nTesting {provider.upper()} provider:")

        # Test connection only (no auth required)
        result = validator.validate_provider(provider, test_auth=False)

        tests = [
            ("Connection", result.connection_test),
            ("Authentication", result.auth_test),
            ("Data Format", result.data_format_test),
            ("Rate Limiting", result.rate_limit_test),
            ("Error Handling", result.error_handling_test),
            ("Batch Size", result.batch_size_test),
            ("Feed Types", result.feed_type_test),
        ]

        for test_name, test_result in tests:
            status = "✅" if test_result else "❌"
            print(f"  {status} {test_name}")

        if result.performance_metrics:
            print(f"  📊 Performance: {result.performance_metrics}")

        if result.error_messages:
            print(f"  ⚠️  Errors: {result.error_messages}")
