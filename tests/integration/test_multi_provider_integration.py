# SPDX-License-Identifier: Apache-2.0
"""Multi-provider integration end-to-end tests.

This test validates MarketPipe's ability to handle multiple data providers
within the same pipeline, including provider switching, data consistency
across providers, and fallback scenarios.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.market_data import IMarketDataProvider, ProviderMetadata
from marketpipe.domain.value_objects import Price, Symbol, TimeRange, Timestamp, Volume
from marketpipe.infrastructure.storage.parquet_engine import ParquetStorageEngine


class FakeAlpacaProvider(IMarketDataProvider):
    """Fake Alpaca provider for multi-provider testing."""

    def __init__(self):
        self.provider_name = "alpaca"
        self._call_count = 0

    async def fetch_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange, max_bars: int = 1000
    ) -> list[OHLCVBar]:
        """Simulate Alpaca data with specific characteristics."""
        self._call_count += 1

        bars = []
        start_time = time_range.start.value

        # Alpaca-style data: consistent, high-quality
        for i in range(min(50, max_bars)):
            timestamp = start_time + timedelta(minutes=i)
            base_price = 150.0 + (i * 0.01)  # Gradual price increase

            bar = OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(timestamp),
                open_price=Price.from_float(base_price),
                high_price=Price.from_float(base_price + 0.25),
                low_price=Price.from_float(base_price - 0.15),
                close_price=Price.from_float(base_price + 0.10),
                volume=Volume(1500 + i * 10),  # Consistent volume pattern
            )
            bars.append(bar)

        return bars

    async def is_available(self) -> bool:
        return True

    async def get_supported_symbols(self) -> list[Symbol]:
        """Get supported symbols for testing."""
        symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
        return [Symbol(s) for s in symbols]

    def get_provider_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="alpaca",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=200,
            minimum_time_resolution="1m",
            maximum_history_days=365,
        )


class FakePolygonProvider(IMarketDataProvider):
    """Fake Polygon provider for multi-provider testing."""

    def __init__(self):
        self.provider_name = "polygon"
        self._call_count = 0

    async def fetch_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange, max_bars: int = 1000
    ) -> list[OHLCVBar]:
        """Simulate Polygon data with different characteristics."""
        self._call_count += 1

        bars = []
        start_time = time_range.start.value

        # Polygon-style data: slightly different timing and pricing
        for i in range(min(45, max_bars)):  # Slightly less data
            timestamp = start_time + timedelta(minutes=i, seconds=30)  # 30-second offset
            base_price = 149.5 + (i * 0.012)  # Slightly different price trajectory

            bar = OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(timestamp),
                open_price=Price.from_float(base_price),
                high_price=Price.from_float(base_price + 0.30),
                low_price=Price.from_float(base_price - 0.20),
                close_price=Price.from_float(base_price + 0.08),
                volume=Volume(1400 + i * 12),  # Different volume pattern
            )
            bars.append(bar)

        return bars

    async def is_available(self) -> bool:
        return True

    async def get_supported_symbols(self) -> list[Symbol]:
        """Get supported symbols for testing."""
        symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META"]
        return [Symbol(s) for s in symbols]

    def get_provider_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="polygon",
            supports_real_time=True,
            supports_historical=True,
            rate_limit_per_minute=5,
            minimum_time_resolution="1m",
            maximum_history_days=730,
        )


class FakeIEXProvider(IMarketDataProvider):
    """Fake IEX provider for multi-provider testing."""

    def __init__(self):
        self.provider_name = "iex"
        self._call_count = 0
        self._should_fail = False

    def set_failure_mode(self, should_fail: bool):
        """Simulate provider failure for testing fallback scenarios."""
        self._should_fail = should_fail

    async def fetch_bars_for_symbol(
        self, symbol: Symbol, time_range: TimeRange, max_bars: int = 1000
    ) -> list[OHLCVBar]:
        """Simulate IEX data with potential failures."""
        self._call_count += 1

        if self._should_fail:
            raise ConnectionError("IEX provider temporarily unavailable")

        bars = []
        start_time = time_range.start.value

        # IEX-style data: limited but reliable
        for i in range(min(30, max_bars)):  # Limited data
            timestamp = start_time + timedelta(minutes=i * 2)  # Sparser data
            base_price = 150.2 + (i * 0.008)

            bar = OHLCVBar(
                id=EntityId.generate(),
                symbol=symbol,
                timestamp=Timestamp(timestamp),
                open_price=Price.from_float(base_price),
                high_price=Price.from_float(base_price + 0.15),
                low_price=Price.from_float(base_price - 0.10),
                close_price=Price.from_float(base_price + 0.05),
                volume=Volume(1200 + i * 8),
            )
            bars.append(bar)

        return bars

    async def is_available(self) -> bool:
        return not self._should_fail

    async def get_supported_symbols(self) -> list[Symbol]:
        """Get supported symbols for testing."""
        symbols = ["AAPL", "GOOGL", "MSFT", "AMZN"]
        return [Symbol(s) for s in symbols]

    def get_provider_metadata(self) -> ProviderMetadata:
        return ProviderMetadata(
            provider_name="iex",
            supports_real_time=False,
            supports_historical=True,
            rate_limit_per_minute=100,
            minimum_time_resolution="1m",
            maximum_history_days=180,
        )


class MultiProviderOrchestrator:
    """Orchestrates multiple providers for comprehensive data collection."""

    def __init__(self, providers: list[IMarketDataProvider]):
        self.providers = {p.provider_name: p for p in providers}
        self.provider_order = [p.provider_name for p in providers]

    async def fetch_with_fallback(
        self, symbol: Symbol, time_range: TimeRange, preferred_provider: str = None
    ) -> dict[str, list[OHLCVBar]]:
        """Fetch data with provider fallback capabilities."""

        results = {}
        order = (
            [preferred_provider] + self.provider_order
            if preferred_provider
            else self.provider_order
        )
        order = [p for p in order if p in self.providers and p is not None]

        for provider_name in order:
            provider = self.providers[provider_name]

            try:
                if await provider.is_available():
                    bars = await provider.fetch_bars_for_symbol(symbol, time_range)
                    if bars:
                        results[provider_name] = bars
                        print(f"✓ {provider_name}: {len(bars)} bars")
                    else:
                        print(f"⚠️  {provider_name}: No data returned")
                else:
                    print(f"⚠️  {provider_name}: Provider not available")

            except Exception as e:
                print(f"❌ {provider_name}: Error - {e}")
                continue

        return results

    def analyze_provider_consistency(self, results: dict[str, list[OHLCVBar]]) -> dict[str, any]:
        """Analyze consistency across providers."""

        if not results:
            return {"error": "No provider data available"}

        analysis = {
            "provider_count": len(results),
            "data_counts": {name: len(bars) for name, bars in results.items()},
            "price_ranges": {},
            "volume_ranges": {},
            "timestamp_coverage": {},
        }

        for provider_name, bars in results.items():
            if bars:
                prices = [bar.close_price.to_float() for bar in bars]
                volumes = [bar.volume.value for bar in bars]
                timestamps = [bar.timestamp.value for bar in bars]

                analysis["price_ranges"][provider_name] = {
                    "min": min(prices),
                    "max": max(prices),
                    "avg": sum(prices) / len(prices),
                }

                analysis["volume_ranges"][provider_name] = {
                    "min": min(volumes),
                    "max": max(volumes),
                    "avg": sum(volumes) / len(volumes),
                }

                analysis["timestamp_coverage"][provider_name] = {
                    "start": min(timestamps),
                    "end": max(timestamps),
                    "span_minutes": (max(timestamps) - min(timestamps)).total_seconds() / 60,
                }

        return analysis


@pytest.mark.integration
@pytest.mark.multi_provider
class TestMultiProviderIntegration:
    """Multi-provider integration testing."""

    def test_provider_registration_and_discovery(self):
        """Test provider registration and discovery mechanisms."""

        # Create test providers
        alpaca = FakeAlpacaProvider()
        polygon = FakePolygonProvider()
        iex = FakeIEXProvider()

        providers = [alpaca, polygon, iex]

        # Test provider metadata
        for provider in providers:
            metadata = provider.get_provider_metadata()
            assert metadata.provider_name in ["alpaca", "polygon", "iex"]
            assert metadata.minimum_time_resolution == "1m"
            print(f"✓ Provider {metadata.provider_name}: {metadata.provider_name} provider")

        # Test provider availability
        async def test_availability():
            for provider in providers:
                available = await provider.is_available()
                assert available
                print(f"✓ Provider {provider.provider_name} is available")

        asyncio.run(test_availability())

        print("✅ Provider registration and discovery test completed")

    def test_multi_provider_data_fetching(self, tmp_path):
        """Test fetching data from multiple providers simultaneously."""

        # Setup providers
        alpaca = FakeAlpacaProvider()
        polygon = FakePolygonProvider()
        iex = FakeIEXProvider()

        orchestrator = MultiProviderOrchestrator([alpaca, polygon, iex])

        # Setup test parameters
        symbol = Symbol("AAPL")
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)),
        )

        # Fetch data from all providers
        async def fetch_all():
            return await orchestrator.fetch_with_fallback(symbol, time_range)

        results = asyncio.run(fetch_all())

        # Verify all providers returned data
        assert len(results) == 3
        assert "alpaca" in results
        assert "polygon" in results
        assert "iex" in results

        # Analyze data consistency
        analysis = orchestrator.analyze_provider_consistency(results)

        print("📊 Multi-Provider Data Analysis:")
        print(f"  Providers: {analysis['provider_count']}")
        print(f"  Data counts: {analysis['data_counts']}")

        # Price consistency check
        price_ranges = analysis["price_ranges"]
        if len(price_ranges) > 1:
            avg_prices = [data["avg"] for data in price_ranges.values()]
            price_spread = max(avg_prices) - min(avg_prices)
            print(f"  Price spread: ${price_spread:.2f}")

            # Reasonable price consistency (within 5%)
            if price_spread / min(avg_prices) > 0.05:
                print(
                    f"⚠️  High price variance across providers: {price_spread / min(avg_prices) * 100:.1f}%"
                )
            else:
                print("✓ Good price consistency across providers")

        print("✅ Multi-provider data fetching test completed")

    def test_provider_fallback_scenarios(self, tmp_path):
        """Test provider fallback when primary provider fails."""

        # Setup providers with failure simulation
        alpaca = FakeAlpacaProvider()
        polygon = FakePolygonProvider()
        iex = FakeIEXProvider()

        orchestrator = MultiProviderOrchestrator([alpaca, polygon, iex])

        symbol = Symbol("AAPL")
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)),
        )

        # Test normal operation
        print("🔄 Testing normal multi-provider operation...")

        async def test_normal():
            return await orchestrator.fetch_with_fallback(symbol, time_range, "alpaca")

        normal_results = asyncio.run(test_normal())
        assert len(normal_results) >= 2  # Should get data from multiple providers
        print(f"✓ Normal operation: {len(normal_results)} providers responded")

        # Test with IEX failure
        print("🔄 Testing with IEX provider failure...")
        iex.set_failure_mode(True)

        async def test_with_iex_failure():
            return await orchestrator.fetch_with_fallback(symbol, time_range, "iex")

        fallback_results = asyncio.run(test_with_iex_failure())

        # Should still get data from other providers
        assert len(fallback_results) >= 1
        assert "iex" not in fallback_results  # IEX should have failed
        assert "alpaca" in fallback_results or "polygon" in fallback_results
        print(f"✓ Fallback successful: {len(fallback_results)} providers responded")

        # Reset IEX
        iex.set_failure_mode(False)

        print("✅ Provider fallback scenarios test completed")

    def test_multi_provider_data_storage_integration(self, tmp_path):
        """Test storing data from multiple providers in unified format."""

        # Setup storage
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True)
        storage_engine = ParquetStorageEngine(raw_dir)

        # Setup providers
        alpaca = FakeAlpacaProvider()
        polygon = FakePolygonProvider()
        iex = FakeIEXProvider()

        orchestrator = MultiProviderOrchestrator([alpaca, polygon, iex])

        symbol = Symbol("AAPL")
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)),
        )

        # Fetch data from all providers
        async def fetch_all():
            return await orchestrator.fetch_with_fallback(symbol, time_range)

        results = asyncio.run(fetch_all())

        # Store data from each provider
        stored_jobs = {}

        for provider_name, bars in results.items():
            # Convert OHLCVBar entities to DataFrame
            rows = []
            for bar in bars:
                rows.append(
                    {
                        "ts_ns": bar.timestamp_ns,
                        "symbol": bar.symbol.value,
                        "open": bar.open_price.to_float(),
                        "high": bar.high_price.to_float(),
                        "low": bar.low_price.to_float(),
                        "close": bar.close_price.to_float(),
                        "volume": bar.volume.value,
                        "trade_count": getattr(bar, "trade_count", 1),
                        "vwap": getattr(bar, "vwap", bar.close_price.to_float()),
                        "provider": provider_name,  # Add provider tracking
                    }
                )

            df = pd.DataFrame(rows)

            job_id = f"multi-provider-{provider_name}"
            stored_jobs[provider_name] = job_id

            storage_engine.write(
                df=df,
                frame="1m",
                symbol=symbol.value,
                trading_day=date(2024, 1, 15),
                job_id=job_id,
                overwrite=True,
            )

            print(f"✓ Stored {len(df)} bars from {provider_name}")

        # Verify data can be loaded back
        for provider_name, job_id in stored_jobs.items():
            job_data = storage_engine.load_job_bars(job_id)
            assert symbol.value in job_data

            provider_df = job_data[symbol.value]
            assert len(provider_df) > 0
            assert "provider" in provider_df.columns
            assert all(provider_df["provider"] == provider_name)

            print(f"✓ Verified stored data for {provider_name}: {len(provider_df)} bars")

        print("✅ Multi-provider data storage integration test completed")

    def test_provider_specific_configurations(self, tmp_path):
        """Test provider-specific configuration and behavior."""

        # Test different provider configurations
        provider_configs = [
            {
                "name": "alpaca",
                "rate_limit": 200,  # requests per minute
                "batch_size": 1000,
                "retry_attempts": 3,
            },
            {
                "name": "polygon",
                "rate_limit": 5,  # free tier limit
                "batch_size": 5000,
                "retry_attempts": 5,
            },
            {
                "name": "iex",
                "rate_limit": 100,
                "batch_size": 500,
                "retry_attempts": 2,
            },
        ]

        # Simulate provider behavior based on configurations
        alpaca = FakeAlpacaProvider()
        polygon = FakePolygonProvider()
        iex = FakeIEXProvider()

        providers = [alpaca, polygon, iex]

        # Test provider metadata matches expectations
        for provider, config in zip(providers, provider_configs):
            metadata = provider.get_provider_metadata()
            assert metadata.provider_name == config["name"]

            print(f"✓ Provider {config['name']} configured:")
            print(f"  Rate limit: {config['rate_limit']} req/min")
            print(f"  Batch size: {config['batch_size']}")
            print(f"  Retry attempts: {config['retry_attempts']}")

        # Test provider-specific data characteristics
        symbol = Symbol("AAPL")
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)),
        )

        provider_data_counts = {}

        async def test_provider_characteristics():
            for provider in providers:
                bars = await provider.fetch_bars_for_symbol(symbol, time_range)
                provider_data_counts[provider.provider_name] = len(bars)

        asyncio.run(test_provider_characteristics())

        print("📊 Provider Data Characteristics:")
        for name, count in provider_data_counts.items():
            print(f"  {name}: {count} bars")

        # Verify different providers return different amounts of data (as designed)
        unique_counts = set(provider_data_counts.values())
        assert len(unique_counts) > 1, "Providers should return different amounts of data"

        print("✅ Provider-specific configurations test completed")

    def test_cross_provider_data_validation(self, tmp_path):
        """Test data validation across multiple providers."""

        # Setup providers
        alpaca = FakeAlpacaProvider()
        polygon = FakePolygonProvider()
        iex = FakeIEXProvider()

        orchestrator = MultiProviderOrchestrator([alpaca, polygon, iex])

        symbol = Symbol("AAPL")
        time_range = TimeRange(
            start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
            end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)),
        )

        # Fetch data from all providers
        async def fetch_all():
            return await orchestrator.fetch_with_fallback(symbol, time_range)

        results = asyncio.run(fetch_all())

        # Cross-provider validation checks
        validation_results = {
            "timestamp_overlaps": {},
            "price_correlations": {},
            "data_quality_scores": {},
        }

        # Check for timestamp overlaps between providers
        provider_names = list(results.keys())
        for i, provider1 in enumerate(provider_names):
            for provider2 in provider_names[i + 1 :]:
                bars1 = results[provider1]
                bars2 = results[provider2]

                timestamps1 = {bar.timestamp.value for bar in bars1}
                timestamps2 = {bar.timestamp.value for bar in bars2}

                overlap = len(timestamps1.intersection(timestamps2))
                total_unique = len(timestamps1.union(timestamps2))

                overlap_pct = (overlap / total_unique * 100) if total_unique > 0 else 0
                validation_results["timestamp_overlaps"][
                    f"{provider1}_vs_{provider2}"
                ] = overlap_pct

        # Price correlation analysis
        for provider1 in provider_names:
            bars1 = results[provider1]
            prices1 = [bar.close_price.to_float() for bar in bars1]

            # Simple validation: prices should be within reasonable range
            price_range = max(prices1) - min(prices1)
            avg_price = sum(prices1) / len(prices1)

            quality_score = 100.0

            # Penalize extreme price volatility (> 10% range)
            if price_range / avg_price > 0.10:
                quality_score -= 20

            # Penalize too little data
            if len(bars1) < 20:
                quality_score -= 30

            # Penalize data gaps (simplified check)
            if len(bars1) < 40:  # Expected around 50 bars
                quality_score -= 10

            validation_results["data_quality_scores"][provider1] = max(quality_score, 0)

        print("📊 Cross-Provider Validation Results:")

        print("  Timestamp Overlaps:")
        for comparison, overlap_pct in validation_results["timestamp_overlaps"].items():
            print(f"    {comparison}: {overlap_pct:.1f}%")

        print("  Data Quality Scores:")
        for provider, score in validation_results["data_quality_scores"].items():
            print(f"    {provider}: {score:.1f}/100")

        # Validation assertions
        avg_quality_score = sum(validation_results["data_quality_scores"].values()) / len(
            validation_results["data_quality_scores"]
        )
        assert avg_quality_score > 50, f"Average data quality too low: {avg_quality_score:.1f}"

        print("✅ Cross-provider data validation test completed")


@pytest.mark.integration
@pytest.mark.multi_provider
def test_provider_performance_comparison(tmp_path):
    """Compare performance characteristics across providers."""

    import time

    # Setup providers
    alpaca = FakeAlpacaProvider()
    polygon = FakePolygonProvider()
    iex = FakeIEXProvider()

    providers = [alpaca, polygon, iex]

    symbol = Symbol("AAPL")
    time_range = TimeRange(
        start=Timestamp(datetime(2024, 1, 15, 13, 30, tzinfo=timezone.utc)),
        end=Timestamp(datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc)),
    )

    performance_results = {}

    async def benchmark_provider(provider: IMarketDataProvider):
        start_time = time.monotonic()

        bars = await provider.fetch_bars_for_symbol(symbol, time_range)

        end_time = time.monotonic()
        duration = end_time - start_time

        return {
            "duration_ms": duration * 1000,
            "bars_count": len(bars),
            "bars_per_second": len(bars) / duration if duration > 0 else 0,
        }

    # Benchmark each provider
    async def run_benchmarks():
        for provider in providers:
            result = await benchmark_provider(provider)
            performance_results[provider.provider_name] = result

    asyncio.run(run_benchmarks())

    print("📊 Provider Performance Comparison:")

    for provider_name, metrics in performance_results.items():
        print(f"  {provider_name}:")
        print(f"    Duration: {metrics['duration_ms']:.1f}ms")
        print(f"    Bars: {metrics['bars_count']}")
        print(f"    Throughput: {metrics['bars_per_second']:.1f} bars/sec")

    # Performance analysis
    fastest_provider = min(performance_results.items(), key=lambda x: x[1]["duration_ms"])
    most_data_provider = max(performance_results.items(), key=lambda x: x[1]["bars_count"])

    print("\n📈 Performance Summary:")
    print(f"  Fastest: {fastest_provider[0]} ({fastest_provider[1]['duration_ms']:.1f}ms)")
    print(f"  Most data: {most_data_provider[0]} ({most_data_provider[1]['bars_count']} bars)")

    # All providers should complete reasonably quickly
    for provider_name, metrics in performance_results.items():
        assert (
            metrics["duration_ms"] < 5000
        ), f"{provider_name} too slow: {metrics['duration_ms']:.1f}ms"
        assert metrics["bars_count"] > 0, f"{provider_name} returned no data"

    print("✅ Provider performance comparison completed")
