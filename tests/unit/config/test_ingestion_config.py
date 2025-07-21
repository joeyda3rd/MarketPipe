# SPDX-License-Identifier: Apache-2.0
"""Tests for IngestionJobConfig Pydantic model."""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import pytest

from marketpipe.config import IngestionJobConfig
from marketpipe.ingestion.infrastructure.provider_registry import (
    is_registered,
    register,
)

# Ensure polygon provider is available for config tests
try:
    from marketpipe.ingestion.infrastructure.fake_adapter import FakeMarketDataAdapter

    if not is_registered("polygon"):
        register("polygon", FakeMarketDataAdapter)
except Exception:
    pass


class TestIngestionJobConfig:

    @pytest.mark.fast
    @pytest.mark.config
    def test_valid_config_creation(self):
        """Test creating a valid config object."""
        config = IngestionJobConfig(
            symbols=["AAPL", "MSFT"],
            start=date(2025, 1, 1),
            end=date(2025, 1, 7),
            batch_size=500,
            provider="alpaca",
            feed_type="iex",
        )

        assert config.symbols == ["AAPL", "MSFT"]
        assert config.start == date(2025, 1, 1)
        assert config.end == date(2025, 1, 7)
        assert config.batch_size == 500
        assert config.provider == "alpaca"
        assert config.feed_type == "iex"

    @pytest.mark.fast
    @pytest.mark.config
    def test_default_values(self):
        """Test that default values are applied correctly."""
        config = IngestionJobConfig(symbols=["AAPL"], start=date(2025, 1, 1), end=date(2025, 1, 7))

        assert config.batch_size == 1000  # Default
        assert config.provider == "alpaca"  # Default
        assert config.feed_type == "iex"  # Default
        assert config.output_path == "./data"  # Default
        assert config.workers == 4  # Default

    def test_symbol_validation_and_normalization(self):
        """Test symbol validation and normalization."""
        # Test normalization to uppercase
        config = IngestionJobConfig(
            symbols=["aapl", "MSFT", " googl "],
            start=date(2025, 1, 1),
            end=date(2025, 1, 7),
        )
        assert config.symbols == ["AAPL", "MSFT", "GOOGL"]

    def test_invalid_symbols(self):
        """Test validation of invalid symbols."""
        # Empty symbols list
        with pytest.raises(ValueError, match="List should have at least 1 item"):
            IngestionJobConfig(symbols=[], start=date(2025, 1, 1), end=date(2025, 1, 7))

        # Invalid symbol format
        with pytest.raises(ValueError, match="Invalid symbol format"):
            IngestionJobConfig(
                symbols=["AAP@"],  # Contains invalid character
                start=date(2025, 1, 1),
                end=date(2025, 1, 7),
            )

        # Symbol too long
        with pytest.raises(ValueError, match="Invalid symbol format"):
            IngestionJobConfig(
                symbols=["VERYLONGSYMBOL"], start=date(2025, 1, 1), end=date(2025, 1, 7)
            )

    def test_date_range_validation(self):
        """Test date range validation."""
        # Start date after end date
        with pytest.raises(ValueError, match="start date must be before end date"):
            IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 7),
                end=date(2025, 1, 1),  # End before start
            )

        # Same start and end date
        with pytest.raises(ValueError, match="start date must be before end date"):
            IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 1),
                end=date(2025, 1, 1),  # Same date
            )

    def test_batch_size_validation(self):
        """Test batch size validation."""
        # Too small
        with pytest.raises(ValueError):
            IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 1),
                end=date(2025, 1, 7),
                batch_size=0,
            )

        # Too large
        with pytest.raises(ValueError):
            IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 1),
                end=date(2025, 1, 7),
                batch_size=50000,
            )

    def test_provider_validation(self):
        """Test provider validation."""
        # Valid providers
        for provider in ["alpaca", "fake", "iex"]:
            config = IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 1),
                end=date(2025, 1, 7),
                provider=provider,
            )
            assert config.provider == provider.lower()

        # Invalid provider
        with pytest.raises(ValueError, match="Unknown provider"):
            IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 1),
                end=date(2025, 1, 7),
                provider="invalid",
            )

    def test_feed_type_validation(self):
        """Test feed type validation."""
        # Valid feed types
        for feed in ["iex", "sip"]:
            config = IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 1),
                end=date(2025, 1, 7),
                feed_type=feed,
            )
            assert config.feed_type == feed.lower()

        # Invalid feed type
        with pytest.raises(ValueError, match="Unknown feed type"):
            IngestionJobConfig(
                symbols=["AAPL"],
                start=date(2025, 1, 1),
                end=date(2025, 1, 7),
                feed_type="invalid",
            )

    def test_load_yaml_success(self):
        """Test successful YAML loading."""
        yaml_content = """
config_version: "1"
symbols: [AAPL, MSFT, NVDA]
start: 2025-06-01
end: 2025-06-07
batch_size: 1500
provider: alpaca
feed_type: iex
output_path: ./test_data
workers: 8
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = IngestionJobConfig.from_yaml(temp_path)

            assert config.symbols == ["AAPL", "MSFT", "NVDA"]
            assert config.start == date(2025, 6, 1)
            assert config.end == date(2025, 6, 7)
            assert config.batch_size == 1500
            assert config.provider == "alpaca"
            assert config.feed_type == "iex"
            assert config.output_path == "./test_data"
            assert config.workers == 8
        finally:
            Path(temp_path).unlink()

    def test_load_yaml_kebab_case(self):
        """Test YAML loading with kebab-case field names."""
        yaml_content = """
config_version: "1"
symbols: [AAPL, MSFT]
start: 2025-06-01
end: 2025-06-07
batch-size: 2000
feed-type: sip
output-path: ./kebab_data
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = IngestionJobConfig.from_yaml(temp_path)

            assert config.batch_size == 2000
            assert config.feed_type == "sip"
            assert config.output_path == "./kebab_data"
        finally:
            Path(temp_path).unlink()

    def test_load_yaml_file_not_found(self):
        """Test YAML loading with non-existent file."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            IngestionJobConfig.from_yaml("/non/existent/file.yaml")

    def test_load_yaml_invalid_yaml(self):
        """Test YAML loading with invalid YAML syntax."""
        invalid_yaml = """
config_version: "1"
symbols: [AAPL, MSFT
start: 2025-06-01
end: 2025-06-07
"""  # Missing closing bracket

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_yaml)
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Invalid YAML"):
                IngestionJobConfig.from_yaml(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_load_yaml_invalid_config(self):
        """Test YAML loading with invalid configuration values."""
        invalid_config = """
config_version: "1"
symbols: []  # Empty symbols list
start: 2025-06-01
end: 2025-06-07
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_config)
            temp_path = f.name

        try:
            with pytest.raises(ValueError):
                IngestionJobConfig.from_yaml(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_merge_overrides(self):
        """Test configuration override functionality."""
        base_config = IngestionJobConfig(
            symbols=["AAPL"],
            start=date(2025, 1, 1),
            end=date(2025, 1, 7),
            batch_size=1000,
            workers=4,
        )

        # Test overriding some values
        overridden_config = base_config.merge_overrides(batch_size=2000, workers=8, provider="iex")

        # Original should be unchanged
        assert base_config.batch_size == 1000
        assert base_config.workers == 4
        assert base_config.provider == "alpaca"

        # New config should have overrides
        assert overridden_config.batch_size == 2000
        assert overridden_config.workers == 8
        assert overridden_config.provider == "iex"

        # Unchanged values should remain
        assert overridden_config.symbols == ["AAPL"]
        assert overridden_config.start == date(2025, 1, 1)
        assert overridden_config.end == date(2025, 1, 7)

    def test_flag_override_yaml(self):
        """Test that CLI flags override YAML config values."""
        # Create a YAML config
        yaml_content = """
config_version: "1"
symbols: [AAPL, MSFT]
start: 2025-06-01
end: 2025-06-07
batch_size: 1000
workers: 4
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            # Load config from YAML
            config = IngestionJobConfig.from_yaml(temp_path)
            assert config.batch_size == 1000
            assert config.workers == 4

            # Override with CLI flags
            overridden_config = config.merge_overrides(
                batch_size=500, workers=8  # Override batch_size  # Override workers
            )

            # Assert that values were replaced
            assert overridden_config.batch_size == 500
            assert overridden_config.workers == 8

            # Assert that other values remain unchanged
            assert overridden_config.symbols == ["AAPL", "MSFT"]
            assert overridden_config.start == date(2025, 6, 1)
            assert overridden_config.end == date(2025, 6, 7)
        finally:
            Path(temp_path).unlink()

    def test_to_dict(self):
        """Test dictionary conversion."""
        config = IngestionJobConfig(symbols=["AAPL"], start=date(2025, 1, 1), end=date(2025, 1, 7))

        config_dict = config.to_dict()

        assert isinstance(config_dict, dict)
        assert config_dict["symbols"] == ["AAPL"]
        assert config_dict["start"] == date(2025, 1, 1)
        assert config_dict["end"] == date(2025, 1, 7)
        assert "batch_size" in config_dict
        assert "provider" in config_dict
