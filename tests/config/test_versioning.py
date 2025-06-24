# SPDX-License-Identifier: Apache-2.0
"""Tests for configuration versioning."""

from __future__ import annotations

import warnings
from datetime import date
from pathlib import Path

import pytest
import yaml

from marketpipe.config import CURRENT_CONFIG_VERSION, MIN_SUPPORTED_VERSION, IngestionJobConfig
from marketpipe.config.loader import ConfigVersionError, load_config


def _create_yaml_file(data: dict, path: Path) -> Path:
    """Helper to create a YAML file with given data."""
    path.write_text(yaml.safe_dump(data))
    return path


@pytest.fixture
def base_config() -> dict:
    """Base configuration for testing."""
    return {
        "config_version": "1",
        "symbols": ["AAPL", "MSFT"],
        "start": "2024-01-01",
        "end": "2024-01-02",
        "provider": "alpaca",
        "feed_type": "iex",
        "output_path": "./data",
        "workers": 3,
        "batch_size": 1000,
    }


def test_valid_version(tmp_path: Path, base_config: dict):
    """Test loading configuration with valid version."""
    config_file = _create_yaml_file(base_config, tmp_path / "config.yml")

    config = load_config(config_file)

    assert config.config_version == "1"
    assert config.symbols == ["AAPL", "MSFT"]
    assert config.start == date(2024, 1, 1)
    assert config.end == date(2024, 1, 2)


def test_missing_version_raises_error(tmp_path: Path, base_config: dict):
    """Test that missing config_version raises ConfigVersionError."""
    # Remove config_version
    config_data = base_config.copy()
    del config_data["config_version"]

    config_file = _create_yaml_file(config_data, tmp_path / "config.yml")

    with pytest.raises(ConfigVersionError) as exc_info:
        load_config(config_file)

    assert "config_version missing" in str(exc_info.value)
    assert 'config_version: "1"' in str(exc_info.value)


def test_too_old_version_raises_error(tmp_path: Path, base_config: dict):
    """Test that too old version raises ConfigVersionError."""
    config_data = base_config.copy()
    config_data["config_version"] = "0"  # Older than MIN_SUPPORTED_VERSION

    config_file = _create_yaml_file(config_data, tmp_path / "config.yml")

    with pytest.raises(ConfigVersionError) as exc_info:
        load_config(config_file)

    assert "too old" in str(exc_info.value)
    assert "Minimum supported is" in str(exc_info.value)


def test_future_version_warns_but_continues(tmp_path: Path, base_config: dict):
    """Test that future version warns but continues parsing."""
    config_data = base_config.copy()
    config_data["config_version"] = "2"  # Higher than CURRENT_CONFIG_VERSION

    config_file = _create_yaml_file(config_data, tmp_path / "config.yml")

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        config = load_config(config_file)

        # Should have warning
        assert len(w) == 1
        assert issubclass(w[0].category, UserWarning)
        assert "best-effort parse" in str(w[0].message)

        # But should still parse successfully
        assert config.config_version == "2"
        assert config.symbols == ["AAPL", "MSFT"]


def test_unknown_keys_rejected(tmp_path: Path, base_config: dict):
    """Test that unknown keys are rejected due to extra='forbid'."""
    config_data = base_config.copy()
    config_data["unknown_field"] = "should_fail"

    config_file = _create_yaml_file(config_data, tmp_path / "config.yml")

    with pytest.raises(ValueError) as exc_info:
        load_config(config_file)

    # Should mention the extra field
    error_msg = str(exc_info.value).lower()
    assert "extra" in error_msg or "unknown" in error_msg or "forbidden" in error_msg


def test_kebab_case_normalization(tmp_path: Path, base_config: dict):
    """Test that kebab-case keys are normalized to snake_case."""
    # Start with minimal config and use only kebab-case
    config_data = {
        "config-version": "1",
        "symbols": ["AAPL", "MSFT"],
        "start": "2024-01-01",
        "end": "2024-01-02",
        "batch-size": 500,
        "feed-type": "sip",
        "output-path": "./custom_data",
        "provider": "alpaca",
        "workers": 3,
    }

    config_file = _create_yaml_file(config_data, tmp_path / "config.yml")

    config = load_config(config_file)

    assert config.batch_size == 500
    assert config.feed_type == "sip"
    assert config.output_path == "./custom_data"
    assert config.config_version == "1"


def test_environment_variable_expansion(tmp_path: Path, base_config: dict, monkeypatch):
    """Test that environment variables are expanded in the YAML."""
    # Set environment variable
    monkeypatch.setenv("TEST_OUTPUT_PATH", "/tmp/test_data")

    config_data = base_config.copy()
    config_data["output_path"] = "$TEST_OUTPUT_PATH"

    config_file = _create_yaml_file(config_data, tmp_path / "config.yml")

    config = load_config(config_file)

    assert config.output_path == "/tmp/test_data"


def test_invalid_yaml_raises_error(tmp_path: Path):
    """Test that invalid YAML raises appropriate error."""
    config_file = tmp_path / "invalid.yml"
    config_file.write_text("invalid: yaml: content: [unclosed bracket")

    with pytest.raises(ValueError) as exc_info:
        load_config(config_file)

    assert "Invalid YAML" in str(exc_info.value)


def test_nonexistent_file_raises_error(tmp_path: Path):
    """Test that nonexistent file raises FileNotFoundError."""
    nonexistent_file = tmp_path / "does_not_exist.yml"

    with pytest.raises(FileNotFoundError) as exc_info:
        load_config(nonexistent_file)

    assert "Configuration file not found" in str(exc_info.value)


def test_non_dict_yaml_raises_error(tmp_path: Path):
    """Test that YAML that doesn't contain a dict raises error."""
    config_file = tmp_path / "list.yml"
    config_file.write_text("- item1\n- item2\n")

    with pytest.raises(ValueError) as exc_info:
        load_config(config_file)

    assert "dictionary at the root level" in str(exc_info.value)


def test_backward_compatibility_with_from_yaml(tmp_path: Path, base_config: dict):
    """Test that IngestionJobConfig.from_yaml still works and uses new loader."""
    config_file = _create_yaml_file(base_config, tmp_path / "config.yml")

    # This should work and use the new loader internally
    config = IngestionJobConfig.from_yaml(config_file)

    assert config.config_version == "1"
    assert config.symbols == ["AAPL", "MSFT"]


def test_backward_compatibility_missing_version_from_yaml(tmp_path: Path, base_config: dict):
    """Test that from_yaml properly propagates ConfigVersionError."""
    config_data = base_config.copy()
    del config_data["config_version"]

    config_file = _create_yaml_file(config_data, tmp_path / "config.yml")

    with pytest.raises(ConfigVersionError):
        IngestionJobConfig.from_yaml(config_file)


def test_version_constants_are_consistent():
    """Test that version constants are properly defined."""
    assert CURRENT_CONFIG_VERSION == "1"
    assert MIN_SUPPORTED_VERSION == "1"
    assert MIN_SUPPORTED_VERSION <= CURRENT_CONFIG_VERSION


def test_config_model_has_correct_defaults():
    """Test that the model has correct default values."""
    # Should be able to create with minimal config
    config = IngestionJobConfig(symbols=["AAPL"], start=date(2024, 1, 1), end=date(2024, 1, 2))

    assert config.config_version == CURRENT_CONFIG_VERSION
    assert config.provider == "alpaca"
    assert config.feed_type == "iex"
    assert config.output_path == "./data"
    assert config.workers == 4
    assert config.batch_size == 1000


def test_pydantic_forbids_extra_fields():
    """Test that Pydantic model directly forbids extra fields."""
    with pytest.raises(ValueError) as exc_info:
        IngestionJobConfig(
            symbols=["AAPL"],
            start=date(2024, 1, 1),
            end=date(2024, 1, 2),
            unknown_field="should_fail",  # This should be rejected
        )

    error_msg = str(exc_info.value).lower()
    assert "extra" in error_msg or "forbidden" in error_msg
