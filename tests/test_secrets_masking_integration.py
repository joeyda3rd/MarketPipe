"""Integration tests to verify API keys are properly masked in logs."""

from __future__ import annotations

import pytest
import logging
import os
from unittest.mock import patch, Mock
import httpx
from marketpipe.security.mask import mask
from marketpipe.ingestion.infrastructure.alpaca_client import AlpacaClient
from marketpipe.ingestion.infrastructure.models import ClientConfig
from marketpipe.ingestion.infrastructure.auth import HeaderTokenAuth
from marketpipe.ingestion.infrastructure.adapters import AlpacaMarketDataAdapter


class TestSecretsInLogs:
    """Test that API keys don't appear in logs during error conditions."""

    @pytest.fixture
    def test_api_key(self):
        return "ABCD1234EFGH5678"

    @pytest.fixture
    def test_api_secret(self):
        return "WXYZ9876IJKL4321"

    @pytest.fixture
    def alpaca_client(self, test_api_key):
        config = ClientConfig(
            api_key=test_api_key,
            base_url="https://data.alpaca.markets/v2"
        )
        auth = HeaderTokenAuth(test_api_key, "test_secret")
        return AlpacaClient(config=config, auth=auth)

    @pytest.fixture
    def alpaca_adapter(self, test_api_key, test_api_secret):
        return AlpacaMarketDataAdapter(
            api_key=test_api_key,
            api_secret=test_api_secret,
            base_url="https://data.alpaca.markets/v2",
            feed_type="iex"
        )

    def test_alpaca_client_json_parse_error_masks_key(self, alpaca_client, test_api_key, caplog):
        """Test that JSON parse errors don't expose API keys in logs."""
        
        # Ensure logging level captures warnings
        caplog.set_level(logging.WARNING)
        
        def mock_get(*args, **kwargs):
            response = Mock()
            response.status_code = 200
            response.json.side_effect = ValueError("Invalid JSON")
            response.text = f"Error response containing key: {test_api_key}"
            return response

        with patch('httpx.get', mock_get):
            with pytest.raises(RuntimeError):
                alpaca_client._request({"symbols": "AAPL"})

        # Verify no full API key in logs
        assert test_api_key not in caplog.text
        # Verify masked version is present
        masked_key = mask(test_api_key)
        assert masked_key in caplog.text

    def test_alpaca_client_retry_limit_error_masks_key(self, alpaca_client, test_api_key, caplog):
        """Test that retry limit errors don't expose API keys in response text."""
        
        def mock_get(*args, **kwargs):
            response = Mock()
            response.status_code = 500
            response.json.return_value = {"error": "Server error"}
            response.text = f"Server error with key {test_api_key} in response"
            return response

        with patch('httpx.get', mock_get):
            with patch.object(alpaca_client, 'should_retry', return_value=True):
                with patch.object(alpaca_client.config, 'max_retries', 0):  # Force retry limit
                    with pytest.raises(RuntimeError):
                        alpaca_client._request({"symbols": "AAPL"})

        # Verify no full API key in logs or error message
        assert test_api_key not in caplog.text
        # Verify masked version might be present
        masked_key = mask(test_api_key)
        # Check if any error message contains our masked key
        error_logs = [record.message for record in caplog.records if record.levelname == "ERROR"]
        if error_logs:
            assert any(masked_key in msg for msg in error_logs)

    def test_adapter_fetch_error_masks_credentials(self, alpaca_adapter, test_api_key, test_api_secret, caplog):
        """Test that adapter fetch errors don't expose API credentials."""
        
        # Mock the alpaca client to raise an exception with credentials
        error_msg = f"Authentication failed: invalid key {test_api_key} or secret {test_api_secret}"
        
        with patch.object(alpaca_adapter._alpaca_client, 'fetch_batch', side_effect=Exception(error_msg)):
            from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
            
            symbol = Symbol.from_string("AAPL")
            time_range = TimeRange(
                start=Timestamp.from_iso("2024-01-01T09:30:00Z"),
                end=Timestamp.from_iso("2024-01-01T16:00:00Z")
            )
            
            with pytest.raises(Exception) as exc_info:  # MarketDataProviderError
                import asyncio
                asyncio.run(alpaca_adapter.fetch_bars_for_symbol(symbol, time_range))

        # Verify no full credentials appear anywhere (logs or exception messages)
        assert test_api_key not in caplog.text
        assert test_api_secret not in caplog.text
        
        # Verify the exception message itself is masked
        exception_message = str(exc_info.value)
        assert test_api_key not in exception_message
        assert test_api_secret not in exception_message
        
        # Verify masked versions are in the exception message
        masked_key = mask(test_api_key)
        masked_secret = mask(test_api_secret)
        assert masked_key in exception_message or masked_secret in exception_message

    def test_adapter_bar_translation_error_masks_credentials(self, alpaca_adapter, test_api_key, test_api_secret, caplog):
        """Test that bar translation errors don't expose API credentials."""
        
        # Mock successful fetch but failing translation
        # Use invalid data that will cause translation to fail and include credentials in error
        bad_bar_data = {
            "timestamp": 1704110400000000000,  # Valid timestamp
            "open": f"price_with_key_{test_api_key}",  # Invalid price containing key
            "high": 100.0,
            "low": 99.0,
            "close": 99.5,
            "volume": 1000
        }
        
        with patch.object(alpaca_adapter._alpaca_client, 'fetch_batch', return_value=[bad_bar_data]):
            from marketpipe.domain.value_objects import Symbol, TimeRange, Timestamp
            
            symbol = Symbol.from_string("AAPL")
            time_range = TimeRange(
                start=Timestamp.from_iso("2024-01-01T09:30:00Z"),
                end=Timestamp.from_iso("2024-01-01T16:00:00Z")
            )
            
            # This should not raise but should log warnings
            import asyncio
            result = asyncio.run(alpaca_adapter.fetch_bars_for_symbol(symbol, time_range))
            
            # Should return empty list due to translation failures
            assert result == []

        # Verify no full credentials in logs
        assert test_api_key not in caplog.text
        assert test_api_secret not in caplog.text
        
        # Verify there are warning logs (translation failures should generate warnings)
        warning_logs = [record.message for record in caplog.records if record.levelname == "WARNING"]
        assert len(warning_logs) > 0  # Should have warnings from translation failures
        
        # The actual error might not contain our test credentials, but safe_for_log should work
        # Let's just verify the mechanism is in place by checking logs don't contain full credentials
        for warning_msg in warning_logs:
            assert test_api_key not in warning_msg
            assert test_api_secret not in warning_msg

    def test_no_hardcoded_test_keys_in_production_logs(self, caplog):
        """Test that our test keys don't accidentally appear in real logs."""
        test_keys = [
            "ABCD1234EFGH5678",
            "WXYZ9876IJKL4321", 
            "ABCD1234EFGH",  # From our unit tests
            "WXYZ5678IJKL"   # From our unit tests
        ]
        
        # Run some normal operations that might log
        from marketpipe.security.mask import mask, safe_for_log
        
        # Test normal masking operations
        for key in test_keys:
            masked = mask(key)
            safe_msg = safe_for_log(f"Using key: {key}", key)
            
            # These operations shouldn't create log entries with full keys
            assert key not in masked
            assert key not in safe_msg

        # Verify no test keys appeared in any logs during this test
        for key in test_keys:
            assert key not in caplog.text

    def test_environment_variable_masking(self, monkeypatch, caplog):
        """Test that environment variables are properly masked when logged."""
        test_key = "ENV_ABCD1234EFGH5678"
        
        # Set environment variable
        monkeypatch.setenv("TEST_API_KEY", test_key)
        
        # Simulate loading from environment and logging
        loaded_key = os.getenv("TEST_API_KEY")
        from marketpipe.security.mask import safe_for_log
        
        # Simulate an error that might log the environment variable
        error_msg = f"Configuration error with key from environment: {loaded_key}"
        safe_msg = safe_for_log(error_msg, loaded_key)
        
        logger = logging.getLogger("test_env")
        logger.error(safe_msg)
        
        # Verify environment variable key doesn't appear in logs
        assert test_key not in caplog.text
        # Verify masked version appears
        masked_key = mask(test_key)
        assert masked_key in caplog.text 