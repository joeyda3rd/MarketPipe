# Security Guidelines

## API Key and Secret Masking

MarketPipe includes utilities to prevent sensitive information like API keys and secrets from appearing in logs, stack traces, or error messages.

### Using the Masking Utility

The `marketpipe.security.mask` module provides two key functions:

#### `mask(value, show=4)`

Masks a secret string, showing only the last `show` characters:

```python
from marketpipe.security.mask import mask

# Example API key masking
api_key = "ABCD1234EFGH5678"
masked = mask(api_key)
print(masked)  # Output: "************5678"

# Custom number of visible characters
masked = mask(api_key, show=6)
print(masked)  # Output: "**********H5678"
```

#### `safe_for_log(message, *secrets)`

Replaces any secrets in a log message with masked versions:

```python
from marketpipe.security.mask import safe_for_log

api_key = "ABCD1234EFGH5678"
api_secret = "WXYZ9876IJKL"

# Safe logging of error messages
error_msg = f"Authentication failed with key {api_key} and secret {api_secret}"
safe_msg = safe_for_log(error_msg, api_key, api_secret)
logger.error(safe_msg)
# Logs: "Authentication failed with key ************5678 and secret ********IJKL"
```

### Best Practices

1. **Always use `safe_for_log()` when logging error messages** that might contain API responses or exception details.

2. **Mask credentials in exception messages** before raising them:
   ```python
   try:
       response = api_client.request(params)
   except Exception as e:
       safe_msg = safe_for_log(f"API request failed: {e}", api_key, api_secret)
       raise RuntimeError(safe_msg) from e
   ```

3. **Use `safe_for_log()` with traceback information**:
   ```python
   import traceback

   try:
       # Some operation that might fail
       pass
   except Exception as e:
       tb = traceback.format_exc()
       safe_tb = safe_for_log(tb, api_key, api_secret)
       logger.error(f"Unexpected error: {safe_tb}")
   ```

4. **Test logging in integration tests** to ensure no full API keys appear:
   ```python
   def test_no_api_keys_in_logs(caplog):
       # Test some functionality that might log errors
       with pytest.raises(SomeException):
           some_function_that_might_log_api_keys()

       # Ensure no full API keys appear in logs
       assert "ABCD1234EFGH5678" not in caplog.text
       # Masked versions should be present
       assert "************5678" in caplog.text
   ```

### Coverage

The following areas have been updated to use secure logging:

- **Alpaca Client**: Error responses and retry limit messages mask API keys
- **Market Data Adapters**: Exception handling masks API keys and secrets
- **Authentication Modules**: Error messages mask credentials

### Environment Variables

Store credentials in environment variables and load them securely:

```bash
# .env file
ALPACA_KEY=your_api_key_here
ALPACA_SECRET=your_api_secret_here
```

```python
import os
from marketpipe.security.mask import mask

api_key = os.getenv("ALPACA_KEY")
if not api_key:
    raise ValueError("ALPACA_KEY environment variable not set")

# Log safely if needed
logger.info(f"Using API key: {mask(api_key)}")
```

### Testing

Always include tests that verify no sensitive information leaks into logs:

```python
def test_error_handling_masks_secrets(caplog):
    """Ensure error handling doesn't expose API keys."""
    api_key = "ABCD1234EFGH5678"

    with pytest.raises(Exception):
        # Code that handles errors with potential API key exposure
        pass

    # Verify no full API key in logs
    assert api_key not in caplog.text
    # Verify masked version might be present
    if any("API" in record.message for record in caplog.records):
        assert mask(api_key) in caplog.text
```

## Additional Security Considerations

- Never hardcode API keys in source code
- Use environment variables or secure configuration management
- Regularly rotate API keys and secrets
- Monitor logs for any accidental exposure of sensitive data
- Consider using more restrictive API key permissions when available
