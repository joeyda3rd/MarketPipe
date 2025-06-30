"""Tests for security masking utilities."""

from marketpipe.security.mask import mask, safe_for_log


class TestMask:
    """Test the mask function."""

    def test_mask_normal_key(self):
        """Test masking a normal API key."""
        result = mask("ABCD1234EFGH")
        assert result == "********EFGH"

    def test_mask_longer_key(self):
        """Test masking a longer API key."""
        result = mask("ABCDEFGH1234567890WXYZ")
        assert result == "******************WXYZ"

    def test_mask_custom_show_amount(self):
        """Test masking with custom show amount."""
        result = mask("ABCD1234EFGH", show=6)
        assert result == "******34EFGH"

    def test_mask_empty_string(self):
        """Test masking empty string."""
        result = mask("")
        assert result == "***"

    def test_mask_none(self):
        """Test masking None value."""
        result = mask(None)
        assert result == "***"

    def test_mask_short_string(self):
        """Test masking string that's too short."""
        result = mask("short")
        assert result == "***"

    def test_mask_very_short_string(self):
        """Test masking very short string."""
        result = mask("ab")
        assert result == "***"

    def test_mask_exactly_minimum_length(self):
        """Test masking string at minimum length threshold."""
        # show=4, so minimum length is 4+2+1=7
        result = mask("1234567")
        assert result == "***4567"

    def test_mask_non_alphanumeric_characters(self):
        """Test masking string with non-alphanumeric characters."""
        # This won't match the regex pattern, so fallback is used
        result = mask("ABC-DEF-GHI-JKL")
        assert result == "***********-JKL"

    def test_mask_only_letters(self):
        """Test masking string with only letters."""
        result = mask("ABCDEFGHIJKL")
        assert result == "********IJKL"

    def test_mask_only_numbers(self):
        """Test masking string with only numbers."""
        result = mask("123456789012")
        assert result == "********9012"


class TestSafeForLog:
    """Test the safe_for_log function."""

    def test_safe_for_log_single_secret(self):
        """Test safe logging with single secret."""
        result = safe_for_log("API key is ABCD1234EFGH", "ABCD1234EFGH")
        assert result == "API key is ********EFGH"

    def test_safe_for_log_multiple_secrets(self):
        """Test safe logging with multiple secrets."""
        result = safe_for_log(
            "Error with key1: ABCD1234EFGH and key2: WXYZ5678IJKL", "ABCD1234EFGH", "WXYZ5678IJKL"
        )
        assert result == "Error with key1: ********EFGH and key2: ********IJKL"

    def test_safe_for_log_no_secrets_in_message(self):
        """Test safe logging when message doesn't contain secrets."""
        result = safe_for_log("This is a normal log message", "SECRETKEY123")
        assert result == "This is a normal log message"

    def test_safe_for_log_empty_secret(self):
        """Test safe logging with empty secret."""
        result = safe_for_log("Log message with key", "")
        assert result == "Log message with key"

    def test_safe_for_log_none_secret(self):
        """Test safe logging with None secret."""
        result = safe_for_log("Log message with key", None)
        assert result == "Log message with key"

    def test_safe_for_log_multiple_occurrences(self):
        """Test safe logging with multiple occurrences of same secret."""
        result = safe_for_log("First: ABCD1234EFGH, Second: ABCD1234EFGH", "ABCD1234EFGH")
        assert result == "First: ********EFGH, Second: ********EFGH"

    def test_safe_for_log_overlapping_secrets(self):
        """Test safe logging with overlapping secrets."""
        result = safe_for_log(
            "Key is ABCD1234EFGH and EFGH1234WXYZ", "ABCD1234EFGH", "EFGH1234WXYZ"
        )
        assert result == "Key is ********EFGH and ********WXYZ"

    def test_safe_for_log_complex_log_message(self):
        """Test safe logging with complex log message format."""
        log_msg = "2024-01-01 10:30:00 ERROR [AlpacaClient] Authentication failed with key=ABCD1234EFGH, retrying..."
        result = safe_for_log(log_msg, "ABCD1234EFGH")
        expected = "2024-01-01 10:30:00 ERROR [AlpacaClient] Authentication failed with key=********EFGH, retrying..."
        assert result == expected


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_mask_zero_show(self):
        """Test masking with show=0."""
        result = mask("ABCD1234EFGH", show=0)
        # With show=0, minimum length is 0+2+1=3, so "ABCD1234EFGH" (12 chars) should be masked
        assert result == "************"

    def test_mask_show_larger_than_string(self):
        """Test masking with show larger than string length."""
        result = mask("ABCD", show=10)
        assert result == "***"

    def test_safe_for_log_with_traceback_like_text(self):
        """Test safe logging with traceback-like text."""
        traceback_text = """
        Traceback (most recent call last):
          File "client.py", line 42, in authenticate
            response = requests.post(url, headers={'Authorization': 'Bearer ABCD1234EFGH'})
        HTTPError: 401 Unauthorized
        """
        result = safe_for_log(traceback_text, "ABCD1234EFGH")
        assert "ABCD1234EFGH" not in result
        assert "********EFGH" in result
