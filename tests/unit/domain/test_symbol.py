"""Unit tests for SymbolRecord Pydantic model.

These tests validate the SymbolRecord model against the specification,
ensuring field validation, business rules, and serialization work correctly.
Coverage target: >= 95%
"""

from __future__ import annotations

import datetime

import pytest
from pydantic import ValidationError

from marketpipe.domain import AssetClass, Status, SymbolRecord

# Test fixtures
SAMPLE_JSON = {
    "ticker": "AAPL",
    "exchange_mic": "XNAS",
    "asset_class": "EQUITY",
    "currency": "USD",
    "status": "ACTIVE",
    "figi": "BBG000B9XRY4",
    "cusip": "037833100",
    "isin": "US0378331005",
    "cik": "320193",
    "country": "US",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "first_trade_date": "1980-12-12",
    "shares_outstanding": 15550061000,
    "free_float": 15500000000,
    "company_name": "Apple Inc.",
    "as_of": "2024-01-01",
}

SAMPLE_CSV = {
    "ticker": " googl ",  # Test whitespace trimming
    "exchange_mic": "xnas",  # Test case conversion
    "asset_class": "EQUITY",
    "currency": "usd",  # Test case conversion
    "status": "ACTIVE",
    "figi": "",  # Test empty string -> None
    "cusip": "",  # Test empty string -> None
    "country": "us",  # Test case conversion
    "company_name": "  Alphabet Inc.  ",  # Test whitespace trimming
    "as_of": "2024-01-01",
}


class TestSymbolRecordValidation:
    """Test field validation and coercion."""

    def test_valid_sample_json_passes(self):
        """Load SAMPLE_JSON fixture and validate successfully."""
        record = SymbolRecord(**SAMPLE_JSON)

        assert record.ticker == "AAPL"
        assert record.exchange_mic == "XNAS"
        assert record.asset_class == AssetClass.EQUITY
        assert record.currency == "USD"
        assert record.status == Status.ACTIVE
        assert record.figi == "BBG000B9XRY4"
        assert record.cusip == "037833100"
        assert record.isin == "US0378331005"
        assert record.cik == "0000320193"  # Zero-padded
        assert record.country == "US"
        assert record.sector == "Technology"
        assert record.industry == "Consumer Electronics"
        assert record.first_trade_date == datetime.date(1980, 12, 12)
        assert record.shares_outstanding == 15550061000
        assert record.free_float == 15500000000
        assert record.company_name == "Apple Inc."
        assert record.as_of == datetime.date(2024, 1, 1)

    def test_empty_strings_become_none(self):
        """Pass empty strings for optional IDs, assert they are None."""
        data = {
            **SAMPLE_JSON,
            "figi": "",
            "cusip": "",
            "isin": "",
            "cik": "",
            "country": "",
            "sector": "",
            "industry": "",
            "company_name": "",
        }

        record = SymbolRecord(**data)

        assert record.figi is None
        assert record.cusip is None
        assert record.isin is None
        assert record.cik is None
        assert record.country is None
        assert record.sector is None
        assert record.industry is None
        assert record.company_name is None

    def test_enum_validation_fails(self):
        """Pass invalid asset_class, expect ValidationError."""
        data = {**SAMPLE_JSON, "asset_class": "INVALID"}

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)

        error = exc_info.value
        assert "asset_class" in str(error)

    def test_status_enum_validation_fails(self):
        """Pass invalid status, expect ValidationError."""
        data = {**SAMPLE_JSON, "status": "INVALID"}

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)

        error = exc_info.value
        assert "status" in str(error)

    def test_date_order_validation(self):
        """Pass delist_date before first_trade_date, expect ValidationError."""
        data = {
            **SAMPLE_JSON,
            "first_trade_date": "2020-01-01",
            "delist_date": "2019-12-31",  # Before first_trade_date
        }

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)

        error = exc_info.value
        assert "delist_date must be >= first_trade_date" in str(error)

    def test_valid_date_order_passes(self):
        """Pass valid date order, expect success."""
        data = {
            **SAMPLE_JSON,
            "first_trade_date": "2020-01-01",
            "delist_date": "2020-01-02",  # After first_trade_date
        }

        record = SymbolRecord(**data)
        assert record.first_trade_date == datetime.date(2020, 1, 1)
        assert record.delist_date == datetime.date(2020, 1, 2)

    def test_free_float_constraint_validation(self):
        """Pass free_float > shares_outstanding, expect ValidationError."""
        data = {
            **SAMPLE_JSON,
            "shares_outstanding": 1000,
            "free_float": 1500,  # Greater than shares_outstanding
        }

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)

        error = exc_info.value
        assert "free_float cannot exceed shares_outstanding" in str(error)

    def test_round_trip_dict(self):
        """Create record, call .model_dump(), then reconstruct, assert equality."""
        original = SymbolRecord(**SAMPLE_JSON)
        dump = original.model_dump()
        reconstructed = SymbolRecord(**dump)

        assert original == reconstructed
        assert original.model_dump() == reconstructed.model_dump()


class TestFieldValidation:
    """Test individual field validators."""

    def test_ticker_validation(self):
        """Test ticker field validation and coercion."""
        base_data = {**SAMPLE_JSON}

        # Test whitespace trimming and uppercasing
        base_data["ticker"] = " aapl "
        record = SymbolRecord(**base_data)
        assert record.ticker == "AAPL"

        # Test empty ticker fails
        base_data["ticker"] = ""
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "ticker cannot be empty" in str(exc_info.value)

        # Test whitespace-only ticker fails
        base_data["ticker"] = "   "
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "ticker cannot be empty" in str(exc_info.value)

    def test_figi_validation(self):
        """Test FIGI field validation."""
        base_data = {**SAMPLE_JSON}

        # Test valid 12-character FIGI
        base_data["figi"] = "BBG000B9XRY4"
        record = SymbolRecord(**base_data)
        assert record.figi == "BBG000B9XRY4"

        # Test invalid length fails
        base_data["figi"] = "BBG000B9X"  # Too short
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "FIGI must be exactly 12 characters" in str(exc_info.value)

        # Test None/empty string becomes None
        base_data["figi"] = None
        record = SymbolRecord(**base_data)
        assert record.figi is None

    def test_cusip_validation(self):
        """Test CUSIP field validation."""
        base_data = {**SAMPLE_JSON}

        # Test valid 9-character CUSIP
        base_data["cusip"] = "037833100"
        record = SymbolRecord(**base_data)
        assert record.cusip == "037833100"

        # Test invalid length fails
        base_data["cusip"] = "037833"  # Too short
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "CUSIP must be exactly 9 characters" in str(exc_info.value)

    def test_isin_validation(self):
        """Test ISIN field validation."""
        base_data = {**SAMPLE_JSON}

        # Test valid 12-character ISIN
        base_data["isin"] = "US0378331005"
        record = SymbolRecord(**base_data)
        assert record.isin == "US0378331005"

        # Test invalid length fails
        base_data["isin"] = "US037833100"  # Too short
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "ISIN must be exactly 12 characters" in str(exc_info.value)

    def test_cik_validation(self):
        """Test CIK field validation and zero-padding."""
        base_data = {**SAMPLE_JSON}

        # Test digits-only and zero-padding
        base_data["cik"] = "320193"
        record = SymbolRecord(**base_data)
        assert record.cik == "0000320193"  # Zero-padded to 10 chars

        # Test non-digits fail
        base_data["cik"] = "32019A"
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "CIK must contain only digits" in str(exc_info.value)

        # Test already 10 digits
        base_data["cik"] = "0000320193"
        record = SymbolRecord(**base_data)
        assert record.cik == "0000320193"

    def test_exchange_mic_validation(self):
        """Test exchange MIC field validation."""
        base_data = {**SAMPLE_JSON}

        # Test case conversion and 4-character requirement
        base_data["exchange_mic"] = "xnas"
        record = SymbolRecord(**base_data)
        assert record.exchange_mic == "XNAS"

        # Test invalid length fails
        base_data["exchange_mic"] = "XNA"  # Too short
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "exchange_mic must be exactly 4 characters" in str(exc_info.value)

        # Test empty fails
        base_data["exchange_mic"] = ""
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "exchange_mic cannot be empty" in str(exc_info.value)

    def test_currency_validation(self):
        """Test currency field validation."""
        base_data = {**SAMPLE_JSON}

        # Test case conversion and 3-character requirement
        base_data["currency"] = "usd"
        record = SymbolRecord(**base_data)
        assert record.currency == "USD"

        # Test invalid length fails
        base_data["currency"] = "US"  # Too short
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "currency must be exactly 3 characters" in str(exc_info.value)

    def test_country_validation(self):
        """Test country field validation."""
        base_data = {**SAMPLE_JSON}

        # Test case conversion and 2-character requirement
        base_data["country"] = "us"
        record = SymbolRecord(**base_data)
        assert record.country == "US"

        # Test invalid length fails
        base_data["country"] = "USA"  # Too long
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "country must be exactly 2 characters" in str(exc_info.value)

        # Test empty string becomes None
        base_data["country"] = ""
        record = SymbolRecord(**base_data)
        assert record.country is None

    def test_shares_outstanding_validation(self):
        """Test shares outstanding validation."""
        base_data = {**SAMPLE_JSON}

        # Test positive value (ensure free_float is compatible)
        base_data["shares_outstanding"] = 1000
        base_data["free_float"] = 800  # Less than shares_outstanding
        record = SymbolRecord(**base_data)
        assert record.shares_outstanding == 1000

        # Test zero/negative fails
        base_data["shares_outstanding"] = 0
        base_data["free_float"] = None  # Remove free_float to avoid constraint
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "shares_outstanding must be positive" in str(exc_info.value)

        base_data["shares_outstanding"] = -100
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "shares_outstanding must be positive" in str(exc_info.value)

    def test_free_float_validation(self):
        """Test free float validation."""
        base_data = {**SAMPLE_JSON}

        # Test positive value
        base_data["free_float"] = 1000
        record = SymbolRecord(**base_data)
        assert record.free_float == 1000

        # Test zero is allowed
        base_data["free_float"] = 0
        record = SymbolRecord(**base_data)
        assert record.free_float == 0

        # Test negative fails
        base_data["free_float"] = -100
        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**base_data)
        assert "free_float cannot be negative" in str(exc_info.value)


class TestProviderMethods:
    """Test provider-specific methods."""

    def test_from_provider_generic(self):
        """Test from_provider with generic provider."""
        payload = {**SAMPLE_JSON}
        as_of = datetime.date(2024, 1, 1)

        record = SymbolRecord.from_provider(payload, provider="generic", as_of=as_of)

        assert record.ticker == "AAPL"
        assert record.as_of == as_of
        assert record.status == Status.ACTIVE  # Default value applied

    def test_from_provider_polygon_mapping(self):
        """Test from_provider with Polygon field mapping."""
        polygon_payload = {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "market": "XNAS",
            "type": "EQUITY",
            "currency_name": "USD",
            "composite_figi": "BBG000B9XRY4",
            "cik": "320193",
        }
        as_of = datetime.date(2024, 1, 1)

        record = SymbolRecord.from_provider(polygon_payload, provider="polygon", as_of=as_of)

        assert record.ticker == "AAPL"
        assert record.company_name == "Apple Inc."
        assert record.exchange_mic == "XNAS"
        assert record.asset_class == AssetClass.EQUITY
        assert record.currency == "USD"
        assert record.figi == "BBG000B9XRY4"
        assert record.cik == "0000320193"
        assert record.as_of == as_of
        assert record.status == Status.ACTIVE  # Default applied

    def test_from_provider_nasdaq_mapping(self):
        """Test from_provider with NASDAQ field mapping."""
        nasdaq_payload = {
            "Symbol": "GOOGL",
            "Security Name": "Alphabet Inc.",
            "Round Lot Size": "100",  # This will be preserved in meta
        }
        as_of = datetime.date(2024, 1, 1)

        # Add required fields that NASDAQ mapping doesn't provide
        nasdaq_payload.update(
            {
                "asset_class": "EQUITY",
                "currency": "USD",
                "exchange_mic": "XNAS",  # Valid exchange MIC
                "status": "ACTIVE",  # Valid status
            }
        )

        record = SymbolRecord.from_provider(nasdaq_payload, provider="nasdaq", as_of=as_of)

        assert record.ticker == "GOOGL"
        assert record.company_name == "Alphabet Inc."
        assert record.exchange_mic == "XNAS"
        assert record.asset_class == AssetClass.EQUITY
        assert record.currency == "USD"
        assert record.status == Status.ACTIVE
        assert record.as_of == as_of
        assert "Round Lot Size" in record.meta  # Preserved in meta


class TestSerialization:
    """Test serialization methods."""

    def test_to_parquet_row(self):
        """Test conversion to Parquet-compatible dict."""
        record = SymbolRecord(**SAMPLE_JSON)
        parquet_row = record.to_parquet_row()

        # Check enum conversion
        assert parquet_row["asset_class"] == "EQUITY"
        assert parquet_row["status"] == "ACTIVE"

        # Check date conversion
        assert parquet_row["first_trade_date"] == "1980-12-12"
        assert parquet_row["as_of"] == "2024-01-01"

        # Check other fields preserved
        assert parquet_row["ticker"] == "AAPL"
        assert parquet_row["figi"] == "BBG000B9XRY4"

    def test_from_parquet_row(self):
        """Test creation from Parquet row data."""
        # Start with a record, convert to Parquet, then back
        original = SymbolRecord(**SAMPLE_JSON)
        parquet_row = original.to_parquet_row()

        # Reconstruct from Parquet data
        reconstructed = SymbolRecord.from_parquet_row(parquet_row)

        assert reconstructed.ticker == original.ticker
        assert reconstructed.asset_class == original.asset_class
        assert reconstructed.status == original.status
        assert reconstructed.first_trade_date == original.first_trade_date
        assert reconstructed.as_of == original.as_of
        assert reconstructed == original

    def test_parquet_round_trip(self):
        """Test full round-trip through Parquet serialization."""
        original = SymbolRecord(**SAMPLE_JSON)

        # Round trip
        parquet_data = original.to_parquet_row()
        reconstructed = SymbolRecord.from_parquet_row(parquet_data)

        assert original == reconstructed
        assert original.model_dump() == reconstructed.model_dump()


class TestRequiredFields:
    """Test required field validation."""

    def test_missing_required_fields_fail(self):
        """Test that missing required fields cause validation errors."""
        # Test missing ticker
        data = {**SAMPLE_JSON}
        del data["ticker"]

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)
        assert "ticker" in str(exc_info.value)

        # Test missing exchange_mic
        data = {**SAMPLE_JSON}
        del data["exchange_mic"]

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)
        assert "exchange_mic" in str(exc_info.value)

        # Test missing asset_class
        data = {**SAMPLE_JSON}
        del data["asset_class"]

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)
        assert "asset_class" in str(exc_info.value)

        # Test missing currency
        data = {**SAMPLE_JSON}
        del data["currency"]

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)
        assert "currency" in str(exc_info.value)

        # Test missing status
        data = {**SAMPLE_JSON}
        del data["status"]

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)
        assert "status" in str(exc_info.value)

        # Test missing as_of
        data = {**SAMPLE_JSON}
        del data["as_of"]

        with pytest.raises(ValidationError) as exc_info:
            SymbolRecord(**data)
        assert "as_of" in str(exc_info.value)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_minimal_required_record(self):
        """Test record with only required fields."""
        minimal_data = {
            "ticker": "TEST",
            "exchange_mic": "XNAS",
            "asset_class": "EQUITY",
            "currency": "USD",
            "status": "ACTIVE",
            "as_of": "2024-01-01",
        }

        record = SymbolRecord(**minimal_data)

        assert record.ticker == "TEST"
        assert record.exchange_mic == "XNAS"
        assert record.asset_class == AssetClass.EQUITY
        assert record.currency == "USD"
        assert record.status == Status.ACTIVE
        assert record.as_of == datetime.date(2024, 1, 1)

        # All optional fields should be None
        assert record.id is None
        assert record.figi is None
        assert record.cusip is None
        assert record.isin is None
        assert record.cik is None
        assert record.country is None
        assert record.sector is None
        assert record.industry is None
        assert record.first_trade_date is None
        assert record.delist_date is None
        assert record.shares_outstanding is None
        assert record.free_float is None
        assert record.company_name is None
        assert record.meta is None

    def test_text_field_whitespace_handling(self):
        """Test text field whitespace handling."""
        data = {
            **SAMPLE_JSON,
            "sector": "  Technology  ",
            "industry": "   Consumer Electronics   ",
            "company_name": "\tApple Inc.\n",
        }

        record = SymbolRecord(**data)

        assert record.sector == "Technology"
        assert record.industry == "Consumer Electronics"
        assert record.company_name == "Apple Inc."

    def test_all_asset_classes(self):
        """Test all asset class enum values."""
        base_data = {**SAMPLE_JSON}

        for asset_class in AssetClass:
            base_data["asset_class"] = asset_class.value
            record = SymbolRecord(**base_data)
            assert record.asset_class == asset_class

    def test_all_status_values(self):
        """Test all status enum values."""
        base_data = {**SAMPLE_JSON}

        for status in Status:
            base_data["status"] = status.value
            record = SymbolRecord(**base_data)
            assert record.status == status
