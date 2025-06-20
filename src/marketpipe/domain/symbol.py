"""Symbol Master domain model.

This module implements the SymbolRecord Pydantic model for the Symbol Master table,
representing tradable instruments with their metadata and corporate structure.

The model enforces strict validation, auto-coerces common provider quirks,
and provides serialization helpers for Parquet storage. This serves as the
single source of truth for all instrument identifiers and metadata across
MarketPipe.

Schema Reference:
- See Symbol Master schema section in main README
- Validates against exact field spec from PRD document
- Supports SCD-2 history tracking with valid_from/valid_to dates

Usage:
    >>> from marketpipe.domain import SymbolRecord, AssetClass
    >>> record = SymbolRecord(
    ...     ticker="AAPL",
    ...     exchange_mic="XNAS",
    ...     asset_class=AssetClass.EQUITY,
    ...     currency="USD",
    ...     status=Status.ACTIVE,
    ...     as_of=date(2024, 1, 1)
    ... )
"""

from __future__ import annotations

import datetime
import logging
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class AssetClass(str, Enum):
    """Asset classification enum for financial instruments."""
    
    EQUITY = "EQUITY"
    ETF = "ETF"
    ADR = "ADR"
    REIT = "REIT"
    FUTURE = "FUTURE"
    INDEX = "INDEX"


class Status(str, Enum):
    """Trading status enum for instruments."""
    
    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    SUSPENDED = "SUSPENDED"


class SymbolRecord(BaseModel):
    """Pydantic model representing a single row in the Symbol Master table.
    
    This model enforces the exact schema specification with automatic field
    coercion and validation. It provides round-trip serialization capabilities
    for Parquet storage and supports provider-specific field mapping.
    
    Fields are validated according to their business rules including format
    constraints, relationships between dates, and enum memberships.
    """
    
    # Core identifiers
    id: Optional[int] = Field(default=None, description="System-assigned surrogate key")
    ticker: str = Field(description="Security ticker symbol")
    figi: Optional[str] = Field(default=None, description="Financial Instrument Global Identifier")
    cusip: Optional[str] = Field(default=None, description="CUSIP identifier")
    isin: Optional[str] = Field(default=None, description="International Securities Identification Number")
    cik: Optional[str] = Field(default=None, description="SEC Central Index Key")
    
    # Exchange and classification
    exchange_mic: str = Field(description="Market Identifier Code (4 chars)")
    asset_class: AssetClass = Field(description="Asset classification")
    currency: str = Field(description="Trading currency (3-letter ISO)")
    
    # Optional metadata
    country: Optional[str] = Field(default=None, description="Country code (2-letter ISO)")
    sector: Optional[str] = Field(default=None, description="Business sector")
    industry: Optional[str] = Field(default=None, description="Industry classification")
    
    # Lifecycle dates
    first_trade_date: Optional[datetime.date] = Field(default=None, description="First trading date")
    delist_date: Optional[datetime.date] = Field(default=None, description="Delisting date")
    status: Status = Field(description="Current trading status")
    
    # Financial metrics
    shares_outstanding: Optional[int] = Field(default=None, description="Total shares outstanding")
    free_float: Optional[int] = Field(default=None, description="Free-floating shares")
    
    # Company information
    company_name: Optional[str] = Field(default=None, description="Company legal name")
    meta: Optional[Dict[str, Any]] = Field(default=None, description="Provider-specific metadata")
    
    # Temporal tracking
    as_of: datetime.date = Field(description="Snapshot effective date")
    
    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """Strip whitespace and convert to uppercase."""
        if not v or not v.strip():
            raise ValueError("ticker cannot be empty")
        return v.strip().upper()
    
    @field_validator("figi")
    @classmethod
    def validate_figi(cls, v: Optional[str]) -> Optional[str]:
        """Validate FIGI format (12 characters) or convert empty/N/A string to None."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            v_clean = v.strip()
            if v_clean == "" or v_clean.upper() in {"N/A", "NA", "NULL"}:
                return None
            if len(v_clean) != 12:
                raise ValueError("FIGI must be exactly 12 characters")
            return v_clean
        return v
    
    @field_validator("cusip")
    @classmethod
    def validate_cusip(cls, v: Optional[str]) -> Optional[str]:
        """Validate CUSIP format (9 characters) or convert empty/N/A string to None."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            v_clean = v.strip()
            if v_clean == "" or v_clean.upper() in {"N/A", "NA", "NULL"}:
                return None
            if len(v_clean) != 9:
                raise ValueError("CUSIP must be exactly 9 characters")
            return v_clean
        return v
    
    @field_validator("isin")
    @classmethod
    def validate_isin(cls, v: Optional[str]) -> Optional[str]:
        """Validate ISIN format (12 characters) or convert empty/N/A string to None."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            v_clean = v.strip()
            if v_clean == "" or v_clean.upper() in {"N/A", "NA", "NULL"}:
                return None
            if len(v_clean) != 12:
                raise ValueError("ISIN must be exactly 12 characters")
            return v_clean
        return v
    
    @field_validator("cik")
    @classmethod
    def validate_cik(cls, v: Optional[str]) -> Optional[str]:
        """Validate CIK contains only digits and zero-pad to length 10."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            v_clean = v.strip()
            if v_clean == "" or v_clean.upper() in {"N/A", "NA", "NULL"}:
                return None
            if not v_clean.isdigit():
                raise ValueError("CIK must contain only digits")
            return v_clean.zfill(10)  # Zero-pad to 10 characters
        return v
    
    @field_validator("exchange_mic")
    @classmethod
    def validate_exchange_mic(cls, v: str) -> str:
        """Convert to uppercase and validate 4-character format."""
        if not v or not v.strip():
            raise ValueError("exchange_mic cannot be empty")
        mic = v.strip().upper()
        if len(mic) != 4:
            raise ValueError("exchange_mic must be exactly 4 characters")
        return mic
    
    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        """Convert to uppercase 3-letter ISO currency code."""
        if not v or not v.strip():
            raise ValueError("currency cannot be empty")
        currency = v.strip().upper()
        if len(currency) != 3:
            raise ValueError("currency must be exactly 3 characters")
        return currency
    
    @field_validator("country")
    @classmethod
    def validate_country(cls, v: Optional[str]) -> Optional[str]:
        """Convert to uppercase 2-letter ISO country code or None."""
        if v == "" or v is None:
            return None
        country = v.strip().upper()
        if len(country) != 2:
            raise ValueError("country must be exactly 2 characters")
        return country
    
    @field_validator("sector", "industry", "company_name")
    @classmethod
    def validate_text_field(cls, v: Optional[str]) -> Optional[str]:
        """Trim whitespace and convert empty/N/A strings to None."""
        if v is None or v == "":
            return None
        if isinstance(v, str):
            v_clean = v.strip()
            if v_clean == "" or v_clean.upper() in {"N/A", "NA", "NULL"}:
                return None
            return v_clean
        return v
    
    @field_validator("shares_outstanding")
    @classmethod
    def validate_shares_outstanding(cls, v: Optional[int]) -> Optional[int]:
        """Validate shares outstanding is positive."""
        if v is None:
            return None
        if v <= 0:
            raise ValueError("shares_outstanding must be positive")
        return v
    
    @field_validator("free_float")
    @classmethod
    def validate_free_float(cls, v: Optional[int]) -> Optional[int]:
        """Validate free float is non-negative."""
        if v is None:
            return None
        if v < 0:
            raise ValueError("free_float cannot be negative")
        return v
    
    @model_validator(mode="after")
    def validate_date_order(self) -> "SymbolRecord":
        """Ensure delist_date >= first_trade_date when both are present."""
        if (
            self.first_trade_date is not None 
            and self.delist_date is not None 
            and self.delist_date < self.first_trade_date
        ):
            raise ValueError("delist_date must be >= first_trade_date")
        return self
    
    @model_validator(mode="after")
    def validate_free_float_constraint(self) -> "SymbolRecord":
        """Ensure free_float <= shares_outstanding when both are present."""
        if (
            self.shares_outstanding is not None 
            and self.free_float is not None 
            and self.free_float > self.shares_outstanding
        ):
            raise ValueError("free_float cannot exceed shares_outstanding")
        return self
    
    @classmethod
    def from_provider(
        cls, 
        payload: Dict[str, Any], 
        *, 
        provider: str,
        as_of: datetime.date
    ) -> "SymbolRecord":
        """Create SymbolRecord from provider-specific payload.
        
        This method provides a convenient way for adapters to map provider
        field names to SymbolRecord fields before instantiation. The mapping
        can be customized per provider.
        
        Args:
            payload: Raw data dict from provider
            provider: Provider identifier for field mapping
            as_of: Effective date for the snapshot
            
        Returns:
            Validated SymbolRecord instance
            
        Raises:
            ValueError: If provider is not supported or required fields missing
        """
        # Common field mappings by provider
        field_mappings = {
            "polygon": {
                "ticker": "ticker",
                "name": "company_name",
                "market": "exchange_mic",
                "type": "asset_class",
                "currency_name": "currency",
                "primary_exchange": "exchange_mic",
                "composite_figi": "figi",
                "share_class_figi": "figi",
                "cik": "cik",
                "sic_code": "sector",
            },
            "nasdaq": {
                "Symbol": "ticker",
                "Security Name": "company_name",
                "Market Category": "exchange_mic",
                "Test Issue": "status",
                "Financial Status": "status",
                "Round Lot Size": "meta",
            },
            "generic": {}  # Direct field mapping
        }
        
        mapping = field_mappings.get(provider.lower(), field_mappings["generic"])
        
        # Apply field mapping
        mapped_payload: Dict[str, Any] = {}
        for source_field, target_field in mapping.items():
            if source_field in payload:
                if target_field == "meta":
                    # Preserve provider-specific fields in meta
                    if "meta" not in mapped_payload:
                        mapped_payload["meta"] = {}
                    mapped_payload["meta"][source_field] = payload[source_field]
                else:
                    mapped_payload[target_field] = payload[source_field]
        
        # Include unmapped fields directly if they match model fields
        model_fields = cls.model_fields.keys()
        for field, value in payload.items():
            if field in model_fields and field not in mapped_payload:
                mapped_payload[field] = value
        
        # Set as_of date
        mapped_payload["as_of"] = as_of
        
        # Apply default values for required fields if missing
        if "status" not in mapped_payload:
            mapped_payload["status"] = Status.ACTIVE
        
        return cls(**mapped_payload)
    
    def to_parquet_row(self) -> Dict[str, Any]:
        """Convert to flat dict suitable for Parquet serialization.
        
        Returns:
            Dictionary with all fields serialized for storage
        """
        data = self.model_dump()
        
        # Convert enums to string values
        if isinstance(data.get("asset_class"), AssetClass):
            data["asset_class"] = data["asset_class"].value
        if isinstance(data.get("status"), Status):
            data["status"] = data["status"].value
            
        # Convert dates to ISO strings for Parquet compatibility
        if data.get("first_trade_date"):
            data["first_trade_date"] = data["first_trade_date"].isoformat()
        if data.get("delist_date"):
            data["delist_date"] = data["delist_date"].isoformat()
        if data.get("as_of"):
            data["as_of"] = data["as_of"].isoformat()
            
        return data
    
    @classmethod
    def from_parquet_row(cls, data: Dict[str, Any]) -> "SymbolRecord":
        """Create SymbolRecord from Parquet row data.
        
        Args:
            data: Dictionary from Parquet deserialization
            
        Returns:
            SymbolRecord instance with proper types restored
        """
        # Convert date strings back to date objects
        if data.get("first_trade_date") and isinstance(data["first_trade_date"], str):
            data["first_trade_date"] = datetime.date.fromisoformat(data["first_trade_date"])
        if data.get("delist_date") and isinstance(data["delist_date"], str):
            data["delist_date"] = datetime.date.fromisoformat(data["delist_date"])
        if data.get("as_of") and isinstance(data["as_of"], str):
            data["as_of"] = datetime.date.fromisoformat(data["as_of"])
            
        return cls(**data)

# Validation logging helper
_val_logger = logging.getLogger("marketpipe.symbols.validation")


def safe_create(record_kwargs: dict, *, provider: str) -> Optional[SymbolRecord]:
    """Safely create a SymbolRecord with structured validation error logging.
    
    Wraps SymbolRecord instantiation to catch ValidationError exceptions and emit
    structured log messages containing provider name, ticker, field name, and error
    message. This allows symbol providers to continue processing when individual
    records fail validation.
    
    Args:
        record_kwargs: Dictionary of field values for SymbolRecord constructor
        provider: Name of the symbol provider for error logging context
        
    Returns:
        SymbolRecord instance if validation passes, None if validation fails
        
    Logs:
        WARNING level message to "marketpipe.symbols.validation" logger when
        validation fails, containing provider, ticker, field, and error details
    """
    try:
        return SymbolRecord(**record_kwargs)
    except ValidationError as err:
        # Extract first validation error for logging
        first_error = err.errors()[0] if err.errors() else {}
        field_path = ".".join(str(loc) for loc in first_error.get("loc", []))
        error_msg = first_error.get("msg", "Unknown validation error")
        
        _val_logger.warning(
            "provider=%s ticker=%s field=%s error=%s",
            provider,
            record_kwargs.get("ticker", "UNKNOWN"),
            field_path,
            error_msg,
        )
        return None 