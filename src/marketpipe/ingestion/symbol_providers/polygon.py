"""Polygon.io symbol provider for MarketPipe.

This adapter integrates with Polygon.io's reference tickers API to retrieve
comprehensive symbol listings for US equity markets. It handles pagination
automatically and converts provider-specific field formats to the canonical
SymbolRecord schema.

API Endpoint: GET https://api.polygon.io/v3/reference/tickers
Query Parameters:
    - market=stocks: Filter to stock instruments
    - active=true|false: Include active and delisted instruments
    - limit=1000: Maximum records per page (API default)
    - sort=ticker: Sort results by ticker symbol
    - order=asc: Ascending sort order
    - apiKey=<TOKEN>: Authentication token
    - as_of=<YYYY-MM-DD>: Optional snapshot date (defaults to today)
    - page_token=<token>: Pagination cursor for subsequent requests

Mapping Logic:
    - Primary exchange codes mapped to standard MIC identifiers
    - Asset type codes converted to MarketPipe AssetClass enum
    - Active/inactive status mapped to Status enum
    - Provider-specific fields preserved in meta JSON field
    - Missing optional fields handled gracefully with None defaults

Rate Limits:
    - Free tier: 50 requests per minute
    - Uses single shared httpx.AsyncClient with 30s timeout
    - Automatic pagination continues until all pages retrieved

Usage:
    >>> provider = get("polygon", token=os.environ["POLYGON_API_KEY"])
    >>> records = await provider.fetch_symbols()
    >>> len(records)  # Total symbols from all pages
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

import httpx

from marketpipe.domain import AssetClass, Status, SymbolRecord
from marketpipe.domain.symbol import safe_create

from . import register
from .base import SymbolProviderBase

# Map Polygon exchange codes to standard Market Identifier Codes
MIC_MAP = {
    "XNYS": "XNYS",  # New York Stock Exchange
    "XNAS": "XNAS",  # NASDAQ
    "ARCX": "ARCX",  # NYSE Arca
    "BATS": "BATS",  # CBOE BZX
    "IEX": "IEXG",  # IEX Exchange
    # Expand as needed for additional exchanges
}

# Map Polygon asset type codes to MarketPipe AssetClass enum
ASSET_MAP = {
    "CS": AssetClass.EQUITY,  # Common Stock
    "ADRC": AssetClass.ADR,  # American Depositary Receipt
    "ETF": AssetClass.ETF,  # Exchange Traded Fund
    "REIT": AssetClass.REIT,  # Real Estate Investment Trust
    "PFD": AssetClass.EQUITY,  # Preferred Stock (classify as equity)
    "FUND": AssetClass.ETF,  # Mutual Fund (classify as ETF)
    "RIGHT": AssetClass.EQUITY,  # Rights (classify as equity)
    "BOND": AssetClass.EQUITY,  # Bond (classify as equity for now)
    "WARRANT": AssetClass.EQUITY,  # Warrant (classify as equity)
}


@register("polygon")
class PolygonSymbolProvider(SymbolProviderBase):
    """Polygon.io symbol provider for US equity market data.

    Fetches comprehensive symbol listings from Polygon's reference API,
    including both active and delisted instruments. Handles pagination
    automatically and converts all records to validated SymbolRecord format.

    Configuration:
        token: Polygon API key (required)
        as_of: Snapshot date (optional, defaults to today)
    """

    async def _fetch_raw(self) -> list[dict[str, Any]]:
        """Fetch raw symbol data from Polygon API with pagination.

        Makes multiple API calls as needed to retrieve all available symbols.
        Each page can contain up to 1000 records. Continues fetching until
        no more pages are available.

        Returns:
            List of raw symbol dictionaries from all API pages

        Raises:
            httpx.HTTPStatusError: If API returns non-2xx response
            KeyError: If required API token not provided in configuration
        """
        if "token" not in self.cfg:
            raise KeyError("Polygon API token not provided in configuration")

        base_params = {
            "market": "stocks",
            "limit": 1000,
            "sort": "ticker",
            "order": "asc",
            "apiKey": self.cfg["token"],
            "as_of": self.as_of.isoformat(),
        }

        url = "https://api.polygon.io/v3/reference/tickers"
        all_records: list[dict[str, Any]] = []

        async with httpx.AsyncClient(timeout=30) as client:
            next_url: str | None = url
            current_params: dict[str, Any] | None = base_params.copy()

            while next_url:
                # Make API request
                response = await client.get(next_url, params=current_params)
                response.raise_for_status()
                data = response.json()

                # Extract records from response
                records = data.get("results", [])
                all_records.extend(records)

                # Check for next page
                next_url = data.get("next_url")
                if next_url:
                    # next_url already contains all query parameters except apiKey
                    current_params = {"apiKey": self.cfg["token"]}
                else:
                    current_params = None

        return all_records

    def _map_to_records(self, payload: list[dict[str, Any]]) -> list[SymbolRecord]:
        """Convert Polygon API records to SymbolRecord objects.

        Transforms provider-specific field formats to canonical schema:
        - Maps exchange codes to standard MIC identifiers
        - Converts asset types to AssetClass enum values
        - Handles active/inactive status mapping
        - Preserves all original data in meta field
        - Applies validation through SymbolRecord constructor

        Args:
            payload: List of raw symbol dictionaries from Polygon API

        Returns:
            List of validated SymbolRecord objects
        """
        records: list[SymbolRecord] = []

        for row in payload:
            # Map exchange code to MIC, fallback to truncated uppercase
            primary_exchange = row.get("primary_exchange", "")
            mic = MIC_MAP.get(primary_exchange, primary_exchange[:4].upper())

            # Map asset type to AssetClass enum
            asset_type = row.get("type", "CS")
            asset_class = ASSET_MAP.get(asset_type, AssetClass.EQUITY)

            # Map active status to Status enum
            is_active = row.get("active", True)
            status = Status.ACTIVE if is_active else Status.DELISTED

            # Parse list date if available
            list_date_str = row.get("list_date")
            first_trade_date = None
            if list_date_str:
                try:
                    first_trade_date = _dt.date.fromisoformat(list_date_str)
                except (ValueError, TypeError):
                    # Invalid date format, keep as None
                    pass

            # Extract currency, default to USD
            currency = row.get("currency_name", "USD")
            if isinstance(currency, str):
                currency = currency[:3].upper()
            else:
                currency = "USD"

            # Extract country from locale
            country = None
            locale = row.get("locale")
            if locale and isinstance(locale, str) and len(locale) >= 2:
                country = locale[:2].upper()

            # Build record kwargs for safe_create
            record_kwargs = {
                "ticker": row["ticker"].upper(),
                "exchange_mic": mic,
                "asset_class": asset_class,
                "currency": currency,
                "status": status,
                "company_name": row.get("name"),
                "shares_outstanding": row.get("share_class_shares_outstanding"),
                "first_trade_date": first_trade_date,
                "delist_date": None,  # Polygon doesn't provide delisting date directly
                "as_of": self.as_of,
                "figi": row.get("figi") or None,
                "country": country,
                "cusip": row.get("cusip") or None,
                "isin": row.get("isin") or None,
                "cik": row.get("cik") or None,
                "sector": row.get("sector") or None,
                "industry": row.get("industry") or None,
                "meta": row,  # Preserve original provider data
            }

            # Use safe_create to handle validation errors
            rec = safe_create(record_kwargs, provider=self.name)
            if rec:
                records.append(rec)

        return records
