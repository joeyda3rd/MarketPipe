"""Nasdaq Daily List symbol provider for MarketPipe.

This adapter integrates with the Nasdaq Trader Daily List text files to retrieve
comprehensive symbol listings for US equity markets. It downloads and parses
pipe-delimited text files from the official Nasdaq FTP server.

Data Sources:
    - nasdaqlisted.txt: Nasdaq-listed securities
    - otherlisted.txt: Non-Nasdaq securities (optional, not implemented in v1)

Format: Pipe-delimited text with header and footer
Example:
    Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
    AAPL|Apple Inc.|Q|N|N|100|N|N
    ...
    File Creation Time: 20250619

Mapping Logic:
    - Symbol → ticker (trimmed, uppercase)
    - Security Name → company_name (trimmed)
    - Market Category → exchange_mic via mapping (Q→XNAS, N→XNYS, A→ARCX)
    - ETF flag → asset_class (Y→ETF, N→EQUITY)
    - Test Issue = Y → skipped (ignored)
    - Footer date → as_of if not specified in constructor

Limitations:
    - Daily list shows only ACTIVE symbols (no delisted entries)
    - ETF flag may not capture all ETNs or structured products
    - Embedded pipe characters in Security Name could cause parsing issues (rare)
    - Timezone: Footer dates are treated as local (ET) dates, not datetime

Usage:
    >>> provider = get("nasdaq_dl")
    >>> records = await provider.fetch_symbols()
    >>> len(records)  # Total active symbols from Nasdaq lists
"""

from __future__ import annotations
from typing import Optional, Union

import datetime as _dt

import httpx

from marketpipe.domain import AssetClass, Status, SymbolRecord
from marketpipe.domain.symbol import safe_create

from . import register
from .base import SymbolProviderBase

# Map Nasdaq Market Category codes to standard Market Identifier Codes
MIC_BY_CAT = {
    "Q": "XNAS",  # Nasdaq Global Select Market
    "N": "XNYS",  # New York Stock Exchange
    "A": "ARCX",  # NYSE Arca
    "G": "XNAS",  # Nasdaq Global Market (map to XNAS)
    "S": "XNAS",  # Nasdaq Capital Market (map to XNAS)
    "P": "ARCX",  # NYSE Arca (backup mapping)
    "Z": "BATS",  # BATS Exchange (if present)
}


@register("nasdaq_dl")
class NasdaqDailyListProvider(SymbolProviderBase):
    """Nasdaq Daily List symbol provider for US equity market data.

    Fetches symbol listings from Nasdaq's official Daily List text files.
    Downloads pipe-delimited text from the Nasdaq FTP server and converts
    each valid row to a SymbolRecord object.

    Configuration:
        as_of: Snapshot date (optional, defaults to footer date from file)
        include_etfs: Include ETF symbols (default: True)
        skip_test_issues: Skip test securities (default: True)

    Known Limitations:
        - Only shows ACTIVE symbols (no delisted entries)
        - ETF classification may not capture all structured products
        - Rare edge case: embedded pipes in company names may cause parsing issues
    """

    def __init__(self, *, as_of: _dt.Optional[date] = None, **provider_cfg):
        # Store whether as_of was explicitly provided by the user
        self._user_provided_as_of = as_of is not None
        super().__init__(as_of=as_of, **provider_cfg)

    async def _fetch_raw(self) -> list[str]:
        """Fetch raw Nasdaq Daily List text file.

        Downloads the nasdaqlisted.txt file from Nasdaq's public FTP server.
        Returns the file content as a list of lines for parsing.

        Returns:
            List of text lines from the Nasdaq Daily List file

        Raises:
            httpx.HTTPStatusError: If download fails
            httpx.RequestError: If network request fails
        """
        url = "https://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text.splitlines()

    def _map_to_records(self, rows: list[str]) -> list[SymbolRecord]:
        """Convert Nasdaq Daily List rows to SymbolRecord objects.

        Parses pipe-delimited text format and converts each valid row
        to a validated SymbolRecord. Handles footer date parsing and
        applies business rules for filtering.

        Args:
            rows: List of text lines from Nasdaq Daily List file

        Returns:
            List of validated SymbolRecord objects
        """
        if not rows:
            return []

        # Parse header to get column positions
        header = rows[0].split("|")

        # Filter out header and footer lines
        data_rows = [row for row in rows[1:] if row and not row.startswith("File Creation Time")]

        # Determine effective as_of date with proper precedence
        effective_as_of = self._determine_effective_date(rows)

        # Configuration options
        include_etfs = self.cfg.get("include_etfs", True)
        skip_test_issues = self.cfg.get("skip_test_issues", True)

        records: list[SymbolRecord] = []

        for row in data_rows:
            record = self._parse_row(row, header, effective_as_of, include_etfs, skip_test_issues)
            if record:
                records.append(record)

        return records

    def _determine_effective_date(self, rows: list[str]) -> _dt.date:
        """Determine effective as_of date with proper precedence.

        Priority:
        1. If constructor provided as_of, use it (ignore footer)
        2. Parse footer date from last line
        3. Fallback to today if footer parsing fails

        Args:
            rows: List of text lines from file

        Returns:
            Effective date for all records
        """
        # As-of precedence: if caller supplied as_of, ignore footer
        if self._user_provided_as_of:
            return self.as_of

        # Parse footer date from last line with improved parsing
        if rows:
            footer_line = rows[-1].strip()
            if footer_line.startswith("File Creation Time"):
                try:
                    # Handle extra spaces: "File Creation Time:  20250619"
                    # Split on colon and take the last non-empty part
                    parts = footer_line.split(":")
                    if len(parts) >= 2:
                        date_str = parts[-1].strip()  # Handle extra spaces
                        if date_str:
                            return _dt.datetime.strptime(date_str, "%Y%m%d").date()
                except (ValueError, IndexError):
                    pass  # Fall through to default

        # Fallback to today if footer parsing fails
        return _dt.date.today()

    def _parse_row(
        self,
        row: str,
        header: list[str],
        as_of: _dt.date,
        include_etfs: bool,
        skip_test_issues: bool,
    ) -> Optional[SymbolRecord]:
        """Parse a single row from the Nasdaq Daily List.

        Args:
            row: Pipe-delimited data row
            header: Column names from header row
            as_of: Effective date for the record
            include_etfs: Whether to include ETF symbols
            skip_test_issues: Whether to skip test securities

        Returns:
            SymbolRecord if row is valid and passes filters, None otherwise
        """
        # Handle potential embedded pipes by splitting carefully
        # Note: This is a simplified approach; true CSV parsing would be more robust
        parts = row.split("|")
        if len(parts) != len(header):
            return None  # Malformed row

        # Create column mapping
        data = dict(zip(header, parts))

        # Apply business rule filters
        if skip_test_issues and data.get("Test Issue", "").strip().upper() == "Y":
            return None  # Skip test securities

        # Extract and validate core fields
        ticker = data.get("Symbol", "").strip().upper()
        if not ticker:
            return None  # Empty ticker

        # Map market category to MIC with blank handling
        market_category = data.get("Market Category", "").strip().upper()
        if not market_category:  # Handle blank market category
            exchange_mic = "XNAS"  # Default to NASDAQ
        else:
            exchange_mic = MIC_BY_CAT.get(market_category, "XNAS")

        # Determine asset class from ETF flag
        etf_flag = data.get("ETF", "").strip().upper()
        if etf_flag == "Y":
            asset_class = AssetClass.ETF
            if not include_etfs:
                return None  # Skip ETFs if configured
        else:
            asset_class = AssetClass.EQUITY

        # Extract company name
        company_name_raw = data.get("Security Name", "").strip()
        company_name = company_name_raw if company_name_raw else None

        # Build record kwargs for safe_create
        record_kwargs = {
            "ticker": ticker,
            "exchange_mic": exchange_mic,
            "asset_class": asset_class,
            "currency": "USD",  # All Nasdaq listings are USD
            "status": Status.ACTIVE,  # Daily list contains active securities
            "company_name": company_name,
            "as_of": as_of,
            "meta": {
                "market_category": market_category,
                "test_issue": data.get("Test Issue", "").strip(),
                "financial_status": data.get("Financial Status", "").strip(),
                "round_lot_size": data.get("Round Lot Size", "").strip(),
                "etf_flag": etf_flag,
                "nextshares": data.get("NextShares", "").strip(),
                "source": "nasdaq_daily_list",
            },
        }

        # Use safe_create to handle validation errors
        return safe_create(record_kwargs, provider=self.name)
