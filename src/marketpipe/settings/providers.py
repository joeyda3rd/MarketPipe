# SPDX-License-Identifier: Apache-2.0
"""Provider settings for MarketPipe data sources.

This module defines Pydantic BaseSettings classes for each market data provider,
following the standardized naming convention: MP_{PROVIDERKEY_UPPER}_{CREDNAME_UPPER}

All settings classes automatically load from environment variables and provide
type validation and documentation for required credentials.
"""

from __future__ import annotations

from pydantic import Field

try:
    from pydantic_settings import BaseSettings
except ImportError:
    # Fallback for older pydantic versions
    from pydantic import BaseSettings


class AlpacaSettings(BaseSettings):
    """Alpaca Markets API settings.

    Alpaca provides free-tier market data through IEX and paid data through SIP.
    Requires key/secret pair for authentication.

    Environment Variables:
        ALPACA_KEY: API key ID from Alpaca dashboard
        ALPACA_SECRET: Secret key from Alpaca dashboard
    """

    api_key: str = Field(..., alias="ALPACA_KEY", description="Alpaca API key ID")
    api_secret: str = Field(..., alias="ALPACA_SECRET", description="Alpaca secret key")
    base_url: str = Field(
        default="https://data.alpaca.markets/v2", description="Alpaca API base URL"
    )
    feed_type: str = Field(default="iex", description="Data feed type (iex or sip)")

    class Config:
        env_prefix = ""  # Use exact env var names
        case_sensitive = True


class IEXSettings(BaseSettings):
    """IEX Cloud API settings.

    IEX Cloud provides US equities data with both secret and publishable tokens.

    Environment Variables:
        IEX_TOKEN: Legacy token (for backward compatibility)
        MP_IEX_SECRET_TOKEN: Secret token for server-side usage
        MP_IEX_PUB_TOKEN: Publishable token for client-side usage
    """

    # Support legacy token name for backward compatibility
    secret_token: str | None = Field(None, alias="IEX_TOKEN", description="Legacy IEX token")
    mp_secret_token: str | None = Field(
        None, alias="MP_IEX_SECRET_TOKEN", description="IEX secret token"
    )
    mp_pub_token: str | None = Field(
        None, alias="MP_IEX_PUB_TOKEN", description="IEX publishable token"
    )

    base_url: str = Field(
        default="https://cloud.iexapis.com/stable", description="IEX Cloud API base URL"
    )
    is_sandbox: bool = Field(default=False, description="Use sandbox environment")

    @property
    def api_token(self) -> str:
        """Get the appropriate API token, preferring new naming convention."""
        return self.mp_secret_token or self.secret_token or ""

    class Config:
        env_prefix = ""
        case_sensitive = True


class FinnhubSettings(BaseSettings):
    """Finnhub API settings.

    Finnhub provides financial market data with good free tier coverage.

    Environment Variables:
        MP_FINNHUB_API_KEY: API key from Finnhub dashboard
    """

    api_key: str = Field(..., alias="MP_FINNHUB_API_KEY", description="Finnhub API key")
    base_url: str = Field(default="https://finnhub.io/api/v1", description="Finnhub API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class PolygonSettings(BaseSettings):
    """Polygon.io API settings.

    Polygon provides tick-level US equities and options data with fast WebSockets.

    Environment Variables:
        MP_POLYGON_API_KEY: API key from Polygon dashboard
    """

    api_key: str = Field(..., alias="MP_POLYGON_API_KEY", description="Polygon API key")
    base_url: str = Field(default="https://api.polygon.io", description="Polygon API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class BinanceSettings(BaseSettings):
    """Binance API settings.

    Binance is the largest spot crypto exchange with global liquidity.
    Public endpoints don't require authentication, private endpoints use HMAC.

    Environment Variables:
        MP_BINANCE_API_KEY: API key from Binance account
        MP_BINANCE_API_SECRET: Secret key from Binance account
    """

    api_key: str = Field(..., alias="MP_BINANCE_API_KEY", description="Binance API key")
    api_secret: str = Field(..., alias="MP_BINANCE_API_SECRET", description="Binance secret key")
    base_url: str = Field(default="https://api.binance.com", description="Binance API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class TiingoSettings(BaseSettings):
    """Tiingo API settings.

    Tiingo provides high-quality US equities & news in CSV/JSON formats.

    Environment Variables:
        MP_TIINGO_API_KEY: API key from Tiingo dashboard
    """

    api_key: str = Field(..., alias="MP_TIINGO_API_KEY", description="Tiingo API key")
    base_url: str = Field(default="https://api.tiingo.com", description="Tiingo API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class TwelveDataSettings(BaseSettings):
    """Twelve Data API settings.

    Twelve Data provides 1-minute global equities/forex with generous free tier.

    Environment Variables:
        MP_TWELVEDATA_API_KEY: API key from Twelve Data dashboard
    """

    api_key: str = Field(..., alias="MP_TWELVEDATA_API_KEY", description="Twelve Data API key")
    base_url: str = Field(
        default="https://api.twelvedata.com", description="Twelve Data API base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True


class FredSettings(BaseSettings):
    """FRED (Federal Reserve Economic Data) settings.

    FRED provides US economic indicators and requires no authentication.
    Included for completeness but no credentials needed.
    """

    base_url: str = Field(
        default="https://api.stlouisfed.org/fred", description="FRED API base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True


# =============================================================================
# BACKLOG PROVIDER SETTINGS (STUBS)
# These are placeholder settings for future provider implementations
# =============================================================================


class AlphaVantageSettings(BaseSettings):
    """Alpha Vantage API settings (BACKLOG).

    Environment Variables:
        MP_ALPHAVANTAGE_API_KEY: API key from Alpha Vantage
    """

    api_key: str = Field(..., alias="MP_ALPHAVANTAGE_API_KEY", description="Alpha Vantage API key")
    base_url: str = Field(
        default="https://www.alphavantage.co", description="Alpha Vantage API base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True


class MarketStackSettings(BaseSettings):
    """MarketStack API settings (BACKLOG).

    Environment Variables:
        MP_MARKETSTACK_API_KEY: API key from MarketStack
    """

    api_key: str = Field(..., alias="MP_MARKETSTACK_API_KEY", description="MarketStack API key")
    base_url: str = Field(
        default="http://api.marketstack.com", description="MarketStack API base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True


class EODHDSettings(BaseSettings):
    """EODHD API settings (BACKLOG).

    Environment Variables:
        MP_EODHD_API_KEY: API key from EODHD
    """

    api_key: str = Field(..., alias="MP_EODHD_API_KEY", description="EODHD API key")
    base_url: str = Field(default="https://eodhistoricaldata.com", description="EODHD API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class IntrinioSettings(BaseSettings):
    """Intrinio API settings (BACKLOG).

    Environment Variables:
        MP_INTRINIO_API_KEY: API key from Intrinio
    """

    api_key: str = Field(..., alias="MP_INTRINIO_API_KEY", description="Intrinio API key")
    base_url: str = Field(
        default="https://api-v2.intrinio.com", description="Intrinio API base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True


class TradierSettings(BaseSettings):
    """Tradier API settings (BACKLOG).

    Environment Variables:
        MP_TRADIER_API_KEY: API key from Tradier
        MP_TRADIER_API_SECRET: Secret key from Tradier
    """

    api_key: str = Field(..., alias="MP_TRADIER_API_KEY", description="Tradier API key")
    api_secret: str = Field(..., alias="MP_TRADIER_API_SECRET", description="Tradier secret key")
    base_url: str = Field(default="https://api.tradier.com", description="Tradier API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class QuandlSettings(BaseSettings):
    """Quandl API settings (BACKLOG).

    Environment Variables:
        MP_QUANDL_API_KEY: API key from Quandl
    """

    api_key: str = Field(..., alias="MP_QUANDL_API_KEY", description="Quandl API key")
    base_url: str = Field(default="https://www.quandl.com/api", description="Quandl API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class RefinitivSettings(BaseSettings):
    """Refinitiv API settings (BACKLOG).

    Environment Variables:
        MP_REFINITIV_APP_KEY: Application key from Refinitiv
    """

    app_key: str = Field(..., alias="MP_REFINITIV_APP_KEY", description="Refinitiv application key")
    base_url: str = Field(default="https://api.refinitiv.com", description="Refinitiv API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class ExegySettings(BaseSettings):
    """Exegy API settings (BACKLOG).

    Environment Variables:
        MP_EXEGY_API_KEY: API key from Exegy
    """

    api_key: str = Field(..., alias="MP_EXEGY_API_KEY", description="Exegy API key")
    base_url: str = Field(default="https://api.exegy.com", description="Exegy API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class CMEDataMineSettings(BaseSettings):
    """CME DataMine API settings (BACKLOG).

    Environment Variables:
        MP_CME_DATAMINE_USERNAME: Username for CME DataMine
        MP_CME_DATAMINE_PASSWORD: Password for CME DataMine
    """

    username: str = Field(
        ..., alias="MP_CME_DATAMINE_USERNAME", description="CME DataMine username"
    )
    password: str = Field(
        ..., alias="MP_CME_DATAMINE_PASSWORD", description="CME DataMine password"
    )
    base_url: str = Field(
        default="https://datamine.cmegroup.com", description="CME DataMine base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True


class KrakenSettings(BaseSettings):
    """Kraken API settings (BACKLOG).

    Environment Variables:
        MP_KRAKEN_API_KEY: API key from Kraken
        MP_KRAKEN_API_SECRET: Secret key from Kraken
    """

    api_key: str = Field(..., alias="MP_KRAKEN_API_KEY", description="Kraken API key")
    api_secret: str = Field(..., alias="MP_KRAKEN_API_SECRET", description="Kraken secret key")
    base_url: str = Field(default="https://api.kraken.com", description="Kraken API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class CoinbaseSettings(BaseSettings):
    """Coinbase API settings (BACKLOG).

    Environment Variables:
        MP_COINBASE_API_KEY: API key from Coinbase
        MP_COINBASE_API_SECRET: Secret key from Coinbase
    """

    api_key: str = Field(..., alias="MP_COINBASE_API_KEY", description="Coinbase API key")
    api_secret: str = Field(..., alias="MP_COINBASE_API_SECRET", description="Coinbase secret key")
    base_url: str = Field(default="https://api.coinbase.com", description="Coinbase API base URL")

    class Config:
        env_prefix = ""
        case_sensitive = True


class YFinanceSettings(BaseSettings):
    """Yahoo Finance settings (BACKLOG).

    Yahoo Finance doesn't require credentials for basic usage.
    Use with caution in production due to rate limiting.
    """

    base_url: str = Field(
        default="https://query1.finance.yahoo.com", description="Yahoo Finance base URL"
    )

    class Config:
        env_prefix = ""
        case_sensitive = True


# =============================================================================
# PROVIDER SETTINGS REGISTRY
# Maps provider keys to their corresponding settings classes
# =============================================================================

PROVIDER_SETTINGS = {
    # Immediate target providers
    "alpaca": AlpacaSettings,
    "iex": IEXSettings,
    "finnhub": FinnhubSettings,
    "polygon": PolygonSettings,
    "binance": BinanceSettings,
    "tiingo": TiingoSettings,
    "twelvedata": TwelveDataSettings,
    "fred": FredSettings,
    # Backlog providers
    "alphavantage": AlphaVantageSettings,
    "marketstack": MarketStackSettings,
    "eodhd": EODHDSettings,
    "intrinio": IntrinioSettings,
    "tradier": TradierSettings,
    "quandl": QuandlSettings,
    "refinitiv": RefinitivSettings,
    "exegy": ExegySettings,
    "cme_datamine": CMEDataMineSettings,
    "kraken": KrakenSettings,
    "coinbase": CoinbaseSettings,
    "yfinance": YFinanceSettings,
}


def get_provider_settings(provider_key: str):
    """Get settings instance for a provider.

    Args:
        provider_key: Provider identifier (e.g., 'alpaca', 'iex')

    Returns:
        Configured settings instance for the provider

    Raises:
        KeyError: If provider_key is not found in registry
        ValidationError: If required environment variables are missing
    """
    if provider_key not in PROVIDER_SETTINGS:
        available = list(PROVIDER_SETTINGS.keys())
        raise KeyError(f"Provider '{provider_key}' not found. Available: {available}")

    settings_class = PROVIDER_SETTINGS[provider_key]
    return settings_class()


def list_provider_settings() -> dict[str, list[str]]:
    """List all providers and their required environment variables.

    Returns:
        Dictionary mapping provider keys to lists of required env vars
    """
    result = {}
    for provider_key, settings_class in PROVIDER_SETTINGS.items():
        # Extract field information to get env var names
        env_vars = []

        # Use model_fields for Pydantic v2, fall back to __fields__ for v1
        if hasattr(settings_class, "model_fields"):
            # Pydantic v2
            fields = settings_class.model_fields
            for _field_name, field_info in fields.items():
                # In Pydantic v2, alias is directly accessible from FieldInfo
                if hasattr(field_info, "alias") and field_info.alias:
                    env_vars.append(field_info.alias)
        else:
            # Pydantic v1
            fields = getattr(settings_class, "__fields__", {})
            for _field_name, field_info in fields.items():
                # In Pydantic v1, check for alias in field_info
                if hasattr(field_info, "alias") and field_info.alias:
                    env_vars.append(field_info.alias)
                # Also check field_info.field_info.extra for legacy compatibility
                elif hasattr(field_info, "field_info") and hasattr(field_info.field_info, "extra"):
                    extra = field_info.field_info.extra
                    if "env" in extra:
                        env_vars.append(extra["env"])
                    elif "alias" in extra:
                        env_vars.append(extra["alias"])

        result[provider_key] = sorted(env_vars)

    return result


__all__ = [
    # Settings classes for immediate providers
    "AlpacaSettings",
    "IEXSettings",
    "FinnhubSettings",
    "PolygonSettings",
    "BinanceSettings",
    "TiingoSettings",
    "TwelveDataSettings",
    "FredSettings",
    # Settings classes for backlog providers
    "AlphaVantageSettings",
    "MarketStackSettings",
    "EODHDSettings",
    "IntrinioSettings",
    "TradierSettings",
    "QuandlSettings",
    "RefinitivSettings",
    "ExegySettings",
    "CMEDataMineSettings",
    "KrakenSettings",
    "CoinbaseSettings",
    "YFinanceSettings",
    # Registry and utility functions
    "PROVIDER_SETTINGS",
    "get_provider_settings",
    "list_provider_settings",
]
