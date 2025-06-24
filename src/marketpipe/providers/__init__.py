# SPDX-License-Identifier: Apache-2.0
"""Provider feature matrix and recommendation system."""

from __future__ import annotations

from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass
class ProviderFeature:
    """Feature capabilities of a market data provider."""

    name: str
    supports_recent_data: bool = False  # Can provide data from last 30 days
    supports_historical: bool = False  # Can provide data older than 1 year
    supports_real_time: bool = False  # Provides real-time/live data
    free_tier_available: bool = False  # Has free tier
    max_historical_days: Optional[int] = None  # Maximum days of historical data
    typical_lag_hours: int = 24  # Typical data lag in hours


class ProviderFeatureMatrix:
    """Provider feature matrix for recommendations."""

    # Define provider capabilities
    FEATURES: Dict[str, ProviderFeature] = {
        "alpaca": ProviderFeature(
            name="alpaca",
            supports_recent_data=False,  # IEX free tier has limited recent data
            supports_historical=True,
            free_tier_available=True,
            max_historical_days=730,
            typical_lag_hours=24 * 365,  # IEX data can be very old
        ),
        "finnhub": ProviderFeature(
            name="finnhub",
            supports_recent_data=True,
            supports_historical=True,
            free_tier_available=True,
            max_historical_days=365,
            typical_lag_hours=24,
        ),
        "polygon": ProviderFeature(
            name="polygon",
            supports_recent_data=True,
            supports_historical=True,
            free_tier_available=True,
            max_historical_days=730,
            typical_lag_hours=24,
        ),
        "iex": ProviderFeature(
            name="iex",
            supports_recent_data=True,
            supports_historical=True,
            free_tier_available=True,
            max_historical_days=365,
            typical_lag_hours=24,
        ),
        "fake": ProviderFeature(
            name="fake",
            supports_recent_data=True,
            supports_historical=True,
            free_tier_available=True,
            max_historical_days=None,  # Unlimited synthetic data
            typical_lag_hours=0,  # Immediate synthetic data
        ),
    }

    @classmethod
    def get_features(cls, provider: str) -> Optional[ProviderFeature]:
        """Get feature information for a provider."""
        return cls.FEATURES.get(provider)

    @classmethod
    def suggest_alternatives(
        cls,
        failed_provider: str,
        requested_start_date: date,
        requested_end_date: date,
        exclude_providers: Optional[Set[str]] = None,
    ) -> List[str]:
        """Suggest alternative providers based on date range requirements.

        Args:
            failed_provider: Provider that failed to provide expected data
            requested_start_date: Start date of the requested range
            requested_end_date: End date of the requested range
            exclude_providers: Providers to exclude from suggestions

        Returns:
            List of recommended provider names, ordered by suitability
        """
        if exclude_providers is None:
            exclude_providers = set()

        exclude_providers.add(failed_provider)  # Don't suggest the failed provider

        # Calculate data requirements
        days_ago = (date.today() - requested_start_date).days
        is_recent_data = days_ago <= 30
        is_historical = days_ago > 365

        candidates = []

        for provider_name, features in cls.FEATURES.items():
            if provider_name in exclude_providers:
                continue

            # Check if provider can handle the date range
            score = 0

            # Recent data capability
            if is_recent_data and features.supports_recent_data:
                score += 10
            elif is_recent_data and not features.supports_recent_data:
                continue  # Skip providers that can't handle recent data

            # Historical data capability
            if is_historical and features.supports_historical:
                score += 5

            # Check max historical days limit
            if features.max_historical_days and days_ago > features.max_historical_days:
                continue  # Skip if outside provider's capability

            # Prefer providers with lower lag for recent requests
            if is_recent_data:
                score += max(0, 10 - features.typical_lag_hours // 24)

            # Bonus for free tier
            if features.free_tier_available:
                score += 3

            candidates.append((provider_name, score))

        # Sort by score (descending) and return provider names
        candidates.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in candidates]

    @classmethod
    def get_suggestion_message(
        cls,
        failed_provider: str,
        returned_start: date,
        returned_end: date,
        requested_start: date,
        requested_end: date,
    ) -> str:
        """Generate a helpful error message with provider suggestions."""
        alternatives = cls.suggest_alternatives(
            failed_provider, requested_start, requested_end
        )

        # Create base error message
        msg = (
            f"{failed_provider.title()} returned data from {returned_start} to {returned_end}, "
            f"which is outside the requested range {requested_start} to {requested_end}."
        )

        # Add provider suggestions
        if alternatives:
            if len(alternatives) == 1:
                msg += f" Try provider={alternatives[0]}."
            else:
                msg += f" Try provider={alternatives[0]} or provider={alternatives[1]}."
        else:
            msg += " No alternative providers available for this date range."

        return msg


# Export the main classes
__all__ = ["ProviderFeature", "ProviderFeatureMatrix"]
