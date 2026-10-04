# SPDX-License-Identifier: Apache-2.0
"""Price changes retain their sign without weakening price validation."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from marketpipe.domain.entities import EntityId, OHLCVBar
from marketpipe.domain.services import OHLCVCalculationService
from marketpipe.domain.value_objects import Price, Symbol, Timestamp, Volume

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("close, expected_change", [("90", "-10"), ("110", "10"), ("100", "0")])
def test_bar_and_daily_summary_preserve_signed_price_changes(close, expected_change):
    bar = OHLCVBar(
        id=EntityId.generate(),
        symbol=Symbol("AAPL"),
        timestamp=Timestamp(datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)),
        open_price=Price(Decimal("100")),
        high_price=Price(Decimal("110")),
        low_price=Price(Decimal("90")),
        close_price=Price(Decimal(close)),
        volume=Volume(100),
    )
    summary = OHLCVCalculationService().daily_summary([bar])

    for result in (bar, summary):
        assert result.calculate_price_change().value == Decimal(expected_change)
        assert result.calculate_price_change().to_float() == float(expected_change)
        assert result.calculate_price_change_percentage() == float(expected_change)


def test_negative_prices_remain_invalid():
    with pytest.raises(ValueError, match="Price cannot be negative"):
        Price(Decimal("-1"))
