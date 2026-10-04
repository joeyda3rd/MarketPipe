"""Opt-in one-request availability probe; never display responses or credentials."""

import os
from datetime import date, timedelta

import httpx


def main():
    day = date.fromisoformat(os.environ["PROBE_DATE"])
    if day >= date.today():
        raise SystemExit("Choose a past trading date")
    provider = os.environ["PROBE_PROVIDER"]
    if provider == "alpaca":
        key, secret = os.environ["ALPACA_KEY"], os.environ["ALPACA_SECRET"]
        if not key or not secret:
            raise SystemExit("Alpaca credentials are required")
        url = "https://data.alpaca.markets/v2/stocks/bars"
        headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        params = {
            "symbols": "AAPL",
            "timeframe": "1Min",
            "feed": "iex",
            "limit": 1,
            "start": f"{day}T00:00:00Z",
            "end": f"{day + timedelta(days=1)}T00:00:00Z",
        }
        field = "bars"
    elif provider == "polygon":
        key = os.environ["POLYGON_API_KEY"]
        if not key:
            raise SystemExit("Polygon credentials are required")
        url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/{day}/{day}"
        headers = {"Authorization": f"Bearer {key}"}
        params = {"limit": 1, "sort": "asc"}
        field = "results"
    else:
        raise SystemExit("Unknown provider")
    try:
        response = httpx.get(url, headers=headers, params=params, timeout=20)
        if response.status_code != 200:
            raise SystemExit(f"Provider returned HTTP {response.status_code}")
        payload = response.json()
        if not payload.get(field):
            raise SystemExit("Provider returned no bars; verify the date and entitlement")
    except (httpx.HTTPError, ValueError):
        raise SystemExit("Provider connection or response failed") from None
    print(f"{provider} probe succeeded with one HTTP request")


if __name__ == "__main__":
    main()
