import os
import asyncio
import datetime as dt

from marketpipe.ingestion.connectors import ClientConfig, RateLimiter, HeaderTokenAuth
from marketpipe.ingestion.connectors.alpaca_client import AlpacaClient


async def main() -> None:
    key = os.environ["ALPACA_KEY"]
    secret = os.environ["ALPACA_SECRET"]

    cfg = ClientConfig(
        api_key=key,
        base_url="https://data.sandbox.alpaca.markets/v2",
        rate_limit_per_min=200,
    )

    auth = HeaderTokenAuth(key, secret)
    limiter = RateLimiter()
    client = AlpacaClient(config=cfg, auth=auth, rate_limiter=limiter)

    start = int((dt.datetime.utcnow() - dt.timedelta(days=1)).timestamp() * 1000)
    end = int(dt.datetime.utcnow().timestamp() * 1000)

    rows = await client.async_fetch_batch("AAPL", start, end)
    print(len(rows))


if __name__ == "__main__":
    asyncio.run(main())
