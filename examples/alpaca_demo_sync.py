# SPDX-License-Identifier: Apache-2.0
import os
import datetime as dt

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    print("❌ python-dotenv is required. Install with: pip install python-dotenv")
    exit(1)

from marketpipe.ingestion.connectors import ClientConfig, RateLimiter, HeaderTokenAuth
from marketpipe.ingestion.connectors.alpaca_client import AlpacaClient

# Check for required credentials in .env file
try:
    key = os.environ["ALPACA_KEY"]
    secret = os.environ["ALPACA_SECRET"]
    print(
        f"📋 Loaded credentials from .env file - Key: {key[:8]}..., Secret: {secret[:8]}..."
    )
except KeyError as e:
    print(f"❌ Missing required credential in .env file: {e}")
    print("\nTo run this demo, create a .env file in the project root with:")
    print("ALPACA_KEY=your_api_key_here")
    print("ALPACA_SECRET=your_secret_key_here")
    print("\nGet your API credentials from: https://alpaca.markets/")
    exit(1)

cfg = ClientConfig(
    api_key=key,
    base_url="https://data.alpaca.markets/v1beta1",  # Use v1beta1 for IEX
    rate_limit_per_min=200,
)

auth = HeaderTokenAuth(key, secret)
limiter = RateLimiter()
client = AlpacaClient(config=cfg, auth=auth, rate_limiter=limiter, feed="iex")

start = int((dt.datetime.utcnow() - dt.timedelta(days=1)).timestamp() * 1000)
end = int(dt.datetime.utcnow().timestamp() * 1000)

# Show the URI that will be created
symbol = "AAPL"
params = client.build_request_params(symbol, start, end)
url = f"{cfg.base_url}/stocks/{symbol}/bars"
query_params = "&".join([f"{k}={v}" for k, v in params.items() if k != "symbol"])
full_uri = f"{url}?{query_params}"
print(f"\n🌐 Market Data API URI: {full_uri}")

# Show the authentication headers that will be sent
auth_headers = {}
auth.apply(auth_headers, {})  # auth.apply modifies the headers dict in-place
print("🔐 Authentication headers:")
for header_name, header_value in auth_headers.items():
    # Show first 8 chars of values for security
    display_value = f"{header_value[:8]}..." if len(header_value) > 8 else header_value
    print(f"   {header_name}: {display_value}")

try:
    print(
        f"\n🔄 Fetching AAPL data from {dt.datetime.utcfromtimestamp(start/1000)} to {dt.datetime.utcfromtimestamp(end/1000)}..."
    )
    rows = client.fetch_batch("AAPL", start, end)
    print(f"✅ Successfully fetched {len(rows)} rows")

    if rows:
        print(f"📊 Sample data: {rows[0]}")
    else:
        print("📭 No data returned (this is normal for weekends/holidays)")

except Exception as e:
    print(f"❌ Error fetching data: {e}")
    if "Failed to parse Alpaca API response as JSON" in str(e):
        print("💡 This may be due to API rate limits or temporary service issues.")
    raise
