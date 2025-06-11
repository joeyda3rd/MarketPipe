# SPDX-License-Identifier: Apache-2.0
import os
import asyncio
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


async def test_credentials(key: str, secret: str) -> bool:
    """Test if credentials work with Alpaca Trading API"""
    import httpx
    
    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret
    }
    
    # Test different endpoints to see which work
    endpoints_to_test = [
        ("Paper Trading API", "https://paper-api.alpaca.markets/v2/account"),
        ("Live Trading API", "https://api.alpaca.markets/v2/account"),
    ]
    
    print("🔍 Testing credentials against different endpoints...")
    
    for name, url in endpoints_to_test:
        try:
            print(f"🌐 Testing {name}: {url}")
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    print(f"✅ {name}: Credentials work!")
                    return True
                elif response.status_code == 403:
                    print(f"❌ {name}: 403 Forbidden")
                else:
                    print(f"⚠️  {name}: HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ {name}: Connection error - {e}")
    
    return False


async def main() -> None:
    # Check for required credentials in .env file
    try:
        key = os.environ["ALPACA_KEY"]
        secret = os.environ["ALPACA_SECRET"]
        print(f"📋 Loaded credentials from .env file - Key: {key[:8]}..., Secret: {secret[:8]}...")
    except KeyError as e:
        print(f"❌ Missing required credential in .env file: {e}")
        print("\nTo run this demo, create a .env file in the project root with:")
        print("ALPACA_KEY=your_api_key_here")
        print("ALPACA_SECRET=your_secret_key_here")
        print("\nGet your API credentials from: https://alpaca.markets/")
        return

    # Test credentials first
    credentials_valid = await test_credentials(key, secret)
    
    if not credentials_valid:
        print("\n❌ Credentials don't work with Trading API endpoints.")
        print("\nPossible issues:")
        print("1. 🔑 Invalid or expired API credentials")
        print("2. 🏢 Account not properly set up")
        print("3. 📝 Missing required account verification")
        print("4. 🌐 Network/firewall issues")
        print("\nFor Market Data API access, you might also need:")
        print("5. 📊 Market Data subscription (required for historical data)")
        print("6. 💰 Proper account tier (some features require paid plans)")
        print("\nCheck your Alpaca dashboard at https://alpaca.markets/ to verify:")
        print("- Account status and verification")
        print("- API key permissions and expiration")
        print("- Market data subscriptions")
        return

    # Try Market Data API with IEX feed (free tier)
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
    print(f"🔐 Authentication headers:")
    for header_name, header_value in auth_headers.items():
        # Show first 8 chars of values for security
        display_value = f"{header_value[:8]}..." if len(header_value) > 8 else header_value
        print(f"   {header_name}: {display_value}")

    try:
        print(f"\n🔄 Fetching AAPL data from {dt.datetime.utcfromtimestamp(start/1000)} to {dt.datetime.utcfromtimestamp(end/1000)}...")
        rows = await client.async_fetch_batch("AAPL", start, end)
        print(f"✅ Successfully fetched {len(rows)} rows")
        
        if rows:
            print(f"📊 Sample data: {rows[0]}")
        else:
            print("📭 No data returned (this is normal for weekends/holidays)")
            
    except Exception as e:
        print(f"❌ Market Data API Error: {e}")
        
        if "403 Forbidden" in str(e):
            print("\n💡 403 Forbidden from Market Data API suggests:")
            print("1. 📊 No Market Data subscription (required for historical data)")
            print("2. 🆓 Account limited to free tier (try IEX data)")
            print("3. 🔐 Trading API credentials may not include Market Data access")
            print("\nTo get Market Data access:")
            print("- Visit https://alpaca.markets/ and check your plan")
            print("- Consider upgrading to AlgoTrader Plus for full market data")
            print("- For testing, you can use free IEX data (limited)")
        
        return


if __name__ == "__main__":
    asyncio.run(main())
