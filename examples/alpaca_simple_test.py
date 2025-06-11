# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
"""
Simple Alpaca API Test - Try the most basic endpoints
"""

import os
import asyncio
import httpx
from dotenv import load_dotenv

load_dotenv()

alpaca_key = os.environ.get('ALPACA_KEY')
alpaca_secret = os.environ.get('ALPACA_SECRET')

if not alpaca_key or not alpaca_secret:
    import pytest
    pytest.skip("ALPACA credentials not set", allow_module_level=True)

print(f"🔑 Testing with credentials: {alpaca_key[:8]}... / {alpaca_secret[:8]}...")

async def test_basic_endpoints():
    headers = {
        "APCA-API-KEY-ID": alpaca_key,
        "APCA-API-SECRET-KEY": alpaca_secret,
        "Accept": "application/json"
    }
    
    # Try the most basic endpoints that should work with any valid key
    basic_endpoints = [
        # These should work if your account is set up at all:
        ("Account Info (Paper)", "https://paper-api.alpaca.markets/v2/account"),
        ("Account Info (Live)", "https://api.alpaca.markets/v2/account"),
        
        # Market data - try free tier endpoints:
        ("Clock (should work for anyone)", "https://api.alpaca.markets/v2/clock"),
        ("Calendar (should work for anyone)", "https://api.alpaca.markets/v2/calendar"),
        
        # IEX data (free tier):
        ("IEX Latest Quote", "https://data.alpaca.markets/v1beta1/stocks/AAPL/quotes/latest", {"feed": "iex"}),
        ("IEX Latest Trade", "https://data.alpaca.markets/v1beta1/stocks/AAPL/trades/latest", {"feed": "iex"}),
    ]
    
    async with httpx.AsyncClient(timeout=10) as client:
        for name, url, *params in basic_endpoints:
            query_params = params[0] if params else {}
            
            print(f"\n🧪 Testing: {name}")
            print(f"   URL: {url}")
            if query_params:
                print(f"   Params: {query_params}")
            
            try:
                response = await client.get(url, headers=headers, params=query_params)
                print(f"   📥 Status: {response.status_code}")
                print(f"   📑 Content-Type: {response.headers.get('content-type', 'unknown')}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        print(f"   ✅ SUCCESS! Response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                        if isinstance(data, dict) and len(data) < 10:  # Small response, show it
                            print(f"   📄 Data: {data}")
                    except:
                        print(f"   ✅ SUCCESS! (Non-JSON response: {response.text[:100]})")
                else:
                    content = response.text[:150]
                    print(f"   ❌ Error: {content}")
                    
            except Exception as e:
                print(f"   ❌ Exception: {e}")

if __name__ == "__main__":
    asyncio.run(test_basic_endpoints()) 