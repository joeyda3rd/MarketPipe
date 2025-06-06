#!/usr/bin/env python3
"""
Alpaca IEX (Free Tier) API Test
Specifically targeting IEX endpoints that should work with free accounts
"""

import os
import asyncio
import httpx
from dotenv import load_dotenv

load_dotenv()

alpaca_key = os.environ.get('ALPACA_KEY')
alpaca_secret = os.environ.get('ALPACA_SECRET')

print(f"🔑 Testing IEX endpoints with credentials: {alpaca_key[:8]}... / {alpaca_secret[:8]}...")

async def test_iex_endpoints():
    headers = {
        "APCA-API-KEY-ID": alpaca_key,
        "APCA-API-SECRET-KEY": alpaca_secret,
        "Accept": "application/json"
    }
    
    # IEX-specific endpoints and parameters
    iex_endpoints = [
        # Basic endpoints (no auth needed, but test with auth)
        ("Market Clock", "https://api.alpaca.markets/v2/clock", {}),
        ("Market Calendar", "https://api.alpaca.markets/v2/calendar", {}),
        
        # IEX Market Data endpoints - v1beta1 with feed=iex
        ("IEX Latest Quote", "https://data.alpaca.markets/v1beta1/stocks/AAPL/quotes/latest", {"feed": "iex"}),
        ("IEX Latest Trade", "https://data.alpaca.markets/v1beta1/stocks/AAPL/trades/latest", {"feed": "iex"}),
        
        # IEX Historical data - v1beta1 
        ("IEX Historical Quotes", "https://data.alpaca.markets/v1beta1/stocks/AAPL/quotes", {
            "start": "2025-01-02T00:00:00Z",
            "end": "2025-01-02T23:59:59Z", 
            "feed": "iex",
            "limit": "10"
        }),
        ("IEX Historical Trades", "https://data.alpaca.markets/v1beta1/stocks/AAPL/trades", {
            "start": "2025-01-02T00:00:00Z",
            "end": "2025-01-02T23:59:59Z",
            "feed": "iex", 
            "limit": "10"
        }),
        
        # IEX Bar data (OHLCV) - this is what MarketPipe typically needs
        ("IEX Latest Bar", "https://data.alpaca.markets/v1beta1/stocks/AAPL/bars/latest", {"feed": "iex"}),
        ("IEX Historical Bars", "https://data.alpaca.markets/v1beta1/stocks/AAPL/bars", {
            "start": "2025-01-02T00:00:00Z", 
            "end": "2025-01-02T23:59:59Z",
            "feed": "iex",
            "timeframe": "1Min",
            "limit": "10"
        }),
        
        # Try v2 endpoints with IEX feed parameter  
        ("IEX v2 Bars", "https://data.alpaca.markets/v2/stocks/AAPL/bars", {
            "start": "2025-01-02T00:00:00Z",
            "end": "2025-01-02T23:59:59Z", 
            "feed": "iex",
            "timeframe": "1Min",
            "limit": "10"
        }),
        
        # Alternative IEX URLs (sometimes different paths work)
        ("IEX Alt Path", "https://data.alpaca.markets/v1/stocks/quotes/latest", {
            "symbols": "AAPL",
            "feed": "iex"
        }),
    ]
    
    async with httpx.AsyncClient(timeout=15) as client:
        success_count = 0
        
        for name, url, params in iex_endpoints:
            print(f"\n🧪 Testing: {name}")
            print(f"   URL: {url}")
            if params:
                print(f"   Params: {params}")
            
            try:
                response = await client.get(url, headers=headers, params=params)
                status_code = response.status_code
                content_type = response.headers.get('content-type', 'unknown')
                
                print(f"   📥 Status: {status_code}")
                print(f"   📑 Content-Type: {content_type}")
                
                if status_code == 200:
                    success_count += 1
                    try:
                        data = response.json()
                        print(f"   ✅ SUCCESS! Response type: {type(data)}")
                        
                        if isinstance(data, dict):
                            keys = list(data.keys())
                            print(f"   📋 Response keys: {keys}")
                            
                            # Show sample data for small responses
                            if len(str(data)) < 500:
                                print(f"   📄 Sample data: {data}")
                            else:
                                # For large responses, show structure
                                for key, value in list(data.items())[:3]:
                                    if isinstance(value, list) and value:
                                        print(f"   📊 {key}: list with {len(value)} items, sample: {value[0] if value else 'empty'}")
                                    else:
                                        print(f"   📊 {key}: {type(value)} = {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
                        else:
                            print(f"   📄 Response: {str(data)[:200]}")
                            
                    except Exception as json_error:
                        print(f"   ✅ SUCCESS! (Non-JSON response)")
                        print(f"   📄 Content preview: {response.text[:200]}")
                        
                elif status_code == 403:
                    content = response.text[:200]
                    if "forbidden" in content.lower():
                        print(f"   ❌ 403 Forbidden: {content}")
                    else:
                        print(f"   ❌ 403 HTML Error (likely subscription required)")
                        
                elif status_code == 422:
                    # Unprocessable entity - often means bad parameters but endpoint exists
                    try:
                        error_data = response.json()
                        print(f"   ⚠️  422 Parameter Error: {error_data}")
                    except:
                        print(f"   ⚠️  422 Parameter Error: {response.text[:200]}")
                        
                else:
                    content = response.text[:200]
                    print(f"   ❌ Error {status_code}: {content}")
                    
            except Exception as e:
                print(f"   ❌ Exception: {e}")
        
        print(f"\n📊 SUMMARY: {success_count}/{len(iex_endpoints)} endpoints successful")
        
        if success_count > 0:
            print("🎉 Great! Some IEX endpoints are working.")
            print("💡 Use the successful endpoints for MarketPipe configuration.")
        else:
            print("❌ No IEX endpoints worked. This suggests:")
            print("1. 🔑 API credentials may still be invalid")
            print("2. 🏢 Account may not be set up properly") 
            print("3. 📅 Try with a different date range (market hours)")
            print("4. 🌐 Check Alpaca account dashboard for restrictions")

if __name__ == "__main__":
    asyncio.run(test_iex_endpoints()) 