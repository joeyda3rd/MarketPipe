# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
"""
Comprehensive Alpaca API Debug Script
Tests every aspect of API authentication and endpoints
"""

import os
import sys
import asyncio
import httpx
from pathlib import Path

print("🔍 COMPREHENSIVE ALPACA API DEBUG SCRIPT")
print("=" * 50)

# ==================== STEP 1: Environment Debug ====================
print("\n📋 STEP 1: ENVIRONMENT & .ENV FILE DEBUG")
print("-" * 40)

# Check current working directory
cwd = Path.cwd()
print(f"📂 Current working directory: {cwd}")

# Check for .env file in multiple locations
env_locations = [
    Path(".env"),
    Path(cwd / ".env"),
    Path.home() / "MarketPipe" / ".env",
    Path("/home/joey/MarketPipe/.env")
]

env_file_found = None
for env_path in env_locations:
    if env_path.exists():
        env_file_found = env_path
        print(f"✅ Found .env file: {env_path}")
        
        # Read and display .env contents (safely)
        with open(env_path, 'r') as f:
            content = f.read()
            print(f"📄 .env file contents ({len(content)} chars):")
            for i, line in enumerate(content.splitlines(), 1):
                if line.strip() and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        # Show key and first/last few chars of value for security
                        safe_value = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else value
                        print(f"   Line {i}: {key}={safe_value}")
                    else:
                        print(f"   Line {i}: {line}")
                else:
                    print(f"   Line {i}: {line}")
        break
else:
    print("❌ No .env file found in any of these locations:")
    for loc in env_locations:
        print(f"   - {loc}")

# Try loading .env with different methods
print(f"\n🔧 Loading .env file methods:")

# Method 1: Manual parsing
if env_file_found:
    print(f"📖 Method 1: Manual parsing of {env_file_found}")
    manual_env = {}
    with open(env_file_found, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                manual_env[key] = value
                safe_val = f"{value[:6]}...{value[-4:]}" if len(value) > 10 else value
                print(f"   {key} = {safe_val}")
    
    # Set in os.environ
    for k, v in manual_env.items():
        os.environ[k] = v
        print(f"   ✅ Set os.environ['{k}']")

# Method 2: python-dotenv
try:
    from dotenv import load_dotenv
    if env_file_found:
        load_dotenv(env_file_found)
        print(f"✅ Method 2: python-dotenv loaded {env_file_found}")
    else:
        load_dotenv()  # Try default locations
        print("✅ Method 2: python-dotenv loaded from default location")
except ImportError:
    print("❌ Method 2: python-dotenv not available")

# Check final environment variables
print(f"\n🔍 Final environment variable check:")
for key in ['ALPACA_KEY', 'ALPACA_SECRET']:
    value = os.environ.get(key)
    if value:
        safe_val = f"{value[:8]}...{value[-6:]}" if len(value) > 14 else f"{value[:8]}..."
        print(f"   ✅ {key} = {safe_val} (length: {len(value)})")
    else:
        print(f"   ❌ {key} = NOT SET")

# ==================== STEP 2: API Endpoint Discovery ====================
print(f"\n🌐 STEP 2: API ENDPOINT DISCOVERY")
print("-" * 40)

# Get credentials
alpaca_key = os.environ.get('ALPACA_KEY')
alpaca_secret = os.environ.get('ALPACA_SECRET')

if not alpaca_key or not alpaca_secret:
    print("❌ Cannot proceed - missing credentials")
    sys.exit(1)

print(f"🔑 Using credentials: {alpaca_key[:8]}... / {alpaca_secret[:8]}...")

# Test different base URLs and endpoints
test_endpoints = [
    # Trading API endpoints
    ("Live Trading API", "https://api.alpaca.markets", "/v2/account"),
    ("Paper Trading API", "https://paper-api.alpaca.markets", "/v2/account"),
    ("Live Trading API (no /account)", "https://api.alpaca.markets", "/v2"),
    ("Paper Trading API (no /account)", "https://paper-api.alpaca.markets", "/v2"),
    
    # Market Data API endpoints  
    ("Market Data API", "https://data.alpaca.markets", "/v2"),
    ("Market Data API /bars", "https://data.alpaca.markets", "/v2/stocks/AAPL/bars"),
    ("Market Data API /latest", "https://data.alpaca.markets", "/v2/stocks/AAPL/bars/latest"),
    
    # Alternative market data endpoints
    ("Market Data v1beta1", "https://data.alpaca.markets", "/v1beta1/bars/AAPL"),
    ("Market Data v1", "https://data.alpaca.markets", "/v1/bars/1Min"),
    
    # Alternative hostnames
    ("Polygon-style", "https://api.polygon.io", "/v2/aggs/ticker/AAPL/range/1/minute/2023-01-01/2023-01-02"),
]

async def test_endpoint(name: str, base_url: str, path: str) -> dict:
    """Test a single endpoint with full debugging"""
    print(f"\n🧪 Testing: {name}")
    full_url = f"{base_url}{path}"
    print(f"   URL: {full_url}")
    
    # Different auth header combinations to try
    auth_variants = [
        # Standard Alpaca headers
        {
            "APCA-API-KEY-ID": alpaca_key,
            "APCA-API-SECRET-KEY": alpaca_secret
        },
        # Alternative header names (kebab-case)
        {
            "APCA-Api-Key-Id": alpaca_key,
            "APCA-Api-Secret-Key": alpaca_secret
        },
        # Bearer token style
        {
            "Authorization": f"Bearer {alpaca_key}:{alpaca_secret}"
        },
        # Basic auth style
        {
            "Authorization": f"Basic {alpaca_key}:{alpaca_secret}"
        },
    ]
    
    for i, auth_headers in enumerate(auth_variants, 1):
        print(f"   🔐 Auth variant {i}: {list(auth_headers.keys())}")
        
        headers = {
            "Accept": "application/json",
            "User-Agent": "MarketPipe-Debug/1.0",
            **auth_headers
        }
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Add some common query parameters for market data endpoints
                params = {}
                if "bars" in path.lower() and "AAPL" in path:
                    params = {
                        "timeframe": "1Min",
                        "start": "2025-01-01T00:00:00Z",
                        "end": "2025-01-02T00:00:00Z",
                        "limit": "10"
                    }
                
                print(f"      📡 Request headers: {list(headers.keys())}")
                if params:
                    print(f"      📋 Query params: {params}")
                
                response = await client.get(full_url, headers=headers, params=params)
                
                print(f"      📥 Response: {response.status_code} {response.reason_phrase}")
                print(f"      📏 Content length: {len(response.content)} bytes")
                print(f"      📑 Content type: {response.headers.get('content-type', 'unknown')}")
                
                # Show response headers that might be helpful
                interesting_headers = [
                    'x-ratelimit-remaining', 'x-ratelimit-limit', 'x-ratelimit-reset',
                    'server', 'via', 'cf-ray', 'access-control-allow-origin'
                ]
                for header in interesting_headers:
                    if header in response.headers:
                        print(f"      🏷️  {header}: {response.headers[header]}")
                
                # Try to parse response
                content_preview = response.text[:200].replace('\n', '\\n')
                print(f"      📄 Content preview: {content_preview}")
                
                if response.status_code == 200:
                    try:
                        json_data = response.json()
                        print(f"      ✅ Valid JSON with keys: {list(json_data.keys()) if isinstance(json_data, dict) else 'non-dict'}")
                        return {"success": True, "status": response.status_code, "auth_variant": i}
                    except:
                        print(f"      ⚠️  200 OK but invalid JSON")
                        return {"success": False, "status": response.status_code, "auth_variant": i, "error": "invalid_json"}
                else:
                    return {"success": False, "status": response.status_code, "auth_variant": i, "error": response.text[:100]}
                    
        except Exception as e:
            print(f"      ❌ Exception: {e}")
            return {"success": False, "auth_variant": i, "error": str(e)}
    
    return {"success": False, "error": "all_auth_variants_failed"}

# ==================== STEP 3: Comprehensive API Testing ====================
print(f"\n🧪 STEP 3: COMPREHENSIVE API TESTING")
print("-" * 40)

async def main():
    results = []
    
    for name, base_url, path in test_endpoints:
        result = await test_endpoint(name, base_url, path)
        result["endpoint_name"] = name
        result["base_url"] = base_url
        result["path"] = path
        results.append(result)
        
        # Brief pause between tests
        await asyncio.sleep(0.5)
    
    # ==================== STEP 4: Results Summary ====================
    print(f"\n📊 STEP 4: RESULTS SUMMARY")
    print("-" * 40)
    
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    
    print(f"✅ Successful endpoints: {len(successful)}")
    for result in successful:
        print(f"   • {result['endpoint_name']} (auth variant {result.get('auth_variant', '?')})")
    
    print(f"\n❌ Failed endpoints: {len(failed)}")
    for result in failed:
        status = result.get('status', 'unknown')
        error = result.get('error', 'unknown')[:50]
        print(f"   • {result['endpoint_name']} - {status} - {error}")
    
    # ==================== STEP 5: Recommendations ====================
    print(f"\n💡 STEP 5: RECOMMENDATIONS")
    print("-" * 40)
    
    if successful:
        print("🎉 Great! Some endpoints are working. Use these for your MarketPipe configuration:")
        for result in successful:
            print(f"\n✅ {result['endpoint_name']}:")
            print(f"   Base URL: {result['base_url']}")
            print(f"   Path: {result['path']}")
            print(f"   Auth variant: {result.get('auth_variant', '?')}")
    else:
        print("❌ No endpoints worked. Check:")
        print("1. 🔑 Verify your API credentials in Alpaca dashboard")
        print("2. 🌐 Check if your IP is whitelisted (if required)")
        print("3. 💰 Verify your account tier supports the APIs you're testing")
        print("4. 🕐 Check if there are any temporary service issues")
        print("5. 📋 Try generating new API credentials")

if __name__ == "__main__":
    asyncio.run(main()) 