#!/usr/bin/env python3
"""Diagnostic script to discover actual Kalshi weather series tickers.

Root cause: Kalshi uses KXHIGHNY not KXHIGHNYC for NYC weather.
This script queries /markets to find actual series_tickers.
"""

import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

def main():
    print("=" * 70)
    print("KALSHI WEATHER SERIES DISCOVERY")
    print("=" * 70)
    print(f"Base URL: {BASE_URL}")
    print()

    # Query markets endpoint with weather-related filters
    # Try to find all KXHIGH* and KXLOW* markets
    print("Querying /markets for weather series...")
    
    all_weather_markets = []
    cursor = None
    
    while True:
        params = {"limit": 200, "status": "open"}
        if cursor:
            params["cursor"] = cursor
            
        resp = requests.get(f"{BASE_URL}/markets", params=params)
        if resp.status_code != 200:
            print(f"Error: {resp.status_code} - {resp.text[:200]}")
            break
            
        data = resp.json()
        markets = data.get("markets", [])
        
        # Filter for weather markets
        weather = [m for m in markets if m.get("series_ticker", "").startswith(("KXHIGH", "KXLOW"))]
        all_weather_markets.extend(weather)
        
        # Check for more pages
        cursor = data.get("cursor")
        if not cursor or not markets:
            break
        print(f"  ... fetched {len(markets)} markets, {len(weather)} weather, continuing...")

    print(f"\nTotal weather markets found: {len(all_weather_markets)}")
    print()

    if all_weather_markets:
        # Group by series_ticker
        series_map = {}
        for m in all_weather_markets:
            st = m.get("series_ticker", "?")
            if st not in series_map:
                series_map[st] = []
            series_map[st].append(m)

        print("WEATHER SERIES FOUND:")
        print("-" * 70)
        for st in sorted(series_map.keys()):
            markets = series_map[st]
            sample = markets[0]
            print(f"  {st:20} ({len(markets):2} markets) | event: {sample.get('event_ticker', '?')[:30]}")

        print()
        print("SAMPLE MARKET DETAILS:")
        print("-" * 70)
        for m in all_weather_markets[:5]:
            print(f"  ticker:        {m.get('ticker')}")
            print(f"  series_ticker: {m.get('series_ticker')}")
            print(f"  event_ticker:  {m.get('event_ticker')}")
            print(f"  title:         {m.get('title', '')[:60]}")
            print()
    else:
        print("NO WEATHER MARKETS FOUND!")
        print()
        print("Trying to query known series directly...")
        
        # Try querying specific series we know should exist
        test_series = ["KXHIGHNY", "KXHIGHNYC", "KXHIGHMIA", "KXHIGHCHI"]
        for series in test_series:
            resp = requests.get(f"{BASE_URL}/series/{series}")
            status = "EXISTS" if resp.status_code == 200 else f"NOT FOUND ({resp.status_code})"
            print(f"  {series}: {status}")

    print()
    print("=" * 70)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()