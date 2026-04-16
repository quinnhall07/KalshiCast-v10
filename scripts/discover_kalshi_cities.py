#!/usr/bin/env python3
"""Discover correct Kalshi city codes - checks both KXHIGH and KXHIGHT patterns."""

import requests
import time

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# All prefixes to try (KXHIGH/KXLOW and KXHIGHT/KXLOWT)
HIGH_PREFIXES = ["KXHIGH", "KXHIGHT"]
LOW_PREFIXES = ["KXLOW", "KXLOWT"]

# Cities and code variants
CITIES = {
    "New York": ["NY", "NYC"],
    "Miami": ["MIA", "MI"],
    "Chicago": ["CHI", "MDW"],
    "Los Angeles": ["LAX", "LA"],
    "Austin": ["AUS"],
    "Denver": ["DEN"],
    "Seattle": ["SEA"],
    "San Francisco": ["SFO", "SF"],
    "Washington DC": ["DC", "DCA", "WAS"],
    "Boston": ["BOS"],
    "Atlanta": ["ATL"],
    "Phoenix": ["PHX"],
    "San Antonio": ["SAT", "SA"],
    "Dallas": ["DFW", "DAL"],
    "Houston": ["HOU", "IAH"],
    "Las Vegas": ["LAS", "LV"],
    "Philadelphia": ["PHL", "PHI"],
    "New Orleans": ["MSY", "NO"],
    "Minneapolis": ["MSP", "MIN"],
    "Oklahoma City": ["OKC"],
}

def check_series(series_ticker):
    """Check if a series exists."""
    try:
        resp = requests.get(f"{BASE_URL}/series/{series_ticker}", timeout=10)
        return resp.status_code == 200
    except:
        return False

def main():
    print("=" * 70)
    print("KALSHI WEATHER CITY CODE DISCOVERY v2")
    print("Checking both KXHIGH/KXLOW and KXHIGHT/KXLOWT patterns")
    print("=" * 70)
    print()
    
    results = {}
    
    for city, codes in CITIES.items():
        print(f"{city}:")
        found_high = None
        found_low = None
        
        # Try all combinations of prefix + city code
        for code in codes:
            if not found_high:
                for prefix in HIGH_PREFIXES:
                    series = f"{prefix}{code}"
                    if check_series(series):
                        found_high = series
                        print(f"  ✓ HIGH: {series}")
                        break
                    time.sleep(0.15)
            
            if not found_low:
                for prefix in LOW_PREFIXES:
                    series = f"{prefix}{code}"
                    if check_series(series):
                        found_low = series
                        print(f"  ✓ LOW:  {series}")
                        break
                    time.sleep(0.15)
            
            if found_high and found_low:
                break
        
        if not found_high:
            tried = [f"{p}{c}" for c in codes for p in HIGH_PREFIXES]
            print(f"  ✗ HIGH: not found (tried {', '.join(tried)})")
        if not found_low:
            tried = [f"{p}{c}" for c in codes for p in LOW_PREFIXES]
            print(f"  ✗ LOW:  not found (tried {', '.join(tried)})")
        
        results[city] = {"high": found_high, "low": found_low}
        print()
    
    print("=" * 70)
    print("SUMMARY - Kalshi series tickers:")
    print("=" * 70)
    print(f"{'City':<20} {'HIGH Series':<20} {'LOW Series':<20}")
    print("-" * 60)
    for city, data in results.items():
        h = data['high'] or '(not found)'
        l = data['low'] or '(not found)'
        print(f"{city:<20} {h:<20} {l:<20}")
    
    print()
    print("=" * 70)
    print("STATION CONFIG UPDATE NEEDED:")
    print("=" * 70)
    for city, data in results.items():
        if data['high'] or data['low']:
            # Extract the pattern
            sample = data['high'] or data['low']
            if 'KXHIGHT' in sample or 'KXLOWT' in sample:
                pattern = "KXHIGHT/KXLOWT"
                code = sample.replace('KXHIGHT', '').replace('KXLOWT', '')
            else:
                pattern = "KXHIGH/KXLOW"
                code = sample.replace('KXHIGH', '').replace('KXLOW', '')
            print(f'  "{city}": kalshi_city="{code}", pattern="{pattern}"')

if __name__ == "__main__":
    main()