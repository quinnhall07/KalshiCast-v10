#!/usr/bin/env python3
"""Discover correct Kalshi city codes for weather series."""

import requests
import time

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Cities we care about and possible code variants to try
CITIES = {
    "New York": ["NY", "NYC", "NEWYORK"],
    "Miami": ["MIA", "MI", "MIAMI"],
    "Chicago": ["CHI", "CH", "CHICAGO", "MDW"],
    "Los Angeles": ["LA", "LAX", "LOS"],
    "Austin": ["AUS", "AU", "AUSTIN"],
    "Denver": ["DEN", "DE", "DENVER"],
    "Seattle": ["SEA", "SE", "SEATTLE"],
    "San Francisco": ["SFO", "SF", "SANFRAN"],
    "Washington DC": ["DCA", "DC", "WAS", "WASH"],
    "Boston": ["BOS", "BO", "BOSTON"],
    "Atlanta": ["ATL", "AT", "ATLANTA"],
    "Phoenix": ["PHX", "PH", "PHOENIX"],
    "San Antonio": ["SAT", "SA", "SANANT"],
    "Dallas": ["DFW", "DAL", "DALLAS"],
    "Houston": ["HOU", "HO", "HOUSTON", "IAH"],
    "Las Vegas": ["LAS", "LV", "VEGAS"],
    "Philadelphia": ["PHL", "PH", "PHILA"],
    "New Orleans": ["MSY", "NO", "NEWORLEANS"],
    "Minneapolis": ["MSP", "MIN", "MPLS"],
    "Oklahoma City": ["OKC", "OK", "OKLAHOMA"],
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
    print("KALSHI WEATHER CITY CODE DISCOVERY")
    print("=" * 70)
    print()
    
    found_codes = {}
    
    for city, variants in CITIES.items():
        print(f"{city}:")
        found_high = None
        found_low = None
        
        for code in variants:
            # Check HIGH
            high_series = f"KXHIGH{code}"
            low_series = f"KXLOW{code}"
            
            if not found_high and check_series(high_series):
                found_high = code
                print(f"  ✓ HIGH: KXHIGH{code}")
            
            time.sleep(0.1)  # Rate limit
            
            if not found_low and check_series(low_series):
                found_low = code
                print(f"  ✓ LOW:  KXLOW{code}")
            
            time.sleep(0.1)
            
            if found_high and found_low:
                break
        
        if not found_high:
            print(f"  ✗ HIGH: not found (tried {', '.join('KXHIGH'+v for v in variants)})")
        if not found_low:
            print(f"  ✗ LOW:  not found (tried {', '.join('KXLOW'+v for v in variants)})")
        
        found_codes[city] = {"high": found_high, "low": found_low}
        print()
    
    print("=" * 70)
    print("SUMMARY - Correct Kalshi city codes:")
    print("=" * 70)
    for city, codes in found_codes.items():
        h = codes['high'] or '???'
        l = codes['low'] or '???'
        print(f"  {city:20} HIGH={h:6} LOW={l:6}")
    
    print()
    print("=" * 70)

if __name__ == "__main__":
    main()