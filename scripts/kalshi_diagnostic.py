#!/usr/bin/env python3
"""Diagnostic script to inspect raw Kalshi API responses.

This script helps debug why certain weather markets aren't being fetched.
It dumps the raw API response to show exactly what Kalshi returns.

Usage:
    python scripts/kalshi_diagnostic.py
"""

import sys
import os

# Ensure the package is importable
sys.path.insert(0, os.getcwd())

from kalshicast.execution.kalshi_api import KalshiClient


def main():
    print("=" * 70)
    print("KALSHI API DIAGNOSTIC")
    print("=" * 70)
    print()

    client = KalshiClient()
    print(f"Base URL: {client.base_url}")
    print(f"API Key ID: {client.api_key_id[:8]}..." if client.api_key_id else "API Key ID: NOT SET")
    print()

    # Fetch ALL open events (no series filter)
    print("Fetching all open events (limit=200)...")
    try:
        all_events = client.get_events(status="open", limit=200)
    except Exception as e:
        print(f"ERROR fetching events: {e}")
        sys.exit(1)

    print(f"Total open events returned: {len(all_events)}")
    if len(all_events) >= 200:
        print("WARNING: Hit 200 limit - there may be more events!")
    print()

    # Filter for weather-related events
    weather_keywords = ["KXHIGH", "KXLOW", "HIGH", "LOW", "TEMP", "WEATHER"]
    weather = [
        e for e in all_events 
        if any(kw in e.get("event_ticker", "").upper() for kw in weather_keywords)
    ]
    
    print(f"Weather-related events found: {len(weather)}")
    print()

    if weather:
        print("WEATHER EVENTS:")
        print("-" * 70)
        for e in sorted(weather, key=lambda x: x.get("event_ticker", "")):
            et = e.get("event_ticker", "?")
            st = e.get("series_ticker", "?")
            title = e.get("title", "")[:50]
            n_markets = len(e.get("markets", []))
            status = e.get("status", "?")
            print(f"  {et:25} series={st:20} mkts={n_markets:2} status={status:6} | {title}")
        print()
    else:
        print("NO WEATHER EVENTS FOUND!")
        print()
        print("First 30 events of ANY type:")
        print("-" * 70)
        for e in all_events[:30]:
            et = e.get("event_ticker", "?")
            st = e.get("series_ticker", "?")
            title = e.get("title", "")[:50]
            print(f"  {et:30} series={st:25} | {title}")
        print()

    # Unique series_tickers breakdown
    all_series = {}
    for e in all_events:
        st = e.get("series_ticker", "(none)")
        all_series[st] = all_series.get(st, 0) + 1

    print("ALL SERIES_TICKERS (count of events):")
    print("-" * 70)
    for st, count in sorted(all_series.items(), key=lambda x: -x[1])[:40]:
        weather_flag = " <-- WEATHER?" if any(kw in st.upper() for kw in ["HIGH", "LOW", "TEMP"]) else ""
        print(f"  {st:35} {count:3} events{weather_flag}")

    print()
    print("=" * 70)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()