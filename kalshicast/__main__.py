"""CLI entry point: python -m kalshicast <command>"""

from __future__ import annotations

import sys


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print("Usage: python -m kalshicast <command>")
        print("Commands: morning, night, market_open [--live], schema, observations,")
        print("          health, rollover, calibrate, backtest")
        sys.exit(1)

    cmd = args[0].lower()

    if cmd == "morning":
        from kalshicast.pipeline.morning import main as morning_main
        morning_main()

    elif cmd == "schema":
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
        from kalshicast.db.connection import init_db, get_conn, close_pool
        from kalshicast.db.schema import ensure_schema, seed_config_tables

        init_db()
        conn = get_conn()
        try:
            created = ensure_schema(conn)
            if created:
                print(f"Created {len(created)} tables: {', '.join(created)}")
            else:
                print("All tables already exist.")
            seed_config_tables(conn)
            print("Config tables seeded.")
        finally:
            conn.close()
            close_pool()

    elif cmd == "night":
        from kalshicast.pipeline.night import main as night_main
        night_main()

    elif cmd == "market_open":
        from kalshicast.pipeline.market_open import main as market_open_main
        market_open_main()

    elif cmd == "observations":
        from kalshicast.collection.collectors.collect_cli import fetch_observations
        from kalshicast.config import get_stations
        from kalshicast.db.connection import init_db, close_pool

        init_db()
        stations = get_stations(active_only=True)
        fetch_observations(stations=stations)
        close_pool()

    elif cmd == "health":
        import json
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
        from kalshicast.db.connection import init_db, get_conn, close_pool
        from kalshicast.pipeline.health import run_health_check

        def print_readable_report(report: dict) -> None:
            healthy = report.get("healthy", False)
            status_icon = "✅ HEALTHY" if healthy else "❌ UNHEALTHY"
            
            # Extract the timestamp and format it slightly to be cleaner
            raw_time = str(report.get('timestamp_utc', ''))
            clean_time = raw_time.split('.')[0].replace('T', ' ') if raw_time else 'Unknown'

            print("\n" + "="*55)
            print(" 🏥 KALSHICAST SYSTEM HEALTH REPORT")
            print("="*55)
            print(f" ⏱️  Timestamp (UTC): {clean_time}")
            print(f" 💡 Overall Status:  {status_icon}")
            
            checks = report.get("checks", {})
            
            # --- Database Status ---
            print("\n--- 🗄️  DATABASE " + "-"*38)
            db_ok = checks.get("db", {}).get("ok", False)
            print(f" Status: {'✅ OK' if db_ok else '❌ ERROR'}")
            
            # --- Missed Pipeline Runs ---
            missed = checks.get("missed_runs", {})
            missed_count = missed.get('count', 0)
            print(f"\n--- 🕒 MISSED PIPELINE RUNS ({missed_count}) " + "-"*23)
            if missed_count == 0:
                print(" ✅ All pipelines are running on schedule.")
            else:
                for run in missed.get("details", []):
                    run_type = run.get('run_type', 'unknown').upper()
                    exp_hour = run.get('expected_hour_utc', 0)
                    hours_ago = run.get('hours_since_check', 0)
                    print(f" • {run_type}: Expected at {exp_hour:02d}:00 UTC ({hours_ago} hours ago)")
                    
            # --- METAR Weather Freshness ---
            metar = checks.get("metar_freshness", {})
            stale_count = metar.get("stale_count", 0)
            print(f"\n--- 🌡️  WEATHER DATA FRESHNESS " + "-"*24)
            if stale_count == 0:
                print(" ✅ All station data is up to date.")
            else:
                print(f" ❌ Stale METAR Readings: {stale_count}")
                stations = ", ".join(metar.get("stations", []))
                print(f"    Affected Stations: {stations}")
                
            # --- Financial Risk (MDD) ---
            mdd = checks.get("mdd", {})
            mdd_status = mdd.get('status', 'UNKNOWN')
            
            if mdd_status == "NO_DATA":
                mdd_icon = "⚪"
            elif mdd_status == "OK":
                mdd_icon = "✅"
            else:
                mdd_icon = "⚠️"
                
            print("\n--- 📉 FINANCIAL RISK (MDD) " + "-"*27)
            print(f" Status: {mdd_icon} {mdd_status}")
            print(f" All-Time Max Drawdown: {mdd.get('mdd_alltime', 0.0)}%")
            print(f" 90-Day Max Drawdown:   {mdd.get('mdd_90', 0.0)}%")
            
            # --- Active Alerts ---
            alerts = checks.get("unresolved_alerts", {})
            alert_count = alerts.get('count', 0)
            print("\n--- ⚠️  ACTIVE ALERTS " + "-"*34)
            if alert_count == 0:
                print(" ✅ No unresolved alerts.")
            else:
                print(f" ❌ Unresolved Alerts: {alert_count}")
                
            print("="*55 + "\n")

        init_db()
        conn = get_conn()
        try:
            report = run_health_check(conn)
            print_readable_report(report)  # <--- Replaced the JSON print with our new function
        finally:
            conn.close()
            close_pool()

    elif cmd == "rollover":
        import json
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
        from kalshicast.db.connection import init_db, get_conn, close_pool
        from kalshicast.pipeline.rollover import run_rollover

        init_db()
        conn = get_conn()
        try:
            result = run_rollover(conn)
            print(json.dumps(result, indent=2, default=str))
        finally:
            conn.close()
            close_pool()

    elif cmd == "calibrate":
        import json
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
        from kalshicast.db.connection import init_db, get_conn, close_pool
        from kalshicast.evaluation.calibration import run_calibration

        init_db()
        conn = get_conn()
        try:
            changes = run_calibration(conn)
            print(f"{len(changes)} parameters updated")
            if changes:
                print(json.dumps(changes, indent=2, default=str))
        finally:
            conn.close()
            close_pool()

    elif cmd == "backtest":
        from kalshicast.backtest import main as backtest_main
        backtest_main()

    else:
        print(f"Unknown command: {cmd}")
        print("Commands: morning, night, market_open [--live], schema, observations,")
        print("          health, rollover, calibrate, backtest")
        sys.exit(1)


if __name__ == "__main__":
    main()
