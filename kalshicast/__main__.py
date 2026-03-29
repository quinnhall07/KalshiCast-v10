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

        init_db()
        conn = get_conn()
        try:
            report = run_health_check(conn)
            print(json.dumps(report, indent=2, default=str))
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
