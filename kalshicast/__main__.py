"""CLI entry point: python -m kalshicast <command>"""

from __future__ import annotations

import sys


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print("Usage: python -m kalshicast <command>")
        print("Commands: morning, night, market_open, schema, observations")
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

    else:
        print(f"Unknown command: {cmd}")
        print("Commands: morning, night, market_open, schema, observations")
        sys.exit(1)


if __name__ == "__main__":
    main()
