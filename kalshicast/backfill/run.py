# kalshicast/backfill/run.py
"""CLI entry point for historical backfill.

Usage:
    # Full backfill with default 2-year window:
    python -m kalshicast.backfill.run

    # Custom window:
    python -m kalshicast.backfill.run --start 2023-01-01 --end 2023-12-31

    # Skip phases (debugging only — breaks load order if misused):
    python -m kalshicast.backfill.run --skip-phases observations forecasts

    # Dry-run: print coverage summary without loading anything:
    python -m kalshicast.backfill.run --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from kalshicast.backfill.config import BACKFILL_START, BACKFILL_END
from kalshicast.backfill.orchestrator import run_backfill
from kalshicast.db.connection import init_db, get_conn, close_pool
from kalshicast.db.operations import get_backfill_coverage
from kalshicast.db.schema import ensure_schema, seed_config_tables
from kalshicast.config.params_bootstrap import load_db_overrides
from kalshicast.db.operations import load_all_params


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="KalshiCast historical data backfill")
    p.add_argument("--start", default=BACKFILL_START.isoformat(),
                   help="Start date YYYY-MM-DD (default: 2 years ago)")
    p.add_argument("--end", default=BACKFILL_END.isoformat(),
                   help="End date YYYY-MM-DD (default: yesterday)")
    p.add_argument("--skip-phases", nargs="+",
                   choices=["observations", "forecasts", "errors", "kalman", "bss"],
                   default=[],
                   help="Force-skip named phases (debugging only)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print coverage summary and exit without loading")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )
    log = logging.getLogger(__name__)

    # DB init
    init_db()
    conn = get_conn()
    try:
        ensure_schema(conn)
        seed_config_tables(conn)
        db_params = load_all_params(conn)
        load_db_overrides(db_params)
    finally:
        conn.close()

    conn = get_conn()
    try:
        if args.dry_run:
            coverage = get_backfill_coverage(conn, args.start, args.end)
            print(f"\n=== Backfill Coverage: {args.start} to {args.end} ===")
            print(f"  Observation dates: {len(coverage['observation_dates'])}")
            for src, dates in coverage["forecast_dates"].items():
                print(f"  Forecast dates ({src}): {len(dates)}")
            print(f"  Error dates:       {len(coverage['error_dates'])}")
            print(f"  Kalman dates:      {len(coverage['kalman_dates'])}")
            print("")
            return

        result = run_backfill(
            conn,
            start_date=args.start,
            end_date=args.end,
            skip_phases=args.skip_phases,
        )
        
        # After result = run_backfill(...) and print block:
        if not args.dry_run and not args.skip_phases:
            from kalshicast.backfill.validate import run_validation
            ok = run_validation(conn, args.start, args.end)
            if not ok:
                log.warning("Validation failed — review output above before running live pipelines")
                sys.exit(2)
        
        print("\n=== Backfill Complete ===")
        for phase, n in result["phases"].items():
            print(f"  {phase}: {n} rows written")
        print("")

    except Exception as e:
        log.exception("Backfill failed: %s", e)
        sys.exit(1)
    finally:
        conn.close()
        close_pool()


if __name__ == "__main__":
    main()