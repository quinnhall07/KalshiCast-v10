"""Logs GitHub Action workflow failures to the SYSTEM_ALERTS table."""

import sys
import json
import uuid
from datetime import datetime, timezone

# Import your existing DB connection utilities
from kalshicast.db.connection import init_db, get_conn, close_pool

def main():
    if len(sys.argv) < 2:
        print("Usage: python log_gh_failure.py <workflow_name> [run_url]")
        sys.exit(1)

    workflow_name = sys.argv[1]
    run_url = sys.argv[2] if len(sys.argv) > 2 else None

    # Generate alert data
    alert_id = uuid.uuid4().hex
    alert_type = "WORKFLOW_FAILURE"
    severity_score = 0.9  # High severity
    ts = datetime.now(timezone.utc)
    
    details = {
        "error": f"GitHub Action '{workflow_name}' failed.",
    }
    if run_url:
        details["html_url"] = run_url

    print(f"Logging failure for {workflow_name} to database...")

    init_db()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO system_alerts 
                (alert_id, alert_type, severity_score, alert_ts, source_id, is_resolved, details_json)
                VALUES 
                (:id, :type, :sev, :ts, :src, :res, :det)
            """, {
                "id": alert_id,
                "type": alert_type,
                "sev": severity_score,
                "ts": ts,
                "src": "github",
                "res": 0,
                "det": json.dumps(details)
            })
        conn.commit()
        print(f"Successfully logged critical alert: {alert_id}")
    except Exception as e:
        print(f"CRITICAL: Failed to log alert to DB: {e}")
    finally:
        conn.close()
        close_pool()

if __name__ == "__main__":
    main()
