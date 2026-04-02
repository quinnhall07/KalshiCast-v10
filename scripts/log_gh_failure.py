"""Logs GitHub Action workflow failures and cleans up ghost runs."""

import sys
import json
import uuid
from datetime import datetime, timezone
from kalshicast.db.connection import init_db, get_conn, close_pool

def main():
    if len(sys.argv) < 2:
        print("Usage: python log_gh_failure.py <workflow_name> [run_url]")
        sys.exit(1)

    workflow_name = sys.argv[1]
    run_url = sys.argv[2] if len(sys.argv) > 2 else None

    alert_id = uuid.uuid4().hex
    alert_type = "WORKFLOW_FAILURE"
    severity_score = 0.9  
    ts = datetime.now(timezone.utc)
    
    details = {"error": f"GitHub Action '{workflow_name}' failed, timed out, or was cancelled."}
    if run_url:
        details["html_url"] = run_url

    print(f"Logging failure and cleaning ghost runs for {workflow_name}...")

    init_db()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 1. Log the Alert
            cur.execute("""
                INSERT INTO system_alerts 
                (alert_id, alert_type, severity_score, alert_ts, source_id, is_resolved, details_json)
                VALUES 
                (:id, :type, :sev, :ts, :src, 0, :det)
            """, {
                "id": alert_id, "type": alert_type, "sev": severity_score,
                "ts": ts, "src": "github", "det": json.dumps(details)
            })
            
            # 2. Fix stuck "RUNNING" ghost runs
            # This closes out the run that was forcefully killed by GitHub
            cur.execute("""
                UPDATE pipeline_runs 
                SET status = 'ERROR', 
                    error_msg = 'Process forcefully terminated by GitHub Action timeout/cancellation.'
                WHERE pipeline_type = :wf AND status = 'RUNNING'
            """, {"wf": workflow_name})
            
        conn.commit()
        print(f"Successfully logged alert {alert_id} and closed orphaned runs.")
    except Exception as e:
        print(f"CRITICAL: Failed to log to DB: {e}")
    finally:
        conn.close()
        close_pool()

if __name__ == "__main__":
    main()
