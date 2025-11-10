# queue/jobs.py
import json
import uuid
from datetime import datetime
from .db import get_connection

def _utc_now():
    """Return current UTC timestamp in ISO8601."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------
#  Enqueue new job
# ---------------------------
def enqueue_job(job_json: str):
    """
    Enqueue a new job from a JSON string.
    Example:
        enqueue_job('{"command": "echo hello"}')
    """
    try:
        job = json.loads(job_json)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON input")

    # Generate ID if not given
    job_id = job.get("id") or str(uuid.uuid4())
    command = job.get("command")
    if not command:
        raise ValueError("Job must include a 'command' field")

    max_retries = int(job.get("max_retries", 3))
    created = _utc_now()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
        INSERT INTO jobs (id, command, state, attempts, max_retries,
                          created_at, updated_at, next_run)
        VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)
        """, (job_id, command, max_retries, created, created, created))
        conn.commit()
        return job_id
    except Exception as e:
        raise RuntimeError(f"Failed to enqueue job: {e}")
    finally:
        conn.close()


# ---------------------------
#  List jobs
# ---------------------------
def list_jobs(state=None):
    """Return list of jobs, optionally filtered by state."""
    conn = get_connection()
    cur = conn.cursor()

    if state:
        cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at DESC", (state,))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at DESC")

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


# ---------------------------
#  Update job state
# ---------------------------
def update_job_state(job_id, new_state, error_message=None):
    """Update a job's state and optionally store last error."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE jobs
        SET state=?, updated_at=?, last_error=?
        WHERE id=?
    """, (new_state, _utc_now(), error_message, job_id))
    conn.commit()
    conn.close()


# ---------------------------
#  Fetch next pending job (for workers)
# ---------------------------
def claim_next_job(worker_name):
    """
    Atomically claim one job that's ready to run.
    Returns job dict or None if none available.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE jobs
        SET state='processing', worker=?, updated_at=?
        WHERE id = (
            SELECT id FROM jobs
            WHERE state='pending' AND datetime(next_run) <= datetime('now')
            ORDER BY created_at ASC LIMIT 1
        )
        RETURNING *
    """, (worker_name, _utc_now()))

    row = cur.fetchone()
    conn.commit()
    conn.close()
    return dict(row) if row else None

# ---------------------------
#  Status and DLQ helpers
# ---------------------------

def get_job_counts():
    """Return count of jobs by state."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state")
    result = {row["state"]: row["count"] for row in cur.fetchall()}
    conn.close()
    return result


def list_dlq_jobs():
    """Return all jobs in Dead Letter Queue (state='dead')."""
    return list_jobs("dead")


def retry_dlq_job(job_id):
    """Reset a DLQ job to pending with 0 attempts."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE jobs
        SET state='pending', attempts=0, updated_at=datetime('now'), next_run=datetime('now'), last_error=NULL
        WHERE id=? AND state='dead'
    """, (job_id,))
    changed = cur.rowcount
    conn.commit()
    conn.close()
    if changed:
        return True
    return False
