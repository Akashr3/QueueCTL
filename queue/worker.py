# queue/worker.py
import subprocess
import time
import multiprocessing
import signal
import sys
from datetime import datetime, timedelta

from .db import get_connection, get_config
from .jobs import claim_next_job, update_job_state, _utc_now


shutdown_flag = multiprocessing.Event()


def graceful_shutdown(signum, frame):
    print("\n🛑 Received stop signal. Shutting down workers gracefully...")
    shutdown_flag.set()


def process_job(job):
    """Execute one job and handle retries + DLQ."""
    job_id = job["id"]
    command = job["command"]

    print(f"⚙️  Worker {multiprocessing.current_process().name} running job {job_id}: {command}")

    try:
        # Execute the command
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        exit_code = result.returncode

        if exit_code == 0:
            print(f"✅ Job {job_id} completed successfully")
            update_job_state(job_id, "completed")
        else:
            print(f"❌ Job {job_id} failed with exit code {exit_code}")
            handle_retry(job, result.stderr or result.stdout)

    except FileNotFoundError:
        print(f"❌ Command not found: {command}")
        handle_retry(job, "Command not found")

    except Exception as e:
        print(f"💥 Unexpected error in job {job_id}: {e}")
        handle_retry(job, str(e))


def handle_retry(job, error_message):
    """Handle retries with exponential backoff."""
    job_id = job["id"]
    attempts = job["attempts"] + 1
    max_retries = job["max_retries"]
    base = int(get_config("backoff_base") or 2)

    if attempts > max_retries:
        print(f"💀 Job {job_id} reached max retries. Moving to DLQ.")
        update_job_state(job_id, "dead", error_message)
        return

    delay = base ** attempts
    next_run = (datetime.utcnow() + timedelta(seconds=delay)).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE jobs
        SET state='pending', attempts=?, updated_at=?, next_run=?, last_error=?
        WHERE id=?
    """, (attempts, _utc_now(), next_run, error_message, job_id))
    conn.commit()
    conn.close()

    print(f"🔁 Retrying job {job_id} in {delay} seconds (attempt {attempts}/{max_retries})")


def worker_loop(worker_name):
    """Single worker loop."""
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    while not shutdown_flag.is_set():
        job = claim_next_job(worker_name)
        if not job:
            time.sleep(2)
            continue
        process_job(job)


def start_workers(count):
    """Spawn multiple worker processes."""
    print(f"🚀 Starting {count} worker(s)... Press Ctrl+C to stop.")
    procs = []
    for i in range(count):
        p = multiprocessing.Process(target=worker_loop, args=(f"worker-{i+1}",))
        p.start()
        procs.append(p)

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        graceful_shutdown(None, None)
        for p in procs:
            p.join()
