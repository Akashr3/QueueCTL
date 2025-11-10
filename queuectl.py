#!/usr/bin/env python3
import argparse
import json
import sys

from queue.db import init_db, get_config, set_config
from queue.jobs import enqueue_job, list_jobs, get_job_counts, list_dlq_jobs, retry_dlq_job
from queue.worker import start_workers

def cmd_enqueue(args):
    try:
        job_id = enqueue_job(args.job_json)
        print(f"✅ Job enqueued successfully: {job_id}")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def cmd_list(args):
    state = args.state
    jobs = list_jobs(state)
    if not jobs:
        print("(no jobs found)")
        return

    for job in jobs:
        print(f"[{job['state']}] {job['id']}  cmd='{job['command']}' attempts={job['attempts']}")


def cmd_config(args):
    if args.action == "get":
        val = get_config(args.key)
        if val is None:
            print(f"⚠️ No config found for key '{args.key}'")
        else:
            print(f"{args.key} = {val}")
    elif args.action == "set":
        set_config(args.key, args.value)
        print(f"✅ Updated {args.key} = {args.value}")


def cmd_init(args):
    init_db()
    print("✅ Database initialized successfully.")

def cmd_status(args):
    counts = get_job_counts()
    total = sum(counts.values()) if counts else 0
    print("\n📊 Job Status Summary")
    print("-" * 30)
    for state in ["pending", "processing", "completed", "failed", "dead"]:
        print(f"{state:<12}: {counts.get(state, 0)}")
    print("-" * 30)
    print(f"Total jobs: {total}")


def cmd_dlq(args):
    if args.action == "list":
        jobs = list_dlq_jobs()
        if not jobs:
            print("🟢 DLQ is empty.")
            return
        print("\n💀 Dead Letter Queue:")
        print("-" * 40)
        for job in jobs:
            print(f"{job['id']} | cmd='{job['command']}' | attempts={job['attempts']} | error={job['last_error']}")
    elif args.action == "retry":
        if not args.job_id:
            print("❌ Please provide a job_id to retry.")
            return
        success = retry_dlq_job(args.job_id)
        if success:
            print(f"🔁 Job {args.job_id} moved back to pending.")
        else:
            print(f"⚠️ Job {args.job_id} not found in DLQ.")



def main():
    parser = argparse.ArgumentParser(prog="queuectl", description="Background Job Queue CLI")
    sub = parser.add_subparsers(dest="command")

    # --- enqueue ---
    p_enqueue = sub.add_parser("enqueue", help="Enqueue a new job")
    p_enqueue.add_argument("job_json", help="Job definition as JSON string")
    p_enqueue.set_defaults(func=cmd_enqueue)

    # --- list ---
    p_list = sub.add_parser("list", help="List jobs")
    p_list.add_argument("--state", help="Filter by job state", choices=["pending", "processing", "completed", "failed", "dead"])
    p_list.set_defaults(func=cmd_list)

    # --- config ---
    p_config = sub.add_parser("config", help="Manage configuration")
    p_config.add_argument("action", choices=["get", "set"], help="Action to perform")
    p_config.add_argument("key", help="Config key")
    p_config.add_argument("value", nargs="?", help="Value (only required for 'set')")
    p_config.set_defaults(func=cmd_config)

    # --- init ---
    p_init = sub.add_parser("init", help="Initialize database")
    p_init.set_defaults(func=cmd_init)

    # --- worker start ---
    p_worker = sub.add_parser("worker", help="Worker process control")
    p_worker.add_argument("action", choices=["start"], help="Action: start workers")
    p_worker.add_argument("--count", type=int, default=1, help="Number of workers to start")
    p_worker.set_defaults(func=lambda args: start_workers(args.count))

        # --- status ---
    p_status = sub.add_parser("status", help="Show job and worker status")
    p_status.set_defaults(func=cmd_status)

    # --- dlq ---
    p_dlq = sub.add_parser("dlq", help="Manage Dead Letter Queue")
    p_dlq.add_argument("action", choices=["list", "retry"], help="List or retry DLQ jobs")
    p_dlq.add_argument("job_id", nargs="?", help="Job ID (required for retry)")
    p_dlq.set_defaults(func=cmd_dlq)


    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    args.func(args)



if __name__ == "__main__":
    main()
