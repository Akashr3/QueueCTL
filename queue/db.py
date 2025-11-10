# queue/db.py
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'queue.db')


def get_connection():
    """Return a SQLite connection (creating directories if needed)."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize DB schema if it doesn't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # Jobs table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        created_at TEXT,
        updated_at TEXT,
        next_run TEXT,
        worker TEXT,
        last_error TEXT
    );
    """)

    # Config table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)

    # Insert defaults if missing
    defaults = {
        "max_retries": "3",
        "backoff_base": "2",
        "processing_timeout": "300"  # seconds before requeue
    }

    for k, v in defaults.items():
        cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (k, v))

    conn.commit()
    conn.close()


def get_config(key):
    """Fetch a config value."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else None


def set_config(key, value):
    """Update or insert config."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()
    conn.close()


def reset_stale_jobs():
    """Reset jobs stuck in 'processing' beyond timeout back to 'pending'."""
    conn = get_connection()
    cur = conn.cursor()

    timeout = int(get_config("processing_timeout") or 300)
    cutoff = datetime.utcnow().timestamp() - timeout

    # Requeue old processing jobs
    cur.execute("""
        UPDATE jobs
        SET state='pending', worker=NULL, updated_at=datetime('now')
        WHERE state='processing'
          AND strftime('%s', updated_at) < ?
    """, (cutoff,))
    conn.commit()
    conn.close()
    
if __name__ == "__main__":
    print("Initializing queue database...")
    init_db()
    print("Database initialized successfully at", DB_PATH)
