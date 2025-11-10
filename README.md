
## queuectl — CLI-Based Background Job Queue System

`queuectl` is a minimal, production-grade background job queue system built in Python. It manages background jobs with worker processes, automatic retries, and a Dead Letter Queue (DLQ) to ensure tasks are handled reliably.

### Features

- Persistent job queue backed by SQLite
- Multiple concurrent worker processes
- Automatic retries with exponential backoff
- Dead Letter Queue (DLQ) for permanently failed jobs
- Configurable retry limits and backoff base
- Delayed and scheduled jobs via `next_run`
- Job state tracking and basic metrics with `queuectl status`
- Graceful worker shutdown

### Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/<your-username>/queuectl.git
   cd queuectl
   ```

2. **Initialize the database**

   ```bash
   python3 queue/db.py
   ```

3. **Verify installation**

   ```bash
   python3 queuectl.py init
   ```

### Usage

- Enqueue a new job:

  ```bash
  ./queuectl.py enqueue '{"command": "echo \"Hello Queue\""}'
  ```

- List all jobs:

  ```bash
  ./queuectl.py list
  ```

- Start worker processes:

  ```bash
  ./queuectl.py worker start --count 2
  ```

- View queue status:

  ```bash
  ./queuectl.py status
  ```

- Inspect the DLQ:

  ```bash
  ./queuectl.py dlq list
  ```

- Retry a DLQ job:

  ```bash
  ./queuectl.py dlq retry <job_id>
  ```

- Inspect configuration:

  ```bash
  ./queuectl.py config get max_retries
  ```

> **Note:** You can also run commands via `python3 queuectl.py …` if the script is not executable.

### Architecture Overview

- `queue/db.py` — database access and configuration management
- `queue/jobs.py` — job definitions and lifecycle helpers
- `queue/worker.py` — worker orchestration and execution logic
- `queuectl.py` — CLI entry point and command routing

### Job Lifecycle

```
pending → processing → completed / failed → dead (DLQ)
```

### Retry Logic

```
delay = base ^ attempts
```

### Assumptions

- SQLite is used for persistence
- Jobs execute via subprocess commands
- FIFO queue without priority handling
- CLI-only interface; no web dashboard

### Testing

```bash
./queuectl.py init
./queuectl.py enqueue '{"command": "echo \"job1\""}'
./queuectl.py worker start --count 2
```