# QueueCTL

CLI-based background job queue system with worker processes, automatic retries with exponential backoff, and a Dead Letter Queue for failed jobs.

## Features

- Job queue with persistent storage (SQLite)
- Multiple parallel worker processes
- Automatic retry with exponential backoff
- Dead Letter Queue for permanently failed jobs
- Runtime configuration management
- Graceful worker shutdown (Unix/Linux)
- Job timeout handling
- Priority queues (high/normal/low)
- Scheduled jobs with run_at timestamps
- Job output and error logging
- Queue metrics and statistics

## Requirements

- Python 3.7+
- Click library

## Installation

```bash
git clone https://github.com/anish797/queuectl.git
cd queuectl
pip install -e .
```

## Quick Start

```bash
# Start workers
queuectl worker start --count 2

# Enqueue a job
queuectl enqueue '{"command": "echo hello world"}'

# Check status
queuectl status

# View jobs
queuectl list

# Stop workers
queuectl worker stop
```

## Demo Video

Watch the full demonstration: [QueueCTL Demo](https://drive.google.com/file/d/1yOaPD8EBY-DrXCk496ICIcFPBVFTPOm3/view?usp=drive_link)

## Usage

### Job Management

**Enqueue a job:**

```bash
queuectl enqueue '{"command": "echo hello"}'
```

**Enqueue with priority:**

```bash
queuectl enqueue '{"command": "urgent task", "priority": 1}'
```

Priority: 1 (high), 2 (normal, default), 3 (low)

**Schedule a job:**

```bash
queuectl enqueue '{"command": "backup.sh", "run_at": "2025-12-25 02:00:00"}'
```

**List jobs:**

```bash
queuectl list
queuectl list --state failed
queuectl list --state dead
```

**View job details:**

```bash
queuectl job <job-id>
```

### Worker Management

**Start workers:**

```bash
queuectl worker start --count 4
```

**Stop workers:**

```bash
queuectl worker stop
```

**Restart workers:**

```bash
queuectl worker restart --count 8
```

**Worker status:**

```bash
queuectl worker status
```

### Dead Letter Queue

**List failed jobs:**

```bash
queuectl dlq list
```

**Retry a failed job:**

```bash
queuectl dlq retry <job-id>
```

### Configuration

**View configuration:**

```bash
queuectl config show
```

**Update configuration:**

```bash
queuectl config set max-retries 5
queuectl config set backoff-base 3
queuectl config set job-timeout 600
```

Configuration options:

- `max-retries`: Maximum retry attempts (default: 3)
- `backoff-base`: Exponential backoff base (default: 2)
- `job-timeout`: Job execution timeout in seconds (default: 300)

### Metrics

**View queue metrics:**

```bash
queuectl metrics
```

Shows:

- Total jobs and distribution by state
- Success rate and average retries
- Recent activity (last 24 hours)

## Testing

Run the test script to verify all functionality:

```bash
chmod +x test.sh
./test.sh
```

This tests:

- Basic job execution
- Job output logging
- Priority queues
- Multiple workers without race conditions
- Retry mechanism and DLQ
- DLQ retry functionality
- Job timeouts
- Scheduled jobs
- Persistence across restarts
- Invalid command handling
- Queue metrics

## Architecture Overview

### Job Lifecycle

Jobs progress through states: `pending` → `processing` → `completed` (success) or `failed` (retriable). Failed jobs retry with exponential backoff (`delay = base^attempts`). After exhausting retries, jobs move to `dead` state in the Dead Letter Queue.

### Data Persistence

Jobs are stored in SQLite (`queue.db`) ensuring persistence across restarts. The database uses transaction-level locking to prevent duplicate job processing by concurrent workers.

### Worker Logic

Workers run as separate processes using Python multiprocessing. The main launcher process spawns multiple worker children and coordinates their execution. Each worker:

1. Claims an available job from the queue (highest priority first)
2. Executes the shell command with timeout
3. Updates job state based on exit code
4. Schedules retry with exponential backoff if failed
5. Moves to DLQ after max retries exceeded

Workers support graceful shutdown on Unix/Linux, completing current jobs before exit.

**Key components:**

- `cli.py` - Command-line interface
- `database.py` - Job persistence and state management
- `worker.py` - Job execution with timeout handling
- `launcher.py` - Worker process orchestration
- `config.py` - Configuration management

## Assumptions & Trade-offs

### SQLite for Persistence

Chosen for simplicity and reliability. Provides ACID guarantees and built-in locking mechanisms for concurrent access.

### Multiprocessing for Workers

Uses Python's multiprocessing module to run workers in separate processes, enabling true parallel execution.

### Exponential Backoff

Prevents overwhelming failing services and provides increasing intervals between retries.

### Graceful Shutdown

Workers finish their current job before exiting (Unix/Linux only). Windows requires force-stop due to platform limitations.

### Local Time for Timestamps

All timestamps use local time for easier debugging and log correlation.

## Limitations

- Graceful shutdown not supported on Windows (force-kill required)
- No distributed job queue (single-machine only)
- No job dependencies or workflows
- No authentication or access control
