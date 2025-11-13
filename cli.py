import click
import json
import os
import signal
import database as db
import config as cfg

PID_FILE = 'queuectl_worker.pid'

def read_pid_file():
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, 'r') as f:
            content = f.read().strip()
            parts = content.split(',')
            if len(parts) == 2:
                return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return None

def is_process_running(pid):
    import platform
    if platform.system() == "Windows":
        import subprocess
        try:
            result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'], 
                                  capture_output=True, text=True)
            return str(pid) in result.stdout
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

def cleanup_stale_pid():
    pid_info = read_pid_file()
    if pid_info:
        pid, _ = pid_info
        if not is_process_running(pid):
            try:
                os.remove(PID_FILE)
            except Exception:
                pass

@click.group()
def cli():
    """queuectl - A CLI-based background job queue system.
    
    Manage job queues, workers, and monitor job execution with retry logic.
    """
    db.init_db()

@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    """Enqueue a new job to the queue.

    JOB_JSON must be a JSON string containing a 'command' field.
    Optional fields:
    - 'id': Custom job ID
    - 'run_at': Schedule job for future execution (format: 'YYYY-MM-DD HH:MM:SS')
    - 'priority': Job priority - 1 (high), 2 (normal, default), 3 (low)

    Examples:
    queuectl enqueue '{"command": "echo hello"}'
    queuectl enqueue '{"command": "urgent task", "priority": 1}'
    queuectl enqueue '{"command": "background task", "priority": 3}'
    queuectl enqueue '{"command": "backup.sh", "run_at": "2025-12-25 02:00:00"}'
    """
    try:
        job_id = db.enqueue_job(job_json)
        click.echo(f"Job enqueued: {job_id}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
def status():
    """Show queue and worker status summary.
    
    Displays job counts by state and information about running workers.
    """
    try:
        stats = db.get_status()
        click.echo("job queue status:")
        click.echo("-" * 40)
        if not stats:
            click.echo("no jobs in queue")
        else:
            for state, count in stats.items():
                click.echo(f"  {state}: {count}")
            total = sum(stats.values())
            click.echo("-" * 40)
            click.echo(f"  total: {total}")
        
        # Worker status
        click.echo("")
        click.echo("worker status:")
        click.echo("-" * 40)
        cleanup_stale_pid()
        pid_info = read_pid_file()
        if pid_info:
            pid, worker_count = pid_info
            if is_process_running(pid):
                click.echo(f"  {worker_count} worker(s) running (PID: {pid})")
            else:
                click.echo("  no workers running (stale PID file removed)")
        else:
            click.echo("  no workers running")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
def metrics():
    """Show queue metrics and statistics.
    
    Displays success rates, job counts, and recent activity.
    """
    try:
        metrics = db.get_metrics()
        
        click.echo("Queue Metrics:")
        click.echo("=" * 50)
        click.echo(f"Total Jobs:          {metrics['total_jobs']}")
        click.echo("")
        
        click.echo("Jobs by State:")
        click.echo("-" * 50)
        state_counts = metrics['state_counts']
        total = metrics['total_jobs']
        
        for state in ['pending', 'processing', 'failed', 'completed', 'dead']:
            count = state_counts.get(state, 0)
            pct = (count / total * 100) if total > 0 else 0
            click.echo(f"  {state.capitalize():<12} {count:>6} ({pct:>5.1f}%)")
        
        click.echo("")
        click.echo("Performance:")
        click.echo("-" * 50)
        click.echo(f"  Success Rate:      {metrics['success_rate']}%")
        click.echo(f"  Avg Retries:       {metrics['avg_attempts']}")
        
        click.echo("")
        click.echo("Recent Activity (last 24h):")
        click.echo("-" * 50)
        click.echo(f"  Jobs Created:      {metrics['recent_created']}")
        click.echo(f"  Jobs Completed:    {metrics['recent_completed']}")
        click.echo(f"  Jobs Failed:       {metrics['recent_failed']}")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
@click.option('--state', default=None, help='Filter jobs by state (pending, processing, completed, failed, dead)')
def list(state):
    """List all jobs in the queue.
    
    Shows job ID, command, current state, and retry attempts.
    Use --state to filter by specific job states.
    
    Examples:
      queuectl list
      queuectl list --state failed
      queuectl list --state dead
    """
    try:
        jobs = db.list_jobs(state)
        if not jobs:
            msg = f"No jobs with state '{state}'" if state else "no jobs in queue"
            click.echo(msg)
            return
        click.echo(f"{'id':<20} {'command':<30} {'state':<12} {'attempts':<10}")
        click.echo("-" * 80)
        for job in jobs:
            job_id = job['id'][:18] + '..' if len(job['id']) > 20 else job['id']
            command = job['command'][:28] + '..' if len(job['command']) > 30 else job['command']
            state = job['state']
            attempts = f"{job['attempts']}/{job['max_retries']}"
            click.echo(f"{job_id:<20} {command:<30} {state:<12} {attempts:<10}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
@click.argument('job_id')
def job(job_id):
    """Show detailed information about a specific job.
    
    Displays full job details including output, errors, and execution history.
    
    Example:
      queuectl job abc123-def456-...
    """
    try:
        job = db.get_job(job_id)
        if not job:
            click.echo(f"Job not found: {job_id}")
            return
        
        click.echo("Job Details:")
        click.echo("=" * 60)
        click.echo(f"ID:          {job['id']}")
        click.echo(f"Command:     {job['command']}")
        click.echo(f"State:       {job['state']}")
        click.echo(f"Attempts:    {job['attempts']}/{job['max_retries']}")
        click.echo(f"Created:     {job['created_at']}")
        click.echo(f"Updated:     {job['updated_at']}")
        
        if job['next_retry_at']:
            click.echo(f"Next Retry:  {job['next_retry_at']}")
        
        click.echo("")
        click.echo("Output:")
        click.echo("-" * 60)
        if job['output']:
            click.echo(job['output'])
        else:
            click.echo("(no output)")
        
        click.echo("")
        click.echo("Error:")
        click.echo("-" * 60)
        if job['error']:
            click.echo(job['error'])
        else:
            click.echo("(no error)")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.group()
def worker():
    """Manage background worker processes.
    
    Start, stop, restart, and monitor workers that process jobs from the queue.
    """
    pass

@worker.command()
@click.option('--count', default=1, help='Number of parallel workers to start')
def start(count):
    """Start worker processes to process jobs.
    
    Workers run in the background and process jobs from the queue.
    Multiple workers can process jobs in parallel. Logs are written to worker.log.
    
    Examples:
      queuectl worker start
      queuectl worker start --count 4
    """
    import subprocess
    import sys
    
    # Check if workers already running
    cleanup_stale_pid()
    pid_info = read_pid_file()
    if pid_info:
        pid, worker_count = pid_info
        if is_process_running(pid):
            click.echo(f"Workers already running (PID: {pid}, count: {worker_count})")
            click.echo(f"  Use 'queuectl worker stop' first, or 'queuectl worker restart --count {count}'")
            return
    
    try:
        subprocess.Popen(
            [sys.executable, 'launcher.py', str(count)],
            stdout=open('worker.log', 'a'),
            stderr=subprocess.STDOUT
        )
        import time
        time.sleep(0.5)
        pid_info = read_pid_file()
        if pid_info:
            pid, _ = pid_info
            click.echo(f"Started {count} worker(s) (PID: {pid}, logs: worker.log)")
        else:
            click.echo(f"Started {count} worker(s) but PID file not found (check worker.log)")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@worker.command()
def stop():
    """Stop all running worker processes.
    
    Attempts graceful shutdown (on Unix/Linux) by allowing workers to finish
    their current job. On Windows, workers are force-stopped immediately.
    """
    cleanup_stale_pid()
    pid_info = read_pid_file()
    
    if not pid_info:
        click.echo("No workers running")
        return
    
    pid, worker_count = pid_info
    
    if not is_process_running(pid):
        click.echo("No workers running (stale PID file removed)")
        try:
            os.remove(PID_FILE)
        except Exception:
            pass
        return
    
    try:
        click.echo(f"Stopping {worker_count} worker(s) (PID: {pid})...")
        import platform
        import time
        
        if platform.system() == "Windows":
            import subprocess
            # Windows doesn't support graceful SIGTERM, use force kill
            click.echo("Note: Windows requires force-stop (graceful shutdown not supported)")
            subprocess.run(['taskkill', '/F', '/PID', str(pid), '/T'], capture_output=True)
            time.sleep(0.5)
            click.echo("Workers stopped")
        else:
            os.kill(pid, signal.SIGTERM)
            for i in range(30):
                time.sleep(1)
                if not is_process_running(pid):
                    click.echo("Workers stopped gracefully")
                    return
            if is_process_running(pid):
                click.echo("Workers didn't stop gracefully, forcing shutdown...")
                os.kill(pid, signal.SIGKILL)
                time.sleep(0.5)
                click.echo("Workers force-stopped (did not finish within 30 seconds)")
            
    except ProcessLookupError:
        click.echo("Workers already stopped")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        try:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
        except Exception:
            pass

@worker.command()
@click.option('--count', default=1, help='Number of workers to restart with')
def restart(count):
    """Restart workers with specified count.
    
    Stops any running workers and starts new ones. Useful for changing
    the number of workers or applying configuration changes.
    
    Examples:
      queuectl worker restart
      queuectl worker restart --count 8
    """
    click.echo("Restarting workers...")
    cleanup_stale_pid()
    pid_info = read_pid_file()
    if pid_info:
        pid, _ = pid_info
        if is_process_running(pid):
            try:
                import platform
                import time
                if platform.system() == "Windows":
                    import subprocess as sp
                    sp.run(['taskkill', '/PID', str(pid), '/T', '/F'], capture_output=True)
                else:
                    os.kill(pid, signal.SIGTERM)
                time.sleep(2)
            except Exception:
                pass
    import subprocess
    import sys
    import time
    
    try:
        subprocess.Popen(
            [sys.executable, 'launcher.py', str(count)],
            stdout=open('worker.log', 'a'),
            stderr=subprocess.STDOUT
        )
        time.sleep(0.5)
        
        pid_info = read_pid_file()
        if pid_info:
            pid, _ = pid_info
            click.echo(f"Restarted with {count} worker(s) (PID: {pid})")
        else:
            click.echo(f"Restarted {count} worker(s) but PID file not found")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@worker.command(name='status')
def worker_status():
    """Show detailed worker status and recent logs.
    
    Displays worker count, PID, and the last 5 log entries.
    """
    cleanup_stale_pid()
    pid_info = read_pid_file()
    
    if not pid_info:
        click.echo("No workers running")
        return
    
    pid, worker_count = pid_info
    
    if not is_process_running(pid):
        click.echo("No workers running (stale PID file found)")
        return
    
    click.echo("Worker Status:")
    click.echo("-" * 40)
    click.echo(f"  Status: Running")
    click.echo(f"  Worker Count: {worker_count}")
    click.echo(f"  Main PID: {pid}")
    click.echo(f"  Log File: worker.log")
    if os.path.exists('worker.log'):
        click.echo("")
        click.echo("Recent log entries (last 5):")
        click.echo("-" * 40)
        import subprocess
        try:
            result = subprocess.run(['tail', '-5', 'worker.log'], 
                                  capture_output=True, text=True)
            if result.stdout:
                click.echo(result.stdout)
            else:
                click.echo("  (no recent logs)")
        except Exception:
            click.echo("  (could not read logs)")

@cli.group()
def dlq():
    """Manage the Dead Letter Queue (DLQ).
    
    Jobs that fail after max retries are moved to the DLQ.
    You can inspect failed jobs and retry them if needed.
    """
    pass

@dlq.command(name='list')
def dlq_list():
    """List all jobs in the Dead Letter Queue.
    
    Shows jobs that have exhausted all retry attempts and failed permanently.
    """
    try:
        jobs = db.list_jobs('dead')
        
        if not jobs:
            click.echo("no jobs in dlq")
            return
        
        click.echo(f"{'id':<20} {'command':<30} {'attempts':<10} {'error':<30}")
        click.echo("-" * 90)
        
        for job in jobs:
            job_id = job['id'][:18] + '..' if len(job['id']) > 20 else job['id']
            command = job['command'][:28] + '..' if len(job['command']) > 30 else job['command']
            attempts = job['attempts']
            error = (job['error'][:28] + '..') if job['error'] and len(job['error']) > 30 else (job['error'] or '')
            
            click.echo(f"{job_id:<20} {command:<30} {attempts:<10} {error:<30}")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@dlq.command()
@click.argument('job_id')
def retry(job_id):
    """Retry a job from the Dead Letter Queue.
    
    Moves a failed job back to the pending queue with reset retry count.
    The job will be processed again by available workers.
    
    Example:
      queuectl dlq retry abc123-def456-...
    """
    try:
        job = db.get_job(job_id)
        if not job:
            click.echo(f"Job not found: {job_id}")
            return
        if job['state'] != 'dead':
            click.echo(f"Job {job_id} is not in dlq (state: {job['state']})")
            return
        db.update_job_state(job_id, 'pending', attempts=0, error=None, next_retry_at=None)
        click.echo(f"Job {job_id} moved back to queue")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.group()
def config():
    """Manage queue configuration settings.
    
    Configure retry behavior, backoff timing, and other queue parameters.
    """
    pass

@config.command(name='set')
@click.argument('key')
@click.argument('value', type=int)
def config_set(key, value):
    """Set a configuration value.
    
    Valid configuration keys:
      max-retries  - Maximum number of retry attempts (default: 3)
      backoff-base - Base for exponential backoff calculation (default: 2)
    
    Examples:
      queuectl config set max-retries 5
      queuectl config set backoff-base 3
    """
    valid_keys = {
        'max-retries': 'max_retries',
        'backoff-base': 'backoff_base',
        'job-timeout': 'job_timeout',
    }
    
    if key not in valid_keys:
        click.echo(f"Invalid config key: {key}")
        click.echo(f"Valid keys: {', '.join(valid_keys.keys())}")
        return
    
    internal_key = valid_keys[key]
    cfg.set_value(internal_key, value)
    click.echo(f"Set {key} = {value}")

@config.command(name='show')
def config_show():
    """Show current configuration values.
    
    Displays all configuration settings and their current values.
    """
    try:
        config_data = cfg.get_all()
        click.echo("current configuration:")
        click.echo("-" * 30)
        for key, value in config_data.items():
            display_key = key.replace('_', '-')
            click.echo(f"  {display_key}: {value}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

if __name__ == '__main__':
    cli()