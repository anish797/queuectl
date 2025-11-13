import click
import json
import os
import signal
import subprocess
import sys
import time
import platform
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
    if platform.system() == "Windows":
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
    """queuectl - a cli-based background job queue system
    
    manage job queues, workers, and monitor job execution with retry logic
    """
    db.init_db()

@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    """enqueue a new job to the queue
    
    job_json must be a json string containing a 'command' field
    optional fields:
      - 'id': custom job id
      - 'run_at': schedule job for future execution (format: 'yyyy-mm-dd hh:mm:ss')
      - 'priority': job priority - 1 (high), 2 (normal, default), 3 (low)
    
    examples:
      queuectl enqueue '{"command": "echo hello"}'
      queuectl enqueue '{"command": "urgent task", "priority": 1}'
      queuectl enqueue '{"command": "background task", "priority": 3}'
      queuectl enqueue '{"command": "backup.sh", "run_at": "2025-12-25 02:00:00"}'
    """
    try:
        job_id = db.enqueue_job(job_json)
        click.echo(f"job enqueued: {job_id}")
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

@cli.command()
def status():
    """show queue and worker status summary
    
    displays job counts by state and information about running workers
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
        
        click.echo("")
        click.echo("worker status:")
        click.echo("-" * 40)
        cleanup_stale_pid()
        pid_info = read_pid_file()
        if pid_info:
            pid, worker_count = pid_info
            if is_process_running(pid):
                click.echo(f"  {worker_count} worker(s) running (pid: {pid})")
            else:
                click.echo("  no workers running (stale pid file removed)")
        else:
            click.echo("  no workers running")
            
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

@cli.command()
def metrics():
    """show queue metrics and statistics
    
    displays success rates, job counts, and recent activity
    """
    try:
        metrics = db.get_metrics()
        
        click.echo("queue metrics:")
        click.echo("=" * 50)
        click.echo(f"total jobs:          {metrics['total_jobs']}")
        click.echo("")
        
        click.echo("jobs by state:")
        click.echo("-" * 50)
        state_counts = metrics['state_counts']
        total = metrics['total_jobs']
        
        for state in ['pending', 'processing', 'failed', 'completed', 'dead']:
            count = state_counts.get(state, 0)
            pct = (count / total * 100) if total > 0 else 0
            click.echo(f"  {state.capitalize():<12} {count:>6} ({pct:>5.1f}%)")
        
        click.echo("")
        click.echo("performance:")
        click.echo("-" * 50)
        click.echo(f"  success rate:      {metrics['success_rate']}%")
        click.echo(f"  avg retries:       {metrics['avg_attempts']}")
        
        click.echo("")
        click.echo("recent activity (last 24h):")
        click.echo("-" * 50)
        click.echo(f"  jobs created:      {metrics['recent_created']}")
        click.echo(f"  jobs completed:    {metrics['recent_completed']}")
        click.echo(f"  jobs failed:       {metrics['recent_failed']}")
        
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

@cli.command()
@click.option('--state', default=None, help='filter jobs by state (pending, processing, completed, failed, dead)')
def list(state):
    """list all jobs in the queue
    
    shows job id, command, current state, and retry attempts
    use --state to filter by specific job states
    
    examples:
      queuectl list
      queuectl list --state failed
      queuectl list --state dead
    """
    try:
        jobs = db.list_jobs(state)
        if not jobs:
            msg = f"no jobs with state '{state}'" if state else "no jobs in queue"
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
        click.echo(f"error: {str(e)}", err=True)

@cli.command()
@click.argument('job_id')
def job(job_id):
    """show detailed information about a specific job
    
    displays full job details including output, errors, and execution history
    
    example:
      queuectl job abc123-def456-...
    """
    try:
        job = db.get_job(job_id)
        if not job:
            click.echo(f"job not found: {job_id}")
            return
        
        click.echo("job details:")
        click.echo("=" * 60)
        click.echo(f"id:          {job['id']}")
        click.echo(f"command:     {job['command']}")
        click.echo(f"state:       {job['state']}")
        click.echo(f"attempts:    {job['attempts']}/{job['max_retries']}")
        click.echo(f"created:     {job['created_at']}")
        click.echo(f"updated:     {job['updated_at']}")
        
        if job['next_retry_at']:
            click.echo(f"next retry:  {job['next_retry_at']}")
        
        click.echo("")
        click.echo("output:")
        click.echo("-" * 60)
        if job['output']:
            click.echo(job['output'])
        else:
            click.echo("(no output)")
        
        click.echo("")
        click.echo("error:")
        click.echo("-" * 60)
        if job['error']:
            click.echo(job['error'])
        else:
            click.echo("(no error)")
            
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

@cli.group()
def worker():
    """manage background worker processes
    
    start, stop, restart, and monitor workers that process jobs from the queue
    """
    pass

@worker.command()
@click.option('--count', default=1, help='number of parallel workers to start')
def start(count):
    """start worker processes to process jobs
    
    workers run in the background and process jobs from the queue
    multiple workers can process jobs in parallel, logs are written to worker.log
    
    examples:
      queuectl worker start
      queuectl worker start --count 4
    """
    cleanup_stale_pid()
    pid_info = read_pid_file()
    if pid_info:
        pid, worker_count = pid_info
        if is_process_running(pid):
            click.echo(f"workers already running (pid: {pid}, count: {worker_count})")
            click.echo(f"  use 'queuectl worker stop' first, or 'queuectl worker restart --count {count}'")
            return
    
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
            click.echo(f"started {count} worker(s) (pid: {pid}, logs: worker.log)")
        else:
            click.echo(f"started {count} worker(s) but pid file not found (check worker.log)")
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

@worker.command()
def stop():
    """stop all running worker processes
    
    attempts graceful shutdown (on unix/linux) by allowing workers to finish
    their current job, on windows, workers are force-stopped immediately
    """
    cleanup_stale_pid()
    pid_info = read_pid_file()
    
    if not pid_info:
        click.echo("no workers running")
        return
    
    pid, worker_count = pid_info
    
    if not is_process_running(pid):
        click.echo("no workers running (stale pid file removed)")
        try:
            os.remove(PID_FILE)
        except Exception:
            pass
        return
    
    try:
        click.echo(f"stopping {worker_count} worker(s) (pid: {pid})...")
        
        if platform.system() == "Windows":
            click.echo("note: windows requires force-stop (graceful shutdown not supported)")
            subprocess.run(['taskkill', '/F', '/PID', str(pid), '/T'], capture_output=True)
            time.sleep(0.5)
            click.echo("workers stopped")
        else:
            os.kill(pid, signal.SIGTERM)
            for i in range(30):
                time.sleep(1)
                if not is_process_running(pid):
                    click.echo("workers stopped gracefully")
                    return
            if is_process_running(pid):
                click.echo("workers didn't stop gracefully, forcing shutdown...")
                os.kill(pid, signal.SIGKILL)
                time.sleep(0.5)
                click.echo("workers force-stopped (did not finish within 30 seconds)")
            
    except ProcessLookupError:
        click.echo("workers already stopped")
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)
    finally:
        try:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
        except Exception:
            pass

@worker.command()
@click.option('--count', default=1, help='number of workers to restart with')
def restart(count):
    """restart workers with specified count
    
    stops any running workers and starts new ones, useful for changing
    the number of workers or applying configuration changes
    
    examples:
      queuectl worker restart
      queuectl worker restart --count 8
    """
    click.echo("restarting workers...")
    cleanup_stale_pid()
    pid_info = read_pid_file()
    if pid_info:
        pid, _ = pid_info
        if is_process_running(pid):
            try:
                if platform.system() == "Windows":
                    subprocess.run(['taskkill', '/PID', str(pid), '/T', '/F'], capture_output=True)
                else:
                    os.kill(pid, signal.SIGTERM)
                time.sleep(2)
            except Exception:
                pass
    
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
            click.echo(f"restarted with {count} worker(s) (pid: {pid})")
        else:
            click.echo(f"restarted {count} worker(s) but pid file not found")
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

@worker.command(name='status')
def worker_status():
    """show detailed worker status and recent logs
    
    displays worker count, pid, and the last 5 log entries
    """
    cleanup_stale_pid()
    pid_info = read_pid_file()
    
    if not pid_info:
        click.echo("no workers running")
        return
    
    pid, worker_count = pid_info
    
    if not is_process_running(pid):
        click.echo("no workers running (stale pid file found)")
        return
    
    click.echo("worker status:")
    click.echo("-" * 40)
    click.echo(f"  status: running")
    click.echo(f"  worker count: {worker_count}")
    click.echo(f"  main pid: {pid}")
    click.echo(f"  log file: worker.log")
    if os.path.exists('worker.log'):
        click.echo("")
        click.echo("recent log entries (last 5):")
        click.echo("-" * 40)
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
    """manage the dead letter queue (dlq)
    
    jobs that fail after max retries are moved to the dlq
    you can inspect failed jobs and retry them if needed
    """
    pass

@dlq.command(name='list')
def dlq_list():
    """list all jobs in the dead letter queue
    
    shows jobs that have exhausted all retry attempts and failed permanently
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
        click.echo(f"error: {str(e)}", err=True)

@dlq.command()
@click.argument('job_id')
def retry(job_id):
    """retry a job from the dead letter queue
    
    moves a failed job back to the pending queue with reset retry count
    the job will be processed again by available workers
    
    example:
      queuectl dlq retry abc123-def456-...
    """
    try:
        job = db.get_job(job_id)
        if not job:
            click.echo(f"job not found: {job_id}")
            return
        if job['state'] != 'dead':
            click.echo(f"job {job_id} is not in dlq (state: {job['state']})")
            return
        db.update_job_state(job_id, 'pending', attempts=0, error=None, next_retry_at=None)
        click.echo(f"job {job_id} moved back to queue")
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

@cli.group()
def config():
    """manage queue configuration settings
    
    configure retry behavior, backoff timing, and other queue parameters
    """
    pass

@config.command(name='set')
@click.argument('key')
@click.argument('value', type=int)
def config_set(key, value):
    """set a configuration value
    
    valid configuration keys:
      max-retries  - maximum number of retry attempts (default: 3)
      backoff-base - base for exponential backoff calculation (default: 2)
      job-timeout  - maximum job execution time in seconds (default: 300)
    
    examples:
      queuectl config set max-retries 5
      queuectl config set backoff-base 3
      queuectl config set job-timeout 600
    """
    valid_keys = {
        'max-retries': 'max_retries',
        'backoff-base': 'backoff_base',
        'job-timeout': 'job_timeout',
    }
    
    if key not in valid_keys:
        click.echo(f"invalid config key: {key}")
        click.echo(f"valid keys: {', '.join(valid_keys.keys())}")
        return
    
    internal_key = valid_keys[key]
    cfg.set_value(internal_key, value)
    click.echo(f"set {key} = {value}")

@config.command(name='show')
def config_show():
    """show current configuration values
    
    displays all configuration settings and their current values
    """
    try:
        config_data = cfg.get_all()
        click.echo("current configuration:")
        click.echo("-" * 30)
        for key, value in config_data.items():
            display_key = key.replace('_', '-')
            click.echo(f"  {display_key}: {value}")
    except Exception as e:
        click.echo(f"error: {str(e)}", err=True)

if __name__ == '__main__':
    cli()