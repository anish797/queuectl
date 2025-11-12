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
    db.init_db()

@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    try:
        job_id = db.enqueue_job(job_json)
        click.echo(f"Job enqueued: {job_id}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
def status():
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
@click.option('--state', default=None, help='filter by state')
def list(state):
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

@cli.group()
def worker():
    pass

@worker.command()
@click.option('--count', default=1, help='number of workers to start')
def start(count):
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
            subprocess.run(['taskkill', '/PID', str(pid), '/T'], capture_output=True)
            for i in range(10):
                time.sleep(1)
                if not is_process_running(pid):
                    click.echo("Workers stopped successfully")
                    return
            subprocess.run(['taskkill', '/F', '/PID', str(pid), '/T'], capture_output=True)
            time.sleep(0.5)
            click.echo("Workers force-stopped")
        else:
            os.kill(pid, signal.SIGTERM)
            for i in range(10):
                time.sleep(1)
                if not is_process_running(pid):
                    click.echo("Workers stopped successfully")
                    return
            if is_process_running(pid):
                click.echo("Workers didn't stop gracefully, forcing shutdown...")
                os.kill(pid, signal.SIGKILL)
                time.sleep(0.5)
                click.echo("Workers force-stopped")
            
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
@click.option('--count', default=1, help='number of workers to start')
def restart(count):
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
    pass

@dlq.command(name='list')
def dlq_list():
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
    pass

@config.command(name='set')
@click.argument('key')
@click.argument('value', type=int)
def config_set(key, value):
    valid_keys = {
        'max-retries': 'max_retries',
        'backoff-base': 'backoff_base',
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