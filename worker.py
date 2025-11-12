# worker.py
import subprocess
from datetime import datetime, timedelta
import database as db
import config

def execute_command(command):
    timeout = config.get('job_timeout')
    try:
        res = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
        success = (res.returncode == 0)
        return success, res.stdout.strip(), res.stderr.strip(), res.returncode
    except subprocess.TimeoutExpired:
        return False, "", f"Job exceeded timeout of {timeout} seconds", -1
    except Exception as e:
        return False, "", str(e), -1
    
def calculate_backoff(attempts):
    base = config.get('backoff_base')
    return base ** attempts

def process_job(job):
    job_id = job['id']
    command = job['command']
    state = job['state']
    attempts = job['attempts']
    max_retries = job['max_retries']

    print(f"[{datetime.now().isoformat()}] Processing job {job_id}: '{command}' (attempt {attempts})")

    success, stdout, stderr, code = execute_command(command)

    if success:
        db.update_job_state(job_id, "completed", attempts=attempts, output=stdout, error=None, next_retry_at=None)
        print(f"Job {job_id} completed successfully.")
        return

    attempts += 1
    delay = calculate_backoff(attempts)
    next_retry_time = (datetime.now() + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")

    if attempts >= max_retries:
        db.update_job_state(job_id, "dead", attempts=attempts, error=stderr or f"Command failed (exit code {code})", next_retry_at=None)
        print(f"Job {job_id} moved to DLQ after {attempts} attempts. Error: {stderr or 'Unknown error'}")
    else:
        db.update_job_state(job_id, "failed", attempts=attempts, error=stderr or f"Command failed (exit code {code})", next_retry_at=next_retry_time)
        print(f"Job {job_id} failed (attempt {attempts}/{max_retries}), retrying in {delay}s at {next_retry_time}.")