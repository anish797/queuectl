import click
import json
import database as db
import config as cfg

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
            return
        for state, count in stats.items():
            click.echo(f"  {state}: {count}")
        total = sum(stats.values())
        click.echo("-" * 40)
        click.echo(f"  total: {total}")
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
    try:
        subprocess.Popen(
            [sys.executable, 'launcher.py', str(count)],
            stdout=open('worker.log', 'a'),
            stderr=subprocess.STDOUT
        )
        click.echo(f"✓ Started {count} worker(s) (logs: worker.log)")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

@worker.command()
def stop():
    click.echo("Send CTRL+C to the launcher.py process or kill it manually")
    click.echo("Use: pkill -f launcher.py")

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