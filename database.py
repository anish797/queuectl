import sqlite3
import json
import uuid
import config
from contextlib import contextmanager

@contextmanager
def get_conn():
    conn = sqlite3.connect('queue.db', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            create table if not exists jobs (
                id text primary key,
                command text not null,
                state text not null default 'pending',
                attempts integer default 0,
                max_retries integer default 3,
                priority integer default 2,
                created_at timestamp default (datetime('now', 'localtime')),
                updated_at timestamp default (datetime('now', 'localtime')),
                next_retry_at timestamp,
                run_at timestamp,
                output text,
                error text
            )
        ''')
        cursor.execute('create index if not exists idx_state on jobs(state)')
        cursor.execute('create index if not exists idx_next_retry on jobs(next_retry_at)')
        cursor.execute('create index if not exists idx_priority on jobs(priority)')
        conn.commit()
    
def enqueue_job(job_data):
    job_json = json.loads(job_data)
    job_id = job_json.get('id') or str(uuid.uuid4())
    command = job_json['command']
    run_at = job_json.get('run_at')
    priority = job_json.get('priority', 2)
    max_retries = config.get('max_retries')
    with get_conn() as conn:
        cursor = conn.cursor()
        if run_at:
            cursor.execute(
                "insert into jobs (id, command, max_retries, priority, run_at, created_at, updated_at) values (?, ?, ?, ?, ?, datetime('now', 'localtime'), datetime('now', 'localtime'))",
                (job_id, command, max_retries, priority, run_at)
            )
        else:
            cursor.execute(
                "insert into jobs (id, command, max_retries, priority, created_at, updated_at) values (?, ?, ?, ?, datetime('now', 'localtime'), datetime('now', 'localtime'))",
                (job_id, command, max_retries, priority)
            )
        conn.commit()
        return job_id

def get_job(job_id):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("select * from jobs where id = ?", (job_id,))
        return cursor.fetchone()
    
def claim_job():
    with get_conn() as conn:
        conn.isolation_level = "EXCLUSIVE"
        cursor = conn.cursor()
        cursor.execute("""select * from jobs where (state = 'pending' or state = 'failed') and (next_retry_at is null or next_retry_at <= datetime('now', 'localtime')) and (run_at is null or run_at <= datetime('now', 'localtime')) order by priority asc, created_at asc limit 1""")
        job = cursor.fetchone()
        if job:
            cursor.execute("""update jobs set state = 'processing', updated_at = datetime('now', 'localtime') where id = ?""", (job[0],))
            conn.commit()
        else:
            conn.rollback()
        return job

def update_job_state(job_id, new_state, **kwargs):
    updates = ["state = ?", "updated_at = datetime('now', 'localtime')"]
    values = [new_state]
    for key, value in kwargs.items():
        updates.append(f"{key} = ?")
        values.append(value)
    values.append(job_id)
    query = f"update jobs set {', '.join(updates)} where id = ?"
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()
    
def list_jobs(state=None):
    with get_conn() as conn:
        cursor = conn.cursor()
        if state:
            cursor.execute("select * from jobs where state = ?", (state,))
        else:
            cursor.execute("select * from jobs")
        return cursor.fetchall()
    
def get_status():
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""select state, count(*) from jobs group by state""")
        return dict(cursor.fetchall())

def get_metrics():
    with get_conn() as conn:
        cursor = conn.cursor()
        
        cursor.execute("select state, count(*) from jobs group by state")
        state_counts = dict(cursor.fetchall())
        
        cursor.execute("select count(*) from jobs")
        total_jobs = cursor.fetchone()[0]
        
        cursor.execute("select avg(attempts) from jobs where state in ('completed', 'dead')")
        avg_attempts = cursor.fetchone()[0] or 0

        completed = state_counts.get('completed', 0)
        dead = state_counts.get('dead', 0)
        success_rate = (completed / (completed + dead) * 100) if (completed + dead) > 0 else 0
        
        cursor.execute("select count(*) from jobs where created_at >= datetime('now', 'localtime', '-1 day')")
        recent_created = cursor.fetchone()[0]
        
        cursor.execute("select count(*) from jobs where state = 'completed' and updated_at >= datetime('now', 'localtime', '-1 day')")
        recent_completed = cursor.fetchone()[0]
        
        cursor.execute("select count(*) from jobs where state in ('failed', 'dead') and updated_at >= datetime('now', 'localtime', '-1 day')")
        recent_failed = cursor.fetchone()[0]
        
        return {
            'total_jobs': total_jobs,
            'state_counts': state_counts,
            'avg_attempts': round(avg_attempts, 2),
            'success_rate': round(success_rate, 1),
            'recent_created': recent_created,
            'recent_completed': recent_completed,
            'recent_failed': recent_failed
        }