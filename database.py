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
                created_at timestamp default current_timestamp,
                updated_at timestamp default current_timestamp,
                next_retry_at timestamp,
                output text,
                error text
            )
        ''')
        cursor.execute('create index if not exists idx_state on jobs(state)')
        cursor.execute('create index if not exists idx_next_retry on jobs(next_retry_at)')
        conn.commit()
    
def enqueue_job(job_data):
    job_json = json.loads(job_data)
    job_id = job_json.get('id') or str(uuid.uuid4())
    command = job_json['command']
    max_retries = config.get('max_retries')
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "insert into jobs (id, command, max_retries) values (?, ?, ?)",
            (job_id, command, max_retries)
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
        cursor.execute("""select * from jobs where (state = 'pending' OR state = 'failed') and (next_retry_at IS NULL OR next_retry_at <= current_timestamp) limit 1""")
        job = cursor.fetchone()
        if job:
            cursor.execute("""update jobs set state = 'processing', updated_at = current_timestamp where id = ?""", (job[0],))
            conn.commit()
        else:
            conn.rollback()
        return job


def update_job_state(job_id, new_state, **kwargs):
    updates = ["state = ?", "updated_at = current_timestamp"]
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
        if(state):
            cursor.execute("select * from jobs where state = ?", (state,))
        else:
            cursor.execute("select * from jobs")
        return cursor.fetchall()
    
def get_status():
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""select state, count(*) from jobs group by state""")
        return dict(cursor.fetchall())