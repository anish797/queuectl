import multiprocessing
import signal
import time
import sys
import os
import database as db
from worker import process_job

stop_event = multiprocessing.Event()
PID_FILE = 'queuectl_worker.pid'

def handle_signal(sig, frame):
    print("\nShutting down gracefully...")
    stop_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

def worker_loop(worker_id):
    print(f"Worker {worker_id} started.")
    while not stop_event.is_set():
        job = db.claim_job()
        if job:
            print(f"Worker {worker_id} processing job {job['id']}, will finish before shutdown.")
            process_job(job)
            # Check stop_event after finishing job
            if stop_event.is_set():
                print(f"Worker {worker_id} received shutdown signal, exiting after completing job.")
                break
        else:
            time.sleep(0.5)  # Check more frequently
    print(f"Worker {worker_id} shutting down cleanly.")

def write_pid_file(pid, num_workers):
    with open(PID_FILE, 'w') as f:
        f.write(f"{pid},{num_workers}\n")

def remove_pid_file():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception as e:
        print(f"Warning: Could not remove PID file: {e}")

def cleanup_and_exit():
    remove_pid_file()
    print("All workers stopped cleanly.")

if __name__ == "__main__":
    num_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    main_pid = os.getpid()
    write_pid_file(main_pid, num_workers)
    print(f"Launching {num_workers} worker(s)... (PID: {main_pid})")
    processes = [
        multiprocessing.Process(target=worker_loop, args=(i,))
        for i in range(num_workers)
    ]
    for p in processes:
        p.start()
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        handle_signal(None, None)
        for p in processes:
            p.join()
    finally:
        cleanup_and_exit()