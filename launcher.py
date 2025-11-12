import multiprocessing
import signal
import time
import sys
import database as db
from worker import process_job

stop_event = multiprocessing.Event()

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
            process_job(job)
        else:
            time.sleep(1)
    print(f"Worker {worker_id} shutting down cleanly.")

if __name__ == "__main__":
    num_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    print(f"Launching {num_workers} worker(s)...")
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
    print("All workers stopped cleanly.")