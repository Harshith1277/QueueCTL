#!/usr/bin/env python3
"""
queuectl.py - SQLite-backed minimal job queue with exponential backoff and DLQ.
Usage examples:
  python queuectl.py enqueue '{"id":"job1","command":"echo hi","max_retries":3}'
  python queuectl.py worker start --count 2
  python queuectl.py worker stop
  python queuectl.py status
  python queuectl.py list --state pending
  python queuectl.py dlq list
  python queuectl.py dlq retry job2
  python queuectl.py smoke-test
  python queuectl.py config set backoff_base 2
"""
import sqlite3, sys, json, os, threading, time, subprocess, pathlib

BASE = pathlib.Path(__file__).parent
DB_PATH = str(BASE / "queue.db")
STOP_FILE = str(BASE / "workers.stop")
LOCK = threading.Lock()

# Default config values (persisted in DB config table)
DEFAULT_CONFIG = {"backoff_base": 2, "max_retries": 3}

def log(msg):
    print(f"[QueueCTL] {msg}")

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT,
            max_retries INTEGER,
            attempts INTEGER DEFAULT 0,
            state TEXT DEFAULT 'pending',
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            next_run INTEGER DEFAULT 0
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS dlq (
            id TEXT PRIMARY KEY,
            command TEXT,
            max_retries INTEGER,
            attempts INTEGER,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        # insert default config if missing
        for k,v in DEFAULT_CONFIG.items():
            cur.execute("INSERT OR IGNORE INTO config(key,value) VALUES (?,?)", (k, str(v)))
        conn.commit()

def get_config(key):
    with sqlite3.connect(DB_PATH) as c:
        row = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

def set_config(key, value):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("INSERT OR REPLACE INTO config(key,value) VALUES (?,?)", (key, str(value)))
        c.commit()
    log(f"Config set: {key} = {value}")

def enqueue(job_json):
    try:
        job = json.loads(job_json)
        job_id = job["id"]
        cmd = job["command"]
        max_retries = int(job.get("max_retries", int(get_config("max_retries") or DEFAULT_CONFIG["max_retries"])))
        with sqlite3.connect(DB_PATH) as c:
            c.execute("INSERT OR REPLACE INTO jobs(id,command,max_retries,attempts,state,next_run) VALUES (?,?,?,?,?,?)",
                      (job_id, cmd, max_retries, 0, "pending", 0))
            c.commit()
        log(f"Enqueued job: {job_id}")
    except Exception as e:
        log(f"Failed to enqueue: {e}")

def _now_ts():
    return int(time.time())

def get_next_job():
    """
    Atomically fetch the oldest pending job whose next_run <= now, mark as processing.
    Uses a DB-level transaction to minimize races.
    """
    with LOCK:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            now = _now_ts()
            cur.execute("SELECT id,command,max_retries,attempts FROM jobs WHERE state='pending' AND next_run<=? ORDER BY enqueued_at LIMIT 1", (now,))
            row = cur.fetchone()
            if not row:
                cur.execute("COMMIT")
                return None
            job_id, cmd, max_retries, attempts = row
            cur.execute("UPDATE jobs SET state='processing' WHERE id=?", (job_id,))
            cur.execute("COMMIT")
            return {"id": job_id, "command": cmd, "max_retries": max_retries, "attempts": attempts}
        except sqlite3.OperationalError:
            try:
                cur.execute("ROLLBACK")
            except:
                pass
            return None
        finally:
            conn.close()

def move_to_dlq(job, attempts):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM jobs WHERE id=?", (job["id"],))
        c.execute("INSERT OR REPLACE INTO dlq(id,command,max_retries,attempts) VALUES (?,?,?,?)",
                  (job["id"], job["command"], job.get("max_retries",0), attempts))
        c.commit()

def finish_job(job_id):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        c.commit()

def schedule_retry(job, attempts, backoff_base):
    delay = backoff_base ** attempts
    next_run = _now_ts() + delay
    with sqlite3.connect(DB_PATH) as c:
        c.execute("UPDATE jobs SET attempts=?, state='pending', next_run=? WHERE id=?", (attempts, next_run, job["id"]))
        c.commit()
    log(f"Job {job['id']} scheduled to retry in {delay}s (attempts={attempts})")

def run_shell(cmd):
    # cross-platform; shell=True to allow shell commands like echo/sleep
    res = subprocess.run(cmd, shell=True)
    return res.returncode

def run_job(job):
    log(f"Running job {job['id']}: {job['command']}")
    rc = run_shell(job["command"])
    if rc == 0:
        log(f"Job {job['id']} completed successfully ✅")
        finish_job(job["id"])
        return True
    else:
        log(f"Job {job['id']} failed ❌ - exit code {rc}")
        # increment attempts, compute backoff, and decide
        with sqlite3.connect(DB_PATH) as c:
            c.execute("UPDATE jobs SET attempts = attempts + 1 WHERE id=?", (job["id"],))
            c.commit()
            attempts = c.execute("SELECT attempts FROM jobs WHERE id=?", (job["id"],)).fetchone()[0]
            max_retries = job.get("max_retries", int(get_config("max_retries") or DEFAULT_CONFIG["max_retries"]))
            backoff_base = int(get_config("backoff_base") or DEFAULT_CONFIG["backoff_base"])
        if attempts > max_retries:
            move_to_dlq(job, attempts)
            log(f"Job {job['id']} moved to DLQ")
        else:
            schedule_retry(job, attempts, backoff_base)
        return False

def worker_loop(wid):
    log(f"Worker-{wid} ready")
    while True:
        if os.path.exists(STOP_FILE):
            log(f"Worker-{wid} stop requested; exiting after current job")
            break
        job = get_next_job()
        if not job:
            time.sleep(1)
            continue
        run_job(job)
    log(f"Worker-{wid} exiting")

def start_workers(count=1):
    try:
        os.remove(STOP_FILE)
    except FileNotFoundError:
        pass
    log(f"Starting {count} worker(s)...")
    threads = []
    for i in range(count):
        t = threading.Thread(target=worker_loop, args=(i+1,), daemon=False)
        t.start()
        threads.append(t)
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        log("KeyboardInterrupt received; writing stop file")
        open(STOP_FILE, "w").close()
        for t in threads:
            t.join()
    log("All workers finished.")

def stop_workers():
    open(STOP_FILE, "w").close()
    log("Stop flag written. Running workers will stop after finishing current job(s).")

def status():
    with sqlite3.connect(DB_PATH) as c:
        pending = c.execute("SELECT COUNT(*) FROM jobs WHERE state='pending' AND next_run<=?", (_now_ts(),)).fetchone()[0]
        delayed = c.execute("SELECT COUNT(*) FROM jobs WHERE state='pending' AND next_run>?", (_now_ts(),)).fetchone()[0]
        processing = c.execute("SELECT COUNT(*) FROM jobs WHERE state='processing'").fetchone()[0]
        dlq = c.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
    log(f"Queue: pending_ready={pending} delayed={delayed} processing={processing} | DLQ={dlq}")

def list_jobs(state):
    with sqlite3.connect(DB_PATH) as c:
        if state == "pending":
            rows = c.execute("SELECT id,command,max_retries,attempts,state,next_run FROM jobs ORDER BY enqueued_at").fetchall()
            log(f"{len(rows)} jobs (pending/delayed/processing)")
            for r in rows:
                print(json.dumps({"id": r[0], "command": r[1], "max_retries": r[2], "attempts": r[3], "state": r[4], "next_run": r[5]}))
        elif state == "dlq":
            rows = c.execute("SELECT id,command,max_retries,attempts FROM dlq ORDER BY failed_at").fetchall()
            log(f"{len(rows)} jobs in DLQ")
            for r in rows:
                print(json.dumps({"id": r[0], "command": r[1], "max_retries": r[2], "attempts": r[3]}))

def dlq_retry(job_id):
    with sqlite3.connect(DB_PATH) as c:
        row = c.execute("SELECT id,command,max_retries,attempts FROM dlq WHERE id=?", (job_id,)).fetchone()
        if not row:
            log(f"No DLQ job with id {job_id}")
            return
        c.execute("DELETE FROM dlq WHERE id=?", (job_id,))
        # reset attempts to 0 and schedule immediate run
        c.execute("INSERT OR REPLACE INTO jobs(id,command,max_retries,attempts,state,next_run) VALUES (?,?,?,?,?,?)",
                  (row[0], row[1], row[2], 0, "pending", 0))
        c.commit()
    log(f"Retried job {job_id}")

def smoke_test():
    log("Running smoke test...")
    enqueue(json.dumps({"id":"job1","command":"echo Hello from job1","max_retries":3}))
    enqueue(json.dumps({"id":"job2","command":"nonexistent_cmd_xyz","max_retries":2}))
    start_workers(2)
    log("DLQ contents:")
    list_jobs("dlq")

def print_help():
    print(__doc__)

if __name__ == "__main__":
    init_db()
    if len(sys.argv) < 2:
        print_help()
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "enqueue":
        enqueue(sys.argv[2])
    elif cmd == "status":
        status()
    elif cmd == "list" and len(sys.argv) > 2 and sys.argv[2] == "--state":
        list_jobs(sys.argv[3])
    elif cmd == "worker" and len(sys.argv) > 2:
        if sys.argv[2] == "start":
            count = 1
            if "--count" in sys.argv:
                count = int(sys.argv[sys.argv.index("--count") + 1])
            start_workers(count)
        elif sys.argv[2] == "stop":
            stop_workers()
    elif cmd == "dlq":
        if len(sys.argv) > 2 and sys.argv[2] == "list":
            list_jobs("dlq")
        elif len(sys.argv) > 3 and sys.argv[2] == "retry":
            dlq_retry(sys.argv[3])
    elif cmd == "smoke-test":
        smoke_test()
    elif cmd == "config" and len(sys.argv) > 2:
        if sys.argv[2] == "set" and len(sys.argv) > 4:
            set_config(sys.argv[3], sys.argv[4])
        elif sys.argv[2] == "get" and len(sys.argv) > 3:
            val = get_config(sys.argv[3])
            log(f"Config {sys.argv[3]} = {val}")
        else:
            log("Usage: config set|get ...")
    else:
        log(f"Unknown command: {cmd}")
