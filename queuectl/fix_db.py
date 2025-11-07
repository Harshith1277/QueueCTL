import sqlite3
import sys
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "queue.db")

def delete_dlq(job_id):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM dlq WHERE id=?", (job_id,))
        c.commit()
    print(f"[fix_db] Deleted job '{job_id}' from DLQ ✅")

def update_job(job_id, command):
    with sqlite3.connect(DB_PATH) as c:
        cur = c.cursor()
        cur.execute(
            "UPDATE jobs SET command=?, attempts=0, state='pending', next_run=0 WHERE id=?",
            (command, job_id),
        )
        c.commit()
    print(f"[fix_db] Updated job '{job_id}' to command: {command} ✅")

def list_dlq():
    with sqlite3.connect(DB_PATH) as c:
        for row in c.execute("SELECT id, command, attempts, max_retries FROM dlq"):
            print(row)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python fix_db.py <job_id> delete              → Delete a DLQ job")
        print("  python fix_db.py <job_id> \"<new command>\"     → Update job command and requeue")
        print("  python fix_db.py list dlq                    → Show all DLQ jobs")
        sys.exit(1)

    if sys.argv[1] == "list":
        list_dlq()
    else:
        job_id = sys.argv[1]
        action = sys.argv[2]
        if action == "delete":
            delete_dlq(job_id)
        else:
            update_job(job_id, action)
