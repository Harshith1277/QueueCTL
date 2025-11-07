🧱 Architecture Overview

QueueCTL follows a simple yet robust three-component architecture that models how real backend job queues (like Celery or AWS SQS) operate.

🧩 System Flow Diagram
          ┌───────────────────────────────┐
          │        User / CLI Tool        │
          │  (queuectl enqueue job.json)  │
          └──────────────┬────────────────┘
                         │
                         ▼
                ┌─────────────────────┐
                │     Job Queue DB     │
                │     (SQLite File)    │
                │  ─────────────────   │
                │  jobs table          │
                │  dlq table           │
                │  config table        │
                └──────────┬───────────┘
                           │
                  ┌────────┴────────┐
                  ▼                 ▼
        ┌─────────────────┐   ┌─────────────────┐
        │   Worker #1     │   │   Worker #2     │
        │ (Job Processor) │   │ (Job Processor) │
        └─────────────────┘   └─────────────────┘
                 │                  │
                 ▼                  ▼
         Executes job → updates DB  │
         Retries on failure (2s,4s) │
                 │                  │
                 ▼                  ▼
       ┌────────────────────────────────────┐
       │ Completed ✅   or   Failed ❌       │
       │ If failed beyond max_retries → DLQ │
       └────────────────────────────────────┘

🧠 Component Breakdown
Component	Description
CLI Layer	The main user interface. You use commands like enqueue, worker start, and status to interact with the system.
Database (SQLite)	The persistent storage engine (queue.db) that holds job states, retry counts, configurations, and DLQ data.
Workers	Background processes that fetch jobs from the DB, execute them, and update their status. They support multiple parallel instances (--count N).
DLQ (Dead Letter Queue)	A storage for jobs that permanently fail even after all retry attempts. You can inspect or retry them manually.
🔁 Job Lifecycle
State	Description
pending	Job is waiting in queue to be picked by a worker.
processing	Worker is executing the job command.
completed	Job ran successfully and exited with code 0.
failed	Job failed temporarily and will retry using exponential backoff.
dead	Job permanently failed and is moved to DLQ.
⚙️ Retry & Backoff Logic

Formula:

delay = base ^ attempts


Example (base=2, max_retries=3):
→ Retry 1 → 2s
→ Retry 2 → 4s
→ Retry 3 → 8s

If still fails → job moves to DLQ.

💾 Persistence Layer

All job details and states are stored in SQLite (queue.db), ensuring:

Jobs are not lost even if you close the terminal or restart your system.

Workers resume from the last known state.

🧰 Worker Management

Start workers in foreground:

python queuectl.py worker start --count 2


Stop all workers gracefully:

python queuectl.py worker stop


Workers finish current jobs before exiting — ensuring no data corruption.

💀 Dead Letter Queue (DLQ)

Automatically holds failed jobs that exceeded retry limit.

Inspect or retry manually using:

python queuectl.py dlq list
python queuectl.py dlq retry <job_id>

🧪 Smoke Test Flow

You can run the built-in smoke test to demonstrate the complete lifecycle:

python queuectl.py smoke-test


✅ It will:

Enqueue a successful and a failing job

Start workers

Process jobs

Retry failed job with exponential backoff

Move failed job to DLQ

Display DLQ contents at the end
