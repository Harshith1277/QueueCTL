QueueCTL is a CLI-based background job management system built in Python that processes and manages background tasks (jobs) asynchronously.
It supports automatic retries, multiple worker processes, exponential backoff, and a Dead Letter Queue (DLQ) for failed jobs — all powered by Python’s standard library and SQLite for persistence.

This project simulates how large-scale backend systems (like Celery, RQ, or AWS SQS) handle asynchronous tasks reliably and efficiently. It’s lightweight, dependency-free, and demonstrates core backend concepts such as fault tolerance, concurrency, and persistence.
