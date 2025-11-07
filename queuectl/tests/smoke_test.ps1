Write-Host "=== Running QueueCTL Smoke Test ==="

# Clean up old DB
if (Test-Path "..\queue.db") {
    Remove-Item "..\queue.db" -Force
    Write-Host "Old database removed."
}

# Run built-in smoke test
python ..\queuectl.py smoke-test

# Enqueue test jobs
python ..\queuectl.py enqueue '{"id":"job_test1","command":"echo hello","max_retries":2}'
python ..\queuectl.py enqueue '{"id":"job_test2","command":"nonexistent_cmd","max_retries":2}'

# Start worker
Start-Process -FilePath python -ArgumentList "..\queuectl.py worker start --count 1"

# Wait a few seconds
Start-Sleep -Seconds 10

# Show results
python ..\queuectl.py status
python ..\queuectl.py dlq list
python ..\queuectl.py list --state pending

Write-Host "=== Smoke Test Complete ==="
