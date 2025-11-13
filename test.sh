#!/bin/bash

echo "QueueCTL Testing"
echo "==================="
echo ""

# Clean setup
echo "1. Cleaning up old files..."
rm -f queue.db worker.log queuectl_worker.pid
pip install -e . > /dev/null 2>&1

# Test basic job execution
echo "2. Testing basic job execution..."
queuectl worker start --count 2
queuectl enqueue '{"command": "echo test"}'
sleep 2
queuectl list

# Test job output logging
echo "3. Testing job output logging..."
FULL_JOB_ID=$(sqlite3 queue.db "SELECT id FROM jobs WHERE command='echo test' LIMIT 1")
queuectl job $FULL_JOB_ID

# Test priority queues
echo "4. Testing priority queues..."
queuectl worker stop
queuectl enqueue '{"command": "sleep 2 && echo low", "priority": 3}'
queuectl enqueue '{"command": "sleep 2 && echo high", "priority": 1}'
queuectl worker start --count 1
sleep 6
queuectl list

# Test multiple workers with no race conditions
echo "5. Testing multiple workers (no race conditions)..."
queuectl worker restart --count 5
for i in {1..10}; do queuectl enqueue "{\"command\": \"echo job $i\"}"; done
sleep 5
COMPLETED=$(queuectl list --state completed | wc -l)
echo "Completed jobs: $COMPLETED"

# Test retry and DLQ
echo "6. Testing retry mechanism and DLQ..."
queuectl config set max-retries 3
queuectl config set backoff-base 2
queuectl enqueue '{"command": "exit 1"}'
sleep 15
queuectl dlq list

# Test DLQ retry
echo "7. Testing DLQ retry..."
DEAD_JOB=$(sqlite3 queue.db "SELECT id FROM jobs WHERE state='dead' LIMIT 1")
queuectl dlq retry $DEAD_JOB
sleep 5
queuectl list --state dead

# Test job timeout
echo "8. Testing job timeout..."
queuectl config set job-timeout 3
queuectl enqueue '{"command": "sleep 10"}'
sleep 10
queuectl list --state dead

# Test scheduled jobs
echo "9. Testing scheduled jobs..."
queuectl config set job-timeout 300
FUTURE=$(python3 -c "from datetime import datetime, timedelta; print((datetime.now() + timedelta(seconds=5)).strftime('%Y-%m-%d %H:%M:%S'))")
queuectl enqueue "{\"command\": \"echo scheduled\", \"run_at\": \"$FUTURE\"}"
echo "Waiting for scheduled time..."
sleep 8
queuectl list | grep scheduled

# Test persistence
echo "10. Testing persistence across restart..."
queuectl enqueue '{"command": "echo persistence"}'
queuectl worker stop
queuectl list --state pending
queuectl worker start --count 1
sleep 2
queuectl list | grep persistence

# Test invalid commands
echo "11. Testing invalid command handling..."
queuectl enqueue '{"command": "nonexistentcommand12345"}'
sleep 15
queuectl dlq list

# View final metrics
echo "12. Final metrics..."
queuectl metrics

# Cleanup
queuectl worker stop
queuectl config set max-retries 3
queuectl config set backoff-base 2
queuectl config set job-timeout 300
echo ""
echo "All tests complete!"