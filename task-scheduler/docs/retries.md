# Phase 5 retry and dead-letter runbook

Workers now classify failures, retry transient attempts with exponential full jitter,
and publish permanent/exhausted/poison outcomes to `tasks.dlq` with their complete
stored error history. AI diagnosis remains unset in this phase.

## Run and verify

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up
python -m alembic current
make check-worker
make check-retry
make test
make test-integration
make migration-check
```

`make up` applies revision `0002`, builds API/worker images, and updates Kafka's
message-size configuration. To keep three worker replicas:

```sh
docker compose up -d --build --wait --wait-timeout 180 --scale worker=3 api worker
python scripts/check_worker.py --expected-workers 3
python scripts/check_retry.py
```

The retry checker submits one fatal validation task and one `http_call` to a refused
worker-local port. It verifies direct DLQ delivery after one attempt and transient
exhaustion after two, including every stored attempt in the actual Kafka DLQ event.
It requires no external HTTP server. These identified examples remain available in
the database and DLQ for inspection.

## Attempt budget: Phase 5 semantic change

This phase follows the requested `attempts >= max_retries` cutoff. The historical
field name `max_retries` now means **total execution-attempt budget**, with at least
one initial attempt. Values 0 and 1 both allow one attempt; 3 allows at most three
attempts. This replaces the earlier phases' “retries after the first attempt” meaning.
The stored column and accepted 0–100 input range are unchanged.

`attempts` increments once when an execution is claimed, not again when it fails.
Every claimed attempt has a unique `(task_id, generation, attempt_no)` row. A transient failure
retries only when the completed count is below `max(1, max_retries)`.

## Classification and backoff

Handlers can raise `RetryableError` or `FatalError` from `shared.errors` to make the
decision explicit. Defaults are:

- Timeouts, connection/network errors, HTTP 5xx, and unclassified exceptions retry.
- Pydantic `ValidationError`, `ValueError`, `TypeError`, unknown handler lookup, and
  HTTP 4xx are fatal. The requested 4xx rule includes 408 and 429. A handler can
  explicitly raise `RetryableError` when a particular 4xx should be retried.

After failed attempt `n`, delay is sampled uniformly from zero through
`min(cap, base * 2**(n-1))`. Defaults are base 5 seconds and cap 300 seconds. Set
`WORKER_RETRY_BASE_SECONDS` and `WORKER_RETRY_CAP_SECONDS` to tune this; zero base
disables intentional delay. Bounds must be finite, nonnegative, and cap >= base.

Failure #1 routes to `tasks.retry.5s`, #2 to `tasks.retry.30s`, and #3+ to
`tasks.retry.5m`. These names identify routing buckets. The event's actual `due_at`
timestamp controls delivery; a bucket name is not a fixed/minimum sleep. For example,
the default second failure samples 0–10 seconds while using the 30s-named topic.

## Why Kafka topics plus a durable delay inbox

The existing Kafka topics retain retry events and make them observable. Each worker
also runs a retry consumer in `<WORKER_GROUP_ID>.retries`. It **does not sleep until
the retry deadline**. It validates each small `RetryMessage`, marks its Postgres
schedule received, commits that transaction, and manually commits the Kafka offset.
A later-due event therefore cannot hold up a due event on the same retry partition.

PostgreSQL `retry_schedules` stores stable event/task/failed-attempt identity,
`due_at`, receipt, and dispatch timestamps. The worker's failure transaction creates
the schedule, RETRYING task state, attempt error/stack trace, result event, and retry
outbox event atomically. The retry consumer only acknowledges receipt after durable
storage. This preserves retries across Redis loss and avoids coordinating a Redis
sorted-set write with a separate DB transaction.

A dispatcher scans received, undispatched, due rows using the database clock and
`FOR UPDATE SKIP LOCKED`. In one transaction it verifies the task is still RETRYING
at that failed attempt, moves it to SCHEDULED, creates a `tasks.pending` outbox event
for attempt `n+1`, and marks the schedule dispatched. Future rows are excluded from
the scan. Duplicate retry events never reset dispatch state. Multiple worker replicas
can ingest/dispatch concurrently without creating extra scheduled executions.

Both redispatch and failure events have deterministic IDs. A crash before dispatch
commit leaves the schedule due; a crash after commit leaves a durable pending event.
The shared outbox relay handles broker publication. Its acknowledged-but-uncommitted
window still permits Kafka duplicates, and worker attempt-generation checks ignore
old dispatches instead of triggering an early extra attempt.

## Fatal outcomes and DLQ history

Fatal errors, exhausted attempt budgets, and repeated worker crashes record FAILED
and create a `dead_letters` row plus DLQ/result outbox events in one transaction.
The DLQ event includes task identity/type, terminal reason, failed attempt number,
and **every stored failed attempt** with worker ID, error, stack trace, and timestamp.
There is no AI call and no automatic requeue after terminal failure.

After Kafka acknowledges the DLQ event, the publisher advances FAILED to
DEAD_LETTERED in the same transaction as its publication marker. Thus FAILED is
durable before publication, and DEAD_LETTERED means the DLQ delivery was recorded.
Redelivery waits for existing outcome events; it never executes a terminal task again.

Per-attempt storage retains the previous bounds: 8,000 error characters and 32,000
traceback characters. No history entries are dropped. Kafka broker/client limits
are now 32 MiB so histories from up to 100 bounded attempts fit, rather than leaving
an oversized DLQ event permanently unpublished. API task envelopes remain limited
to 900,000 bytes. Existing topic partition counts are unchanged.

## Crash poison protection

The retry engine also reaps RUNNING tasks whose Redis visibility lease is absent.
It locks the task, closes the unfinished attempt with `WorkerCrash`, and schedules
a retry or dead-letter outcome. This works without depending solely on redelivery
of the original pending message. Active/renewed leases are left alone.

`WORKER_MAX_CRASH_CLAIMS` defaults to 3. Consecutive claims that end without handler
completion count toward this limit; a completed handler failure breaks the crash
streak. Once the limit is reached, the task goes to the DLQ even if its normal
attempt budget is larger. The normal attempt limit can terminate it earlier.
Missing crash tracebacks are represented explicitly as unavailable. Lease-token and
attempt fencing still prevent an old owner from completing or releasing a newer
owner's task.

Each worker owns a fixed number of background loops, using bounded batches rather
than one sleeping coroutine per delayed task. `WORKER_RETRY_BATCH_SIZE` defaults to
100 and `WORKER_RETRY_ENGINE_POLL_SECONDS` to 0.25. These settings are in `.env.example`.
Graceful shutdown drains current execution/outcome publication, then stops retry
ingestion/maintenance; uncommitted receipts and dispatches remain recoverable.

## Tests

Unit tests cover custom/default classification, actual Pydantic and HTTP status
errors, full-jitter endpoints/exponential bounds/cap, huge-exponent safety, invalid
bounds, and topic routing. Integration tests use real PostgreSQL, Redis, and Kafka:

- Actual `flaky_task`: two forced failures, then success on attempt three.
- Fatal validation error: one attempt and direct DLQ delivery.
- Budgets 0, 1, and 3: exact exhaustion counts with complete stored history.
- Independent crash-claim limit, active-lease protection, and competing reapers.
- Nonblocking due dispatch, duplicate ingestion, competing dispatchers, and stale
  pending envelopes that must not start an extra attempt.
- Actual Kafka delivery of a DLQ history larger than 1 MiB with all 30 entries.
- Migration upgrade/downgrade/upgrade and ORM drift checks, including the inbox.

Backoff reference: [AWS exponential backoff and jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).

Phase 7 adds generation-fenced manual DLQ retries, retained audit receipts, and queue
inspection/discard endpoints. See [the DLQ runbook](dlq.md).
