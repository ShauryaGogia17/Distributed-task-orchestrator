# Worker runbook through Phase 6

Phase 5 adds [delayed retries and DLQ delivery](retries.md). Its attempt-budget and
failure-outcome rules below supersede Phase 4 behavior.

The worker consumes `tasks.pending` in the configurable `task-workers` group with
automatic offset commits disabled. It executes registered async handlers, records
attempts in PostgreSQL, and publishes durable success/failure results to
`tasks.results` through the shared transactional outbox.

## Run and verify

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up
make check-worker
make test
make test-integration
```

`make up` now builds and starts the worker after infrastructure/API initialization.
To run three replicas sharing the same consumer group:

```sh
docker compose up -d --build --wait --wait-timeout 180 --scale worker=3 api worker
python scripts/check_worker.py --expected-workers 3
docker compose ps worker
docker compose logs -f worker
```

No fixed container name or published worker port prevents scaling. Leave `WORKER_ID`
empty so each process generates a hostname/UUID identity. Each container has an
independent readiness file. On the host, use distinct IDs and health-file paths
when starting multiple processes:

```sh
WORKER_ID=host-worker-1 WORKER_HEALTH_FILE=/tmp/host-worker-1.json python -m services.worker
```

The live checker submits an identified report through the API, verifies one
successful attempt/result, receives its Kafka result event, and verifies the input
offset committed by the actual worker group. It also checks the requested number
of Redis worker heartbeats. Its sample task remains in the database for inspection.

## Handler registry and samples

Handlers use `@register("task_type")` from `services.worker.registry` and accept
`(payload, context)`. They must be async functions returning a JSON-compatible
value. Duplicate registration and synchronous functions are rejected. The
`HandlerContext` provides the worker ID, a shared async HTTP client, and HTTP bounds.
Import custom handler modules during service startup alongside the built-ins.

All six built-ins are runnable:

- `send_email`: `{"to":"demo@example.invalid","subject":"Hello","body":"Example"}`.
  Simulates delivery; returns a message UUID. `recipient` is an alias for `to`.
- `generate_report`: `{"title":"demo","data":[1,2,3],"seconds":0.2}`.
  Sleeps asynchronously and returns count, sum, and mean.
- `http_call`: `{"url":"http://api:8000/health","timeout_seconds":3}`.
  Makes a real HTTP request. Optional `method` and `json` support request bodies.
  Non-success HTTP status codes raise; redirects are not followed. Response bodies
  are size-bounded and returned as UTF-8 text. A host worker should use
  `http://localhost:8002/health` for that example instead.
- `flaky_task`: `{}`. Fails with probability 0.5 per execution.
- `bad_input_task`: `{}`. Always raises a Pydantic `ValidationError`.
- `slow_task`: `{"seconds":2}`. Sleeps for the configured duration; `sleep_seconds`
  is also accepted. Durations exceeding the worker timeout produce FAILED attempts.

Submit a report directly:

```sh
curl -fsS http://localhost:8002/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"generate_report","priority":"high","payload":{"data":[1,2,3],"seconds":0.2}}'
```

Use the returned ID with `GET /tasks/{id}` to inspect status, result, and ordered
`attempt_history`. Unknown handler types produce a recorded FAILED attempt.

## Execution and idempotency

Each dispatch is validated as a `TaskMessage`. The worker consults both the Redis
success hint and PostgreSQL. A cache hit never skips a task whose database state
is still PENDING. A missing cache entry never causes a SUCCEEDED task to execute
again. PostgreSQL is authoritative, including after Redis eviction or restart.

The worker acquires a random-token Redis lease at `task:lease:{task_id}` using
SET NX PX, then locks the task row. PENDING tasks pass through SCHEDULED before
RUNNING; the worker increments the durable attempt counter and inserts a unique
`task_attempts` row in the same transaction. Handler type/payload come from the
database rather than trusting a replayed envelope's contents.

The handler executes under `WORKER_TASK_TIMEOUT_SECONDS`. The lease is renewed
every third of `WORKER_VISIBILITY_TIMEOUT_SECONDS` using token-checked Lua. Loss of
lease ownership or a Redis renewal failure cancels the handler and prevents stale
completion. Finalization locks the task again and verifies the attempt number,
worker identity, unfinished attempt, and current lease token.

On success, one DB transaction stores SUCCEEDED, result, finished attempt, and a
result outbox event. On handler error or timeout it stores RETRYING or FAILED,
last error, attempt error/traceback, and result plus retry/DLQ events according to
classification and remaining attempt budget. Result IDs are deterministic
from task UUID and attempt number and are included as the Kafka `outbox-id` header.
Result envelopes include task/type/status, attempt number, worker ID, result/error,
and finish timestamp. Oversized/non-JSON handler results become recorded failures.

API and worker processes share the outbox relay implementation and SKIP LOCKED
coordination. The worker waits until the result event's publication is committed
before acknowledging the input offset. If Kafka is unavailable, the durable event
remains unpublished and the input is retried. If the task has already succeeded,
redelivery waits for that existing event instead of invoking the handler again.
Shared relay tuning currently uses the existing `API_OUTBOX_*` settings in both
service types; worker result-wait duration is configured separately.

Delivery and external handler effects remain at least once. Redis leases and DB
attempt fencing protect durable task state; they cannot undo an HTTP request or
other external side effect completed just before a worker crash. Real handlers
should pass task/attempt idempotency identities to downstream systems when supported.

## Visibility and failed execution scope

Worker liveness uses `worker:{id}` with a TTL. Heartbeat renewal requires the same
owner token; a duplicate live worker ID is rejected. Heartbeats continue during
graceful draining. The container health probe verifies its local identity still
owns the Redis heartbeat. If heartbeat renewal fails, the worker stops admission,
drains existing work, and exits with failure so Compose can restart it.

A RUNNING task with no live lease can be reclaimed by redelivery or the lease reaper.
Its old attempt is closed with `WorkerCrash`; another attempt goes through the retry
inbox and RETRYING/SCHEDULED. Since Phase 5, the total attempt limit is
`max(1, max_retries)`, with a separate consecutive-crash claim limit. Terminal failure
records FAILED, then DEAD_LETTERED when Kafka publication is recorded. An expired
owner cannot finish the replacement attempt or release/renew its lease.

Retryable handler failures and timeouts now use delayed retry topics; fatal errors
and exhausted/poison tasks go to the DLQ with full stored history. AI diagnosis is
reserved for a later phase.
The worker skips CANCELLED/other completed recovery states without executing them.
Missing DB tasks are skipped (for example, dispatches left by isolated tests).
Malformed dispatch envelopes remain uncommitted and are logged/retried rather than
silently discarded; automatic quarantine is not part of this phase.

Schedule eligibility and attempt start/finish timestamps use the database clock.
Phase 6 holds future tasks in a durable scheduler ledger and Redis sorted set,
publishing them only when due. Premature legacy dispatches backed by an unreleased
ledger are acknowledged without execution; scheduler recovery guarantees later
publication. They do not hold a Kafka partition until the future deadline. Raw
future messages without a durable ledger still defer rather than executing early.

## Concurrency, offsets, and shutdown

An asyncio semaphore enforces `WORKER_CONCURRENCY`. Admission is bounded to at most
one active record per assigned partition, so no later record can commit past an
unfinished earlier record. Active partitions are paused while other partitions
continue polling. There is no unbounded queue of handler tasks. Kafka's internal
fetch buffers are bounded separately.

The six input partitions limit active handlers to six across the group with this
ordering strategy, even if configured concurrency or replica count is larger.
Priority lanes retain the Phase 3 routing contract, without global priority
ordering between partitions.

SIGTERM/SIGINT stops polling/admission, waits for in-flight handlers and their result
events, commits completed input offsets, then closes consumer/publisher/heartbeat
clients. Set `WORKER_SHUTDOWN_TIMEOUT_SECONDS` above task timeout plus expected
publication time. Deadline overruns cancel unfinished work and leave input offsets
uncommitted for recovery. Partition revocation uses the same drain-before-commit
logic with `WORKER_REBALANCE_TIMEOUT_SECONDS`. Commit failures are propagated into
redelivery, never treated as successful acknowledgement. Polls continue while
partitions are paused to preserve Kafka group liveness.

Compose allows 120 seconds before force-killing the worker; the default worker
drain deadline is 90 seconds. To stop gracefully or restart replicas:

```sh
docker compose stop worker
docker compose up -d --no-deps --wait --scale worker=3 worker
```

Settings and defaults are in `.env.example`. Defaults are concurrency 4, task
timeout 30s, visibility timeout 60s, heartbeat interval 5s/TTL 20s, and result
publication wait 30s. Heartbeat interval must be shorter than its TTL.

## Tests

Unit tests cover decorator registration, duplicate/sync rejection, all sample
handler behaviors, HTTP timeout/size errors, bounded admission, per-partition commit
ordering, graceful draining, forced deadline cancellation without commits, rebalance
draining, and retry/seek after infrastructure failure.

Integration tests use isolated schemas and real Redis/Kafka. They cover success and
result delivery, timeout persistence, cache-independent SUCCEEDED deduplication,
validation/unknown-handler failures, lease renewal/replacement fencing, crash recovery
and budget exhaustion, a real local HTTP request, heartbeat ownership/TTL, and a
graceful full-service stop with the actual Kafka group offset verified afterwards.

Phase 7 adds generation-fenced manual DLQ retries, retained audit receipts, and queue
inspection/discard endpoints. See [the DLQ runbook](dlq.md).
