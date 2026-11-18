# Phase 7 DLQ pipeline

The dedicated `dlq` service consumes `tasks.dlq` with the `DLQ_GROUP_ID` consumer
group and manual offset commits. It commits the incident, source receipt, and any
eligible FAILED → DEAD_LETTERED transition together before acknowledging Kafka.
The worker still creates its canonical incident and outbox atomically at failure;
ingestion enriches that incident instead of creating a duplicate.

## Run and verify

From `task-scheduler`:

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up
python -m alembic current
make migration-check
make check-dlq
make test
make test-integration
```

The migration head is `0004`. `make up` builds/starts the DLQ service as well as
API, worker, and two scheduler replicas. To exercise three workers:

```sh
docker compose up -d --wait --wait-timeout 180 --scale worker=3 --scale scheduler=2 api worker scheduler dlq
python scripts/check_worker.py --expected-workers 3
python scripts/check_dlq.py
```

The DLQ checker submits two new simulated email tasks with invalid recipients. It
waits for actual Kafka ingestion, corrects the first task through the retry API,
checks success and preserved attempt history, then republishes its original DLQ
event and verifies replay safety. It discards the second incident and verifies a
republished copy cannot resurrect it. It also checks the Prometheus gauge. All
checks print `[OK]`; failures exit nonzero. Only these new task IDs are modified.

If the Phase 8 agent is running, pause its automatic decisions while verifying the
manual DLQ workflow, then restore the `.env` configuration:

```sh
AGENT_ENABLED=false docker compose up -d --wait agent
make check-dlq
docker compose up -d --wait agent
```

## API

Examples assume the default host API port 8002; use your `API_PORT` override when
needed. Interactive request/response schemas are at `/docs`.

```sh
curl -fsS 'http://localhost:8002/dlq?type=send_email&category=fatal&status=OPEN&limit=20&offset=0'
curl -fsS 'http://localhost:8002/dlq?date=2026-11-10'
curl -fsS 'http://localhost:8002/dlq?date_from=2026-11-10T00:00:00Z&date_to=2026-11-11T00:00:00Z'
```

`GET /dlq` returns `items`, `total`, `limit`, and `offset`. Type, category, status,
UTC calendar `date`, aware `date_from`/`date_to`, and `failure_signature` filters
combine with AND. Ranges include the start and exclude the end. `status` defaults
to **OPEN**. Categories emitted by workers are `fatal`, `exhausted`, and `poison`;
unclassified external failures use `unknown`. Queue statuses are **OPEN**,
**RETRIED**, **DISCARDED**, and **ESCALATED**, separate from task execution states.
List pages have a maximum of 100 incidents.

`GET /dlq/{task_id}` returns the latest incident with its full error history,
worker ID, attempts, fingerprint, optional `ai_diagnosis`, all earlier `incidents`,
all `attempt_history`, and the current `task` (nullable for an orphan audit).
No LLM call is made in this phase. A discarded latest incident returns 404; its
retained audit remains queryable with `GET /dlq?status=DISCARDED`.

```sh
# Set TASK_ID to an existing open incident's task UUID.
export TASK_ID='replace-with-task-uuid'
curl -fsS "http://localhost:8002/dlq/$TASK_ID"
curl -fsS -X POST "http://localhost:8002/dlq/$TASK_ID/retry" \
  -H 'Content-Type: application/json' -d '{"payload":{"to":"corrected@example.com"}}'
```

`POST /dlq/{task_id}/retry` accepts no body, `{}`, or `{"payload":{...}}`. Omitted
or null payload preserves the stored payload; a supplied object **fully replaces**
it. Empty `{}` as the payload is a valid replacement. The operation row-locks the
task, requires a current OPEN incident and DEAD_LETTERED/AI_DIAGNOSED task, then
commits a new execution generation, attempts=0, cleared result/error, corrected
payload, PENDING state, closed incident, and pending outbox event together. The
response captures that PENDING task even if a worker starts immediately afterward.
Oversized messages fail with 413 and leave the incident open. Repeating a retry
against the same resolved generation returns the existing task; it does not enqueue
another execution. Original idempotency keys and total attempt budgets are retained.

```sh
curl -fsS -X POST http://localhost:8002/dlq/retry-bulk \
  -H 'Content-Type: application/json' \
  -d '{"type":"send_email","category":"fatal","status":"OPEN","limit":25}'
```

`POST /dlq/retry-bulk` accepts the same incident filters, optional replacement
`payload`, and a `limit`. It considers at most `min(limit, DLQ_BULK_LIMIT)` distinct
tasks, in UUID order. The default configured bound is 100 (maximum 500). Response
fields are `matched` (distinct matching task count), `retried`, `task_ids`, and
`skipped` with reasons. Each retry is independently atomic. Orphans, races, resolved
incidents, or a latest incident outside the selected filter are reported as skips.
Run another bounded request to process further OPEN incidents.

```sh
curl -fsS -X DELETE "http://localhost:8002/dlq/$TASK_ID"
curl -fsS http://localhost:8002/metrics
```

`DELETE /dlq/{task_id}` returns 204 and tombstones the latest generation's incidents
as DISCARDED. An eligible current failed/dead-lettered task becomes CANCELLED.
Repeated deletion is safe. Deleting an older incident after retry never cancels the
new execution. Audit rows and source receipts remain available; discard does not
physically delete history.

## Replay and execution identity

A canonical incident uses the event UUID. Every distinct Kafka source
`(topic, partition, offset)` also has a unique `dlq_receipts` row with its full input
payload. The same offset can be redelivered without another row; republishing the
same incident at a new offset adds a receipt but not another incident. Source IDs
that collide across task/generation identities receive separate audit identities.
Missing/invalid IDs use stable source-derived UUIDs. Malformed JSON and JSONB-
incompatible text retain raw base64 bytes; removed/unknown task IDs retain orphan
incidents without blocking a partition. Orphan retry returns 409 because no task
can be reconstructed safely from error metadata alone.

Task execution starts at `generation=0`, which is also the default for older wire
envelopes. Each manual retry increments it. Attempt identity is now
`(task_id, generation, attempt_no)`; retry inbox identity, outcome event IDs, leases,
and completion caches are generation-aware. Old pending records are acknowledged
without running; stale retry schedules and DLQ acknowledgements cannot change a
new generation. The SHA-256 failure signature is for clustering, **not** event
identity or deduplication.

Fingerprint normalization removes UUIDs, long hex IDs, named task/worker/request
IDs, ISO timestamps, numeric values, whitespace variation, and absolute traceback
path prefixes. It retains exception/function/file names and surrounding text.
Similar errors cluster stably; different root errors remain distinct. Original
errors and stack traces remain available for diagnosis. The normalization algorithm
is explicitly versioned internally as v1.

`dlq_size{task_type="send_email"}` counts durable **OPEN incidents** by their stored
task type, including retained orphan incidents. Each API scrape reads PostgreSQL;
replicas report the same global count (do not sum identical replicas). Retry and
discard remove incidents from the count. It is not Kafka consumer lag.

`DLQ_BATCH_SIZE` bounds ingestion per poll, `DLQ_RETRY_SECONDS` controls failure
retry pauses, and `DLQ_GROUP_ID` identifies the durable ingestion group. Multiple
DLQ replicas may share that group. The internal health endpoint is on port 8005;
Compose probes it without publishing another host port.

Revision 0004 migrates existing rows to generation zero, backfills incident
metadata, retains audits independently of task deletion, and changes attempt/retry
uniqueness. Fingerprints for legacy incidents are filled by the consumer's bounded
backfill loop. Downgrading refuses when generation>0 executions or orphan audit
rows would lose information. For a rolling upgrade, update workers first (they accept legacy generation-zero
envelopes), then API and scheduler images, before allowing manual retry.

## Tests

`tests/unit/test_fingerprint.py` covers normalization stability, distinct roots,
categories, and generation-aware event IDs. `tests/integration/test_dlq.py` covers
concurrent/repeated ingestion, retained diagnoses, malformed/orphan audit, filters,
UTC ranges, full histories, gauge changes, concurrent corrected retries through
real Kafka, repeated failure/recovery generations, old retry-schedule fencing,
bounded bulk actions, and discard replay safety. Shared migration and worker tests
also cover the expanded primary key and independent audit lifecycle.

## Phase 7 file inventory

Created:

- `services/dlq/__init__.py`, `consumer.py`, `main.py`, `__main__.py`, `Dockerfile`
- `services/api/dlq.py`
- `shared/fingerprint.py`
- `migrations/versions/0004_dlq_audit_and_execution_generations.py`
- `scripts/check_dlq.py`
- `tests/unit/test_fingerprint.py`, `tests/integration/test_dlq.py`
- `docs/dlq.md`

Changed:

- `docker-compose.yml`, `.env.example`, `Makefile`, `README.md`
- `shared/config.py`, `models.py`, `tables.py`, `outbox.py`
- `services/api/main.py`, `schemas.py`
- `services/worker/executor.py`, `retry_policy.py`, `retry_engine.py`
- `services/scheduler/core.py`
- `tests/unit/test_models.py`, `tests/integration/test_shared.py`, `test_worker.py`
- `docs/architecture.md`, `worker.md`, `retries.md`

Final validation for this phase: **220 tests passed**, Ruff passed, Compose
configuration validated, Alembic reports `0004 (head)` with no schema drift, and
`python scripts/check_dlq.py` verifies the deployed end-to-end pipeline.
