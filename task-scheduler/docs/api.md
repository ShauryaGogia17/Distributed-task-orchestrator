# API runbook through Phase 6

See [scheduler.md](scheduler.md) for cron endpoints and future-task release.

The FastAPI service accepts tasks durably before publishing them to Kafka. OpenAPI
is available at <http://localhost:8002/docs>, with ReDoc at `/redoc` and the schema
at `/openapi.json`. The default host port is 8002; the container listens on 8000.

## Run

From the repository root with the Phase 1 `.env` configured:

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up
curl -fsS http://localhost:8002/health
curl -fsS http://localhost:8002/metrics
make test
make test-integration
```

`make up` now builds the API image, runs the `migrate` one-shot service, and waits
for the API and infrastructure health checks. Direct Compose usage also works:

```sh
docker compose up -d --build --wait --wait-timeout 180 api
docker compose logs -f api
```

The image uses Python 3.11, runs as UID 10001, and excludes `.env`, local virtual
environments, and test files from its build context. Connection settings are passed
through the Compose environment using Docker-network service names and fixed
internal ports. Published ports remain bound to loopback.

To run another API process on the host while the Compose stack stays up:

```sh
API_PORT=8003 python -m services.api
```

Both instances can publish the same database outbox safely using SKIP LOCKED.
Set `API_OUTBOX_ENABLED=false` to inspect committed backlog without publishing from
that instance. Other running replicas can still publish it.

## Submit and inspect

```sh
task_response=$(curl -fsS http://localhost:8002/tasks \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: local-example-1' \
  -d '{"type":"demo.echo","payload":{"message":"hello"},"priority":"high","idempotency_key":"phase3-example-1"}')
printf '%s\n' "$task_response"
task_identifier=$(printf '%s' "$task_response" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS "http://localhost:8002/tasks/$task_identifier"
curl -fsS 'http://localhost:8002/tasks?status=PENDING&type=demo.echo&limit=20&offset=0'
curl -fsS -X POST "http://localhost:8002/tasks/$task_identifier/cancel"
```

`POST /tasks` returns 201 for a new task and 200 for an existing idempotency key.
The response includes `Location`, `Idempotency-Replayed`, and `X-Request-ID` headers.
A repeated key always returns the original task, including when the submitted
payload differs or that task has since been cancelled. It creates no additional
outbox event. Requests without an idempotency key create separate tasks. The
database UNIQUE constraint handles concurrent requests across replicas; Redis is
not required to enforce this invariant.

Inputs use the shared `TaskCreate` contract plus priority aliases. The API defaults
to normal priority (numeric 5), three total execution attempts in Phase 5, and an
immediate schedule. In Phase 6, future `scheduled_at` values create SCHEDULED tasks
and a durable delay ledger; Redis indexes them until the scheduler releases them
to PENDING with a Kafka outbox event. Immediate tasks retain atomic task/outbox submission.
Serialized task envelopes remain limited to 900,000 bytes. Larger envelopes
return 413 and roll back both task and event. `API_MAX_TASK_MESSAGE_BYTES` can lower
this ceiling; the larger Phase 5 Kafka limit accommodates complete DLQ histories.

Phase 5 follows attempts >= max_retries: the historical `max_retries` field is now
a total-attempt budget, with values 0 and 1 both allowing one initial attempt.
See retries.md for delayed dispatch and terminal DLQ states.

`GET /tasks/{id}` includes `attempt_history` ordered by `attempt_no`; the existing
`attempts` field remains a counter. `GET /tasks` returns `items`, `total`, `limit`,
and `offset`, ordered newest first by creation timestamp and UUID. Filters match
`status`, `type`, and `schedule_id` exactly. Limits are 1–100 and offsets must be nonnegative.
The count and page are read at PostgreSQL's normal READ COMMITTED isolation, so
concurrent inserts or updates may change pagination between requests.

Cancellation acquires a task row lock and succeeds only for PENDING or SCHEDULED.
Other states, including an already CANCELLED task, return 409. Missing tasks return
404. Cancellation records the authoritative DB state. It cannot retract an event
already present in Kafka; workers recheck task state before executing.
Pending outbox events remain durable and may still be published after cancellation.

## Priority routing

Named priorities normalize to `high=9`, `normal=5`, and `low=1`; numeric 0–9 values
remain accepted. Values 7–9 use the high lane, 3–6 normal, and 0–2 low. The outbox
persists the key `<lane>:<task UUID>`. Stable SHA-256 hashing selects one of two
partitions within each lane: high uses 0–1, normal 2–3, and low 4–5.

This contract requires exactly six partitions on `tasks.pending`, which `/health`
checks. Kafka preserves order within a partition, but does not prioritize one
partition over another. Worker/scheduler preference for high lanes belongs to a
later phase. Priority routing by itself does not provide global priority ordering.

## Outbox recovery

Immediate submission inserts the task and versioned `TaskMessage` outbox row in one
PostgreSQL transaction. Future submission inserts task plus delayed ledger instead;
its outbox event is created at release. HTTP responses follow the DB commit.
Kafka availability is not needed for accepting the transaction.

The background publisher selects unpublished rows ordered by creation time and UUID
using `FOR UPDATE SKIP LOCKED`. It holds those locks through broker acknowledgement,
sets `published_at`, and commits. Multiple API processes claim different rows. A
failed send, timeout, cancellation, or failed DB commit rolls back the publication
transaction. Rows stay eligible after API termination; a restarted process resumes
polling. Broker/database failures are logged and retried without crashing the HTTP
service. The outbox ID is sent as a Kafka `outbox-id` header and stays stable on replay.

Delivery is **at least once**. A crash after Kafka acknowledges but before Postgres
commits `published_at` can cause replay. Downstream consumers must deduplicate and
check task state. The API publisher uses `acks=all` and bounded non-idempotent client
delivery; durable retry comes from PostgreSQL. Other shared producer users retain
the default idempotent producer configuration. No published outbox cleanup runs yet.

Publication uses `API_OUTBOX_BATCH_SIZE`, `API_OUTBOX_POLL_SECONDS`,
`API_OUTBOX_RETRY_SECONDS`, `API_OUTBOX_PUBLISH_TIMEOUT_SECONDS`, and
`API_OUTBOX_SHUTDOWN_TIMEOUT_SECONDS`. On shutdown the publisher first tries to
finish its transaction; cancellation rolls back any unfinished publication. A
batch failure can replay earlier acknowledged events from that batch.

## Operations and errors

`GET /health` probes the task/outbox schema, Redis PING, and Kafka topic metadata.
It also checks publisher-task liveness. It returns 200 when checks pass and 503
when degraded; publisher liveness does not prove the backlog is making progress.
Health probes are bounded by `API_HEALTH_TIMEOUT_SECONDS`.

`GET /metrics` exposes shared durable submission/outcome counters, attempt latency,
DLQ/outbox state, agent/circuit metrics, and local process gauges. Use the supplied
`scheduler:*` recording rules to deduplicate replicas. See [observability](observability.md)
for the metrics contract and the provisioned Prometheus/Grafana stack.

Every HTTP response has an `X-Request-ID`. A supplied ID is retained only when it
contains 1–128 ASCII letters/digits or `_.:-`; otherwise a UUID is generated.
Structured request logs include this ID. Global HTTP, validation, database, and
unexpected-error handlers use this response shape:

```json
{"error":{"code":"http_409","message":"Only PENDING or SCHEDULED tasks can be cancelled"},"request_id":"local-example-1"}
```

Validation errors return 422 with field locations, without echoing submitted input.
Database failures return 503 and unexpected failures return 500, with generic public
messages and private exception logs.

## Verification coverage

Integration tests use unique PostgreSQL schemas and test consumer groups. They
verify HTTP submission → committed row/outbox → actual Kafka delivery, recovery
after restarting the app, eight concurrent idempotent requests creating one event,
cancellation across all states, attempt history, filters, pagination, errors,
request IDs, OpenAPI, health, and metrics. They also verify atomic rollback,
oversized-message rejection, retry after broker errors, replay after real Kafka
acknowledgement followed by a simulated crash, and concurrent publisher row locking.
Kafka test events remain in their topics until retention removes them.
