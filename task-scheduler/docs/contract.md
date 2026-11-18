# Python / Java shared message contract (version 1)

Kafka values are UTF-8 JSON **objects**, with no Java type headers or Python-specific
serialization. Topic names, stable task UUIDs, execution generations, row locks,
Redis leases, and outbox identities are language-independent. Alembic owns the schema;
Hibernate uses `ddl-auto=validate` and never creates or modifies tables.

## Canonical JSON Schemas

Generated Draft 2020-12 schemas are checked in and shipped with the Python package:

- [Task dispatch](../shared/contracts/task-message.schema.json): `tasks.pending`.
- [Retry receipt](../shared/contracts/retry-message.schema.json): `tasks.retry.5s`,
  `tasks.retry.30s`, `tasks.retry.5m`.
- [Attempt outcome](../shared/contracts/task-result.schema.json): `tasks.results`.
- [Terminal failure](../shared/contracts/dead-letter.schema.json): `tasks.dlq`.
- [Nested error record](../shared/contracts/error-record.schema.json).

The schemas come directly from Pydantic and are the authority. This equivalent
compact dispatch schema shows the wire fields and defaults:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "TaskMessage v1",
  "type": "object",
  "additionalProperties": false,
  "required": ["task_id", "type", "scheduled_at"],
  "properties": {
    "version": {"type": "integer", "const": 1, "default": 1},
    "task_id": {"type": "string", "format": "uuid"},
    "type": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[a-zA-Z][a-zA-Z0-9_.-]*$"},
    "payload": {"type": "object", "default": {}},
    "priority": {"type": "integer", "minimum": 0, "maximum": 9, "default": 0},
    "attempt_no": {"type": "integer", "minimum": 1, "maximum": 2147483647, "default": 1},
    "max_retries": {"type": "integer", "minimum": 0, "maximum": 100, "default": 3},
    "generation": {"type": "integer", "minimum": 0, "maximum": 2147483647, "default": 0},
    "ai_modified": {"type": "boolean", "default": false},
    "scheduled_at": {"type": "string", "format": "date-time"},
    "created_at": {"type": "string", "format": "date-time"}
  }
}
```

`payload` contains recursively valid JSON values, including nested arrays, booleans,
nulls, and arbitrary-size JSON integers. NaN and Infinity are forbidden. Root payload
must be an object. Counters fit signed PostgreSQL/Java 32-bit integers. Priority,
attempt, generation, and retry budget use JSON integers rather than quoted strings
or fractional values. UUIDs are conventionally lowercase hyphenated strings.

Timestamps use aware RFC 3339 strings, commonly UTC `Z` or `+00:00`, with up to
microsecond precision for Postgres. Offsets are accepted; comparisons are by instant.
`created_at` defaults to current UTC if absent. Other defaults are shown above;
explicit null is rejected for dispatch fields. Schema `format` validation must be
enabled when using an independent JSON Schema validator. No class-name/type-id
Kafka header is necessary. Unknown fields on owned version-one envelopes are rejected.

## Consumer groups and namespace ownership

Both workers use Kafka's classic group membership protocol and manual offset
commits. **Their pending group IDs must differ**:

- Python: `WORKER_GROUP_ID=task-workers`, owns types not starting `java.`.
- Java: `JAVA_WORKER_GROUP_ID=task-workers-java`, owns only types starting `java.`.

Different groups each receive every record. A worker acknowledges the other
language's namespace before applying its own schema/handler logic, creating no
attempt and changing no task state. Within each group, replicas distribute all six
partitions normally. Using the same group ID would make filtering lose ownership
of some records; the Java service rejects that configuration at startup.

Kafka order is per partition, not global. Java runs a synchronous listener with
`MANUAL_IMMEDIATE`, at most one fetched record per consumer, and bounded concurrent
consumers/handler threads. Python retains its per-partition ordering/semaphore.
A record is acknowledged after committed outcome events are broker-acknowledged.
A processing/commit failure remains unacknowledged and is retried. Malformed owned
records are not silently discarded or automatically "recovered" by Spring.

## Authority, idempotency, and lifecycle

The task must already exist in Postgres. The API commits task plus outbox atomically;
a Kafka message alone never creates a task. Workers use the row's payload, priority,
budget, generation, and status as authoritative. Missing tasks, cancelled tasks,
stale generations, and already-completed attempts are safely skipped. A future
generation is not acknowledged as completed work.

Attempts have identity `(task_id, generation, attempt_no)`. Generations begin at
zero and increase on manual/AI recovery; attempt numbers reset without deleting old
history. `max_retries` remains the **total attempt budget**, minimum one initial
attempt, from Phase 5. An attempt increments once on claim. Delayed/circuit-parked
tasks consume no attempt until they actually run.

Claims and outcome writes row-lock the task. Each running attempt holds
`task:lease:{task_id}` for generation zero or `task:lease:{task_id}:g{generation}`
for a later generation. SET NX PX uses a random owner token; Lua renewal/release
compare that exact token. Completion checks generation, attempt ownership, and the
current lease. Java uses the same generated Lua circuit scripts and key hashing as
Python, including renewable token-fenced half-open probes. Worker heartbeat values
are JSON at `worker:{id}` with a TTL; Java IDs start `java-` and identify language.

Success atomically stores SUCCEEDED, attempt finish, result JSONB, and result outbox.
A transient failure below budget atomically stores RETRYING, error/trace, retry
inbox, and retry/result outboxes. Fatal/exhausted/poison failures store FAILED,
full generation error history, canonical dead-letter row, and DLQ/result outboxes.
DLQ broker acknowledgement or durable ingestion advances FAILED to DEAD_LETTERED.
The existing agent may later diagnose/requeue/escalate the same row.

Both runtimes can reap expired Java leases. Postgres locks and the shared Redis
lease prevent duplicate crash recovery. Java's reaper filters Java types; Python's
existing reaper can safely recover either language. Crash streaks are independent
of retry budgets and share the same poison threshold/configuration.

## Routing and identity

`tasks.pending` has six partitions. Keys are `<lane>:<task UUID>`, with high for
priority 7–9, normal for 3–6, and low for 0–2. The partition is:

```text
base(high=0, normal=2, low=4) + SHA256(UUID's 16 network-order bytes)[0] % 2
```

All other topic keys are the plain task UUID string. No language uses the client's
default key partitioner for `tasks.pending`.

Outcome identities use UUIDv5 with the standard URL namespace:

```text
task-scheduler:{kind}:{task_id}:{attempt_no}
```

For generation > 0 append `:generation:{generation}`. Kinds include result, retry,
dlq, and redispatch. Generation-zero IDs remain compatible with earlier phases.
The outbox row ID is also supplied as the UTF-8 `outbox-id` Kafka header. Publisher
acknowledgement before its DB marker commit can duplicate a message; database state
and stable identities make the consumers idempotent. External effects remain at
least once, not globally exactly once.

Retry #1 routes to `.5s`, #2 to `.30s`, and #3+ to `.5m`. Actual due time uses
exponential full jitter `uniform(0, min(cap, base * 2**(attempt_no-1)))` and the DB
clock. Names are buckets, not guaranteed minimum sleeps. Both languages persist
retry receipt before committing its Kafka offset and dispatch due inbox rows with
SKIP LOCKED, without sleeping until due on a Kafka partition. Their independent
retry groups may observe the same event safely. The shared dispatcher may release
either language's retry; execution ownership still follows the type prefix.

Both relays publish any unpublished outbox row, regardless of originating language,
under a transaction lock. They use identical pending partition rules and DLQ
acknowledgement fences. Java therefore also supports retries/outbox publication
when Python workers are absent; future scheduled tasks still use the scheduler.

## Result, retry, and DLQ examples

A Java result is directly validated by `shared.contracts.TaskResultMessage`:

```json
{
  "version": 1, "generation": 0,
  "task_id": "77342418-78a5-4bdf-8591-ab0467e5a9f4",
  "type": "java.data_transform", "status": "SUCCEEDED", "attempt_no": 1,
  "worker_id": "java-example", "result": {"values": [3, 7.5], "count": 2},
  "error": null, "finished_at": "2026-11-10T12:30:00.123456Z"
}
```

A retry contains version, generation, event_id, task_id, failed_attempt, due_at,
and delay_seconds. A DLQ envelope contains:

```json
{
  "version": 1, "kind": "task.dead_letter", "generation": 0,
  "id": "77342418-78a5-4bdf-8591-ab0467e5a9f4",
  "task_id": "b8a1195f-2065-4a49-936a-20d22275aad6",
  "type": "java.data_transform", "attempt_no": 1,
  "status": "DEAD_LETTERED", "reason": "fatal error", "category": "fatal",
  "worker_id": "java-example",
  "error_history": [{"generation": 0, "attempt_no": 1, "worker_id": "java-example",
    "error": "ValidationError: scale requires numeric values", "stack_trace": "retained trace",
    "recorded_at": "2026-11-10T12:30:00Z"}],
  "ai_diagnosis": null, "resolution": null, "created_at": "2026-11-10T12:30:00Z"
}
```

Example UUIDs above are illustrative; real event IDs use the algorithm above.
`failure_signature` is stored on the canonical dead-letter row and computed by
both languages with the same v1 normalization/hash. The DLQ ingestor may recompute
it. It is a clustering key, never an event/deduplication identity. Full raw inputs
remain in DLQ source receipts. The persisted incident status OPEN/RETRIED/etc. is
separate from the event's terminal task status.

## Contract tests and generation

```sh
source .venv/bin/activate
make contracts-check
make test-java
```

`make test-java` uses a pinned Maven + JDK 17 Docker container, so it does not depend
on the host's selected Java version. The Python-generated fixture reaches the real
Java listener with a mocked executor and verifies manual acknowledgement. Java
writes a reverse task which Python consumes through its actual raw-record parser
with an executor spy. The test preserves nested JSON, Unicode, integers beyond
IEEE-754's exact range, generation/AI flags, defaults, and microsecond timestamps.
JVM tests also compare Python-generated UUID/partition/fingerprint vectors and
exercise real PDF parsing, transformations, retry injection, and namespace filtering.

```sh
make up JAVA=1
make check-java
python -m pytest tests/integration/test_worker_java.py --run-integration --run-java -q
```

The live checker adds real Kafka/JPA verification in both directions: Python API
submissions execute in Java, and a Java CLI producer emits a task executed by Python.
It also validates transient retry, fatal DLQ JSON/history/fingerprint, actual PDF
bytes, and Actuator/Micrometer. For the Java-to-Python producer fixture, the script
commits a test-only task row before publishing; normal applications must use the
transactional API/outbox instead of reproducing that test setup.

Regenerate after an intentional protocol change:

```sh
python scripts/export_contracts.py
make test-java
```

The generator owns JSON Schemas, Python fixtures/vectors, and Java's shared Lua
resources. `make contracts-check` and the Python unit suite detect stale artifacts.
See [the Java worker runbook](worker-java.md) for configuration and service commands.

## Phase 10 correlation and execution timing

Every service outbox publication includes these Kafka headers:

- `task_id`: UTF-8 lowercase hyphenated task UUID.
- `trace_id`: ASCII lowercase nonzero 32-hex trace ID, stable across retries,
  scheduling, recovery, and manual requeue of the same task.
- `outbox-id`: the durable outbox UUID; unchanged across publication replay.

These are transport headers, separate from strict version-one JSON payloads.
Older messages without headers remain consumable; workers use the persisted task
trace when claiming execution. Both SQLAlchemy and JPA outbox inserts inherit
`tasks.trace_id` through a database trigger, including independently published
retry and DLQ records. API `TaskOut` responses include the trace UUID and submission
responses echo its 32-hex form in `X-Trace-ID`. The standalone Java producer accepts
`TASK_TRACE_ID`; direct Python `publish_json` accepts `trace_id` or logging context.

`tasks.results` adds optional `duration_seconds` (finite number >= 0 or null),
measured from the attempt's database start to its outcome. Both worker languages
emit it for executed attempts; absent historical values are valid. Regenerate
schemas and the shared Java telemetry SQL with `python scripts/export_contracts.py`.
See [observability](observability.md) for metric aggregation and correlation details.

Upgrade strict result-envelope consumers before enabling new worker producers;
older strict schemas reject unknown `duration_seconds` fields even within version 1.
