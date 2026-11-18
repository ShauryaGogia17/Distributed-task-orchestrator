# Architecture through Phase 10

The host runs Python 3.11 tools. Docker Compose runs FastAPI, scalable workers, two schedulers, a DLQ consumer, an AI orchestrator, one combined
Kafka KRaft broker/controller, Redis 7, and PostgreSQL 16 on a project-scoped bridge network.
There is no ZooKeeper. The optional Kafka UI uses the same network.

Kafka listens internally at `kafka:19092`, externally at the published host port
(9092 by default), and on an unpublished KRaft controller port 9093. Kafka metadata
advertises the appropriate address for each listener. Each store has a named volume
and a readiness health check. Redis uses AOF with one-second fsync; Kafka and
PostgreSQL retain data in their own volumes.

After Kafka becomes healthy, `kafka-init` creates the six topics from the shared
manifest, using replication factor 1. It exits nonzero on bootstrap failures. The
Makefile waits for initialization separately from long-running service health.
Kafka UI waits for successful initialization and is enabled with the debug profile.

The Python checker concurrently performs Kafka topic metadata checks, Redis PING,
and an authenticated PostgreSQL SELECT 1, each bounded by a configurable timeout.
It verifies topic counts and partition leaders and reports independent results.

Phase 2 adds PostgreSQL mappings and migration revision 0001. `tasks` holds the
current task state. `task_attempts` uses `(task_id, generation, attempt_no)` since Phase 7 to identify each
execution. `dead_letters` retains recovery history with optional JSON diagnoses.
`outbox` stores events to publish in the same database transaction as task changes.
An index covers unpublished
events, and a status/schedule/priority index supports future scheduler scans.

State changes use a validated graph. `transition_task` locks and refreshes the row
inside its caller's transaction, then validates the transition. ORM assignment also
validates changes; raw SQL status updates bypass these Python guards. The database
enforces status membership, uniqueness, numeric bounds, attempt identity, foreign
keys, and timestamp ordering. A database trigger maintains task `updated_at`.

Async sessions are independent per concurrent operation and commit or roll back as
a unit. Engines and network clients are created explicitly rather than on import.
Kafka helpers wait for producer acknowledgement and expose per-record manual offset
commits. Consumers process each partition in order, committing only after durable
handling. Redelivery remains possible; an outbox does not remove the need for
idempotent consumers.

Redis locks use expiring SET NX PX leases and random tokens, with atomic Lua
comparison before release. Expiry limits the lock's protection window; row locks
and database uniqueness provide durable correctness. Redis idempotency reservations
are a TTL-bound mapping to task UUIDs, backed by the database unique key.

Pydantic schemas define API and Kafka contracts, with aware timestamps and JSON-safe
payloads. ContextVar-based logging carries task/worker IDs independently across
asyncio tasks and emits one JSON object per log record.

Phase 3 adds HTTP submission, list/detail queries, cancellation, readiness, and a
Prometheus metrics stub. Submission uses PostgreSQL INSERT ON CONFLICT to arbitrate
idempotency keys across replicas, then inserts the event in the same transaction.
It returns only after commit and does not wait for Kafka. Detail responses include
attempt history. Cancellation row-locks the task and permits only PENDING/SCHEDULED.

Each API process owns an outbox publisher. Publishers select unpublished batches
FOR UPDATE SKIP LOCKED and hold their row locks through Kafka acknowledgement and
publication commit. Failure rolls back the marker so restarted processes can retry.
The acknowledgement/commit crash window permits duplicate Kafka delivery, with a
stable outbox-id header. Consumers must deduplicate and consult current task status,
including cancellation. Workers perform that check before claiming execution.

Dispatch keys encode a high/normal/low lane and task UUID. Stable hashing distributes
each lane across two of tasks.pending's six partitions. This gives lane isolation;
Kafka itself does not prefer high partitions. Global priority preference is separate
from scheduled release. Phase 6 holds future tasks
outside Kafka until due. The API caps serialized messages
below the broker's default size ceiling before committing them.

FastAPI lifespan owns database/client cleanup and publisher shutdown. Request-ID
middleware binds structured logging context and normalizes public errors. Compose
runs an explicit migration one-shot before API startup. See api.md for run commands
and the HTTP contract.

Phase 4 workers consume tasks.pending with manual commits. A semaphore and one active
record per partition bound admission and ensure commits cannot skip unfinished prior
records. Paused partitions continue membership polling; rebalance drains owned work
before partition transfer. SIGTERM stops admission, drains handlers/result publication,
commits completed offsets, and leaves deadline-overrun work uncommitted.

Each execution holds a renewable token-owned Redis visibility lease and a uniquely
numbered PostgreSQL attempt. Completion checks both attempt ownership and the current
lease, preventing an expired owner from overwriting a reclaimed attempt. Worker:{id}
heartbeat keys have separately renewed TTLs and back container readiness. PostgreSQL
remains authoritative when Redis's advisory success cache is missing or stale.

Success/failure state, attempt completion, and a result outbox event commit atomically.
The shared API/worker outbox relay publishes tasks.results, after which the worker
commits the input offset. Redelivery after task completion waits for the existing
result event instead of executing again. External side effects remain at least once.
Expired RUNNING leases allow bounded crash recovery. See worker.md for handlers,
settings, and checks, and retries.md for Phase 5 failure outcomes.

Phase 5 records transient failures as RETRYING with exponentially jittered due times.
The failure transaction creates the retry schedule and retry-topic outbox event.
Retry-topic consumers persist receipt and commit promptly; a separate due-row scan
using SKIP LOCKED creates pending events without holding a Kafka partition asleep.
Stable event identity and task attempt-generation checks protect replay boundaries.

Fatal, exhausted, and poison outcomes create FAILED state, a complete stored error
history in dead_letters, and DLQ/result events atomically. DLQ acknowledgement advances
the task to DEAD_LETTERED in the publication transaction. A reaper closes expired
RUNNING attempts and bounds consecutive crashes independently of the execution budget.
Revision 0002 adds the durable inbox. Kafka/client message limits accommodate full
bounded history; API task size limits remain unchanged.

Phase 6 adds delayed-task recovery and recurring definitions. Future API submissions
commit SCHEDULED plus a durable delayed_tasks ledger and index the task in Redis
scheduled_tasks by epoch. The elected leader reconciles the ledger, atomically pops
due items via token-fenced Lua, and commits SCHEDULED → PENDING plus a unique outbox
event. A crash after pop is recovered from the ledger; a crash after DB commit is
covered by the outbox. Legacy premature records are acknowledged without execution.

Cron definitions store a next-run cursor. Row locks and a unique
(schedule_id, scheduled_for) task identity prevent duplicate materialization across
instances. Missed runs coalesce to one latest occurrence or skip to the next future
occurrence. Both use the DB clock and an explicit lateness grace period. Revision
0003 creates these records and preserves legacy future-task dispatch intent.

Scheduler replicas elect with a 5s Redis lease renewed every second. Standbys poll
every 0.25s and remain healthy; per-instance metrics expose actual Redis ownership.
DB locks/uniqueness remain the safety net during leadership turnover. See scheduler.md
for duplicate-delivery limits, operational commands, and killed-leader verification.

Phase 7 adds a dedicated raw Kafka DLQ consumer. Canonical incidents and unique
source-offset receipts commit before manual offset acknowledgement. Existing worker
incidents are enriched, closed incidents remain closed on replay, and malformed or
orphan messages retain audit records instead of blocking ingestion. Fingerprints
normalize volatile values for later clustering while preserving original histories.

DLQ inspection and bounded bulk/manual actions use incident status separate from
current task state. Manual retry commits corrected payload, zero attempts, a new
execution generation, PENDING state, closed incident, and an outbox event together.
Attempts, retry inboxes, event IDs, leases, and completion caches include generation,
so retained old messages cannot rerun or regress a recovered task. Discard retains
an audit tombstone. API metrics count OPEN incidents by task type from PostgreSQL.
Revision 0004 preserves existing generation-zero execution identities and retains
DLQ audits after task deletion. See dlq.md for the HTTP contract and live checker.

Phase 8 adds the AI orchestrator as a consumer of the durable DLQ inbox. It assigns
received new incidents to unique task-type/signature/time-window clusters. A leased
representative collects bounded redacted evidence and uses a provider-neutral LLM
interface with four read-only scoped tools. A strict diagnosis schema permits one
repair within step, timeout, token, and cost limits. MockLLM is the offline default;
the Claude adapter uses bounded Messages API retries and explicit usage accounting.

Cluster diagnoses commit before actions and are reused by late members. Each action
row-locks current task/incident state, applies a confidence/safety/loop policy, and
commits recovery generation, future scheduler ledger or pending outbox, diagnosis,
and decision audit atomically. Unsafe, unknown, low-confidence, or guard-failed cases
escalate through a durable local webhook/Slack stub. Original private task data is
retained locally; model context, tool results, repair turns, and audit are redacted.

Repeated distinct task failures open a Redis task-type circuit. Workers park OPEN
tasks without consuming attempts or blocking Kafka partitions. After cooldown only
one renewable fenced probe runs; success closes the circuit and failure reopens it.
Redis-time hourly reservations bound model spending across agents. Postgres stores
claims, shared diagnosis/usage, decision history, and restorable circuit intent.
Revision 0005 opts historical incidents out of automatic processing and adds these
records. See agent.md for policies, kill switch, live verification, and crash limits.

Phase 9 adds an optional Java 17 Spring Boot worker with a separate classic Kafka
group. Each language acknowledges the other namespace without execution, while all
replicas share task/attempt/generation semantics, renewable leases, circuit probes,
and outbox/retry identities. Java uses JPA against the Alembic-managed schema and
provides its own bounded retry receipt/dispatch and publication loops. Its PDF and
data transformation handlers produce ordinary shared result/DLQ events; Actuator
and Micrometer expose operational state. Generated schemas/vectors/Lua and actual
bidirectional Kafka checks guard interoperability. No new migration is required.
See contract.md and worker-java.md for ownership, JSON schemas, and exact commands.


Phase 10 persists a canonical trace UUID on each task and propagates it through
outbox/incident insert triggers, Kafka headers, and scoped Python/Java JSON logs.
Attempt result envelopes capture execution duration. Every service exposes a
consistent read-only snapshot of retained task, outbox, incident, and agent audit
history, plus instance-local inflight/leadership gauges. Global metric replicas
are deduplicated with max-based recording rules before rates or sums. Prometheus
uses Docker DNS discovery for every service replica; Grafana receives a provisioned
datasource and dashboard. Alert rules cover net DLQ growth, stuck RUNNING tasks,
and missing scheduler leadership, including absent scrape targets. Revision 0006
adds/backfills correlation; see [observability](observability.md) for queries,
retention semantics, operational limits, and verification commands.
