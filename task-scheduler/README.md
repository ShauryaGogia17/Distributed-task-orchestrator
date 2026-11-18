# Distributed Task Scheduler with AI Agent Orchestrator

Phases 1–11 provide local infrastructure, shared utilities/migrations, FastAPI,
scalable workers, retries, delayed/cron scheduling, an auditable DLQ pipeline,
guarded AI diagnosis/recovery, an optional Java worker, and shared metrics/log correlation with Prometheus/Grafana. AI diagnosis defaults to offline MockLLM;
Claude is explicitly configurable. See the [API](docs/api.md), [worker](docs/worker.md),
[retries](docs/retries.md), [scheduler](docs/scheduler.md), [DLQ](docs/dlq.md),
[AI agent](docs/agent.md), and [Java worker](docs/worker-java.md) runbooks.
See the [observability runbook](docs/observability.md) for metric semantics, dashboard, alerts, and Phase 10 files/tests.
The [shared JSON contract](docs/contract.md) documents Python/Java interoperability.

See the [testing and chaos runbook](docs/testing.md) for coverage reports, isolated Compose suites,
fault evidence, and CI.

Requirements: Python 3.11+, Docker with a running daemon, Docker Compose v2.20+
(including `up --wait` and `wait`), and Make. Allow approximately 4 GiB of Docker memory for development; use 6 GiB or more when
running the full stack alongside Java, builds, and test deployments.

## Start

Run from this directory:

```sh
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
cp .env.example .env
python -c 'from pathlib import Path; import secrets; p = Path(".env"); p.write_text(p.read_text().replace("POSTGRES_PASSWORD=\n", "POSTGRES_PASSWORD=" + secrets.token_hex(24) + "\n"))'
make up
make migrate
python scripts/check_infra.py
make topics
curl -fsS http://localhost:8002/health
make check-worker
make check-retry
make check-scheduler
make check-dlq
make check-agent
make check-observability
make test-alerts
```

The password-generation command is for the newly copied example. Do not replace an
existing `.env`. Alternatively, set your own nonempty `POSTGRES_PASSWORD` there.
No secret credentials are committed. The checker loads this project's `.env`
regardless of the invoking working directory; process environment overrides it.

Expected output contains `[OK] Kafka`, `[OK] Redis`, and `[OK] Postgres`.
Kafka verification includes all six topics, exact partition counts, and active
partition leaders. Failures print `[FAIL]` and exit with status 1. Connection errors
show their exception class; inspect `make logs` for details without printing secrets.

`make up` waits for the three infrastructure services, initializes Kafka topics,
builds API/worker images, applies migrations through a one-shot service, and waits
for their health checks. Repeating it preserves data. Run `python scripts/init_observability.py` before a direct Compose invocation. Direct `docker compose up -d` launches these
services too, but does not wait for API readiness.

## Commands

- `make up`: start infrastructure, migrate, and build/start API, workers, two schedulers, DLQ ingestion, the AI agent, Prometheus (9090), and Grafana (3001).
- `make up JAVA=1`: also build/start the Java 17 worker at port 8007.
- `make test-java`: run JUnit and bidirectional Python/Java contract checks in a JDK 17 container.
- `make check-java`: verify live Kafka/JPA execution, retry, DLQ, real PDF, and Java-produced Python work.
- `make contracts-check`: detect stale generated JSON schemas, fixtures, vectors, and Lua.
- `make up UI=1`: also start Kafka UI at <http://localhost:8080> (`debug` profile).
- `make check-observability`: verify all scrape targets, Grafana provisioning, alert rules, and live Kafka trace correlation.
- `make test-alerts`: run replica-aware alert scenarios using promtool.
- `make down`: stop containers and keep named volumes.
- `make logs`: follow the last 100 lines of service logs.
- `make reset`: **delete all project volumes and data**, then start fresh.
- `make topics`: describe topics, partitions, replicas, leaders, and ISR.
- `make psql`: open psql using configured database and user.
- `make redis-cli`: open the Redis CLI.
- `make check`: run the Python checker.
- `make check-worker`: verify live API → worker → result event → committed input offset.
- `make check-retry`: verify fatal and retry-exhausted tasks reach the actual Kafka DLQ.
- `make check-scheduler`: verify two schedulers, delayed delivery, cron execution, and leader metrics.
- `make check-dlq`: verify live DLQ ingestion, corrected retry, discard, replay safety, and metrics.
- `make check-agent`: verify live offline clustering, AI payload correction, delayed recovery, and escalation.
- `make test`: run isolated unit tests.
- `make test-unit-coverage`: generate HTML/XML/JSON reports and enforce >80% independently on shared/Python services.
- `make test-compose`: run integration tests in a fresh, automatically removed Compose project.
- `make chaos`: run all real container fault scenarios with PASS/FAIL evidence.
- `make test-all-compose`: run integration and chaos in one isolated deployment.
- `make lint` / `make typecheck`: run Ruff and mypy.
- `make test-integration`: run infrastructure/shared-library/API tests after `make up`.
- `make migrate`: apply database migrations through the current head.
- `make migration-check`: check ORM mappings against the migrated database.

For Kafka UI alone: `docker compose --profile debug up -d --wait kafka-ui`.
Use `make up UI=1` again after `make down` to enable it. The base services have no UI
dependency, so UI image/network failures do not prevent the base stack starting.

## Connections and defaults

Host Python clients use `localhost:9092` (Kafka), `localhost:6379` (Redis), and
`localhost:5432` (Postgres), subject to local `.env` overrides. Containers on the shared network use
`KAFKA_BOOTSTRAP_SERVERS=kafka:19092`, `REDIS_HOST=redis`, and
`POSTGRES_HOST=postgres`, with Redis port 6379 and PostgreSQL port 5432.
The API's Compose entry already sets those internal addresses. HTTP defaults to
<http://localhost:8002>, with [interactive docs](http://localhost:8002/docs).

All published ports bind to `127.0.0.1`. Kafka and Redis use plaintext local
connections. This is a single-broker development setup with replication factor 1,
not a highly available deployment. Images use explicit version tags.

`shared/kafka-topics.txt` is the common bootstrap/checker topic manifest:

- `tasks.pending`: 6 partitions.
- `tasks.retry.5s`, `tasks.retry.30s`, `tasks.retry.5m`: 3 partitions each.
- `tasks.dlq`, `tasks.results`: 3 partitions each.

Kafka auto topic creation is disabled. Retry topic names express the intended
delays; Phase 1 only creates topics and does not implement delayed delivery.
Initializer reruns do not change existing topic partition counts; the checker
reports mismatches. Increasing partitions requires a deliberate Kafka admin action.

Kafka data, Redis AOF data, and PostgreSQL data use independent named volumes.
Keep `KAFKA_CLUSTER_ID` stable while retaining the Kafka volume. Changing Postgres
credentials in `.env` does not change credentials already stored in its volume;
change them through SQL or use `make reset` if losing development data is acceptable.

For occupied host ports, edit `KAFKA_PORT` and the port in
`KAFKA_BOOTSTRAP_SERVERS` together; edit `REDIS_PORT`, `POSTGRES_PORT`, or
`KAFKA_UI_PORT` as needed. Internal service ports stay fixed. `LLM_PROVIDER=mock`
requires no key; Phase 8 provides the Claude adapter behind the same interface as MockLLM.

## Phase 2 shared library

Install the updated project dependencies before running migrations:

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up
make migrate
python -m alembic current
make migration-check
make test
make test-integration
```

`shared/models.py` contains `TaskCreate`, `TaskOut`, `TaskMessage`, `TaskStatus`,
`ErrorRecord`, and `DeadLetterOut`. Inputs reject unknown fields, naive timestamps,
non-finite JSON numbers, negative counters, and invalid priorities. Priorities are
0–9, higher first. Since Phase 5, `max_retries` is the total attempt budget (0–100,
minimum one initial attempt), following the requested attempts >= max_retries cutoff.
Message attempt numbers start at 1. `TaskOut` and `DeadLetterOut` support
`model_validate(orm_instance)`. Kafka dispatch envelopes have schema version 1.

The allowed state changes are explicit in `validate_transition`; illegal changes,
including self-transitions and exits from terminal states, raise `InvalidTransition`.
Execution follows PENDING → SCHEDULED → RUNNING, then SUCCEEDED, RETRYING, or FAILED.
Phase 6 delayed tasks use SCHEDULED → PENDING when due, then enter normal execution.
RETRYING can return to SCHEDULED or become FAILED. FAILED → DEAD_LETTERED →
AI_DIAGNOSED → REQUEUED, ESCALATED, or CANCELLED. Phase 7 manual recovery also permits
DEAD_LETTERED → REQUEUED without waiting for AI diagnosis. REQUEUED → PENDING starts another
cycle while retaining attempt history. Cancellation is also supported from PENDING,
SCHEDULED, RETRYING, DEAD_LETTERED, and REQUEUED. RUNNING cannot be cancelled without
the cooperative cancellation behavior that a later worker phase would need.
SUCCEEDED, ESCALATED, and CANCELLED are terminal.

`shared/tables.py` maps `Task`, `TaskAttempt`, `DeadLetter`, and `Outbox`. Attempts
have a composite primary key `(task_id, generation, attempt_no)` since Phase 7. Each dead-letter event has
its own UUID, allowing multiple events across recovery cycles. Outbox and dead-letter
rows also include `created_at` for stable ordering and audit. A nullable task
idempotency key is unique when provided. JSONB fields store payloads, results,
histories, and diagnoses. Task updates advance `updated_at` through a database
trigger, including raw SQL writes. Task deletion cascades to attempts; dead-letter audit and outbox events have independent lifecycles.

Use `create_engine()` and `create_session_factory(engine)` when owning the lifecycle
explicitly, or `get_session_factory()` with `close_database()` at service shutdown.
`session_scope()` commits on success and rolls back/closes on exception. Create one
session per concurrent operation. `transition_task(session, task_id, target,
expected_status=...)` locks and refreshes the row before changing status, preventing
two schedulers from winning the same transition. Call it before modifying that
task's other fields; refresh replaces unflushed state. The caller's transaction can
then insert its outbox event atomically. ORM status assignment is also validated;
raw SQL/bulk status changes bypass Python lifecycle guards and should not be used
by services. PostgreSQL enforces allowed status values and numeric constraints.

`shared/kafka.py` provides owned producer/consumer async contexts. `publish_json`
waits for broker acknowledgement with an idempotent producer. Consumers disable
automatic commits. Call `commit_record(consumer, record)` only after durable handling;
it commits that record's partition at offset + 1. Process each partition in order:
committing a later record also acknowledges earlier offsets. Rebalance commit
failures propagate. `shared/outbox.py` now provides the relay shared by API and worker
replicas; `shared/priority.py` owns their common routing contract. Producer idempotence
does not make the database/outbox/Kafka
combination exactly-once; later consumers must tolerate redelivery. Malformed JSON
raises during deserialization; dead-letter handling belongs to the worker phase.

`shared/redis.py` provides `create_redis_client()`, `RedisLock`, and `IdempotencyStore`.
Call `aclose()` on owned clients. A lock attempts acquisition once with SET NX PX;
`acquire()` returns False on contention, while its async context raises
`LockNotAcquired`. Release compares the random owner token in Lua before deleting.
The lease expires after `ttl_ms` and is not automatically renewed. Keep critical
sections shorter than the lease and use database constraints/row locks for durable
correctness. Idempotency reservations use SET NX EX, hashed namespaced keys, and
owner-checked release. The database UNIQUE constraint remains authoritative after
Redis expiry or data loss.

`shared/logging.py` provides `configure_logging()` and `log_context(task_id=...,
worker_id=..., request_id=...)`. Each log line is JSON with an aware timestamp, level,
logger, message, task/worker/request IDs, and an optional escaped exception traceback. ContextVars
isolate concurrent asyncio tasks and restore nested scopes. Configure logging at
service startup; imports do not alter root logging or open network connections.

Alembic reads PostgreSQL settings directly from `.env`; credentials are not stored
in `alembic.ini`. Revision `0001` creates the original four tables, constraints,
indexes, and timestamp trigger. Revision `0002` adds the delayed retry inbox; `0003`
adds cron definitions, delayed-task recovery, and unique task occurrence identity. Revision `0004`
adds DLQ source receipts, incident metadata, and execution generations for safe manual retries.
Revision `0005` adds durable AI clusters, redacted decision audit, and AI recovery metadata.
`python -m alembic upgrade head --sql` renders offline DDL.
Migration reversal is exercised in isolated test schemas; `alembic downgrade base`
against the application database would delete its tables and data.

## Tests and validation

```sh
make test
make test-integration
make migration-check
make check-worker
python -m ruff check .
docker compose config --quiet
```

Unit tests cover the Phase 1 infrastructure checks plus schema boundaries, legal
and illegal lifecycle paths, lock ownership/contention/release errors, idempotency
reservations, Kafka serialization/offset commits, and async logging context isolation.
Integration tests use real PostgreSQL, Redis, and Kafka. They verify migration
upgrade/downgrade/upgrade, task/outbox atomicity, database constraints and cascades,
concurrent transitions, generated timestamps, stale Redis lock ownership, and Kafka
JSON delivery/manual commits, and the complete Phase 3 API/outbox contract described
in the [API runbook](docs/api.md), plus worker execution, leases, timeout handling,
redelivery checks, and graceful shutdown described in the [worker runbook](docs/worker.md).
Phase 5 adds backoff/classification, eventual retry success, fatal/exhausted/poison
DLQ delivery, inbox idempotency/concurrency, and large complete history coverage.
Phase 6 adds due ordering, two-scheduler safety, renewal, killed-leader failover,
cron missed-run policies, migration recovery, and leader metrics coverage.
Phase 7 adds DLQ idempotency, fingerprint stability, inspection/filter APIs, bulk/discard safety,
and corrected manual retry with immutable history across execution generations.
Phase 8 adds offline agent category/repair/budget/redaction tests, concurrent durable clusters,
AI-corrected execution, bounded recovery, and renewable single-probe circuit coverage.
Database tests use temporary isolated schemas and
Redis tests use unique keys. The Kafka test leaves one identified test event on
`tasks.results` and API tests leave identified dispatch events on `tasks.pending`,
with isolated test consumer groups per run. Integration tests are
skipped unless `--run-integration` is provided.

Implementation references: [Apache Kafka Docker guidance](https://hub.docker.com/r/apache/kafka),
[Compose dependency readiness](https://docs.docker.com/compose/how-tos/startup-order/),
and [Pydantic settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).
Shared-library references: [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html),
[Alembic async recipe](https://alembic.sqlalchemy.org/en/latest/cookbook.html#using-asyncio-with-alembic),
[aiokafka manual commits](https://aiokafka.readthedocs.io/en/stable/examples/manual_commit.html),
and [Redis token-owned locks](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

Phase 10 adds durable counter/histogram snapshots, per-instance inflight/leader gauges,
Kafka/JSON log traces, a provisioned [Grafana dashboard](http://localhost:3001/d/task-scheduler),
and tested Prometheus alerts. Global metrics repeat across services; use the supplied
`scheduler:*` recording rules to deduplicate replicas before rates or sums. See
[observability](docs/observability.md) for retained-history counter semantics and limits.

Phase 11 adds an enforced unit coverage report, isolated Compose integration profile,
six chaos cases spanning the five requested failure categories, and GitHub Actions
lint/type/test/image-build jobs. See [testing](docs/testing.md) for exact commands,
file inventory, cleanup, and evidence. The crash check proves one durable logical
completion across retries/replay; external effects still need handler idempotency.
