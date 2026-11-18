# Phase 9 Java Spring Boot worker

`services/worker-java` is a Java 17, Spring Boot 3.5.16, Maven service using
spring-kafka, JPA/Hibernate, Redis, Apache PDFBox, Actuator, and Micrometer. It joins
`task-workers-java`, separate from Python's `task-workers`, and executes only
`java.*` types. Python acknowledges that namespace without trying its handler
registry. The shared [JSON contract](contract.md) documents all schema/routing and
transactional invariants.

## Start and verify

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up JAVA=1
make contracts-check
make test-java
make check-java
make test
make test-integration
make migration-check
```

Java is an optional Compose profile. `make up` alone starts the existing Python
stack; `JAVA=1` also builds/waits for the Java worker. Existing `.env` connection
settings are reused and passed internally as kafka:19092, postgres:5432, redis:6379.
No credentials are stored in Maven/application YAML. Alembic remains at revision
0005; Phase 9 requires no database DDL changes. Hibernate validates the existing
shared tables at startup and fails if migrations have not run.

```sh
curl -fsS http://localhost:8007/actuator/health
curl -fsS http://localhost:8007/actuator/prometheus
```

Health includes datasource/Redis, worker heartbeat/listener liveness, and Kafka's
six priority partitions/leaders. Metrics include JVM, Spring Kafka, outbox sends,
and `java_worker_attempts_total` by task type/outcome. Ports bind locally; set
JAVA_WORKER_PORT if 8007 is occupied. Docker health checks target the internal port.

To preserve three Python workers and two schedulers:

```sh
docker compose --profile java up -d --wait --wait-timeout 240 --scale worker=3 --scale scheduler=2 api worker scheduler dlq agent worker-java
python scripts/check_worker.py --expected-workers 3
make check-java
```

The Python checker counts Python heartbeats separately from Java. Java's heartbeat
has a `java-` worker ID and language marker. Custom JAVA_WORKER_ID values also receive
that prefix; leave the value blank for a unique runtime identity.

## Handlers

Real PDF rendering (Helvetica/WinAnsi text, at most 48 lines per page, 100 characters
per line, 20,000 characters overall, and 1–10 pages):

```sh
curl -fsS -X POST http://localhost:8002/tasks -H 'Content-Type: application/json' \
  -d '{"type":"java.pdf_render","payload":{"text":"Hello from Java","pages":2}}'
```

Result contains `content_type=application/pdf`, `pdf_base64`, `size_bytes`, SHA-256,
and page count. The bytes are a real parseable PDF. Other font repertoires require
an explicitly embedded font; unsupported characters fail validation rather than
silently changing the document. PDFs are returned in bounded result JSON, not
written to a shared disk or external object store.

```sh
curl -fsS -X POST http://localhost:8002/tasks -H 'Content-Type: application/json' \
  -d '{"type":"java.data_transform","payload":{"operation":"scale","values":[1,2.5],"factor":3}}'
curl -fsS -X POST http://localhost:8002/tasks -H 'Content-Type: application/json' \
  -d '{"type":"java.data_transform","payload":{"operation":"uppercase","values":["hello","résumé"]}}'
```

`scale` uses BigDecimal arithmetic; `uppercase` uses Locale.ROOT. Results contain
values and count; arrays are bounded to 10,000 entries. Missing/invalid fields and
unknown `java.*` handler names fail fatally. Optional `transient_failures=N` (0–100)
raises a deterministic retryable error for the first N attempts; `sleep_ms` (0–60000)
allows deadline testing. These test controls do not replace either handler's logic.
For a deterministic successful retry:

```sh
curl -fsS -X POST http://localhost:8002/tasks -H 'Content-Type: application/json' \
  -d '{"type":"java.data_transform","max_retries":2,"payload":{"values":["retry"],"transient_failures":1}}'
```

## Execution and operations

Listener concurrency defaults to 2, with one poll record per consumer and a fixed
handler pool. Handlers run outside DB transactions. RUNNING/attempt claim and outcome
writes are separate short row-locked transactions fenced by the shared Redis token.
A renewable visibility lease stays active through outcome publication. Timeouts
cancel the handler; a lost lease prevents finalization and leaves crash recovery
possible. DB success is authoritative when advisory Redis completion hints are lost.

Success/failure and result/retry/DLQ events use the existing outbox. Both Java and
Python relays can publish those rows safely; Kafka duplicates remain possible at
the acknowledgement/DB-commit boundary. Manual commits do not skip unfinished work.
Spring's error handler retries indefinitely instead of automatically discarding
unhandled/malformed owned input. Fatal *handler* input errors still take the normal
transactional DLQ path. Redis leases and DB rows protect business execution when an
input is redelivered or two Java workers race.

Retry topics, jitter, total attempt budgets, error-history fields, poison limits,
UUIDv5 identities, due-row inbox dispatch, and failure signatures match Python.
The Java service includes its own retry listener/dispatcher and relay, so Java retry
execution does not depend on a Python worker staying alive. The scheduler is still
required for explicitly delayed tasks and circuit-parked work. Circuits use the
Phase 8 shared scripts; the agent diagnoses Java dead letters through the same
collector and policy. AI-modified flags and recovery generations are preserved.

SIGTERM stops Kafka admission and drains current listeners before owned executors
and heartbeat shutdown. Compose grants 120 seconds. Defaults keep handler + publish
deadlines below the Kafka max poll interval. Increase JAVA_WORKER_MAX_POLL_INTERVAL_MS
if raising handler deadlines. Task side effects remain at least once.

Environment controls include JAVA_WORKER_GROUP_ID, JAVA_WORKER_CONCURRENCY,
JAVA_WORKER_TASK_TIMEOUT_SECONDS, JAVA_WORKER_VISIBILITY_TIMEOUT_SECONDS,
JAVA_WORKER_PUBLISH_TIMEOUT_SECONDS, JAVA_WORKER_MAX_POLL_INTERVAL_MS, and JVM options.
Retry/poison/circuit settings reuse the common WORKER_RETRY_*, WORKER_MAX_CRASH_CLAIMS,
and CIRCUIT_* settings. The pending group ID must differ from Python's configured
one. Do not put different language filters in members of one group.

## Build and tests

`make test-java` mounts the module into `maven:3.9.11-eclipse-temurin-17`, retains
only a named Maven dependency cache, and runs JUnit + both-direction Python contract
verification. It requires Docker and installed Python dependencies, but no local
JDK and no running Postgres/Kafka for its unit/fixture tests. The runtime image also
uses Java 17; its Ubuntu base supports the workspace's ARM64 host. It runs as a
non-root user and installs fonts for PDF rendering.

With a locally selected JDK 17 and Maven:

```sh
mvn -f services/worker-java/pom.xml test
mvn -f services/worker-java/pom.xml package
```

The Python contract runner additionally checks the Java-generated fixture after
Maven. `make check-java` requires the running Java profile and Python workers; it
creates only identified example tasks, validates both Kafka directions and shared
JPA state, exercises retry/fatal outcomes, checks PDF bytes, and checks health/metrics.

The two Java-dependent pytest cases are opt-in so earlier Python-only workflows
remain usable:

```sh
python -m pytest tests/integration/test_worker_java.py --run-integration --run-java -q
```

Source formatting uses the Java 17-compatible Spotify fmt Maven plugin, for example:
`mvn -f services/worker-java/pom.xml com.spotify.fmt:fmt-maven-plugin:2.23:format`.

Implementation references: [Spring Boot 3.5 Java/Maven requirements](https://docs.spring.io/spring-boot/3.5/system-requirements.html),
[manual acknowledgements/error handling](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html),
and [PDFBox 3 font API](https://pdfbox.apache.org/3.0/migration.html).

## Phase 9 files

Created:

- `services/worker-java/pom.xml`, `Dockerfile`, `src/main/resources/application.yml`
- `src/main/java/io/taskscheduler/worker/`: application/configuration, wire codec,
  JPA rows/store, Redis state, handlers, runtime, listeners, outbox relay, fingerprint,
  and standalone producer/contract probe
- Java JUnit tests, Python-generated fixtures/vectors, generated shared Lua resources
- `shared/contracts.py`, `shared/contracts/*.schema.json`
- `scripts/export_contracts.py`, `test_java_contract.py`, `check_java_worker.py`
- `tests/unit/test_java_protocol.py`, `tests/integration/test_worker_java.py`
- `docs/contract.md`, `docs/worker-java.md`

Changed:

- `services/worker/consumer.py`, `executor.py`: reserve the Java execution namespace
- `shared/models.py`, `config.py`: explicit shared counter bounds and Java configuration
- `docker-compose.yml`, `.env.example`, `Makefile`, `pyproject.toml`
- `scripts/check_worker.py`, `tests/conftest.py`, `tests/integration/test_worker.py`
- `.gitignore`, `.dockerignore`, `README.md`, `docs/architecture.md`

## Recorded validation

- **277 Python regression tests passed**, with the two Java-dependent cases skipped by default.
- Both opt-in Java contract/live pytest cases passed separately.
- **7 JUnit tests passed** on the pinned Java 17 Maven toolchain.
- The live checker verified Python-to-Java PDF/transformation, Java-to-Python Kafka
  production/execution, transient retry, fatal DLQ/history/fingerprint, and Actuator/Micrometer.
- Three Python workers, one Java worker, and two schedulers were healthy together.
- Ruff/format, generated-contract checks, Compose validation, and Alembic schema-drift checks passed.
