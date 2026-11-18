# Phase 11: testing and chaos engineering

Run commands from the repository root with Python 3.11 and the virtual environment
active. Install/update test dependencies first:

```sh
python -m pip install -e '.[dev]'
make lint
make typecheck
make test-unit-coverage
make test-compose
make chaos
```

`make test-all-compose` runs integration and chaos sequentially in one fresh test
project. Ordinary `pytest` skips integration, Java, and chaos tests unless their
explicit switches are supplied. Prefer the wrapper for chaos instead of supplying
`--run-chaos` yourself: the wrapper provisions the required isolated environment.

## Unit coverage

`make test-unit-coverage` runs only `tests/unit/`. No real database, Redis, Kafka,
Docker daemon, or model provider is needed. Application settings still need a
nonempty `POSTGRES_PASSWORD`; the existing development `.env` supplies it locally,
and CI generates an unused random value. HTTP, transaction/session, broker, Redis,
and LLM boundaries are replaced with explicit scripted outcomes.

Coverage includes **all Python modules under `shared/` and `services/`**, including
entry points and persistence/service code. Core modules are not omitted to improve
the percentage. The gate is **at least 81% line coverage independently for each
package**, which satisfies the requested >80% target. The combined gate is also
81%. Java source is not measured by Python coverage; its JUnit/contract job remains
separate, without a Java percentage claim.

Reports are generated under:

- `artifacts/coverage/html/index.html`: navigable source/line report.
- `artifacts/coverage/coverage.xml`: Cobertura report for CI consumers.
- `artifacts/coverage/coverage.json`: complete machine-readable line data.
- `artifacts/coverage/package-summary.json`: independent package totals and gates.
- `artifacts/junit/unit.xml`: unit test results.

The new unit suites exercise API idempotency/rollback and state fences, scoped DLQ
retries and tombstones, scheduler missed-run/release policies, worker claim/finish
and replay behavior, immutable retry histories, agent cluster ownership and action
guards, collector scoping, lifecycle cleanup, dependency failures, metrics snapshots,
and the fault-controller/coverage gates. Scripted sessions test service decisions;
they do not claim to prove PostgreSQL locking or SQL semantics. The integration
suite exercises those boundaries against real infrastructure.

## Isolated Compose integration environment

`docker-compose.test.yml` is independent of the development Compose file. It uses
Kafka KRaft, Redis 7 with AOF, PostgreSQL 16, topic initialization, migrations, and a
Python test-runner image. The `test` profile enables the runner and one-shot setup;
the `chaos` profile additionally enables the API, two workers, two schedulers, and
DLQ ingestion. All application containers reuse the single test image.

```sh
python scripts/test_stack.py integration
python scripts/test_stack.py chaos
python scripts/test_stack.py all
python scripts/test_stack.py chaos --case worker-crash
```

Each invocation generates a unique `task-scheduler-test-*` project, random database
credential and KRaft cluster ID, dedicated network and named volumes, and no host
port bindings. It never reuses development data or groups. The script creates all
six topics and migrates the test database before starting a suite. Integration
fixtures retain their per-test schemas and Redis namespaces inside this deployment.

Containers carry both their Compose project labels and
`io.task-scheduler.test-stack=true`. Fault commands inspect and recheck those labels
before operating on an allowed service. A matching name prefix alone is insufficient.
The runner needs access to the Docker socket to inject real faults; its Docker CLI
is bundled in `tests/Dockerfile`. This facility is for the ephemeral test deployment.

On success or failure, the wrapper collects logs and removes **only its generated
project and volumes**. Fault contexts also restore killed containers' restart
policies and start them on exceptional exit. CI has an additional unconditional
cleanup step. Runtime settings files are mode 0600 and excluded from Git/artifact
uploads. Each invocation uses its own settings path, so a second invocation cannot
redirect the first invocation's Compose cleanup. Reports at the common artifact
paths describe the latest run; run suites serially in one checkout for unambiguous
reports.

Use `--no-build` after building the current `task-scheduler-test-runner:local` image.
Use `--keep` to retain the test deployment for debugging; the script prints the
project and settings path. For example, using that printed file:

```sh
docker compose --env-file artifacts/PROJECT.env -f docker-compose.test.yml --profile test --profile chaos ps
docker compose --env-file artifacts/PROJECT.env -f docker-compose.test.yml --profile test --profile chaos down --volumes --remove-orphans
```

Do not upload the generated environment files. Test evidence/logs and JUnit reports
are separate. The wrapper does not pause the development stack automatically.
On a small Docker VM, stop it before running the second stack and restore it after:

```sh
docker compose --profile java --profile debug stop
make test-all-compose
docker compose --profile java --profile debug start
```

Allow CPU availability for short-lease timing tests. A VM with at least 6 GiB RAM
is practical for running development services, Java/Grafana, and builds/tests
together; the isolated Python test stack can run separately on a smaller VM.
Builds, JVM tests, and short-lease suites should run sequentially on a two-CPU VM.

## Fault scenarios and evidence

Every case prints a literal `PASS [scenario]` or `FAIL [scenario]` with evidence.
The same record is appended to `artifacts/chaos/evidence.jsonl`. Failures propagate
as nonzero CLI/pytest exits. A named timeout identifies the missing invariant.
`artifacts/junit/chaos.xml` records all six cases; the two LLM modes cover one of the
five requested scenario categories.

1. **Worker crash:** submit a slow task, identify the worker from its committed
   RUNNING attempt, disable automatic restart, and SIGKILL it. A surviving worker
   reaps/reclaims the expired lease and completes the retry. Repeat the HTTP
   idempotency key and explicitly republish a Kafka dispatch. Require the replay
   offset to commit, the task ID to remain identical, the attempt count to remain
   unchanged after replay, and exactly one durable SUCCEEDED result outbox row.
   Evidence includes victim, attempts, task ID, replay offset, and success count.
2. **Kafka outage:** stop the broker for at least 30 seconds while submitting twelve
   tasks through the API. Verify twelve task rows and twelve unpublished outboxes
   during the outage. Restart Kafka, require all twelve tasks to succeed, and
   require zero unpublished outboxes for them. Evidence includes elapsed outage,
   task IDs, durable counts, and recovery counts.
3. **Scheduler leader loss:** identify the Redis-elected leader, SIGKILL it, require
   a different leader within fifteen seconds, then restart the old replica. Seed
   a recurring definition's next-run cursor two seconds ahead so the test need not
   wait for a minute boundary. With both replicas running, require one occurrence,
   one task identity, and one pending outbox. A yearly cron expression keeps a
   second legitimate occurrence outside the observation window. Evidence includes
   old/new leaders, failover time, cursor, task ID, and event counts.
4. **LLM timeout and invalid output:** inject `FaultLLM` through the real client
   interface into `AgentCore`, using real DLQ/task/audit rows and Redis budget
   reservations. One mode exceeds the diagnosis deadline; the other returns invalid
   JSON twice. Require ESCALATED state, an audited ESCALATE action, no recovery
   generation, and exactly one timeout call or two invalid-output calls (one repair).
   No external Claude request is made. The test stack has no autonomous agent process
   competing with this deterministic injection.
5. **Redis restart:** restart the actual Redis container, verify its start timestamp
   changes, require Redis and both worker heartbeats to recover, submit four new
   tasks, and require all four to succeed. Evidence includes timestamps, worker
   identities, and completed task IDs.

“Exactly once” here means **one durable logical completion despite at-least-once
attempts and Kafka replay**. The crash test intentionally expects multiple attempts.
It does not promise exactly-once delivery of arbitrary external handler effects.
Handlers that charge cards, send real email, or write external systems must use a
stable task/generation idempotency key or an equivalent transactional effect record.

For direct use inside an already-provisioned runner, `scripts/chaos.py` accepts
`worker-crash`, `kafka-outage`, `scheduler-failover`, `agent-timeout`, `agent-invalid`,
`redis-restart`, or `all`. Its mandatory `CHAOS_PROJECT` setting must match the owned
container labels; the wrapper supplies it. Normal development project names are
rejected before any Docker mutation.

## CI

`.github/workflows/ci.yml` runs on pushes, pull requests, and manual dispatch:

- Ruff lint/format, mypy over all Python application modules, contract freshness,
  unit tests, independent coverage gates, and promtool alert regression scenarios.
- Real Compose integration tests and JUnit/log artifacts.
- Real Compose chaos scenarios with JSONL/JUnit/log artifacts.
- Docker builds for API, worker, scheduler, DLQ, agent, and Java worker in separate
  matrix jobs so build resource use does not disturb timing tests.
- Java 17 JUnit and bidirectional Python/Java contract checks.

Jobs have bounded timeouts, read-only repository permissions, cancellation grouping,
random test credentials, and cleanup that runs after failures. Test environment
files are never uploaded. Local equivalents were executed; hosted GitHub jobs start
when this repository/workflow is pushed to GitHub.

## Files created or changed

Created `docker-compose.test.yml`, `tests/Dockerfile`, `scripts/test_stack.py`,
`scripts/chaos.py`, `scripts/check_coverage.py`, `tests/chaos/test_fault_scenarios.py`,
`.github/workflows/ci.yml`, and this runbook. Added unit suites:
`test_api_units.py`, `test_runtime_units.py`, `test_worker_core_units.py`,
`test_agent_core_units.py`, `test_scheduler_dlq_units.py`, `test_test_tooling.py`,
and the shared `tests/unit/fakes.py` boundary fixtures.

Updated `pyproject.toml`, `Makefile`, ignore rules, `tests/conftest.py`, and README.
Mypy-driven application changes add a checked database-clock helper and typed async
Lua boundary, narrow nullable database results, preserve HTTP priority aliases with
accurate stored types, and type response/metrics/lifecycle boundaries. These touch
`shared/{db,redis,tracing,metrics,outbox,circuit}.py`, API schemas/errors/main/DLQ,
worker executor/retry policy/retry engine/heartbeat/service, scheduler core/leader,
and agent collector/core/audit/diagnosis/guardrails. Migration 0006 only receives
SQL line wrapping; no new migration or schema change is introduced in Phase 11.

## Recorded results

The local run produced 97 passing real-container integration tests (two optional
Java wrappers skipped), all six passing chaos cases, and nine passing Java JUnit
checks plus both contract directions. Unit coverage was 97.25% for `shared/`,
92.18% for Python `services/`, and 93.64% combined. The worker crash used two attempts
and one durable success; Kafka was stopped for 30.031 s with twelve of twelve tasks
recovered; scheduler failover took 3.330 s with one materialized run; both LLM modes
escalated with bounded calls; Redis recovery completed four of four new tasks.
Consult the generated JUnit and JSONL artifacts for task IDs and the current unit
case count. All six production Docker images were built successfully.
