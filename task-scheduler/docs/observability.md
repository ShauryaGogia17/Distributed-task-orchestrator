# Phase 10: observability

Run from the repository root with the Python 3.11 virtual environment active:

```sh
make up JAVA=1
make check-observability
make test-alerts
python -m pytest tests/unit tests/integration --run-integration -q
make test-java
make migration-check
make contracts-check
```

Omit `JAVA=1` for a Python-only deployment. Prometheus discovers Java when its profile
is running; no Java target exists when that DNS name has no address. Docker DNS A
record discovery scrapes **every replica**, including all workers and both schedulers.
To exercise the topology used for verification:

```sh
docker compose --profile java up -d --wait --scale worker=3 --scale scheduler=2
```

The dashboard is [Distributed Task Scheduler](http://localhost:3001/d/task-scheduler),
Prometheus is [localhost:9090](http://localhost:9090), and alert states are available
at [Prometheus alerts](http://localhost:9090/alerts). Host port overrides are
`OBSERVABILITY_GRAFANA_PORT` and `OBSERVABILITY_PROMETHEUS_PORT`. Both listeners bind
localhost. Grafana permits anonymous **Viewer** access for this local development
stack. Admin credentials use `GRAFANA_ADMIN_USER` (default `admin`) and a random
`GRAFANA_ADMIN_PASSWORD` generated into the ignored, mode-0600 `.env` by `make up`.
Existing credentials are preserved. With an existing Grafana volume, changing the
environment password does not reset its stored account password.

For direct Compose invocations on an existing checkout, first run
`python scripts/init_observability.py`. Standard `.env` initialization and the
Postgres credential are still required. Neither generated passwords nor task
payloads are stored in dashboard JSON. Prometheus and Grafana each use a named volume;
`make down` preserves them and `make reset` deletes them with all other project data.
Prometheus retains 15 days by default.

## Metrics contract

Every application service exposes common metric families. API, scheduler, DLQ,
agent, and Python workers serve `/metrics`; Java serves `/actuator/prometheus`.
Worker telemetry listens privately on port 8003. The other internal ports are API
8000, scheduler 8001, DLQ 8005, agent 8006, Java 8007. Host API port is configured
separately. Prometheus itself exposes its native runtime metrics.

Global metrics come from the **same committed database history** in every service:

- `tasks_submitted_total`: retained task rows, including cron materializations;
  duplicate idempotency submissions do not increment it.
- `tasks_processed_total{status,type}`: durable result outbox events with
  `SUCCEEDED`, `RETRYING`, or `FAILED` status. This measures attempt outcomes,
  including recovered crashes, rather than only final task outcomes.
- `task_latency_seconds{type}`: histogram of attempt execution duration, from
  attempt start to outcome time. It excludes scheduling and backoff wait. Buckets
  range from 5 ms to 120 s, followed by `+Inf`. Historical result events are
  backfilled when their attempt row remains available; unavailable durations are
  excluded. The dashboard displays p95 by type.
- `retries_total{type}`: committed `RETRYING` outcomes; repeated Kafka delivery
  cannot add another count for the same outcome identity.
- `dlq_size{type}`: incidents whose DLQ status is `OPEN`. Resolved incident types
  retain zero while their audit rows are retained. This replaces the earlier
  `task_type` metric label with the common `type` label.
- `outbox_lag`: seconds since the oldest unpublished outbox row was created;
  zero for an empty outbox. `outbox_pending` counts those rows.
- `agent_diagnoses_total{category,action}`: committed per-incident decisions;
  applying a cached cluster diagnosis to another member counts its decision.
- `agent_tokens_total{direction}`: provider-reported input/output tokens, counted
  once per `LLM_RESPONSE` audit, including repair/tool turns. Shared cluster
  diagnoses never multiply token usage. `agent_tokens_charged_total` and
  `agent_cost_usd_total` additionally expose conservative budget accounting,
  including unknown provider usage charged by the guardrail.
- `circuit_breaker_open{type}`: 1 when a known task type has a Redis circuit key,
  including the half-open probe interval; otherwise 0.
- `running_tasks`, `running_tasks_oldest_age_seconds`, and `stuck_running_tasks`:
  current RUNNING state and age. Stuck means `updated_at` is older than
  `METRICS_STUCK_RUNNING_SECONDS` (default 300 s).

`worker_inflight` and `scheduler_is_leader` are **instance-local gauges**. Inflight
counts handlers while executing, including cancellation cleanup. Only schedulers
can report leadership; other services report zero. Scheduler scrapes verify Redis
ownership instead of trusting a cached process flag. Java retains its JVM,
Micrometer, and native worker metrics. Python services expose process/GC metrics.
Task IDs, trace IDs, worker IDs, and payload values are never metric labels.
Dynamic labeled families acquire samples after the first matching activity.

### Aggregation and retention

Global histories are intentionally repeated across endpoints. **Do not sum raw
replica series.** Provisioned `scheduler:*` recording rules take `max` by semantic
labels and then dashboards apply `rate`/`increase`. For example:

```promql
sum by (status) (rate(scheduler:tasks_processed_total[5m]))
histogram_quantile(0.95, sum by (le,type) (rate(scheduler:task_latency_seconds_bucket[5m])))
sum(worker_inflight{job=~"worker|worker-java"})
sum(scheduler_is_leader{job="scheduler"})
```

All service snapshots must refer to the same Postgres/Redis deployment. Separate
clusters should have separate Prometheus instances or a retained cluster label in
both recording rules and dashboard queries. A scrape can differ slightly between
replicas while transactions commit; `max` gives a coherent non-duplicated view over
time. Every scrape performs bounded read-only history queries; dependency failures
fail the scrape, and `up` shows the failure. Python's snapshot deadline is
`METRICS_SCRAPE_TIMEOUT_SECONDS` (5 s); Java uses a five-second SQL/Redis bound.
Prometheus scrapes every 10 s with a seven-second deadline. Alert/dashboard rates
need multiple scrapes and may initially be empty.

These counters derive from **retained history**. Deleting task, outbox, DLQ, or audit
rows can reset their values. Prometheus rates handle counter resets, but historical
long-term accounting requires archiving counters before pruning the underlying
history. Full-history SQL aggregation is appropriate for this development stack;
large deployments should maintain transactional aggregate tables before adding a
retention policy. Broker acknowledgements are tracked separately: an unpublished
outcome is already processed, and outbox lag reveals delivery delay.

## Correlation

HTTP accepts `X-Trace-ID` as a UUID or 32 hexadecimal digits, or extracts the trace
ID from W3C `traceparent`. Invalid/zero IDs start a new trace. Responses include
`X-Trace-ID`; task submission responses and `TaskOut.trace_id` return the original
task's ID when an idempotency key repeats.

Migration `0006` persists `tasks.trace_id`, `outbox.trace_id`, and
`dead_letters.trace_id`. Insert triggers inherit the task's canonical trace for
both SQLAlchemy and JPA writers; orphan incidents preserve the incoming header or
receive a fresh trace. Retry generations, scheduler release, AI recovery, and
manual DLQ retry retain the original task trace. New recurring occurrences each
have their own trace.

Each outbox Kafka record carries UTF-8 `task_id`, ASCII lowercase 32-hex `trace_id`,
and the existing `outbox-id`. Payload schemas remain version 1; attempt results
add optional nonnegative `duration_seconds`. Consumers tolerate older missing
headers and bind correlation for execution. Direct `publish_json` callers can pass
`trace_id` or inherit the active logging context; producers that bypass the outbox
must supply the persisted task trace. The Java standalone producer accepts
`TASK_TRACE_ID` for the same purpose.

Python JSON logs and Java structured Logstash logs contain `task_id` and
`trace_id` on submissions, attempt execution, outbox publication, scheduled release,
DLQ ingestion, and agent decisions. Java propagates MDC into its handler executor;
Python uses scoped ContextVars. Unrelated lifecycle logs may have null/no IDs.
Inspect a trace locally without changing consumer offsets:

```sh
docker compose --profile java logs --no-color api worker worker-java scheduler dlq agent | rg 'YOUR_TRACE_ID'
```

This is log/message correlation, with no tracing backend or span exporter required.

## Alerts and checks

`observability/prometheus/rules.yml` provisions these rules:

- `DLQGrowing`: open queue grows by more than five over ten minutes, sustained for
  two minutes. This measures net queue growth; successful agent/manual resolutions
  can offset new incidents.
- `TasksStuckRunning`: any task exceeds the configured RUNNING age, sustained for
  two minutes. Set the threshold above the longest legitimate handler timeout;
  worker lease recovery will normally resolve abandoned attempts first.
- `SchedulerLeaderAbsent`: all scheduler ownership gauges are zero **or missing**
  for 15 seconds. A failed metrics dependency can also trigger it; inspect scrape
  health before assuming election failed.

Alerts appear in Prometheus; notification delivery needs an Alertmanager deployment
and receiver configuration, outside this phase's scope.

`make test-alerts` runs five promtool scenarios, including duplicate global series,
a healthy leader plus standby, zero leaders, absent targets, stuck RUNNING state,
and sustained queue growth. `make check-observability` checks all live targets,
leader ownership, Grafana's datasource/dashboard and every panel query, loaded
alerts, then submits an isolated report and validates task/trace headers and
execution latency through real Kafka pending/results records. It uses its own
consumer group and leaves a uniquely identified successful demonstration task.
`make check-java` additionally validates Java result/DLQ correlation and duration.

Python tests cover trace normalization/fallback, concurrent context isolation,
direct Kafka helper propagation, histogram exposition, persistent traces across
idempotency/outbox topics, schema-isolated trigger behavior, shared durable counts,
and one token accounting record across multiple diagnosed cluster members. Java
JUnit covers correlation context restoration and invalid header fallback. Existing
worker timeout/shutdown, scheduler failover, and agent guardrail regressions remain
part of the full suite.

## Files added or changed

- Common telemetry: `shared/metrics.py`, `shared/observability.sql`,
  `shared/tracing.py`, `shared/logging.py`, `shared/kafka.py`, `shared/config.py`,
  `shared/tables.py`, `shared/models.py`, `shared/contracts.py`, and the generated
  `shared/contracts/task-result.schema.json`; `pyproject.toml` packages the SQL.
- Persistence: `migrations/versions/0006_observability_and_correlation.py`.
- Python services: API middleware/submission/exporter; worker metrics HTTP server,
  consumer/executor/lifecycle/retry result instrumentation; scheduler, DLQ, and
  agent exporters and correlated event/audit logs.
- Java: `DurableMetrics.java`, `Correlation.java`, JPA rows, runtime/listeners,
  outbox relay, contract producer, `application.yml`, generated
  `src/main/resources/observability.sql`, and `CorrelationTest.java`.
- Operations: `docker-compose.yml`, `.env.example`, `Makefile`,
  `scripts/init_observability.py`, `scripts/check_observability.py`,
  `scripts/check_java_worker.py`, `scripts/export_contracts.py`, all files under
  `observability/`, and the observability/architecture/API/contract/README documentation.
- Tests: `tests/unit/test_observability.py`,
  `tests/integration/test_telemetry_integration.py`, adjusted API/DLQ metrics
  expectations, isolated worker fixture port settings, and promtool rule fixtures.

## Recorded verification

Verified on 2026-11-10 with three Python workers, two schedulers, one Java worker,
and the complete Compose stack healthy:

- Python: 283 passed, two Java opt-in wrapper tests skipped; their underlying
  contract/live scripts were run separately.
- Java: nine JUnit tests passed; bidirectional DTO/consumer contracts and actual
  Kafka/JPA execution, retry, DLQ, trace headers, and duration checks passed.
- Promtool: configuration and 16 rules validated; all five alert scenarios passed.
- Live telemetry: ten healthy scrape targets, exactly one leader, provisioned
  Grafana datasource/dashboard, and all twelve panel queries accepted.
- HTTP → Kafka → Python worker → result trace correlation passed. Java task/result/
  DLQ header correlation passed. All 170 histogram samples and labels matched
  between the Python and Java exporters after activity settled.

Builds and test suites were run separately on the small development Docker VM;
short-lease timing tests require CPU availability. See the commands above to repeat
verification in another environment.
