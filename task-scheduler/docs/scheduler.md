# Phase 6 scheduler runbook

Two scheduler replicas coordinate delayed tasks and recurring cron definitions.
One owns a renewable Redis leader lease; the other stays healthy and takes over
after the leader's lease expires. PostgreSQL keeps dispatch intent and recurring
occurrence identity durable independently of Redis.

## Run and verify

```sh
source .venv/bin/activate
python -m pip install -e '.[dev]'
make up
make check-scheduler
make test
make test-integration
make migration-check
```

`make up` builds the scheduler image, applies revision `0003`, and runs two replicas.
To explicitly keep three workers and two schedulers:

```sh
docker compose up -d --build --wait --wait-timeout 180 \
  --scale worker=3 --scale scheduler=2 api worker scheduler
docker compose ps scheduler
python scripts/check_scheduler.py
```

The live checker verifies two healthy replicas with leader metrics 1 and 0, submits
a future report, confirms its Redis entry, and verifies execution after its due
time. It also creates a short recurring example, verifies one unique occurrence
executes, and pauses that definition in cleanup.

To prove actual Docker SIGKILL failover:

```sh
python scripts/check_scheduler.py --kill-leader
```

This opt-in check temporarily disables the current leader container's automatic
restart, SIGKILLs it, verifies the original standby takes over, then restores the
restart policy and two healthy replicas. The killed leader's Redis lease is not
gracefully released. A delayed task submitted before the kill still executes once.

Scheduler HTTP ports are dynamically assigned on loopback so replicas can scale.
Find either replica's health/metrics port:

```sh
docker compose port --index 1 scheduler 8001
docker compose port --index 2 scheduler 8001
```

Both expose `/health` and `/metrics`. Inside Docker, each process listens on port
8001; a host process defaults to 8004. For a host scheduler alongside Docker:

```sh
SCHEDULER_ID=host-scheduler SCHEDULER_PORT=8004 python -m services.scheduler
```

## One-time delayed tasks

`POST /tasks` still accepts an aware `scheduled_at` timestamp. If it is in the
future according to the database clock, the API commits a task in SCHEDULED and a
`delayed_tasks` recovery row in one transaction. It does not create a pending Kafka
event yet. After commit it indexes the UUID in Redis `scheduled_tasks`, scored by
the UTC epoch timestamp. Redis failure does not undo the accepted task; the leader
rebuilds missing entries from PostgreSQL.

For example, submit a report due in ten seconds:

```sh
python -c 'import json; from datetime import UTC, datetime, timedelta; print(json.dumps({"type":"generate_report","payload":{"data":[1,2,3],"seconds":0},"scheduled_at":(datetime.now(UTC)+timedelta(seconds=10)).isoformat()}))' \
  | curl -fsS http://localhost:8002/tasks -H 'Content-Type: application/json' --data-binary @-
```

The default one-second tick materializes cron occurrences, reconciles the Redis
index, and invokes Lua to atomically verify the leader token, read due members in
ascending score order, and remove a bounded batch. A future member is never popped.
Equal-score members follow Redis lexicographic UUID ordering.

Each popped task is then row-locked in PostgreSQL. If its delayed ledger is still
undispatched and its state is SCHEDULED, one transaction advances it to PENDING,
inserts a deterministic `tasks.pending` outbox event, and marks the ledger dispatched.
The shared relay publishes it to Kafka. Releasing one task per DB transaction
preserves dequeue order in outbox creation order. Kafka delivery order remains
per-partition, rather than a global order across the priority lanes.

If the process dies after Redis pop but before commit, the undispatched ledger
restores the entry on the next leader's reconciliation. If it dies after commit,
the outbox retains publication intent. Repeated release attempts cannot create an
extra event. Broker acknowledgement followed by a publication-marker crash can
still replay a Kafka event, as in earlier phases; workers retain idempotency checks.

Cancelling a held task works through the existing PENDING/SCHEDULED cancellation
endpoint. The scheduler removes the cancelled entry without publishing it. Workers
also acknowledge premature/legacy dispatches backed by an unreleased delayed
ledger: the scheduler guarantees later publication, so these records do not hold
a Kafka partition waiting for the future deadline. Retry-inbox tasks remain under
the Phase 5 retry dispatcher and are not indexed a second time here.

## Recurring definitions

The API provides:

- `POST /schedules`: create a recurring definition.
- `GET /schedules/{id}`: inspect the definition and its next-run cursor.
- `PATCH /schedules/{id}` with `{"enabled":false}`: pause future materialization.
- The same PATCH with `true`: resume from the next future occurrence.
- `GET /tasks?schedule_id={id}`: list tasks materialized by that definition.

Create an example:

```sh
definition=$(curl -fsS http://localhost:8002/schedules \
  -H 'Content-Type: application/json' \
  -d '{"name":"five-minute-report","cron":"*/5 * * * *","timezone":"UTC","type":"generate_report","payload":{"data":[1,2,3]},"priority":"normal","missed_run_policy":"fire_once"}')
printf '%s\n' "$definition"
schedule_identifier=$(printf '%s' "$definition" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')
curl -fsS "http://localhost:8002/schedules/$schedule_identifier"
curl -fsS "http://localhost:8002/tasks?schedule_id=$schedule_identifier"
curl -fsS -X PATCH "http://localhost:8002/schedules/$schedule_identifier" \
  -H 'Content-Type: application/json' -d '{"enabled":false}'
```

Expressions use croniter: standard five-field minute/hour/day/month/weekday cron,
or six fields with **seconds last**, such as `* * * * * */2` for two-second demos.
Timezone defaults to UTC; IANA names such as `Asia/Kolkata` are supported. Unknown
zones, malformed expressions, unreachable calendar dates, and unsupported field
counts return 422. Cron search is bounded to eight years. Calendar/DST behavior
follows croniter with timezone-aware reference dates.

Definitions are stored in `schedules`, including type/payload/priority/attempt
budget, cron/timezone, enabled state, missed-run policy, next occurrence, and last
materialized occurrence. The leader locks due definitions with SKIP LOCKED and
materializes at most one run per definition per tick. Each run has a deterministic
task UUID and a DB UNIQUE constraint on `(schedule_id, scheduled_for)`. Advancing
the schedule cursor and inserting its task/delayed ledger commit together.

This protects against duplicates across scheduler instances even during lease
turnover or racing materialization calls. `scheduled_for` retains the original
cron occurrence even if the task is later retried. Pausing a schedule does not
cancel already-materialized tasks; cancel those separately if desired.

## Missed-run policy

Set `SCHEDULER_MISSED_RUN_POLICY` to `fire_once` (default) or `skip`. POST can override
it per definition; the selected policy is stored, so changing the deployment default
affects newly created definitions. Late polling within `SCHEDULER_MISFIRE_GRACE_SECONDS`
(default 2s) is treated as an ordinary due run.

- `fire_once`: after downtime, coalesce missed occurrences into one task for the
  latest occurrence at or before now, then advance to the next future occurrence.
- `skip`: when the cursor is older than the grace period, create no catch-up task
  and advance directly to the next future occurrence.

There is no task-per-missed-minute catch-up storm. Both policies use the database
clock. Re-enabling an explicitly paused definition starts at its next future cron
occurrence, rather than replaying time spent disabled.

## Leader election and metrics

Leadership uses Redis SET NX PX with a fresh JSON owner/token value. Default TTL is
5s, renewal interval 1s, and standby election polling 0.25s. Renewal and release use
token-checked Lua, so an old owner cannot extend or remove its successor's lease.
Renewal runs independently while the leader awaits database work. Due popping is
also fenced by that token within the same Lua operation.

The default takeover bound is approximately the remaining lease TTL plus a standby
poll, assuming Redis/DB are available. A graceful SIGTERM releases leadership
immediately; SIGKILL relies on TTL expiry. Redis/DB failures stop leader work and
leave durable records for recovery. Row locks, occurrence uniqueness, and the
delayed ledger provide the database safety net during lease races.

Each replica exposes:

- `scheduler_is_leader{instance_id="..."}`: authoritative current ownership, 0 or 1.
- `scheduler_released_tasks_total`: durable delayed releases.
- `scheduler_materialized_runs_total`: materialized recurring tasks.
- `scheduler_skipped_misfires_total`: definitions advanced under skip policy.

Standby `/health` remains 200 when its election loop and dependencies are healthy.
Only the leader performs schedule materialization and queue popping. Both replicas
may relay already-committed outbox events using SKIP LOCKED, alongside API/workers.
Replica heartbeats use `scheduler:instance:{id}` with TTL and are separate from
the authoritative `scheduler:leader` lock.

## Migration and tests

Revision `0003` adds schedules, the delayed-task ledger, task schedule/occurrence
columns, indexes, and occurrence uniqueness. Existing future PENDING tasks with no
attempts are moved into the delayed path; unpublished early outbox entries are
removed only after durable recovery intent is recorded. Already-published legacy
events cannot be retracted, and are safely ignored until scheduler release.
Downgrade restores the earlier pending-event delivery path before dropping the ledger.

Tests cover due ordering/future exclusion, idempotent release, two schedulers and
two actual Kafka relays producing one event, crash-after-pop recovery, cron validation
and timezones, both missed-run policies, API delay/cancel/pause behavior, authoritative
metrics/standby health, renewal beyond the original TTL, real subprocess SIGKILL
failover, and data-preserving migration up/down. The live Docker checker separately
proves killed-leader takeover and delayed/cron execution with two restored replicas.
