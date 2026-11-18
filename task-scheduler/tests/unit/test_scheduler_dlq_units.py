"""Scheduler and DLQ component contracts around fenced clocks, durable rows, and source receipts."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fakes import NOW, Factory, Session, letter, task

from services.dlq.consumer import DLQConsumer, counter, identifier, timestamp
from services.scheduler.core import SchedulerCore
from services.scheduler.leader import LeadershipLost
from shared.config import get_settings
from shared.models import TaskStatus
from shared.tables import DelayedTask, Schedule


def scheduler(session):
    core = SchedulerCore(Factory(session), AsyncMock(), get_settings())
    core.lease = SimpleNamespace(
        require=AsyncMock(),
        held=True,
        value="owner",
        renew=AsyncMock(return_value=False),
        is_owner=AsyncMock(return_value=False),
        acquire=AsyncMock(return_value=True),
        release=AsyncMock(),
    )
    return core


@pytest.mark.parametrize("policy", ["skip", "fire_once", "on_time", "duplicate"])
async def test_recurring_materialization_uses_occurrence_identity_and_missed_policy(policy):
    row = task()
    definition = Schedule(
        id=uuid4(),
        type=row.type,
        payload={},
        priority=5,
        max_retries=3,
        cron="* * * * *",
        timezone="UTC",
        enabled=True,
        next_run_at=NOW if policy in ("on_time", "duplicate") else NOW - timedelta(hours=2),
        missed_run_policy="skip" if policy == "skip" else "fire_once",
    )
    session = Session(scalar=[None if policy == "duplicate" else row], scalars=[[definition]])
    core = scheduler(session)
    count = await core.materialize_recurring()
    assert count == (0 if policy in ("skip", "duplicate") else 1)
    assert definition.next_run_at > NOW
    if count:
        assert row.status == TaskStatus.SCHEDULED and isinstance(session.added[0], DelayedTask)


@pytest.mark.parametrize("case", ["scheduled", "missing", "cancelled", "lost"])
async def test_scheduler_rebuilds_or_tombstones_due_index(case):
    row = task(TaskStatus.CANCELLED if case == "cancelled" else TaskStatus.SCHEDULED)
    delayed = DelayedTask(task_id=row.id, release_at=NOW)
    session = Session(scalars=[[delayed]], get=[None if case == "missing" else row])
    core = scheduler(session)
    core.redis.eval.return_value = case != "lost"
    if case == "lost":
        with pytest.raises(LeadershipLost):
            await core.reconcile()
    else:
        assert await core.reconcile() == int(case == "scheduled")
        if case != "scheduled":
            assert delayed.dispatched_at == NOW
    core.redis.eval.return_value = [str(row.id), "123"]
    assert await core.pop_due(NOW) == [row.id]


@pytest.mark.parametrize("case", ["missing", "dispatched", "future", "cancelled", "valid"])
async def test_scheduler_release_is_a_single_fenced_outbox_write(case):
    row = task(TaskStatus.CANCELLED if case == "cancelled" else TaskStatus.SCHEDULED)
    delayed = DelayedTask(
        task_id=row.id,
        release_at=NOW + timedelta(seconds=2) if case == "future" else NOW,
        dispatched_at=NOW if case == "dispatched" else None,
    )
    session = Session(scalar=[None if case == "missing" else delayed, row])
    core = scheduler(session)
    assert await core.release_one(row.id) == (case == "valid")
    if case == "valid":
        assert (
            session.added[0].payload["task_id"] == str(row.id) and row.status == TaskStatus.PENDING
        )


async def test_scheduler_tick_and_election_recover_from_dependency_errors():
    core = scheduler(Session())
    identifier = uuid4()
    core.materialize_recurring = AsyncMock()
    core.reconcile = AsyncMock()
    core.pop_due = AsyncMock(return_value=[identifier])
    core.release_one = AsyncMock(return_value=True)
    assert await core.tick() == 1
    await core._heartbeat()
    core.tick = AsyncMock(side_effect=ConnectionError("redis down"))

    async def pause(_):
        core.stop_event.set()

    core._pause = pause
    await core.run()
    assert not core.running
    core.lease.release.assert_awaited_once()
    core.stop_event.clear()
    core.lease.held = True

    async def stop_after_renew(_):
        pass

    core._pause = stop_after_renew
    await core._renew()
    assert core.leader_metric._value.get() == 0


@pytest.mark.parametrize(
    "case",
    ["canonical", "raw", "malformed", "old_generation", "discarded", "conflicting", "receipt"],
)
async def test_dlq_ingestion_persists_or_reuses_incident_and_source_receipt(case):
    row = task(TaskStatus.CANCELLED if case == "discarded" else TaskStatus.FAILED, attempts=1)
    incident = letter(row)
    payload = {
        "id": str(incident.id),
        "task_id": str(row.id),
        "reason": "timeout",
        "error_history": [{"error": "timeout", "attempt_no": 1}],
        "generation": 0,
    }
    if case == "old_generation":
        row.generation = 1
    if case == "raw":
        payload = {"error_history": "malformed history", "task_id": str(row.id)}
    if case == "malformed":
        payload = b"not-json"
    if case == "conflicting":
        incident.task_id = uuid4()
    session = Session(
        scalar=[row, None],
        get=[
            SimpleNamespace(dead_letter_id=incident.id) if case == "receipt" else None,
            incident if case == "conflicting" else None,
        ],
    )
    consumer = DLQConsumer(Factory(session), get_settings())
    record = SimpleNamespace(topic="tasks.dlq", partition=0, offset=1, value=payload, headers=[])
    result = await consumer.ingest(record)
    assert result is not None
    assert session.execute.await_count == (0 if case == "receipt" else 2)
    if case == "canonical":
        assert row.status == TaskStatus.DEAD_LETTERED


async def test_dlq_metadata_fallback_and_legacy_fingerprinting():
    assert counter(object(), 7) == 7 and counter(-3) == 0
    assert timestamp("naive", NOW) == NOW and timestamp("2026-01-01", NOW) == NOW
    assert identifier("bad", "source") == identifier("bad", "source")
    row = task()
    incident = letter(
        row,
        category="unknown",
        reason="fatal",
        failure_signature=None,
        error_history=[{"error": "error 123", "stack_trace": "trace"}],
    )
    consumer = DLQConsumer(Factory(Session(scalars=[[incident]])), get_settings())
    await consumer.backfill()
    assert len(incident.failure_signature) == 64
    consumer.stop_event.set()
    await consumer._pause()
    await consumer.run()
    assert not consumer.running
