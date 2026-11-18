"""Fenced worker claims, immutable outcomes, retry receipts, and lifecycle failures."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fakes import NOW, Factory, Session, attempt, task

from services.worker.executor import Completed, Execution, LeaseLost, TaskExecutor
from services.worker.registry import HandlerContext
from services.worker.retry_engine import RetryEngine
from services.worker.retry_policy import RetryPolicy, event_id
from shared.circuit import Permit
from shared.config import get_settings
from shared.errors import FatalError, RetryableError
from shared.models import RetryMessage, TaskMessage, TaskStatus
from shared.tables import DeadLetter, DelayedTask, Outbox, RetrySchedule


def executor(session):
    return TaskExecutor(
        Factory(session), AsyncMock(), get_settings(), HandlerContext("unit-worker", MagicMock())
    )


def dispatch(row, **updates):
    return TaskMessage(
        task_id=row.id, type=row.type, payload=row.payload, scheduled_at=row.scheduled_at, **updates
    )


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "old_generation",
        "future_generation",
        "finished",
        "delayed",
        "old_attempt",
        "retrying",
        "future",
        "lost",
        "running",
        "wrong_attempt",
        "circuit",
        "claim",
    ],
)
async def test_claim_fences_stale_or_unsafe_dispatches(case, monkeypatch):
    row = task()
    token = "owner"
    msg = dispatch(row)
    session = Session(scalar=[None if case == "missing" else row])
    run = executor(session)
    run.redis.get.return_value = token
    run.breaker.acquire = AsyncMock(return_value=Permit(True))
    if case == "old_generation":
        row.generation = 1
    if case == "future_generation":
        msg = dispatch(row, generation=1)
    if case == "finished":
        row.__dict__["status"] = TaskStatus.SUCCEEDED
    if case == "delayed":
        session.get.return_value = DelayedTask(task_id=row.id, release_at=NOW, dispatched_at=None)
    if case == "old_attempt":
        row.attempts = 2
    if case == "retrying":
        row.__dict__["status"] = TaskStatus.RETRYING
    if case == "future":
        row.scheduled_at = NOW + timedelta(hours=1)
    if case == "lost":
        run.redis.get.return_value = "successor"
    if case == "running":
        row.__dict__["status"] = TaskStatus.RUNNING
        monkeypatch.setattr(run, "_recover_locked", AsyncMock(return_value=Completed()))
    if case == "wrong_attempt":
        msg = dispatch(row, attempt_no=2)
    if case == "circuit":
        run.breaker.acquire.return_value = Permit(False, retry_after_s=3)
    if case == "lost":
        with pytest.raises(LeaseLost):
            await run._claim(msg, token)
    else:
        result = await run._claim(msg, token)
        if case == "claim":
            assert isinstance(result, Execution) and result.trace_id == row.trace_id
            assert row.status == TaskStatus.RUNNING and row.attempts == 1
            assert session.added[0].worker_id == "unit-worker"
        elif case == "circuit":
            assert isinstance(result, Completed) and row.status == TaskStatus.SCHEDULED
        elif case in {"future_generation", "retrying", "future", "wrong_attempt"}:
            assert result is None
        else:
            assert isinstance(result, Completed)


@pytest.mark.parametrize("failure", [None, RetryableError("temporary"), FatalError("invalid")])
async def test_finish_commits_outcome_and_preserves_error_history(failure):
    row = task(TaskStatus.RUNNING, attempts=1)
    record = attempt(row, finished_at=None, error=None)
    session = Session(scalar=[row], get=[record], scalars=[[record]])
    run = executor(session)
    run.redis.get.return_value = "owner"
    run.breaker.complete = AsyncMock()
    claim = Execution(row.id, row.type, row.payload, 1, "owner", trace_id=row.trace_id)
    ids = await run._finish(claim, {"ok": True}, failure)
    expected = (
        TaskStatus.SUCCEEDED
        if failure is None
        else TaskStatus.FAILED
        if isinstance(failure, FatalError)
        else TaskStatus.RETRYING
    )
    assert row.status == expected and record.finished_at == NOW
    results = [
        event
        for event in session.added
        if isinstance(event, Outbox) and event.topic == "tasks.results"
    ]
    assert len(results) == 1 and results[0].payload["duration_seconds"] == 0
    assert ids[0] == results[0].id
    if failure is not None:
        assert str(failure) in record.error and record.stack_trace


async def test_finish_and_reaper_reject_stale_or_missing_attempts():
    row = task(TaskStatus.RUNNING, attempts=1)
    run = executor(Session(scalar=[row], get=[None]))
    claim = Execution(row.id, row.type, {}, 1, "owner")
    with pytest.raises(LeaseLost):
        await run._finish(claim, None, None)
    with pytest.raises(RuntimeError):
        await run._recover_locked(Session(get=[None]), row)
    session = Session(scalars=[[row]])
    run = executor(session)
    run.redis.exists.return_value = False
    run._recover_locked = AsyncMock(return_value=Completed())
    assert await run.recover_abandoned() == 1
    run._recover_locked.assert_awaited_once_with(session, row)


@pytest.mark.parametrize(
    "case",
    [
        "java",
        "missing",
        "older",
        "newer",
        "delayed",
        "done",
        "leased",
        "claim_none",
        "claim_done",
        "success",
        "failure",
    ],
)
async def test_process_handles_replay_leases_and_cleanup(case):
    row = task()
    session = Session(get=[None if case == "missing" else row, None])
    run = executor(session)
    msg = dispatch(row)
    if case == "java":
        msg = msg.model_copy(update={"type": "java.report"})
    if case == "older":
        row.generation = 1
    if case == "newer":
        msg = msg.model_copy(update={"generation": 1})
    if case == "delayed":
        session.get.side_effect = [row, DelayedTask(task_id=row.id, release_at=NOW)]
    if case == "done":
        row.__dict__["status"] = TaskStatus.SUCCEEDED
    run.redis.set.return_value = case != "leased"
    claim = Execution(row.id, row.type, row.payload, 1, "owner", trace_id=row.trace_id)
    run._claim = AsyncMock(
        return_value=None
        if case == "claim_none"
        else Completed()
        if case == "claim_done"
        else claim
    )
    run._execute = AsyncMock(return_value={"ok": True})
    if case == "failure":
        run._execute.side_effect = ValueError("invalid")
    run._finish = AsyncMock(return_value=())
    assert await run.process(msg) == (case not in {"newer", "leased", "claim_none"})
    if case in {"success", "failure"}:
        assert run._finish.await_count == 1
        assert bool(run._finish.call_args.args[-1]) == (case == "failure")
    if case in {"claim_none", "claim_done", "success", "failure"}:
        run.redis.eval.assert_awaited_once()


@pytest.mark.parametrize("kind", ["retry", "fatal", "exhausted", "poison"])
async def test_retry_policy_preserves_auditable_terminal_or_delayed_events(kind):
    row = task(
        TaskStatus.RUNNING,
        attempts=3 if kind in {"poison", "exhausted"} else 1,
        max_retries=3 if kind == "exhausted" else 5,
    )
    history = [
        attempt(
            row,
            attempt_no=i + 1,
            error="WorkerCrash: terminated" if kind == "poison" else "TimeoutError: unavailable",
        )
        for i in range(row.attempts)
    ]
    session = Session(scalars=[history])
    error = FatalError("invalid") if kind == "fatal" else RetryableError("temporary")
    ids = await RetryPolicy(get_settings()).fail(
        session, row, history[-1], error, NOW, crashed=kind == "poison"
    )
    assert len(ids) == 2
    if kind == "retry":
        assert row.status == TaskStatus.RETRYING and any(
            isinstance(x, RetrySchedule) for x in session.added
        )
    else:
        assert row.status == TaskStatus.FAILED
        incident = next(x for x in session.added if isinstance(x, DeadLetter))
        assert len(incident.error_history) == row.attempts
        assert ("poison" in incident.reason) == (kind == "poison")


async def test_retry_receipt_identity_validation_and_due_dispatch():
    row = task(TaskStatus.RETRYING, attempts=1)
    identifier = event_id(row.id, 1, "retry")
    schedule = RetrySchedule(
        id=identifier, task_id=row.id, generation=0, failed_attempt=1, due_at=NOW, received_at=None
    )
    msg = RetryMessage(
        event_id=identifier, task_id=row.id, failed_attempt=1, due_at=NOW, delay_seconds=0
    )
    session = Session(scalar=[schedule, schedule, None, row], scalars=[[schedule]])
    engine = RetryEngine(Factory(session), get_settings(), executor(session))
    with pytest.raises(ValueError):
        await engine.ingest(msg.model_copy(update={"event_id": uuid4()}))
    await engine.ingest(msg)
    assert schedule.received_at == NOW
    with pytest.raises(ValueError):
        await engine.ingest(msg.model_copy(update={"due_at": NOW + timedelta(seconds=1)}))
    await engine.ingest(msg)
    assert await engine.dispatch_due() == 1 and row.status == TaskStatus.SCHEDULED
    assert session.added[0].topic == "tasks.pending"
    engine.stop_event.set()
    await engine._pause()


async def test_renewal_and_visibility_deadline_are_fenced():
    row = task()
    run = executor(Session())
    run.settings = run.settings.model_copy(
        update={
            "worker": run.settings.worker.model_copy(update={"visibility_timeout_seconds": 0.003})
        }
    )
    run.redis.eval.return_value = 0
    claim = Execution(row.id, row.type, {}, 1, "owner")
    with pytest.raises(LeaseLost):
        await run._renew(claim)
    assert await run.wait_published(())
    run.factory = Factory(Session(scalar=[0]))
    assert await run.wait_published((uuid4(),))
