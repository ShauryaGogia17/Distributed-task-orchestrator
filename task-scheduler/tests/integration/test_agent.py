"""Real Postgres/Redis/Kafka orchestration with deterministic offline model responses."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.agent.collector import Collector
from services.agent.core import AgentCore
from services.agent.guardrails import BudgetExceeded, HourlyBudget
from services.agent.llm import MockLLM
from services.agent.schemas import Category, Diagnosis
from services.scheduler.core import SchedulerCore
from services.worker.executor import TaskExecutor
from services.worker.registry import HandlerContext, load_builtin_handlers
from shared.config import (
    AgentSettings,
    CircuitSettings,
    SchedulerSettings,
    Settings,
    WorkerSettings,
    get_settings,
)
from shared.db import create_session_factory
from shared.fingerprint import failure_signature
from shared.models import DLQStatus, ErrorRecord, TaskMessage, TaskStatus
from shared.outbox import OutboxPublisher
from shared.redis import create_redis_client
from shared.tables import (
    AgentAudit,
    AgentCluster,
    DeadLetter,
    DelayedTask,
    Outbox,
    Task,
    TaskAttempt,
)

pytestmark = pytest.mark.integration


@dataclass
class Resources:
    """Private schema and Redis namespaces; all LLM calls remain offline."""

    factory: async_sessionmaker[AsyncSession]
    redis: Redis
    settings: Settings
    llm: MockLLM
    core: AgentCore
    executor: TaskExecutor


def canned(category=Category.TRANSIENT, **overrides) -> Diagnosis:
    """A high-confidence, explicitly safe diagnosis with zero test-only scheduling delay."""
    data = {
        "root_cause": "Temporary dependency failure.",
        "category": category,
        "confidence": 0.95,
        "suggested_fix": "Retry after recovery.",
        "safe_to_auto_retry": True,
        "modified_payload": None,
        "recommended_backoff_s": 0,
    }
    return Diagnosis(**{**data, **overrides})


@pytest.fixture
async def agent(database, tmp_path) -> AsyncIterator[Resources]:
    """Run only the outbox loop automatically; tests control each agent boundary explicitly."""
    prefix = f"agent-test:{uuid4().hex}:"
    settings = get_settings().model_copy(
        update={
            "agent": AgentSettings(
                _env_file=None,
                cluster_debounce_seconds=0,
                min_backoff_seconds=0,
                budget_key_prefix=prefix + "budget:",
            ),
            "circuit": CircuitSettings(
                _env_file=None, key_prefix=prefix + "circuit:", cooldown_seconds=0.15
            ),
            "scheduler": SchedulerSettings(
                _env_file=None, leader_key=prefix + "leader", queue_key=prefix + "due"
            ),
            "worker": WorkerSettings(
                _env_file=None,
                id=prefix,
                task_timeout_seconds=2,
                result_publish_timeout_seconds=5,
                health_file=tmp_path / "health.json",
            ),
        }
    )
    factory = create_session_factory(database[0])
    redis = create_redis_client(settings.redis)
    llm = MockLLM([canned()])
    core = AgentCore(factory, redis, settings, lambda: llm)
    publisher = OutboxPublisher(factory, settings)
    relay = asyncio.create_task(publisher.run())
    load_builtin_handlers()
    async with httpx.AsyncClient() as http:
        executor = TaskExecutor(factory, redis, settings, HandlerContext(prefix, http))
        try:
            yield Resources(factory, redis, settings, llm, core, executor)
        finally:
            await publisher.shutdown(relay)
            keys = [key async for key in redis.scan_iter(match=prefix + "*")]
            async with factory() as session:
                tasks = list(await session.scalars(select(Task)))
            keys += [
                key
                for task in tasks
                for generation in range(task.generation + 1)
                for key in (
                    executor.lease_key(task.id, generation),
                    executor.completed_key(task.id, generation),
                )
            ]
            if keys:
                await redis.delete(*keys)
            await redis.aclose()


async def seed(
    agent: Resources,
    *,
    error="TimeoutError: temporary dependency timeout",
    payload=None,
    task_type="generate_report",
    ai_requeues=0,
):
    """Commit one terminal failure with a stable UTC cluster window and received Kafka metadata."""
    async with agent.factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        task = Task(
            type=task_type,
            payload={"seconds": 0, "data": [1, 2]} if payload is None else payload,
            max_retries=1,
            attempts=1,
            ai_requeues=ai_requeues,
        )
        session.add(task)
        await session.flush()
        for status in (
            TaskStatus.SCHEDULED,
            TaskStatus.RUNNING,
            TaskStatus.FAILED,
            TaskStatus.DEAD_LETTERED,
        ):
            task.status = status
        history = ErrorRecord(
            attempt_no=1,
            worker_id="test-worker",
            error=error,
            stack_trace='File "/app/handler.py", line 12',
            recorded_at=now,
        )
        session.add(
            TaskAttempt(
                task_id=task.id,
                generation=0,
                attempt_no=1,
                worker_id="test-worker",
                started_at=now,
                finished_at=now,
                error=error,
                stack_trace=history.stack_trace,
            )
        )
        incident = DeadLetter(
            task_id=task.id,
            task_type=task_type,
            generation=0,
            attempts=1,
            worker_id="test-worker",
            reason="fatal error",
            error_history=[history.model_dump(mode="json")],
            failure_signature=failure_signature(error, history.stack_trace),
            received_at=now,
            created_at=now.replace(second=1, microsecond=0),
        )
        session.add(incident)
        await session.flush()
        return task.id, incident.id


@pytest.mark.parametrize(
    ("category", "expected"),
    [
        (Category.TRANSIENT, "REQUEUE"),
        (Category.DEPENDENCY_DOWN, "REQUEUE"),
        (Category.BAD_INPUT, "REQUEUE_MODIFIED"),
        (Category.BUG, "ESCALATE"),
        (Category.RESOURCE_EXHAUSTION, "ESCALATE"),
        (Category.UNKNOWN, "ESCALATE"),
    ],
)
async def test_categories_store_diagnosis_action_and_audit(agent, category, expected) -> None:
    """Every category reaches the correct durable state, notification, and execution generation."""
    task_id, incident_id = await seed(agent, payload={"seconds": -1})
    agent.llm.responses.clear()
    agent.llm.responses.append(
        canned(
            category, modified_payload={"seconds": 0} if category == Category.BAD_INPUT else None
        )
    )
    await agent.core.tick()
    async with agent.factory() as session:
        incident = await session.get(DeadLetter, incident_id)
        task = await session.get(Task, task_id)
        assert incident.ai_diagnosis["action"] == expected
        assert (
            incident.ai_diagnosis["category"] == category.value
            and incident.ai_diagnosis["reasoning"]
        )
        assert incident.agent_processed_at is not None
        events = list(await session.scalars(select(AgentAudit.event)))
        assert "LLM_REQUEST" in events and "LLM_RESPONSE" in events and "ACTION_TAKEN" in events
        if expected.startswith("REQUEUE"):
            assert task.status == TaskStatus.PENDING and task.generation == 1 and task.attempts == 0
            assert task.ai_requeues == 1 and task.ai_modified == (category == Category.BAD_INPUT)
            assert incident.status == DLQStatus.RETRIED
        else:
            assert task.status == TaskStatus.ESCALATED and task.generation == 0
            assert "ESCALATION_NOTIFICATION" in events and incident.status == DLQStatus.ESCALATED


async def test_one_representative_and_late_members_reuse_diagnosis(agent) -> None:
    """Two agents race safely, and a later member gets the stored diagnosis with no extra call."""
    settings = agent.settings.model_copy(
        update={"agent": agent.settings.agent.model_copy(update={"circuit_failure_threshold": 10})}
    )
    agent.core.settings = settings
    first, second = await seed(agent), await seed(agent)
    standby = AgentCore(agent.factory, agent.redis, settings, lambda: agent.llm)
    await asyncio.gather(agent.core.tick(), standby.tick())
    assert len(agent.llm.calls) == 1
    later = await seed(agent)
    await agent.core.tick()
    assert len(agent.llm.calls) == 1
    async with agent.factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentCluster)) == 1
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AgentAudit)
                .where(AgentAudit.event == "ACTION_TAKEN")
            )
            == 3
        )
        for _, identifier in (first, second, later):
            assert (await session.get(DeadLetter, identifier)).ai_diagnosis["action"] == "REQUEUE"
    await standby.tick()
    assert len(agent.llm.calls) == 1


async def test_corrected_payload_succeeds_and_history_is_retained(agent) -> None:
    """A corrected task executes through the outbox and retains prior history."""
    task_id, incident_id = await seed(
        agent,
        error="ValidationError: seconds must be >= 0",
        payload={"seconds": -1, "data": [4, 5]},
    )
    agent.llm.responses.clear()
    agent.llm.responses.append(
        canned(Category.BAD_INPUT, modified_payload={"seconds": 0, "data": [4, 5]})
    )
    await agent.core.tick()
    async with agent.factory() as session:
        event = await session.scalar(select(Outbox).where(Outbox.topic == "tasks.pending"))
        message = TaskMessage.model_validate(event.payload)
        assert message.ai_modified
    assert await agent.executor.process(message)
    async with agent.factory() as session:
        task = await session.get(Task, task_id)
        assert task.status == TaskStatus.SUCCEEDED and task.result["sum"] == 9
        assert task.ai_modified and task.ai_requeues == 1
        assert [
            (attempt.generation, attempt.attempt_no)
            for attempt in await session.scalars(
                select(TaskAttempt).order_by(TaskAttempt.generation)
            )
        ] == [(0, 1), (1, 1)]
        assert (await session.get(DeadLetter, incident_id)).ai_diagnosis[
            "action"
        ] == "REQUEUE_MODIFIED"


@pytest.mark.parametrize("mode", ["invalid", "low_confidence", "budget", "kill", "retry_limit"])
async def test_guardrails_escalate_without_unsafe_recovery(agent, mode) -> None:
    """Repair, confidence, budget, kill-switch, and retry ceilings all fail closed."""
    task_id, incident_id = await seed(agent, ai_requeues=1 if mode == "retry_limit" else 0)
    agent.llm.responses.clear()
    if mode == "invalid":
        agent.llm.responses.extend(["{broken", "{}"])
    elif mode == "low_confidence":
        agent.llm.responses.append(canned(confidence=0.2))
    else:
        agent.llm.responses.append(canned())
    if mode in {"budget", "kill"}:
        overrides = {"hourly_token_budget": 0} if mode == "budget" else {"kill_switch": True}
        agent.core.settings = agent.settings.model_copy(
            update={"agent": agent.settings.agent.model_copy(update=overrides)}
        )
    await agent.core.tick()
    async with agent.factory() as session:
        assert (await session.get(Task, task_id)).status == TaskStatus.ESCALATED
        assert (await session.get(DeadLetter, incident_id)).ai_diagnosis["action"] == "ESCALATE"
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
    if mode in {"budget", "kill"}:
        assert not agent.llm.calls
    if mode == "invalid":
        assert len(agent.llm.calls) == 2


async def test_longer_backoff_uses_durable_scheduler_ledger(agent) -> None:
    """Transient recovery waits outside Kafka until its release deadline."""
    task_id, _ = await seed(agent)
    agent.llm.responses.clear()
    agent.llm.responses.append(canned(recommended_backoff_s=42))
    await agent.core.tick()
    async with agent.factory() as session:
        task = await session.get(Task, task_id)
        delayed = await session.get(DelayedTask, task_id)
        assert task.status == TaskStatus.SCHEDULED and task.generation == 1 and task.attempts == 0
        assert delayed.release_at == task.scheduled_at and delayed.dispatched_at is None
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_cluster_repetition_opens_type_circuit(agent) -> None:
    """A cluster of three distinct failed tasks opens the shared circuit and escalates members."""
    identifiers = [await seed(agent) for _ in range(3)]
    await agent.core.tick()
    assert not (await agent.core.breaker.acquire("generate_report", 1000)).allowed
    async with agent.factory() as session:
        for task_id, incident_id in identifiers:
            assert (await session.get(Task, task_id)).status == TaskStatus.ESCALATED
            assert (await session.get(DeadLetter, incident_id)).ai_diagnosis[
                "action"
            ] == "OPEN_CIRCUIT_ESCALATE"
        assert (await session.scalars(select(AgentCluster))).one().circuit_until is not None


async def test_worker_parks_open_type_then_single_probe_recovers(agent) -> None:
    """Open circuits use no attempt and do not hold a Kafka partition while waiting for cooldown."""
    async with agent.factory.begin() as session:
        task = Task(type="generate_report", payload={"seconds": 0, "data": [1]})
        session.add(task)
        await session.flush()
        task_id = task.id
        message = TaskMessage(
            task_id=task.id, type=task.type, payload=task.payload, scheduled_at=task.scheduled_at
        )
    await agent.core.breaker.open("generate_report")
    assert await agent.executor.process(message)
    async with agent.factory() as session:
        task = await session.get(Task, task_id)
        assert task.status == TaskStatus.SCHEDULED and task.attempts == 0
        assert await session.scalar(select(func.count()).select_from(TaskAttempt)) == 0
        assert (await session.get(DelayedTask, task_id)).dispatched_at is None
    await asyncio.sleep(0.2)
    scheduler = SchedulerCore(agent.factory, agent.redis, agent.settings)
    assert await scheduler.lease.acquire()
    try:
        assert await scheduler.release_one(task_id)
    finally:
        await scheduler.lease.release()
    async with agent.factory() as session:
        task = await session.get(Task, task_id)
        message = message.model_copy(update={"scheduled_at": task.scheduled_at})
    assert await agent.executor.process(message)
    assert not await agent.redis.exists(agent.core.breaker.key("generate_report"))
    async with agent.factory() as session:
        assert (await session.get(Task, task_id)).status == TaskStatus.SUCCEEDED


async def test_half_open_probe_is_atomic_renewable_and_fenced(agent) -> None:
    """Only one worker probes; expired owners cannot close the next probe's circuit."""
    breaker = agent.core.breaker
    await breaker.open("probe")
    assert not (await breaker.acquire("probe", 100)).allowed
    await asyncio.sleep(0.2)
    permits = await asyncio.gather(*(breaker.acquire("probe", 100) for _ in range(8)))
    assert sum(item.allowed for item in permits) == 1
    old = next(item for item in permits if item.allowed)
    assert await breaker.renew("probe", old, 100)
    await asyncio.sleep(0.15)
    current = await breaker.acquire("probe", 1000)
    assert current.allowed and current.token != old.token
    await breaker.complete("probe", old, True)
    assert await agent.redis.exists(breaker.key("probe"))
    await breaker.complete("probe", current, False)
    assert not (await breaker.acquire("probe", 1000)).allowed
    await asyncio.sleep(0.2)
    final = await breaker.acquire("probe", 1000)
    await breaker.complete("probe", final, True)
    assert not await agent.redis.exists(breaker.key("probe"))


async def test_hourly_budget_reservations_are_shared_and_idempotent(agent) -> None:
    """Concurrent agents cannot oversubscribe either limit or refund a reservation twice."""
    settings = agent.settings.model_copy(
        update={"agent": agent.settings.agent.model_copy(update={"hourly_token_budget": 100})}
    )
    budget = HourlyBudget(agent.redis, settings)
    results = await asyncio.gather(
        budget.reserve(60, 1), budget.reserve(60, 1), return_exceptions=True
    )
    assert sum(isinstance(item, BudgetExceeded) for item in results) == 1
    reservation = next(item for item in results if not isinstance(item, Exception))
    await budget.settle(reservation, 30, 1)
    await budget.settle(reservation, 0, 0)
    assert int(await agent.redis.hget(reservation.key, "tokens")) == 30
    await budget.reserve(65, 1)
    with pytest.raises(BudgetExceeded):
        await budget.reserve(6, 1)


async def test_redacted_tools_and_audit_never_store_raw_secrets(agent) -> None:
    """Payload fields, error strings, and every tool result are scrubbed before LLM/audit use."""
    secret = agent.settings.postgres.password.get_secret_value()
    _, incident_id = await seed(
        agent,
        payload={"seconds": -1, "password": "private-password", "to": "private@example.com"},
        error=f"ValidationError: password={secret} email=private@example.com",
    )
    collector = Collector(agent.factory, agent.settings, agent.core.redactor, incident_id)
    await collector.gather()
    for tool in (
        "get_task_history",
        "get_similar_failures",
        "get_task_type_stats",
        "get_worker_logs",
    ):
        assert "private@example.com" not in json.dumps(await collector.call(tool, {}))
    with pytest.raises(ValueError, match="scope"):
        await collector.call("get_task_history", {"task_id": str(uuid4())})
    agent.llm.request_tools = True
    agent.llm.responses.clear()
    await agent.core.tick()
    assert "private-password" not in json.dumps(agent.llm.calls) and secret not in json.dumps(
        agent.llm.calls
    )
    async with agent.factory() as session:
        details = list(await session.scalars(select(AgentAudit.details)))
        assert secret not in json.dumps(details) and "private@example.com" not in json.dumps(
            details
        )


async def test_orphan_escalation_does_not_pollute_live_clusters(agent) -> None:
    """Unknown task audits receive a local fallback without consuming a representative LLM call."""
    async with agent.factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        row = DeadLetter(
            task_id=uuid4(),
            task_type="generate_report",
            reason="fatal error",
            failure_signature="b" * 64,
            received_at=now,
            error_history=[],
        )
        session.add(row)
        await session.flush()
        identifier = row.id
    await agent.core.tick()
    assert not agent.llm.calls
    async with agent.factory() as session:
        row = await session.get(DeadLetter, identifier)
        assert row.status == DLQStatus.ESCALATED and row.agent_processed_at is not None
        assert row.ai_diagnosis["provider"] == "none"
        assert await session.scalar(select(func.count()).select_from(AgentCluster)) == 0


async def test_kill_switch_activated_after_diagnosis_still_blocks_action(
    agent, monkeypatch
) -> None:
    """A validated stored diagnosis does not bypass the kill switch during action application."""
    task_id, incident_id = await seed(agent)
    await agent.core.ingest()
    assert await agent.core.diagnose_one()
    monkeypatch.setenv("AGENT_KILL_SWITCH", "true")
    await agent.core.apply_ready()
    async with agent.factory() as session:
        assert (await session.get(Task, task_id)).status == TaskStatus.ESCALATED
        assert (
            "kill switch" in (await session.get(DeadLetter, incident_id)).ai_diagnosis["reasoning"]
        )
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_expired_cluster_claim_is_recovered(agent) -> None:
    """A process dying after claiming leaves durable members recoverable by another instance."""
    _, incident_id = await seed(agent)
    await agent.core.ingest()
    identifier, old_token, _ = await agent.core.claim()
    async with agent.factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        (await session.get(AgentCluster, identifier)).lease_until = now - timedelta(seconds=1)
    standby = AgentCore(agent.factory, agent.redis, agent.settings, lambda: agent.llm)
    assert await standby.diagnose_one()
    await standby.apply_ready()
    async with agent.factory() as session:
        cluster = await session.get(AgentCluster, identifier)
        assert cluster.lease_token != old_token and cluster.status == "DONE"
        assert (await session.get(DeadLetter, incident_id)).status == DLQStatus.RETRIED
    assert len(agent.llm.calls) == 1


async def test_hourly_cost_cap_is_enforced_independently(agent) -> None:
    """A large token allowance cannot bypass the separate micro-dollar cap."""
    settings = agent.settings.model_copy(
        update={
            "agent": agent.settings.agent.model_copy(
                update={"hourly_token_budget": 100000, "hourly_cost_budget_usd": 0.0001}
            )
        }
    )
    budget = HourlyBudget(agent.redis, settings)
    await budget.reserve(10, 60)
    with pytest.raises(BudgetExceeded):
        await budget.reserve(10, 41)


async def test_old_circuit_reconciliation_cannot_replace_newer_trip(agent) -> None:
    """Restoring an older cluster's intent cannot shorten a newer circuit's deadline."""
    breaker = agent.core.breaker
    await breaker.open("ordered", epoch="newer", seconds=10)
    await breaker.open("ordered", epoch="older", seconds=1)
    assert await agent.redis.hget(breaker.key("ordered"), "epoch") == "newer"


async def test_default_mock_handles_fractional_success_rate(agent) -> None:
    """Real collector statistics with repeating decimals survive the egress redaction gate."""
    async with agent.factory.begin() as session:
        task = Task(type="generate_report", payload={"seconds": 0})
        session.add(task)
        await session.flush()
        for status in (TaskStatus.SCHEDULED, TaskStatus.RUNNING, TaskStatus.SUCCEEDED):
            task.status = status
    _, incident_id = await seed(
        agent, error="ValidationError: seconds must be >= 0", payload={"seconds": -1}
    )
    await seed(agent, error="ValidationError: seconds must be >= 0", payload={"seconds": -1})
    agent.llm.responses.clear()
    await agent.core.tick()
    assert len(agent.llm.calls) == 1
    async with agent.factory() as session:
        incident = await session.get(DeadLetter, incident_id)
        assert incident.ai_diagnosis["action"] == "REQUEUE_MODIFIED"
        assert incident.ai_diagnosis["category"] == "bad_input"
