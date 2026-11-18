"""Offline agent inbox, collector scope, cached actions, and restart-safe orchestration."""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fakes import NOW, Factory, Session, attempt, letter, task

from services.agent.collector import Collector
from services.agent.core import AgentCore
from services.agent.guardrails import Redactor
from services.agent.llm import MockLLM
from services.agent.schemas import Category, Diagnosis
from shared.config import AgentSettings, get_settings
from shared.models import DLQStatus, TaskStatus
from shared.tables import AgentAudit, AgentCluster


def diagnosis(category=Category.TRANSIENT, **updates):
    data = dict(
        root_cause="Dependency failed",
        category=category,
        confidence=0.95,
        suggested_fix="Retry safely",
        safe_to_auto_retry=True,
        modified_payload=None,
        recommended_backoff_s=0,
    )
    return Diagnosis(**{**data, **updates})


def cluster(row, incident, **updates):
    data = dict(
        id=uuid4(),
        task_type=row.type,
        failure_signature="a" * 64,
        window_start=NOW,
        ready_at=NOW,
        status="DONE",
        representative_id=incident.id,
        representative_payload=row.payload,
        diagnosis=diagnosis().model_dump(mode="json"),
        guard_reason=None,
        usage={},
        circuit_until=None,
        lease_token=uuid4(),
        lease_until=NOW + timedelta(seconds=180),
    )
    data.update(updates)
    return AgentCluster(**data)


def core(session, **agent_options):
    settings = get_settings().model_copy(
        update={"agent": AgentSettings(_env_file=None, min_backoff_seconds=0, **agent_options)}
    )
    redis = AsyncMock()
    from services.agent.guardrails import RESERVE

    redis.eval.side_effect = lambda script, *args: [1, "unit-budget"] if script == RESERVE else 1
    return AgentCore(Factory(session), redis, settings, lambda: MockLLM([diagnosis()]))


@pytest.mark.parametrize("orphan", [False, True])
async def test_agent_assigns_cluster_or_escalates_orphan_without_model(orphan):
    row = task(TaskStatus.DEAD_LETTERED)
    incident = letter(row)
    session = Session(scalars=[[incident]], get=[None if orphan else row])
    agent = core(session)
    assert await agent.ingest() == 1
    if orphan:
        assert (
            incident.status == DLQStatus.ESCALATED and incident.ai_diagnosis["provider"] == "none"
        )
    else:
        assert incident.agent_cluster_id is not None
    assert any(isinstance(x, AgentAudit) for x in session.added)


async def test_agent_claim_is_leased_and_empty_inbox_is_noop():
    row = task()
    incident = letter(row)
    group = cluster(row, incident, status="NEW")
    session = Session(scalar=[None, group, incident.id])
    agent = core(session)
    assert await agent.claim() is None
    identifier, token, representative = await agent.claim()
    assert identifier == group.id and token == group.lease_token and representative == incident.id
    assert group.status == "PROCESSING" and group.lease_until > NOW


@pytest.mark.parametrize("category", list(Category))
async def test_cached_diagnosis_applies_only_allowed_category_actions(category):
    row = task(TaskStatus.DEAD_LETTERED, payload={"seconds": -1})
    incident = letter(row)
    value = diagnosis(
        category,
        modified_payload={"seconds": 0} if category == Category.BAD_INPUT else None,
        recommended_backoff_s=5 if category == Category.DEPENDENCY_DOWN else 0,
    )
    group = cluster(row, incident, diagnosis=value.model_dump(mode="json"))
    session = Session(scalar=[group, 1, row, incident, None], execute=[[(incident.id, row.id)], []])
    agent = core(session)
    agent.breaker.open = AsyncMock()
    assert await agent.apply(group.id) == 1
    can_retry = category in {Category.TRANSIENT, Category.DEPENDENCY_DOWN, Category.BAD_INPUT}
    assert incident.ai_diagnosis["action"].startswith("REQUEUE") == can_retry
    if can_retry:
        assert row.generation == 1 and row.ai_requeues == 1
        assert row.status in {TaskStatus.PENDING, TaskStatus.SCHEDULED}
        if category == Category.BAD_INPUT:
            assert row.payload == {"seconds": 0} and row.ai_modified
    else:
        assert row.status == TaskStatus.ESCALATED


@pytest.mark.parametrize(
    "case",
    [
        "circuit",
        "low_confidence",
        "kill",
        "missing_task",
        "closed",
        "missing_incident",
        "already_processed",
        "oversized",
        "no_cluster",
    ],
)
async def test_action_guards_never_apply_unreviewed_model_changes(case):
    row = task(TaskStatus.DEAD_LETTERED)
    incident = letter(row)
    value = diagnosis(
        confidence=0.2 if case == "low_confidence" else 0.95,
        category=Category.BAD_INPUT if case == "oversized" else Category.TRANSIENT,
        modified_payload={"text": "x" * 10000} if case == "oversized" else None,
    )
    group = cluster(row, incident, diagnosis=value.model_dump(mode="json"))
    if case == "closed":
        incident.status = DLQStatus.DISCARDED
    if case == "already_processed":
        incident.agent_processed_at = NOW
    session = Session(
        scalar=[None]
        if case == "no_cluster"
        else [
            group,
            3 if case == "circuit" else 1,
            None if case == "missing_task" else row,
            None if case == "missing_incident" else incident,
            None,
        ],
        execute=[[(incident.id, row.id)], []],
    )
    agent = core(session, kill_switch=case == "kill")
    agent.breaker.open = AsyncMock()
    if case == "oversized":
        agent.settings = agent.settings.model_copy(
            update={"api": agent.settings.api.model_copy(update={"max_task_message_bytes": 1024})}
        )
    count = await agent.apply(group.id)
    if case in {"no_cluster", "missing_incident", "already_processed"}:
        assert count == 0
    elif case == "closed":
        assert incident.agent_processed_at == NOW
    else:
        assert count == 1 and incident.ai_diagnosis["action"].endswith("ESCALATE")
        if case == "circuit":
            agent.breaker.open.assert_awaited_once()
            assert group.circuit_until > NOW


async def test_collector_scope_stats_tool_validation_and_fallback_history():
    row = task(TaskStatus.DEAD_LETTERED)
    incident = letter(row, error_history=[{"error": "old", "stack_trace": "trace"}])
    session = Session(
        get=[incident, row, incident, None, incident],
        scalars=[[attempt(row)], [], []],
        execute=[(3, 4)],
    )
    collector = Collector(Factory(session), get_settings(), Redactor([]), incident.id)
    await collector.initialize()
    history = await collector.history()
    assert history["task"]["trace_id"] == row.trace_id.hex
    missing = await collector.history()
    assert missing["task"] is None and missing["errors"][0]["error"] == "old"
    similar = await collector.similar()
    assert "failures" in similar
    stats = await collector.stats()
    assert stats["success_rate"] == 0.75
    collector.history = AsyncMock(return_value={"errors": []})
    collector.similar = AsyncMock(return_value={})
    collector.stats = AsyncMock(return_value={})
    for name in (
        "get_task_history",
        "get_similar_failures",
        "get_task_type_stats",
        "get_worker_logs",
    ):
        assert isinstance(await collector.call(name, {}), dict)
    with pytest.raises(ValueError):
        await collector.call("get_task_history", {"task_id": str(uuid4())})
    with pytest.raises(ValueError):
        await collector.call("untrusted_tool", {})
    collector.initialize = AsyncMock()
    collector.history = AsyncMock(return_value={"task": {"id": str(row.id)}, "errors": []})
    assert "task" in await collector.gather()
    session.get.side_effect = None
    session.get.return_value = None
    another = Collector(Factory(session), get_settings(), Redactor([]), uuid4())
    with pytest.raises(LookupError):
        await another.initialize()


@pytest.mark.parametrize("stale", [False, True])
async def test_diagnosis_is_persisted_only_by_live_owner(monkeypatch, stale):
    from services.agent import core as module

    row = task()
    incident = letter(row)
    group = cluster(row, incident)
    token = group.lease_token
    if stale:
        group.lease_until = NOW - timedelta(seconds=1)
    session = Session(scalar=[group])
    agent = core(session)
    agent.claim = AsyncMock(return_value=(group.id, token, incident.id))
    collector = type(
        "Collector",
        (),
        {
            "gather": AsyncMock(
                return_value={
                    "task": {"id": str(row.id), "trace_id": row.trace_id.hex, "payload": {}},
                    "errors": [],
                }
            ),
            "encode": staticmethod(lambda value: "{}"),
        },
    )()
    monkeypatch.setattr(module, "Collector", lambda *args: collector)
    assert await agent.diagnose_one() == (not stale)
    if not stale:
        assert group.status == "DONE" and group.diagnosis["category"] == "transient", (
            group.guard_reason
        )
    agent.claim.return_value = None
    assert not await agent.diagnose_one()


async def test_agent_restores_circuit_deadline_and_runs_bounded_pass():
    row = task()
    incident = letter(row)
    group = cluster(row, incident, circuit_until=NOW + timedelta(seconds=5))
    session = Session(scalars=[[group], [group.id]])
    agent = core(session)
    agent.breaker.open = AsyncMock()
    await agent.reconcile_circuits()
    assert agent.circuit_cursor == group.id
    agent.apply = AsyncMock(return_value=1)
    assert await agent.apply_ready() == 1
    for name in ("ingest", "diagnose_one", "apply_ready", "reconcile_circuits"):
        setattr(agent, name, AsyncMock())
    await agent.tick()
    agent.diagnose_one.assert_awaited_once()
    agent.tick = AsyncMock(side_effect=RuntimeError("temporary"))
    runner = asyncio.create_task(agent.run())
    await asyncio.sleep(0.01)
    agent.stop_event.set()
    await runner
    assert not agent.running
