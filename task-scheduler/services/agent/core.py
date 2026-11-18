"""Durable leased failure clusters, one representative diagnosis, and atomic policy actions."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import JsonValue
from redis.asyncio import Redis
from sqlalchemy import func, or_, select, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.agent.audit import AuditLog, escalation_event
from services.agent.collector import Collector
from services.agent.diagnosis import diagnose
from services.agent.guardrails import GuardedCalls, Redactor, bounded
from services.agent.llm import AnthropicClient, LLMClient, MockLLM
from services.agent.policy import decide
from services.agent.schemas import Diagnosis, fallback
from services.worker.retry_policy import event_id
from shared.circuit import CircuitBreaker
from shared.config import Settings
from shared.db import database_now
from shared.kafka import serialize_json
from shared.models import DLQStatus, TaskMessage, TaskStatus
from shared.priority import message_key
from shared.tables import (
    AgentAudit,
    AgentCluster,
    DeadLetter,
    DelayedTask,
    Outbox,
    RetrySchedule,
    Task,
)

logger = logging.getLogger(__name__)


def cluster_identity(
    task_type: str, signature: str, created_at: datetime, window_seconds: int
) -> tuple[UUID, datetime]:
    """Fixed UTC windows are deterministic across replicas, restarts, and local time zones."""
    epoch = int(created_at.timestamp()) // window_seconds * window_seconds
    return uuid5(
        NAMESPACE_URL, f"agent-cluster:v1:{task_type}:{signature}:{epoch}"
    ), datetime.fromtimestamp(epoch, UTC)


class AgentCore:
    """Postgres is the inbox; the Kafka DLQ consumer has already durably acknowledged input."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        redis: Redis,
        settings: Settings,
        llm_factory: Callable[[], LLMClient] | None = None,
    ) -> None:
        self.factory, self.redis, self.settings = factory, redis, settings
        self.redactor = Redactor(
            [settings.llm.api_key.get_secret_value(), settings.postgres.password.get_secret_value()]
        )
        self.llm_factory = llm_factory or (
            lambda: MockLLM() if settings.llm.provider == "mock" else AnthropicClient(settings.llm)
        )
        self.breaker = CircuitBreaker(redis, settings.circuit)
        self.stop_event = asyncio.Event()
        self.running = False
        self.circuit_cursor: UUID | None = None

    async def ingest(self) -> int:
        """Assign received OPEN incidents once; old migration-era incidents opt out by default."""
        count = 0
        async with self.factory.begin() as session:
            now = await database_now(session)
            rows = list(
                await session.scalars(
                    select(DeadLetter)
                    .where(
                        DeadLetter.agent_eligible.is_(True),
                        DeadLetter.agent_cluster_id.is_(None),
                        DeadLetter.agent_processed_at.is_(None),
                        DeadLetter.status == DLQStatus.OPEN,
                        DeadLetter.received_at.is_not(None),
                        DeadLetter.failure_signature.is_not(None),
                    )
                    .order_by(DeadLetter.received_at, DeadLetter.id)
                    .limit(self.settings.agent.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for row in rows:
                if await session.get(Task, row.task_id) is None:
                    reason = "Original task is missing; audit retained for manual investigation"
                    row.agent_processed_at = now
                    row.status, row.resolution = DLQStatus.ESCALATED, "ESCALATED"
                    row.ai_diagnosis = {
                        **fallback(reason).model_dump(mode="json"),
                        "action": "ESCALATE",
                        "reasoning": reason,
                        "provider": "none",
                        "prompt_version": self.settings.agent.prompt_version,
                        "usage": {"input_tokens": 0, "output_tokens": 0, "micro_usd": 0},
                    }
                    session.add(
                        AgentAudit(
                            dead_letter_id=row.id,
                            event="ACTION_TAKEN",
                            details={"action": "ESCALATE", "reasoning": reason},
                        )
                    )
                    escalation_event(session, None, row.id, reason)
                    count += 1
                    continue
                identifier, start = cluster_identity(
                    row.task_type,
                    row.failure_signature or "",
                    row.created_at,
                    self.settings.agent.cluster_window_seconds,
                )
                await session.execute(
                    insert(AgentCluster)
                    .values(
                        id=identifier,
                        task_type=row.task_type,
                        failure_signature=row.failure_signature,
                        window_start=start,
                        ready_at=now
                        + timedelta(seconds=self.settings.agent.cluster_debounce_seconds),
                        representative_id=row.id,
                    )
                    .on_conflict_do_nothing()
                )
                row.agent_cluster_id = identifier
                session.add(
                    AgentAudit(
                        cluster_id=identifier,
                        dead_letter_id=row.id,
                        event="CLUSTER_ASSIGNED",
                        details={
                            "task_id": str(row.task_id),
                            "signature": row.failure_signature,
                            "window_start": start.isoformat(),
                        },
                    )
                )
                count += 1
        return count

    async def claim(self) -> tuple[UUID, UUID, UUID] | None:
        """Lease one cluster; NO KEY UPDATE allows new member FK references during diagnosis."""
        async with self.factory.begin() as session:
            now = await database_now(session)
            cluster = await session.scalar(
                select(AgentCluster)
                .where(
                    AgentCluster.ready_at <= now,
                    or_(
                        AgentCluster.status == "NEW",
                        (AgentCluster.status == "PROCESSING") & (AgentCluster.lease_until <= now),
                    ),
                )
                .order_by(AgentCluster.ready_at)
                .limit(1)
                .with_for_update(skip_locked=True, key_share=True)
            )
            if cluster is None:
                return None
            representative = await session.scalar(
                select(DeadLetter.id)
                .join(Task, Task.id == DeadLetter.task_id)
                .where(
                    DeadLetter.agent_cluster_id == cluster.id,
                    DeadLetter.status == DLQStatus.OPEN,
                    Task.generation == DeadLetter.generation,
                    Task.status == TaskStatus.DEAD_LETTERED,
                )
                .order_by(DeadLetter.created_at, DeadLetter.id)
                .limit(1)
            )
            cluster.representative_id = representative or cluster.representative_id
            token = uuid4()
            cluster.status, cluster.lease_token = "PROCESSING", token
            cluster.lease_until = now + timedelta(seconds=self.settings.agent.lease_seconds)
            session.add(
                AgentAudit(
                    cluster_id=cluster.id,
                    event="CLUSTER_CLAIMED",
                    details={
                        "lease_token": str(token),
                        "representative_id": str(cluster.representative_id),
                    },
                )
            )
            if cluster.representative_id is None:
                raise RuntimeError("Claimed cluster has no representative")
            return cluster.id, token, cluster.representative_id

    async def diagnose_one(self) -> bool:
        """Persist the validated representative result before any cluster member is acted on."""
        claimed = await self.claim()
        if claimed is None:
            return False
        identifier, token, representative = claimed
        collector = Collector(self.factory, self.settings, self.redactor, representative)
        audit = AuditLog(self.factory, self.redactor, identifier)
        evidence = await collector.gather()
        evidence_task = evidence.get("task") or {}
        audit.correlation = {
            "task_id": evidence_task.get("id"),
            "trace_id": evidence_task.get("trace_id"),
        }
        await audit.record("COLLECTED", bounded(evidence, self.settings.agent.context_max_bytes))
        client = self.llm_factory()
        calls = GuardedCalls(client, self.redis, self.settings, self.redactor, audit.record)
        try:
            result = await diagnose(collector, calls, evidence)
        finally:
            await client.aclose()
        async with self.factory.begin() as session:
            cluster = await session.scalar(
                select(AgentCluster)
                .where(AgentCluster.id == identifier)
                .with_for_update(key_share=True)
            )
            now = await database_now(session)
            if (
                cluster is None
                or cluster.lease_until is None
                or cluster.lease_token != token
                or cluster.lease_until <= now
            ):
                await audit.record("LEASE_LOST", {"reason": "Discarded stale diagnosis owner"})
                return False
            cluster.diagnosis = self.redactor.redact(result.diagnosis.model_dump(mode="json"))
            cluster.guard_reason = result.guard_reason
            cluster.representative_payload = (evidence.get("task") or {}).get("payload")
            usage: dict[str, JsonValue] = {key: value for key, value in calls.usage.items()}
            cluster.usage = usage
            cluster.status = "DONE"
            cluster.lease_until = None
        return True

    async def apply(self, identifier: UUID) -> int:
        """Commit diagnosis, state, recovery intent, and decision audit in one member batch."""
        processed = 0
        async with self.factory.begin() as session:
            cluster = await session.scalar(
                select(AgentCluster)
                .where(AgentCluster.id == identifier)
                .with_for_update(key_share=True)
            )
            if cluster is None or cluster.status != "DONE":
                return 0
            diagnosis = Diagnosis.model_validate(cluster.diagnosis)
            now = await database_now(session)
            failures = await session.scalar(
                select(func.count(func.distinct(DeadLetter.task_id)))
                .join(Task, Task.id == DeadLetter.task_id)
                .where(DeadLetter.agent_cluster_id == identifier)
            )
            members = (
                await session.execute(
                    select(DeadLetter.id, DeadLetter.task_id)
                    .where(
                        DeadLetter.agent_cluster_id == identifier,
                        DeadLetter.agent_processed_at.is_(None),
                    )
                    .order_by(DeadLetter.task_id, DeadLetter.id)
                    .limit(self.settings.agent.batch_size)
                )
            ).all()
            for incident_id, task_id in members:
                task = await session.scalar(
                    select(Task).where(Task.id == task_id).with_for_update()
                )
                incident = await session.scalar(
                    select(DeadLetter).where(DeadLetter.id == incident_id).with_for_update()
                )
                if incident is None or incident.agent_processed_at is not None:
                    continue
                if incident.status != DLQStatus.OPEN or (
                    task is not None
                    and (
                        task.generation != incident.generation
                        or task.status != TaskStatus.DEAD_LETTERED
                    )
                ):
                    incident.agent_processed_at = now
                    session.add(
                        AgentAudit(
                            cluster_id=identifier,
                            dead_letter_id=incident_id,
                            event="ACTION_SKIPPED",
                            details={"reason": "Incident resolved or execution generation changed"},
                        )
                    )
                    processed += 1
                    continue
                previous = await session.scalar(
                    select(RetrySchedule)
                    .where(
                        RetrySchedule.task_id == task_id,
                        RetrySchedule.generation == incident.generation,
                    )
                    .order_by(RetrySchedule.failed_attempt.desc())
                    .limit(1)
                )
                previous_delay = (
                    max(0, (previous.due_at - previous.created_at).total_seconds())
                    if previous
                    else 0
                )
                guard_reason = cluster.guard_reason
                if task is None:
                    guard_reason = (
                        "Original task is missing; audit retained for manual investigation"
                    )
                if self.settings.agent.kill_switch:
                    guard_reason = "Agent kill switch is enabled"
                decision = decide(
                    diagnosis,
                    self.settings,
                    failures=failures or 0,
                    ai_requeues=task.ai_requeues if task else 0,
                    current_payload=self.redactor.redact(task.payload) if task else {},
                    representative_payload=cluster.representative_payload,
                    previous_delay_s=previous_delay,
                    guard_reason=guard_reason,
                )
                if decision.action.startswith("REQUEUE") and task is not None:
                    payload = (
                        diagnosis.modified_payload
                        if decision.action == "REQUEUE_MODIFIED"
                        else task.payload
                    )
                    assert payload is not None, "Modified requeue requires a payload"
                    message = TaskMessage(
                        task_id=task.id,
                        generation=task.generation + 1,
                        type=task.type,
                        payload=payload,
                        priority=task.priority,
                        max_retries=task.max_retries,
                        scheduled_at=now + timedelta(seconds=decision.backoff_s),
                        created_at=task.created_at,
                        ai_modified=decision.action == "REQUEUE_MODIFIED" or task.ai_modified,
                    )
                    if len(serialize_json(message)) > self.settings.api.max_task_message_bytes:
                        from services.agent.schemas import Decision

                        decision = Decision(
                            action="ESCALATE",
                            reasoning="AI replacement exceeds task message size limit",
                        )
                if decision.action == "OPEN_CIRCUIT_ESCALATE" and cluster.circuit_until is None:
                    cluster.circuit_until = now + timedelta(
                        seconds=self.settings.circuit.cooldown_seconds
                    )
                    await self.breaker.open(cluster.task_type, epoch=str(cluster.id))
                if task is not None:
                    task.status = TaskStatus.AI_DIAGNOSED
                    if decision.action.startswith("REQUEUE"):
                        task.status = TaskStatus.REQUEUED
                        task.generation = message.generation
                        task.attempts = 0
                        task.ai_requeues += 1
                        task.ai_modified = message.ai_modified
                        task.payload, task.result, task.last_error = message.payload, None, None
                        task.scheduled_at = message.scheduled_at
                        task.status = TaskStatus.PENDING
                        if decision.backoff_s > 0:
                            task.status = TaskStatus.SCHEDULED
                            await session.execute(
                                insert(DelayedTask)
                                .values(task_id=task.id, release_at=task.scheduled_at)
                                .on_conflict_do_update(
                                    index_elements=[DelayedTask.task_id],
                                    set_={"release_at": task.scheduled_at, "dispatched_at": None},
                                )
                            )
                        else:
                            session.add(
                                Outbox(
                                    id=event_id(task.id, 0, "ai-retry", task.generation),
                                    topic="tasks.pending",
                                    key=message_key(task.id, task.priority),
                                    payload=message.model_dump(mode="json"),
                                )
                            )
                    else:
                        task.status = TaskStatus.ESCALATED
                if decision.action.startswith("REQUEUE"):
                    incident.status, incident.resolution = DLQStatus.RETRIED, "REQUEUED"
                else:
                    incident.status, incident.resolution = DLQStatus.ESCALATED, "ESCALATED"
                    escalation_event(session, identifier, incident_id, decision.reasoning)
                incident.ai_diagnosis = self.redactor.redact(
                    {
                        **diagnosis.model_dump(mode="json"),
                        "action": decision.action,
                        "reasoning": decision.reasoning,
                        "cluster_id": str(identifier),
                        "prompt_version": self.settings.agent.prompt_version,
                        "provider": self.settings.llm.provider,
                        "model": self.settings.llm.model
                        if self.settings.llm.provider != "mock"
                        else "MockLLM",
                        "usage": cluster.usage,
                        "decided_at": now.isoformat(),
                    }
                )
                incident.agent_processed_at = now
                logger.info(
                    "Agent decision action=%s",
                    decision.action,
                    extra={"task_id": str(task_id), "trace_id": incident.trace_id.hex},
                )
                session.add(
                    AgentAudit(
                        cluster_id=identifier,
                        dead_letter_id=incident_id,
                        event="ACTION_TAKEN",
                        details=self.redactor.redact(
                            {
                                "task_id": str(task_id),
                                "generation": incident.generation,
                                **decision.model_dump(mode="json"),
                            }
                        ),
                    )
                )
                processed += 1
        return processed

    async def reconcile_circuits(self) -> None:
        """Restore committed circuit intent after Redis loss without extending its DB deadline."""
        async with self.factory() as session:
            now = await database_now(session)
            rows = list(
                await session.scalars(
                    select(AgentCluster)
                    .where(
                        AgentCluster.circuit_until > now,
                        AgentCluster.id > self.circuit_cursor if self.circuit_cursor else true(),
                    )
                    .order_by(AgentCluster.id)
                    .limit(self.settings.agent.batch_size)
                )
            )
            for cluster in rows:
                if cluster.circuit_until is None:
                    continue
                await self.breaker.open(
                    cluster.task_type,
                    epoch=str(cluster.id),
                    seconds=(cluster.circuit_until - now).total_seconds(),
                )
            self.circuit_cursor = rows[-1].id if rows else None

    async def apply_ready(self) -> int:
        """Apply cached diagnoses to new/late members without another model call."""
        async with self.factory() as session:
            ids = list(
                await session.scalars(
                    select(AgentCluster.id)
                    .join(DeadLetter, DeadLetter.agent_cluster_id == AgentCluster.id)
                    .where(AgentCluster.status == "DONE", DeadLetter.agent_processed_at.is_(None))
                    .distinct()
                    .limit(self.settings.agent.batch_size)
                )
            )
        total = 0
        for identifier in ids:
            total += await self.apply(identifier)
        return total

    async def tick(self) -> None:
        """One bounded inbox/diagnosis/action pass, independently restartable at every boundary."""
        await self.ingest()
        await self.diagnose_one()
        await self.apply_ready()
        await self.reconcile_circuits()

    async def run(self) -> None:
        """Keep durable state recoverable on provider, database, or Redis failures."""
        self.running = True
        try:
            while not self.stop_event.is_set():
                if self.settings.agent.enabled:
                    try:
                        await self.tick()
                    except Exception:
                        logger.exception("Agent pass failed; durable inbox and leases retained")
                try:
                    await asyncio.wait_for(self.stop_event.wait(), self.settings.agent.poll_seconds)
                except TimeoutError:
                    pass
        finally:
            self.running = False
