"""DLQ inspection, bounded bulk actions, and atomic generation-fenced manual retry."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from datetime import date as Date
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import AwareDatetime, Field, JsonValue, model_validator
from sqlalchemy import distinct, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.api.schemas import AttemptOut
from services.worker.retry_policy import event_id
from shared.db import database_now
from shared.kafka import serialize_json
from shared.models import DeadLetterOut, DLQStatus, JSONSchema, TaskMessage, TaskOut, TaskStatus
from shared.priority import message_key
from shared.tables import DeadLetter, Outbox, Task, TaskAttempt

router = APIRouter(prefix="/dlq", tags=["dlq"])


class DLQFilters(JSONSchema):
    """Incident filters; dates refer to original failure time in UTC."""

    type: str | None = Field(default=None, max_length=128)
    category: str | None = Field(default=None, max_length=32)
    status: DLQStatus | None = DLQStatus.OPEN
    date: Date | None = None
    date_from: AwareDatetime | None = None
    date_to: AwareDatetime | None = None
    failure_signature: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def valid_range(self) -> DLQFilters:
        """Reject reversed date ranges instead of silently returning an empty queue."""
        if self.date_from and self.date_to and self.date_from >= self.date_to:
            raise ValueError("date_from must be before date_to")
        return self


class DLQQuery(DLQFilters):
    """A bounded incident page; default results are OPEN only."""

    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class DLQPage(JSONSchema):
    """Queue page with an independent total count."""

    items: list[DeadLetterOut]
    total: int
    limit: int
    offset: int


class DLQDetail(DeadLetterOut):
    """Latest incident, earlier incident history, and all execution generations."""

    task: TaskOut | None
    incidents: list[DeadLetterOut]
    attempt_history: list[AttemptOut]


class RetryRequest(JSONSchema):
    """Omitted payload preserves it; a supplied object fully replaces it."""

    payload: dict[str, JsonValue] | None = None


class BulkRetry(DLQFilters):
    """One bounded batch of distinct tasks matching the queue filters."""

    limit: int = Field(default=100, ge=1, le=500)
    payload: dict[str, JsonValue] | None = None


def predicates(filters: DLQFilters) -> list:
    """Use SQL expressions and bound values for all caller-supplied filters."""
    expressions = []
    for field, column in (
        ("type", DeadLetter.task_type),
        ("category", DeadLetter.category),
        ("status", DeadLetter.status),
        ("failure_signature", DeadLetter.failure_signature),
    ):
        value = getattr(filters, field)
        if value is not None:
            expressions.append(column == value)
    if filters.date:
        start = datetime.combine(filters.date, time.min, tzinfo=UTC)
        expressions.extend(
            [DeadLetter.created_at >= start, DeadLetter.created_at < start + timedelta(days=1)]
        )
    if filters.date_from:
        expressions.append(DeadLetter.created_at >= filters.date_from)
    if filters.date_to:
        expressions.append(DeadLetter.created_at < filters.date_to)
    return expressions


async def latest(
    session: AsyncSession, task_id: UUID, *, locked: bool = False, include_discarded: bool = False
) -> DeadLetter | None:
    """Select the latest generation first, independently of out-of-order Kafka receipt."""
    query = select(DeadLetter).where(DeadLetter.task_id == task_id)
    query = query.order_by(
        DeadLetter.generation.desc(), DeadLetter.created_at.desc(), DeadLetter.id.desc()
    ).limit(1)
    incident = await session.scalar(query.with_for_update() if locked else query)
    if incident is not None and incident.status == DLQStatus.DISCARDED and not include_discarded:
        return None
    return incident


async def retry_task(
    factory: async_sessionmaker[AsyncSession],
    task_id: UUID,
    payload: dict[str, JsonValue] | None,
    max_bytes: int,
    expected_incident: UUID | None = None,
) -> tuple[TaskOut, bool]:
    """Reset current counters while retaining immutable old attempts and outcome events."""
    async with factory.begin() as session:
        task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
        incident = await latest(session, task_id, locked=True)
        if incident is None:
            raise HTTPException(404, "DLQ task not found")
        if task is None:
            raise HTTPException(409, "Original task no longer exists; its DLQ audit is retained")
        if expected_incident is not None and incident.id != expected_incident:
            raise HTTPException(409, "The latest incident no longer matches the selected filter")
        if incident.status == DLQStatus.RETRIED and task.generation > incident.generation:
            return TaskOut.model_validate(task), False
        if (
            incident.status != DLQStatus.OPEN
            or incident.generation != task.generation
            or task.status not in {TaskStatus.DEAD_LETTERED, TaskStatus.AI_DIAGNOSED}
        ):
            raise HTTPException(409, "Task is not an open, delivered DLQ incident")
        now = await database_now(session)
        generation = task.generation + 1
        message = TaskMessage(
            task_id=task.id,
            generation=generation,
            ai_modified=task.ai_modified if payload is None else False,
            type=task.type,
            payload=task.payload if payload is None else payload,
            priority=task.priority,
            max_retries=task.max_retries,
            attempt_no=1,
            scheduled_at=now,
            created_at=task.created_at,
        )
        if len(serialize_json(message)) > max_bytes:
            raise HTTPException(413, "Serialized task message exceeds the configured size limit")
        task.status = TaskStatus.REQUEUED
        task.generation = generation
        task.attempts = 0
        task.payload = message.payload
        task.ai_modified = message.ai_modified
        task.result = None
        task.last_error = None
        task.scheduled_at = now
        task.status = TaskStatus.PENDING
        await session.execute(
            update(DeadLetter)
            .where(
                DeadLetter.task_id == task_id,
                DeadLetter.generation == incident.generation,
                DeadLetter.status == DLQStatus.OPEN,
            )
            .values(status=DLQStatus.RETRIED, resolution="REQUEUED")
        )
        session.add(
            Outbox(
                id=event_id(task.id, 0, "manual-retry", generation),
                topic="tasks.pending",
                key=message_key(task.id, task.priority),
                payload=message.model_dump(mode="json"),
            )
        )
        await session.flush()
        result = TaskOut.model_validate(task)
    return result, True


@router.get("", response_model=DLQPage)
async def list_dlq(request: Request, filters: Annotated[DLQQuery, Query()]) -> DLQPage:
    """List incident snapshots by task type, category, queue status, signature, and date."""
    conditions = predicates(filters)
    async with request.app.state.session_factory() as session:
        total = await session.scalar(
            select(func.count()).select_from(DeadLetter).where(*conditions)
        )
        rows = await session.scalars(
            select(DeadLetter)
            .where(*conditions)
            .order_by(DeadLetter.created_at.desc(), DeadLetter.id.desc())
            .limit(filters.limit)
            .offset(filters.offset)
        )
        return DLQPage(
            items=[DeadLetterOut.model_validate(row) for row in rows],
            total=total or 0,
            limit=filters.limit,
            offset=filters.offset,
        )


@router.post("/retry-bulk")
async def retry_bulk(body: BulkRetry, request: Request) -> dict:
    """Retry distinct matching tasks in bounded independent transactions, reporting skips."""
    factory = request.app.state.session_factory
    conditions = predicates(body)
    limit = min(body.limit, request.app.state.settings.dlq.bulk_limit)
    async with factory() as session:
        matched = await session.scalar(
            select(func.count(distinct(DeadLetter.task_id))).where(*conditions)
        )
        candidates = (
            await session.execute(
                select(DeadLetter.task_id, DeadLetter.id)
                .where(*conditions)
                .distinct(DeadLetter.task_id)
                .order_by(
                    DeadLetter.task_id,
                    DeadLetter.generation.desc(),
                    DeadLetter.created_at.desc(),
                    DeadLetter.id.desc(),
                )
                .limit(limit)
            )
        ).all()
    retried, skipped = [], []
    for identifier, incident_id in candidates:
        try:
            task, created = await retry_task(
                factory,
                identifier,
                body.payload,
                request.app.state.settings.api.max_task_message_bytes,
                expected_incident=incident_id,
            )
            if created:
                retried.append(str(task.id))
            else:
                skipped.append({"task_id": str(identifier), "reason": "Already retried"})
        except HTTPException as exc:
            skipped.append({"task_id": str(identifier), "reason": str(exc.detail)})
    return {
        "matched": matched or 0,
        "retried": len(retried),
        "skipped": skipped,
        "task_ids": retried,
    }


@router.get("/{task_id}", response_model=DLQDetail)
async def get_dlq(task_id: UUID, request: Request) -> DLQDetail:
    """Return full incident/attempt history and preserve any attached AI diagnosis."""
    async with request.app.state.session_factory() as session:
        incident = await latest(session, task_id)
        if incident is None:
            raise HTTPException(404, "DLQ task not found")
        task = await session.get(Task, task_id)
        incidents = await session.scalars(
            select(DeadLetter)
            .where(DeadLetter.task_id == task_id)
            .order_by(DeadLetter.generation, DeadLetter.created_at)
        )
        attempts = await session.scalars(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.generation, TaskAttempt.attempt_no)
        )
        return DLQDetail(
            **DeadLetterOut.model_validate(incident).model_dump(),
            task=TaskOut.model_validate(task) if task is not None else None,
            incidents=[DeadLetterOut.model_validate(item) for item in incidents],
            attempt_history=[AttemptOut.model_validate(item) for item in attempts],
        )


@router.post("/{task_id}/retry", response_model=TaskOut)
async def retry_one(task_id: UUID, request: Request, body: RetryRequest | None = None) -> TaskOut:
    """Return the task to PENDING with a new execution generation and optional corrected payload."""
    result, _ = await retry_task(
        request.app.state.session_factory,
        task_id,
        body.payload if body is not None else None,
        request.app.state.settings.api.max_task_message_bytes,
    )
    return result


@router.delete("/{task_id}", status_code=204)
async def discard(task_id: UUID, request: Request) -> Response:
    """Tombstone the latest incident so replay cannot recreate it or cancel a newer run."""
    async with request.app.state.session_factory.begin() as session:
        task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
        incident = await latest(session, task_id, locked=True, include_discarded=True)
        if incident is None:
            raise HTTPException(404, "DLQ task not found")
        await session.execute(
            update(DeadLetter)
            .where(
                DeadLetter.task_id == task_id,
                DeadLetter.generation == incident.generation,
            )
            .values(status=DLQStatus.DISCARDED, resolution="CANCELLED")
        )
        if task is not None and task.generation == incident.generation:
            if task.status == TaskStatus.FAILED:
                task.status = TaskStatus.DEAD_LETTERED
            if task.status in {TaskStatus.DEAD_LETTERED, TaskStatus.AI_DIAGNOSED}:
                task.status = TaskStatus.CANCELLED
    return Response(status_code=204)
