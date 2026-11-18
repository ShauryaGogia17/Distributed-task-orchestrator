"""Fenced task execution with transactional retry and dead-letter outcomes."""

from __future__ import annotations

import asyncio
import logging
import traceback
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from prometheus_client import Gauge
from pydantic import JsonValue, TypeAdapter
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.worker.registry import HandlerContext, get_handler
from services.worker.retry_policy import RetryPolicy, event_id, result_event
from shared.circuit import CircuitBreaker, Permit
from shared.config import Settings
from shared.db import database_now
from shared.errors import RetryableError
from shared.kafka import serialize_json
from shared.logging import log_context
from shared.models import TaskMessage, TaskStatus
from shared.redis import RELEASE_IF_OWNER, RENEW_IF_OWNER, evaluate
from shared.tables import DelayedTask, Outbox, Task, TaskAttempt

logger = logging.getLogger(__name__)
JSON_VALUE: TypeAdapter[JsonValue] = TypeAdapter(cast(Any, JsonValue))
FINISHED = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.DEAD_LETTERED,
        TaskStatus.AI_DIAGNOSED,
        TaskStatus.ESCALATED,
    }
)


class LeaseLost(RuntimeError):
    """A stale owner cannot complete a task whose lease it no longer owns."""


@dataclass(frozen=True)
class Execution:
    """A committed attempt and the token fencing its completion."""

    task_id: UUID
    task_type: str
    payload: dict[str, JsonValue]
    attempt_no: int
    token: str
    generation: int = 0
    circuit_epoch: str = ""
    circuit_token: str = ""
    trace_id: UUID | None = None


@dataclass(frozen=True)
class Completed:
    """A duplicate or finished dispatch and its durable publication dependencies."""

    event_ids: tuple[UUID, ...] = ()


def result_event_id(task_id: UUID, attempt_no: int) -> UUID:
    """Retain the result identity used by earlier phases."""
    return event_id(task_id, attempt_no, "result")


class TaskExecutor:
    """Run one fenced attempt, retaining all outcome events until broker publication."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        redis: Redis,
        settings: Settings,
        context: HandlerContext,
    ) -> None:
        self.factory = factory
        self.redis = redis
        self.settings = settings
        self.context = context
        self.inflight: Gauge | None = None
        self.breaker = CircuitBreaker(redis, settings.circuit)

    @staticmethod
    def lease_key(task_id: UUID, generation: int = 0) -> str:
        """Visibility lease renewed only by its current random-token owner."""
        return f"task:lease:{task_id}" + (f":g{generation}" if generation else "")

    @staticmethod
    def completed_key(task_id: UUID, generation: int = 0) -> str:
        """Advisory cache; Postgres remains authoritative."""
        return f"task:completed:{task_id}" + (f":g{generation}" if generation else "")

    @property
    def lease_ms(self) -> int:
        """Configured visibility duration in Redis milliseconds."""
        return int(self.settings.worker.visibility_timeout_seconds * 1000)

    async def _completion(
        self, session: AsyncSession, task: Task, attempt_no: int | None = None
    ) -> Completed:
        number = task.attempts if attempt_no is None else attempt_no
        identifiers = [
            event_id(task.id, number, kind, task.generation) for kind in ("result", "retry", "dlq")
        ]
        return Completed(
            tuple(await session.scalars(select(Outbox.id).where(Outbox.id.in_(identifiers))))
        )

    async def _recover_locked(self, session: AsyncSession, task: Task) -> Completed:
        now = await database_now(session)
        attempt = await session.get(TaskAttempt, (task.id, task.generation, task.attempts))
        if attempt is None or attempt.finished_at is not None:
            raise RuntimeError("RUNNING task has no unfinished current attempt")
        attempt.finished_at = now
        attempt.error = "WorkerCrash: visibility lease expired without completion"
        attempt.stack_trace = "Unavailable: worker terminated before recording a traceback"
        identifiers = await RetryPolicy(self.settings).fail(
            session, task, attempt, RetryableError(attempt.error), now, crashed=True
        )
        return Completed(identifiers)

    async def recover_abandoned(self) -> int:
        """Reap expired RUNNING attempts even if their original consumer is gone."""
        count = 0
        async with self.factory.begin() as session:
            tasks = list(
                await session.scalars(
                    select(Task)
                    .where(Task.status == TaskStatus.RUNNING)
                    .order_by(Task.updated_at)
                    .limit(self.settings.worker.retry_batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for task in tasks:
                if not await self.redis.exists(self.lease_key(task.id, task.generation)):
                    await self._recover_locked(session, task)
                    count += 1
        return count

    async def _claim(self, message: TaskMessage, token: str) -> Execution | Completed | None:
        async with self.factory.begin() as session:
            task = await session.scalar(
                select(Task).where(Task.id == message.task_id).with_for_update()
            )
            if task is None:
                return Completed()
            if message.generation != task.generation:
                return Completed() if message.generation < task.generation else None
            if task.status in FINISHED:
                return await self._completion(session, task)
            delayed = await session.get(DelayedTask, task.id)
            if delayed is not None and delayed.dispatched_at is None:
                return Completed()
            if message.attempt_no < task.attempts or (
                message.attempt_no <= task.attempts and task.status != TaskStatus.RUNNING
            ):
                return await self._completion(session, task, message.attempt_no)
            if task.status not in {TaskStatus.PENDING, TaskStatus.SCHEDULED, TaskStatus.RUNNING}:
                return None
            now = await database_now(session)
            if task.scheduled_at > now:
                return None
            if await self.redis.get(self.lease_key(task.id, task.generation)) != token:
                raise LeaseLost("Lease expired while acquiring the task row")
            if task.status == TaskStatus.RUNNING:
                return await self._recover_locked(session, task)
            if message.attempt_no != task.attempts + 1:
                return None
            permit = await self.breaker.acquire(task.type, self.lease_ms)
            if not permit.allowed:
                task.scheduled_at = now + timedelta(seconds=permit.retry_after_s)
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.SCHEDULED
                await session.execute(
                    insert(DelayedTask)
                    .values(task_id=task.id, release_at=task.scheduled_at)
                    .on_conflict_do_update(
                        index_elements=[DelayedTask.task_id],
                        set_={"release_at": task.scheduled_at, "dispatched_at": None},
                    )
                )
                # Durable scheduler recovery releases this task later; acknowledge
                # its Kafka input now so unrelated types can use the partition.
                return Completed()
            if task.status == TaskStatus.PENDING:
                task.status = TaskStatus.SCHEDULED
            task.status = TaskStatus.RUNNING
            task.attempts += 1
            task.last_error = None
            session.add(
                TaskAttempt(
                    task_id=task.id,
                    generation=task.generation,
                    attempt_no=task.attempts,
                    worker_id=self.context.worker_id,
                    started_at=now,
                )
            )
            execution = Execution(
                task.id,
                task.type,
                task.payload,
                task.attempts,
                token,
                task.generation,
                permit.epoch,
                permit.token,
                task.trace_id,
            )
        return execution

    async def _renew(self, execution: Execution) -> None:
        while True:
            await asyncio.sleep(self.settings.worker.visibility_timeout_seconds / 3)
            if not await evaluate(
                self.redis,
                RENEW_IF_OWNER,
                1,
                self.lease_key(execution.task_id, execution.generation),
                execution.token,
                self.lease_ms,
            ):
                raise LeaseLost("Task lease was replaced or expired")
            if not await self.breaker.renew(
                execution.task_type,
                Permit(True, execution.circuit_epoch, execution.circuit_token),
                self.lease_ms,
            ):
                raise LeaseLost("Circuit probe lease was replaced or expired")

    async def _execute(self, execution: Execution) -> JsonValue:
        """Bind the durable trace and count only active handlers, including cleanup."""
        if self.inflight is not None:
            self.inflight.inc()
        try:
            with log_context(trace_id=execution.trace_id):
                logger.info("Attempt started")
                return await self._execute_handler(execution)
        finally:
            if self.inflight is not None:
                self.inflight.dec()

    async def _execute_handler(self, execution: Execution) -> JsonValue:
        async def invoke() -> JsonValue:
            try:
                async with asyncio.timeout(self.settings.worker.task_timeout_seconds):
                    return await get_handler(execution.task_type)(execution.payload, self.context)
            except TimeoutError as exc:
                raise TimeoutError(
                    f"Handler exceeded {self.settings.worker.task_timeout_seconds:g}s"
                ) from exc

        handler = asyncio.create_task(invoke())
        renewal = asyncio.create_task(self._renew(execution))
        try:
            done, _ = await asyncio.wait({handler, renewal}, return_when=asyncio.FIRST_COMPLETED)
            if renewal in done:
                try:
                    await renewal
                except Exception as exc:
                    raise LeaseLost("Cannot renew task lease") from exc
            result = JSON_VALUE.validate_python(await handler)
            if (
                len(serialize_json({"result": result}))
                > self.settings.api.max_task_message_bytes - 2048
            ):
                raise ValueError("Handler result exceeds Kafka result envelope limit")
            return result
        finally:
            handler.cancel()
            renewal.cancel()
            await asyncio.gather(handler, renewal, return_exceptions=True)

    async def _finish(
        self, execution: Execution, result: JsonValue, failure: Exception | None
    ) -> tuple[UUID, ...]:
        async with self.factory.begin() as session:
            task = await session.scalar(
                select(Task).where(Task.id == execution.task_id).with_for_update()
            )
            attempt = await session.get(
                TaskAttempt, (execution.task_id, execution.generation, execution.attempt_no)
            )
            if (
                task is None
                or task.generation != execution.generation
                or task.status != TaskStatus.RUNNING
                or task.attempts != execution.attempt_no
                or attempt is None
                or attempt.worker_id != self.context.worker_id
                or attempt.finished_at is not None
                or await self.redis.get(self.lease_key(execution.task_id, execution.generation))
                != execution.token
            ):
                raise LeaseLost("A stale attempt cannot finalize this task")
            now = await database_now(session)
            attempt.finished_at = now
            if failure is not None:
                attempt.error = f"{type(failure).__name__}: {failure}"[:8000]
                attempt.stack_trace = "".join(traceback.format_exception(failure))[-32000:]
                identifiers = await RetryPolicy(self.settings).fail(
                    session, task, attempt, failure, now
                )
            else:
                task.status = TaskStatus.SUCCEEDED
                task.result = result
                task.last_error = None
                event = result_event(
                    task, self.context.worker_id, now, started_at=attempt.started_at
                )
                session.add(event)
                identifiers = (event.id,)
        await self.breaker.complete(
            execution.task_type,
            Permit(True, execution.circuit_epoch, execution.circuit_token),
            failure is None,
        )
        return identifiers

    async def wait_published(self, identifiers: tuple[UUID, ...]) -> bool:
        """All result/retry/DLQ events must be durably published before input commit."""
        if not identifiers:
            return True
        try:
            async with asyncio.timeout(self.settings.worker.result_publish_timeout_seconds):
                while True:
                    async with self.factory() as session:
                        pending = await session.scalar(
                            select(func.count())
                            .select_from(Outbox)
                            .where(Outbox.id.in_(identifiers), Outbox.published_at.is_(None))
                        )
                    if pending == 0:
                        return True
                    await asyncio.sleep(0.05)
        except TimeoutError:
            return False

    async def process(self, message: TaskMessage) -> bool:
        """Return True only once this input attempt can safely be acknowledged."""
        if message.type.startswith("java."):
            # Separate consumer groups each receive every record. Java owns this
            # namespace; acknowledging here must never create a Python attempt.
            return True
        with log_context(task_id=message.task_id, worker_id=self.context.worker_id):
            hint = await self.redis.get(self.completed_key(message.task_id, message.generation))
            completed = None
            async with self.factory() as session:
                task = await session.get(Task, message.task_id)
                if task is None:
                    logger.warning("Skipping dispatch for a missing task")
                    return True
                if message.generation != task.generation:
                    return message.generation < task.generation
                delayed = await session.get(DelayedTask, task.id)
                if delayed is not None and delayed.dispatched_at is None:
                    # The scheduler's durable ledger guarantees later publication.
                    # A stale pre-scheduler dispatch must not hold a partition asleep.
                    return True
                if task.status in FINISHED:
                    completed = await self._completion(session, task)
                    logger.info(
                        "Skipping finished task status=%s cached=%s", task.status, bool(hint)
                    )
                elif message.attempt_no < task.attempts or (
                    message.attempt_no <= task.attempts and task.status != TaskStatus.RUNNING
                ):
                    completed = await self._completion(session, task, message.attempt_no)
            if completed is not None:
                return await self.wait_published(completed.event_ids)
            token = uuid4().hex
            key = self.lease_key(message.task_id, message.generation)
            if not await self.redis.set(key, token, nx=True, px=self.lease_ms):
                return False
            try:
                claimed = await self._claim(message, token)
                if claimed is None:
                    return False
                if isinstance(claimed, Completed):
                    return await self.wait_published(claimed.event_ids)
                failure = None
                result = None
                try:
                    result = await self._execute(claimed)
                except LeaseLost:
                    raise
                except Exception as exc:
                    failure = exc
                identifiers = await self._finish(claimed, result, failure)
                if failure is None:
                    await self.redis.set(
                        self.completed_key(message.task_id, message.generation),
                        "SUCCEEDED",
                        ex=self.settings.worker.completion_ttl_seconds,
                    )
                logger.info(
                    "Attempt %d finished; outcome events committed",
                    claimed.attempt_no,
                    extra={"trace_id": claimed.trace_id.hex if claimed.trace_id else None},
                )
                return await self.wait_published(identifiers)
            finally:
                await evaluate(self.redis, RELEASE_IF_OWNER, 1, key, token)
