"""Fast retry-topic ingestion and database-clock delayed dispatch without partition sleeps."""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.worker.executor import TaskExecutor
from services.worker.retry_policy import RETRY_TOPICS, event_id
from shared.config import Settings
from shared.db import database_now
from shared.kafka import commit_record
from shared.models import RetryMessage, TaskMessage, TaskStatus
from shared.priority import message_key
from shared.tables import Outbox, RetrySchedule, Task

logger = logging.getLogger(__name__)


class RetryEngine:
    """Persist receipt before Kafka commit, then dispatch due rows transactionally.

    Neither future deadlines nor queue size create sleeping coroutine-per-message
    backlogs. Each replica processes bounded SKIP LOCKED batches from Postgres.
    """

    def __init__(
        self, factory: async_sessionmaker[AsyncSession], settings: Settings, executor: TaskExecutor
    ) -> None:
        self.factory = factory
        self.settings = settings
        self.executor = executor
        self.stop_event = asyncio.Event()

    async def ingest(self, message: RetryMessage) -> None:
        """Mark a failure-created schedule received; duplicate Kafka delivery is harmless."""
        if message.event_id != event_id(
            message.task_id, message.failed_attempt, "retry", message.generation
        ):
            raise ValueError("Retry event identity does not match task/attempt")
        async with self.factory.begin() as session:
            schedule = await session.scalar(
                select(RetrySchedule).where(RetrySchedule.id == message.event_id).with_for_update()
            )
            if schedule is None:
                # Every valid event and its inbox row commit together. Missing rows
                # are removed tasks or schema-isolated test events, not new work.
                return
            if (
                schedule.task_id != message.task_id
                or schedule.generation != message.generation
                or schedule.failed_attempt != message.failed_attempt
                or schedule.due_at != message.due_at
            ):
                raise ValueError("Retry event does not match its durable schedule")
            if schedule.received_at is None:
                schedule.received_at = await database_now(session)

    async def dispatch_due(self) -> int:
        """Create one pending outbox event per due retry and move RETRYING to SCHEDULED."""
        count = 0
        async with self.factory.begin() as session:
            now = await database_now(session)
            schedules = list(
                await session.scalars(
                    select(RetrySchedule)
                    .where(
                        RetrySchedule.received_at.is_not(None),
                        RetrySchedule.dispatched_at.is_(None),
                        RetrySchedule.due_at <= now,
                    )
                    .order_by(RetrySchedule.due_at, RetrySchedule.id)
                    .limit(self.settings.worker.retry_batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for schedule in schedules:
                task = await session.scalar(
                    select(Task).where(Task.id == schedule.task_id).with_for_update()
                )
                if (
                    task is not None
                    and task.generation == schedule.generation
                    and task.status == TaskStatus.RETRYING
                    and task.attempts == schedule.failed_attempt
                ):
                    task.status = TaskStatus.SCHEDULED
                    message = TaskMessage(
                        task_id=task.id,
                        generation=task.generation,
                        ai_modified=task.ai_modified,
                        type=task.type,
                        payload=task.payload,
                        priority=task.priority,
                        attempt_no=task.attempts + 1,
                        max_retries=task.max_retries,
                        scheduled_at=schedule.due_at,
                        created_at=task.created_at,
                    )
                    session.add(
                        Outbox(
                            id=event_id(task.id, task.attempts, "redispatch", task.generation),
                            topic="tasks.pending",
                            key=message_key(task.id, task.priority),
                            payload=message.model_dump(mode="json"),
                        )
                    )
                    count += 1
                schedule.dispatched_at = now
        return count

    async def _pause(self) -> None:
        try:
            await asyncio.wait_for(
                self.stop_event.wait(), self.settings.worker.retry_engine_poll_seconds
            )
        except TimeoutError:
            pass

    async def _consume(self) -> None:
        while not self.stop_event.is_set():
            consumer = AIOKafkaConsumer(
                *RETRY_TOPICS,
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                client_id=f"{self.settings.kafka.client_id}-retry-{self.executor.context.worker_id}",
                group_id=f"{self.settings.worker.group_id}.retries",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                request_timeout_ms=self.settings.kafka.request_timeout_ms,
                max_poll_records=self.settings.worker.retry_batch_size,
            )
            try:
                await consumer.start()
                while not self.stop_event.is_set():
                    batches = await consumer.getmany(
                        timeout_ms=100, max_records=self.settings.worker.retry_batch_size
                    )
                    for partition, records in batches.items():
                        for record in records:
                            try:
                                await self.ingest(RetryMessage.model_validate_json(record.value))
                                await commit_record(consumer, record)
                            except Exception:
                                logger.exception("Retry receipt was not acknowledged")
                                if partition in consumer.assignment():
                                    consumer.seek(partition, record.offset)
                                await self._pause()
                                break
            except Exception:
                logger.exception("Retry consumer unavailable; durable schedules are retained")
                await self._pause()
            finally:
                await consumer.stop()

    async def _maintain(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.executor.recover_abandoned()
                await self.dispatch_due()
            except Exception:
                logger.exception("Retry maintenance failed; transaction rolled back")
            await self._pause()

    async def run(self) -> None:
        """Run ingestion and dispatch/reaping concurrently with constant task count."""
        async with asyncio.TaskGroup() as group:
            group.create_task(self._consume(), name="retry-consumer")
            group.create_task(self._maintain(), name="retry-dispatcher")

    async def shutdown(self, task: asyncio.Task[None]) -> None:
        """Stop polling promptly; incomplete receipts/dispatches remain recoverable."""
        self.stop_event.set()
        await asyncio.wait_for(task, self.settings.worker.shutdown_timeout_seconds)
