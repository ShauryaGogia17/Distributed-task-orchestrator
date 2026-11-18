"""Crash-recoverable, at-least-once publication of transactional outbox rows."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import UUID

from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.config import Settings
from shared.kafka import producer_context
from shared.models import TaskStatus
from shared.priority import pending_partition
from shared.tables import Outbox, Task
from shared.tracing import headers_for

logger = logging.getLogger(__name__)


class OutboxPublisher:
    """Hold row locks through Kafka acknowledgement and the publication DB commit.

    Multiple API replicas skip one another's locked rows. If any send or commit
    fails, the transaction rolls back: acknowledged messages may be repeated, but
    committed events remain eligible until their publication is recorded durably.
    """

    def __init__(self, factory: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        """Keep clients and shutdown state owned by this application instance."""
        self.factory = factory
        self.settings = settings
        self.stop_event = asyncio.Event()

    async def publish_batch(self, producer: AIOKafkaProducer) -> int:
        """Publish one locked batch; propagate errors so no failed event is marked."""
        async with self.factory.begin() as session:
            rows = list(
                await session.scalars(
                    select(Outbox)
                    .where(Outbox.published_at.is_(None))
                    .order_by(Outbox.created_at, Outbox.id)
                    .limit(self.settings.api.outbox_batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for row in rows:
                partition = pending_partition(row.key) if row.topic == "tasks.pending" else None
                async with asyncio.timeout(self.settings.api.outbox_publish_timeout_seconds):
                    await producer.send_and_wait(
                        row.topic,
                        key=row.key,
                        value=row.payload,
                        partition=partition,
                        headers=[
                            ("outbox-id", str(row.id).encode("ascii")),
                            *headers_for(
                                str(row.payload["task_id"])
                                if row.payload.get("task_id") is not None
                                else None,
                                row.trace_id,
                            ),
                        ],
                    )
                row.published_at = datetime.now(UTC)
                logger.info(
                    "Outbox published topic=%s",
                    row.topic,
                    extra={"task_id": row.payload.get("task_id"), "trace_id": row.trace_id.hex},
                )
                if row.topic == "tasks.dlq" and row.payload.get("kind") == "task.dead_letter":
                    task = await session.scalar(
                        select(Task)
                        .where(Task.id == UUID(str(row.payload["task_id"])))
                        .with_for_update()
                    )
                    if (
                        task is not None
                        and task.generation == row.payload.get("generation", 0)
                        and task.status == TaskStatus.FAILED
                        and task.attempts == row.payload["attempt_no"]
                    ):
                        task.status = TaskStatus.DEAD_LETTERED
        return len(rows)

    async def _pause(self, seconds: float) -> None:
        """Sleep between polls while allowing immediate shutdown."""
        try:
            await asyncio.wait_for(self.stop_event.wait(), seconds)
        except TimeoutError:
            pass

    async def run(self) -> None:
        """Reconnect after broker/database failures without blocking task submissions."""
        while not self.stop_event.is_set():
            try:
                # Outbox replay supplies durability. Bounded non-idempotent delivery
                # avoids indefinitely flushing retrying batches during broker outages.
                async with producer_context(
                    self.settings.kafka, enable_idempotence=False
                ) as producer:
                    while not self.stop_event.is_set():
                        try:
                            if not await self.publish_batch(producer):
                                await self._pause(self.settings.api.outbox_poll_seconds)
                        except Exception:
                            logger.exception("Outbox batch failed; its transaction was rolled back")
                            await self._pause(self.settings.api.outbox_retry_seconds)
            except Exception:
                logger.exception("Outbox publication failed; committed events will be retried")
                await self._pause(self.settings.api.outbox_retry_seconds)

    async def shutdown(self, task: asyncio.Task[None]) -> None:
        """Allow an in-flight batch to finish, then cancel and roll it back if needed."""
        self.stop_event.set()
        try:
            await asyncio.wait_for(task, self.settings.api.outbox_shutdown_timeout_seconds)
        except TimeoutError:
            logger.info("Outbox shutdown deadline reached; uncommitted events remain pending")
