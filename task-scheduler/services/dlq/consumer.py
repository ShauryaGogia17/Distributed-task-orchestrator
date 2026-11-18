"""Persist every Kafka DLQ source record before committing its offset."""

from __future__ import annotations

import asyncio
import base64
import logging
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.config import Settings
from shared.db import database_now
from shared.fingerprint import failure_category, failure_signature
from shared.kafka import commit_record, deserialize_json, serialize_json
from shared.models import ErrorRecord, TaskStatus
from shared.tables import DeadLetter, DLQReceipt, Task
from shared.tracing import record_trace

logger = logging.getLogger(__name__)


def identifier(value: object, fallback: str) -> UUID:
    """Retain valid IDs and give malformed/orphan records stable source-derived identities."""
    try:
        return UUID(str(value))
    except ValueError:
        return uuid5(NAMESPACE_URL, fallback)


def counter(value: object, default: int = 0) -> int:
    """Prevent malformed external metadata overflowing SQL integer columns."""
    try:
        return max(
            0,
            min(
                int(value if isinstance(value, (str, bytes, int, float)) else str(value)),
                2147483647,
            ),
        )
    except (TypeError, ValueError, OverflowError):
        return default


def timestamp(value: object, fallback: datetime) -> datetime:
    """Use only aware source timestamps; otherwise use the canonical receipt clock."""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.utcoffset() is not None else fallback
    except (ValueError, TypeError):
        return fallback


class DLQConsumer:
    """Canonical incident identity plus one immutable receipt per topic/partition/offset."""

    def __init__(self, factory: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        self.factory = factory
        self.settings = settings
        self.stop_event = asyncio.Event()
        self.running = False
        self.connected = False

    async def ingest(self, record: ConsumerRecord) -> UUID:
        """Merge receipts without resetting resolved incidents or newer task generations."""
        source = f"{record.topic}:{record.partition}:{record.offset}"
        try:
            payload = (
                deserialize_json(record.value)
                if isinstance(record.value, bytes)
                else deserialize_json(serialize_json(record.value))
            )
            # JSONB cannot store NUL or lone surrogate code points; preserve raw bytes instead.
            if b"\\u0000" in serialize_json(payload):
                raise ValueError("JSON contains a PostgreSQL-incompatible NUL")
        except (ValueError, TypeError, UnicodeError):
            raw = record.value if isinstance(record.value, bytes) else repr(record.value).encode()
            payload = {
                "reason": "Malformed DLQ message",
                "raw_base64": base64.b64encode(raw).decode(),
            }
        incident_id = identifier(payload.get("id"), f"dlq:{source}")
        task_id = identifier(payload.get("task_id"), f"dlq-task:{source}")
        generation = counter(payload.get("generation"))
        async with self.factory.begin() as session:
            receipt = await session.get(DLQReceipt, (record.topic, record.partition, record.offset))
            if receipt is not None:
                return receipt.dead_letter_id
            now = await database_now(session)
            task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
            old = await session.get(DeadLetter, incident_id)
            if old is not None and (old.task_id != task_id or old.generation != generation):
                incident_id = identifier(None, f"dlq-conflict:{source}")
            reason = str(payload.get("reason") or "Unspecified DLQ failure")
            records = []
            history = payload.get("error_history", [])
            for number, item in enumerate(history if isinstance(history, list) else [history], 1):
                try:
                    entry = ErrorRecord.model_validate(item)
                except ValueError:
                    data = item if isinstance(item, dict) else {}
                    entry = ErrorRecord(
                        attempt_no=max(1, counter(data.get("attempt_no"), number)),
                        generation=generation,
                        worker_id=str(data.get("worker_id") or "unknown")[:255],
                        error=str(data.get("error") or item or reason),
                        stack_trace=str(data["stack_trace"]) if data.get("stack_trace") else None,
                        recorded_at=timestamp(data.get("recorded_at"), now),
                    )
                records.append(entry.model_dump(mode="json"))
            last = records[-1] if records else {}
            default_attempts = (
                task.attempts if task is not None and task.generation == generation else 0
            )
            attempts = counter(
                payload.get(
                    "attempt_no", payload.get("attempts", last.get("attempt_no", default_attempts))
                ),
                default_attempts,
            )
            task_type = str(payload.get("type") or (task.type if task is not None else "unknown"))[
                :128
            ]
            worker_id = payload.get("worker_id") or last.get("worker_id")
            closed = await session.scalar(
                select(DeadLetter)
                .where(
                    DeadLetter.task_id == task_id,
                    DeadLetter.generation == generation,
                    DeadLetter.status != "OPEN",
                )
                .order_by(DeadLetter.created_at.desc())
                .limit(1)
            )
            status, resolution = "OPEN", None
            if task is not None and task.generation > generation:
                status, resolution = "RETRIED", "REQUEUED"
            elif closed is not None:
                status, resolution = closed.status, closed.resolution
            elif task is not None and task.status == TaskStatus.CANCELLED:
                status, resolution = "DISCARDED", "CANCELLED"
            values = {
                "id": incident_id,
                "task_id": task_id,
                "generation": generation,
                "task_type": task_type,
                "worker_id": str(worker_id)[:255] if worker_id else None,
                "attempts": attempts,
                "reason": reason,
                "error_history": records,
                "category": str(payload.get("category") or failure_category(reason))[:32],
                "failure_signature": failure_signature(
                    last.get("error", reason), last.get("stack_trace")
                ),
                "trace_id": UUID(record_trace(getattr(record, "headers", None))),
                "created_at": timestamp(payload.get("created_at"), now),
                "received_at": now,
                "status": status,
                "resolution": resolution,
                "ai_diagnosis": payload.get("ai_diagnosis")
                if isinstance(payload.get("ai_diagnosis"), dict)
                else None,
            }
            updates = {
                key: values[key]
                for key in (
                    "task_type",
                    "worker_id",
                    "attempts",
                    "category",
                    "failure_signature",
                    "error_history",
                    "received_at",
                )
            }
            await session.execute(
                insert(DeadLetter)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=[DeadLetter.id],
                    set_=updates,
                    where=DeadLetter.received_at.is_(None),
                )
            )
            await session.execute(
                insert(DLQReceipt)
                .values(
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    dead_letter_id=incident_id,
                    payload=payload,
                    received_at=now,
                )
                .on_conflict_do_nothing()
            )
            if (
                task is not None
                and task.generation == generation
                and task.status == TaskStatus.FAILED
            ):
                task.status = TaskStatus.DEAD_LETTERED
        logger.info(
            "DLQ incident persisted",
            extra={
                "task_id": str(task_id),
                "trace_id": task.trace_id.hex
                if task is not None
                else record_trace(getattr(record, "headers", None)),
            },
        )
        return incident_id

    async def backfill(self) -> None:
        """Fingerprint legacy DB incidents even when their Kafka records have expired."""
        async with self.factory.begin() as session:
            rows = list(
                await session.scalars(
                    select(DeadLetter)
                    .where(DeadLetter.failure_signature.is_(None))
                    .limit(self.settings.dlq.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for row in rows:
                last = row.error_history[-1] if row.error_history else {}
                row.failure_signature = failure_signature(
                    str(last.get("error") or row.reason),
                    str(last["stack_trace"]) if last.get("stack_trace") is not None else None,
                )
                if row.category == "unknown":
                    row.category = failure_category(row.reason)

    async def _pause(self) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), self.settings.dlq.retry_seconds)
        except TimeoutError:
            pass

    async def run(self) -> None:
        """Bounded raw ingestion retries failures without acknowledging unpersisted input."""
        self.running = True
        try:
            while not self.stop_event.is_set():
                consumer = AIOKafkaConsumer(
                    "tasks.dlq",
                    bootstrap_servers=self.settings.kafka.bootstrap_servers,
                    group_id=self.settings.dlq.group_id,
                    client_id=f"{self.settings.kafka.client_id}-dlq",
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    max_partition_fetch_bytes=self.settings.kafka.max_message_bytes,
                    fetch_max_bytes=self.settings.kafka.max_message_bytes,
                    request_timeout_ms=self.settings.kafka.request_timeout_ms,
                )
                try:
                    await consumer.start()
                    self.connected = True
                    while not self.stop_event.is_set():
                        batches = await consumer.getmany(
                            timeout_ms=100, max_records=self.settings.dlq.batch_size
                        )
                        for partition, records in batches.items():
                            for record in records:
                                try:
                                    await self.ingest(record)
                                    await commit_record(consumer, record)
                                except Exception:
                                    logger.exception("DLQ record retained for redelivery")
                                    if partition in consumer.assignment():
                                        consumer.seek(partition, record.offset)
                                    await self._pause()
                                    break
                        await self.backfill()
                except Exception:
                    logger.exception("DLQ consumer unavailable; input remains durable")
                    self.connected = False
                    await self._pause()
                finally:
                    self.connected = False
                    await consumer.stop()
        finally:
            self.running = False
