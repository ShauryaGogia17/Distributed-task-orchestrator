"""Real PostgreSQL migrations/transactions, Redis Lua, and Kafka manual commits."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka.structs import TopicPartition
from alembic import command
from alembic.config import Config
from sqlalchemy import delete, inspect, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from shared.config import PROJECT_ROOT
from shared.db import create_session_factory, session_scope, transition_task
from shared.kafka import commit_record, consumer_context, producer_context, publish_json
from shared.models import DeadLetterOut, ErrorRecord, InvalidTransition, TaskOut, TaskStatus
from shared.redis import IdempotencyStore, RedisLock, create_redis_client
from shared.tables import DeadLetter, Outbox, Task, TaskAttempt

pytestmark = pytest.mark.integration


def migrate(connection: Connection, *, down: bool = False) -> None:
    """Run real revision code on a caller-owned, schema-isolated connection."""
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.attributes["connection"] = connection
    if down:
        command.downgrade(config, "base")
    else:
        command.upgrade(config, "head")


async def test_migration_roundtrip(database: tuple[AsyncEngine, str]) -> None:
    """Initial DDL must downgrade cleanly and upgrade again without leftovers."""
    engine, schema = database
    async with engine.begin() as connection:
        await connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        await connection.run_sync(lambda sync: migrate(sync, down=True))
        tables = await connection.run_sync(
            lambda sync: inspect(sync).get_table_names(schema=schema)
        )
        assert tables == ["alembic_version"]
        await connection.run_sync(migrate)
        tables = await connection.run_sync(
            lambda sync: inspect(sync).get_table_names(schema=schema)
        )
        assert set(tables) == {
            "tasks",
            "task_attempts",
            "dead_letters",
            "dlq_receipts",
            "agent_clusters",
            "agent_audit",
            "outbox",
            "retry_schedules",
            "schedules",
            "delayed_tasks",
            "alembic_version",
        }


async def test_atomic_task_outbox_and_constraints(database: tuple[AsyncEngine, str]) -> None:
    """Persist all mappings and prove rollback, uniqueness, FK cascades, and JSON schemas."""
    engine, _ = database
    factory = create_session_factory(engine)
    task_id = uuid4()
    outbox_id = uuid4()
    with pytest.raises(RuntimeError, match="abort"):
        async with session_scope(factory) as session:
            session.add(Task(id=task_id, type="test", payload={"nested": [1, True]}))
            session.add(Outbox(id=outbox_id, topic="tasks.pending", key=str(task_id), payload={}))
            await session.flush()
            raise RuntimeError("abort")
    async with session_scope(factory) as session:
        assert await session.get(Task, task_id) is None
        assert await session.get(Outbox, outbox_id) is None
        task = Task(id=task_id, type="test", idempotency_key="unique-key")
        session.add(task)
        await session.flush()
        assert TaskOut.model_validate(task).status == TaskStatus.PENDING
        session.add_all(
            [
                TaskAttempt(task_id=task_id, attempt_no=1, worker_id="test-worker"),
                DeadLetter(
                    task_id=task_id,
                    reason="test",
                    error_history=[
                        ErrorRecord(attempt_no=1, worker_id="test-worker", error="test").model_dump(
                            mode="json"
                        )
                    ],
                ),
                Outbox(
                    id=outbox_id,
                    topic="tasks.pending",
                    key=str(task_id),
                    payload={"task_id": str(task_id)},
                ),
            ]
        )
    async with session_scope(factory) as session:
        dead_letter = await session.scalar(select(DeadLetter).where(DeadLetter.task_id == task_id))
        assert DeadLetterOut.model_validate(dead_letter).error_history[0].error == "test"
        assert (await session.get(Outbox, outbox_id)).published_at is None
    with pytest.raises(IntegrityError):
        async with session_scope(factory) as session:
            session.add(Task(type="duplicate", idempotency_key="unique-key"))
    with pytest.raises(IntegrityError):
        async with session_scope(factory) as session:
            session.add(Task(type="bad-priority", priority=100))
    async with session_scope(factory) as session:
        await session.execute(delete(Task).where(Task.id == task_id))
    async with session_scope(factory) as session:
        assert await session.scalar(select(TaskAttempt)) is None
        assert await session.scalar(select(DeadLetter)) is not None  # Audit survives task deletion.
        assert await session.get(Outbox, outbox_id) is not None


async def test_concurrent_transition_revalidates_state(database: tuple[AsyncEngine, str]) -> None:
    """Two schedulers racing for one PENDING task cannot both schedule it."""
    engine, _ = database
    factory = create_session_factory(engine)
    task_id = uuid4()
    async with session_scope(factory) as session:
        session.add(Task(id=task_id, type="test"))

    async def schedule() -> bool:
        try:
            async with session_scope(factory) as session:
                task = await transition_task(
                    session, task_id, TaskStatus.SCHEDULED, expected_status=TaskStatus.PENDING
                )
                assert TaskOut.model_validate(task).status == TaskStatus.SCHEDULED
            return True
        except InvalidTransition:
            return False

    assert sorted(await asyncio.gather(schedule(), schedule())) == [False, True]
    async with session_scope(factory) as session:
        assert (await session.get(Task, task_id)).status == TaskStatus.SCHEDULED


async def test_updated_at_trigger(database: tuple[AsyncEngine, str]) -> None:
    """Raw/Core SQL writes must advance updated_at without ORM callbacks."""
    engine, _ = database
    factory = create_session_factory(engine)
    task_id = uuid4()
    old_timestamp = datetime(2000, 1, 1, tzinfo=UTC)
    async with session_scope(factory) as session:
        session.add(Task(id=task_id, type="test", updated_at=old_timestamp))
    async with engine.begin() as connection:
        # Explicit timestamp defeats Column.onupdate; the database trigger still overrides it.
        row = await connection.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(priority=1, updated_at=old_timestamp)
            .returning(Task.updated_at)
        )
        assert row.scalar_one() > old_timestamp


async def test_real_redis_stale_owner_and_idempotency() -> None:
    """Execute actual Lua: an expired lock holder cannot release a new owner's lease."""
    client = create_redis_client()
    name = f"phase2-test:{uuid4().hex}"
    old = RedisLock(client, name)
    new = RedisLock(client, name)
    store = IdempotencyStore(client, namespace=name)
    task_id, other_id = uuid4(), uuid4()
    try:
        assert await old.acquire()
        assert not await new.acquire()
        assert await client.pttl(old.key) > 0
        await client.pexpire(old.key, 0)
        assert await new.acquire()
        assert not await old.release()
        assert await client.exists(new.key)
        assert await new.release()
        assert await store.reserve("request", task_id, ttl_seconds=60)
        assert not await store.reserve("request", other_id)
        assert await store.get("request") == task_id
        assert not await store.release("request", other_id)
        assert await store.release("request", task_id)
        assert await store.get("request") is None
    finally:
        await old.release()
        await new.release()
        await store.release("request", task_id)
        await client.aclose()


async def test_kafka_json_delivery_and_manual_commit() -> None:
    """Publish with acknowledgement, consume the JSON event, and inspect committed offset."""
    identifier = str(uuid4())
    async with consumer_context("tasks.results", group_id=f"phase2-test-{identifier}") as consumer:
        async with producer_context() as producer:
            metadata = await publish_json(
                producer,
                "tasks.results",
                key=identifier,
                payload={"phase2_test_id": identifier, "unicode": "hello 🌏"},
            )
        async with asyncio.timeout(30):
            while True:
                record = await consumer.getone()
                if record.key == identifier:
                    break
        assert record.value["phase2_test_id"] == identifier
        assert record.value["unicode"] == "hello 🌏"
        assert record.partition == metadata.partition
        partition = TopicPartition(record.topic, record.partition)
        assert await consumer.committed(partition) is None
        await commit_record(consumer, record)
        assert await consumer.committed(partition) == record.offset + 1
