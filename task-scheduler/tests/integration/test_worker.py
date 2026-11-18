"""Worker execution against real PostgreSQL, Redis, Kafka, and a local HTTP server."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.worker import registry
from services.worker.executor import LeaseLost, TaskExecutor
from services.worker.heartbeat import WorkerHeartbeat
from services.worker.registry import HandlerContext, load_builtin_handlers, register
from services.worker.retry_engine import RetryEngine
from services.worker.retry_policy import event_id
from services.worker.service import WorkerService
from shared.config import Settings, WorkerSettings, get_settings
from shared.db import create_session_factory
from shared.errors import RetryableError
from shared.kafka import consumer_context, producer_context, serialize_json
from shared.models import RetryMessage, TaskMessage, TaskStatus
from shared.outbox import OutboxPublisher
from shared.priority import message_key, pending_partition
from shared.redis import create_redis_client
from shared.tables import DeadLetter, Outbox, RetrySchedule, Task, TaskAttempt

pytestmark = pytest.mark.integration


@dataclass
class WorkerResources:
    """Dependencies owned by one isolated integration test."""

    executor: TaskExecutor
    factory: async_sessionmaker[AsyncSession]
    redis: Redis
    settings: Settings
    context: HandlerContext


@pytest.fixture
async def worker(database, tmp_path, monkeypatch) -> AsyncIterator[WorkerResources]:
    """Use unique task IDs, schema, worker identity, and local heartbeat path."""
    load_builtin_handlers()
    monkeypatch.setattr(registry, "_handlers", dict(registry._handlers))
    settings = get_settings().model_copy(
        update={
            "worker": WorkerSettings(
                metrics_enabled=False,
                _env_file=None,
                id=f"integration-{uuid4().hex}",
                group_id=f"integration-{uuid4().hex}",
                task_timeout_seconds=1,
                visibility_timeout_seconds=0.3,
                heartbeat_interval_seconds=0.05,
                heartbeat_ttl_seconds=2,
                result_publish_timeout_seconds=5,
                retry_poll_seconds=0.02,
                shutdown_timeout_seconds=5,
                rebalance_timeout_seconds=5,
                health_file=tmp_path / "worker.json",
            )
        }
    )
    factory = create_session_factory(database[0])
    client = create_redis_client(settings.redis)
    publisher = OutboxPublisher(factory, settings)
    publication = asyncio.create_task(publisher.run())
    async with httpx.AsyncClient() as http_client:
        context = HandlerContext(settings.worker.id, http_client)
        executor = TaskExecutor(factory, client, settings, context)
        try:
            yield WorkerResources(executor, factory, client, settings, context)
        finally:
            await publisher.shutdown(publication)
            async with factory() as session:
                identifiers = list(await session.scalars(select(Task.id)))
            keys = [
                key
                for identifier in identifiers
                for key in (
                    executor.lease_key(identifier),
                    executor.completed_key(identifier),
                )
            ]
            if keys:
                await client.delete(*keys)
            await client.aclose()


async def seed(
    worker: WorkerResources, task_type: str, payload: dict | None = None, max_retries: int = 3
) -> TaskMessage:
    """Commit a task before passing its dispatch to an executor or Kafka consumer."""
    async with worker.factory.begin() as session:
        task = Task(type=task_type, payload=payload or {}, max_retries=max_retries)
        session.add(task)
        await session.flush()
        message = TaskMessage(
            task_id=task.id,
            type=task.type,
            payload=task.payload,
            scheduled_at=task.scheduled_at,
            max_retries=max_retries,
        )
    return message


async def test_success_is_durable_and_result_is_published(worker) -> None:
    """A successful handler stores its attempt/result and reaches the result topic."""
    message = await seed(worker, "generate_report", {"data": [1, 2, 3], "seconds": 0})
    assert await worker.executor.process(message)
    async with worker.factory() as session:
        task = await session.get(Task, message.task_id)
        assert task.status == TaskStatus.SUCCEEDED
        assert task.result["sum"] == 6
        assert task.attempts == 1
        attempt = await session.get(TaskAttempt, (message.task_id, 0, 1))
        assert attempt.finished_at is not None and attempt.error is None
        assert (await session.scalars(select(Outbox))).one().published_at is not None
    async with consumer_context("tasks.results", group_id=f"worker-result-{uuid4()}") as consumer:
        async with asyncio.timeout(10):
            while True:
                record = await consumer.getone()
                if record.value.get("task_id") == str(message.task_id):
                    break
        assert record.value["status"] == "SUCCEEDED"
        assert record.value["result"]["sum"] == 6


async def test_handler_timeout_is_recorded(worker) -> None:
    """Timeout cancels the coroutine and durably records FAILED and its traceback."""
    worker.executor.settings = worker.settings.model_copy(
        update={"worker": worker.settings.worker.model_copy(update={"task_timeout_seconds": 0.02})}
    )
    message = await seed(worker, "slow_task", {"seconds": 1})
    assert await worker.executor.process(message)
    async with worker.factory() as session:
        task = await session.get(Task, message.task_id)
        assert task.status == TaskStatus.RETRYING
        assert "TimeoutError" in task.last_error
        attempt = await session.get(TaskAttempt, (message.task_id, 0, 1))
        assert "TimeoutError" in attempt.stack_trace
        assert attempt.finished_at is not None


async def test_already_succeeded_is_not_reexecuted_without_cache(worker) -> None:
    """A stale positive cache cannot skip PENDING, and a lost cache cannot rerun SUCCEEDED."""
    calls = 0
    name = f"test_skip_{uuid4().hex}"

    @register(name)
    async def once(payload, context):
        nonlocal calls
        calls += 1
        return {"ok": True}

    message = await seed(worker, name)
    await worker.redis.set(worker.executor.completed_key(message.task_id), "SUCCEEDED")
    assert await worker.executor.process(message)
    await worker.redis.delete(worker.executor.completed_key(message.task_id))
    assert await worker.executor.process(message)
    assert calls == 1
    async with worker.factory() as session:
        assert (await session.get(Task, message.task_id)).attempts == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize("task_type", ["bad_input_task", "unregistered_task"])
async def test_handler_failures_are_persisted(worker, task_type: str) -> None:
    """Validation errors and unknown handlers produce durable failed attempts."""
    message = await seed(worker, task_type)
    assert await worker.executor.process(message)
    async with worker.factory() as session:
        task = await session.get(Task, message.task_id)
        assert task.status == TaskStatus.DEAD_LETTERED
        assert (
            "ValidationError" if task_type == "bad_input_task" else "LookupError"
        ) in task.last_error


async def test_running_attempt_renews_visibility_lease(worker) -> None:
    """A handler longer than the initial lease remains the sole valid owner."""
    message = await seed(worker, "slow_task", {"seconds": 0.5})
    running = asyncio.create_task(worker.executor.process(message))
    try:
        async with asyncio.timeout(2):
            while True:
                if await worker.redis.exists(worker.executor.lease_key(message.task_id)):
                    break
                await asyncio.sleep(0.005)
        await asyncio.sleep(0.35)
        assert await worker.redis.pttl(worker.executor.lease_key(message.task_id)) > 0
        assert await running
    finally:
        if not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


@pytest.mark.parametrize("max_retries", [0, 3])
async def test_expired_worker_attempt_is_recovered_or_budget_exhausted(worker, max_retries) -> None:
    """Missing leases permit fenced crash recovery with a bounded attempt budget."""
    message = await seed(worker, "slow_task", {"seconds": 0}, max_retries=max_retries)
    async with worker.factory.begin() as session:
        task = await session.get(Task, message.task_id)
        task.status = TaskStatus.SCHEDULED
        task.status = TaskStatus.RUNNING
        task.attempts = 1
        session.add(TaskAttempt(task_id=task.id, attempt_no=1, worker_id="dead-worker"))
    assert await worker.executor.process(message)
    async with worker.factory() as session:
        task = await session.get(Task, message.task_id)
        assert task.status == (
            TaskStatus.DEAD_LETTERED if max_retries == 0 else TaskStatus.RETRYING
        )
        assert task.attempts == 1
        assert "WorkerCrash" in (await session.get(TaskAttempt, (task.id, 0, 1))).error


async def test_replaced_lease_cancels_handler_and_fences_completion(worker) -> None:
    """A previous owner must neither finalize RUNNING nor delete the replacement lease."""
    message = await seed(worker, "slow_task", {"seconds": 0.8})
    running = asyncio.create_task(worker.executor.process(message))
    key = worker.executor.lease_key(message.task_id)
    async with asyncio.timeout(2):
        while True:
            async with worker.factory() as session:
                state = (await session.get(Task, message.task_id)).status
            if state == TaskStatus.RUNNING:
                break
            await asyncio.sleep(0.005)
    await worker.redis.set(key, "replacement-owner", px=2000)
    with pytest.raises(LeaseLost):
        await running
    assert await worker.redis.get(key) == "replacement-owner"
    async with worker.factory() as session:
        assert (await session.get(Task, message.task_id)).status == TaskStatus.RUNNING
        assert (await session.get(TaskAttempt, (message.task_id, 0, 1))).finished_at is None


async def test_http_handler_uses_real_http(worker) -> None:
    """Make a real request to an ephemeral local HTTP server, without internet access."""

    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    async with server:
        port = server.sockets[0].getsockname()[1]
        message = await seed(worker, "http_call", {"url": f"http://127.0.0.1:{port}/test"})
        assert await worker.executor.process(message)
    async with worker.factory() as session:
        assert (await session.get(Task, message.task_id)).result == {
            "status_code": 200,
            "body": "hello",
        }


async def test_heartbeat_ownership_and_ttl(worker, monkeypatch) -> None:
    """The heartbeat has a TTL and refuses to remove another owner's replacement."""
    heartbeat = WorkerHeartbeat(worker.redis, worker.context.worker_id, worker.settings.worker)
    await heartbeat.start()
    assert await worker.redis.ttl(heartbeat.key) > 0
    assert worker.settings.worker.health_file.exists()
    await worker.redis.set(heartbeat.key, "replacement", ex=2)
    with pytest.raises(RuntimeError, match="ownership"):
        await heartbeat.run()
    await heartbeat.close()
    assert await worker.redis.get(heartbeat.key) == "replacement"
    await worker.redis.delete(heartbeat.key)


async def bootstrap_groups(settings: Settings) -> None:
    """Start private test groups after retained input belonging to other schemas."""
    for topics, group in (
        (("tasks.pending",), settings.worker.group_id),
        (
            ("tasks.retry.5s", "tasks.retry.30s", "tasks.retry.5m"),
            f"{settings.worker.group_id}.retries",
        ),
    ):
        async with consumer_context(*topics, group_id=group, settings=settings.kafka) as consumer:
            async with asyncio.timeout(10):
                while not consumer.assignment():
                    await consumer.getmany(timeout_ms=100, max_records=1)
            await consumer.seek_to_end()
            await consumer.commit()


async def test_graceful_service_stop_finishes_and_commits_real_kafka_input(worker) -> None:
    """A stop request during a handler drains the attempt, result relay, and input commit."""
    message = await seed(worker, "slow_task", {"seconds": 0.3})
    await bootstrap_groups(worker.settings)
    service = WorkerService(worker.factory, worker.redis, worker.settings)
    running = asyncio.create_task(service.run())
    try:
        async with producer_context() as producer:
            key = message_key(message.task_id, message.priority)
            metadata = await producer.send_and_wait(
                "tasks.pending", key=key, value=message, partition=pending_partition(key)
            )
        async with asyncio.timeout(15):
            while True:
                async with worker.factory() as session:
                    state = (await session.get(Task, message.task_id)).status
                if state == TaskStatus.RUNNING:
                    break
                if running.done():
                    running.result()
                    pytest.fail("Worker stopped before executing the task")
                await asyncio.sleep(0.01)
            service.request_stop()
            await running
        async with worker.factory() as session:
            task = await session.get(Task, message.task_id)
            assert task.status == TaskStatus.SUCCEEDED and task.attempts == 1
            assert (await session.scalars(select(Outbox))).one().published_at is not None
        admin = AIOKafkaAdminClient(bootstrap_servers=worker.settings.kafka.bootstrap_servers)
        try:
            await admin.start()
            partition = TopicPartition("tasks.pending", metadata.partition)
            offsets = await admin.list_consumer_group_offsets(
                worker.settings.worker.group_id, partitions=[partition]
            )
            assert offsets[partition].offset >= metadata.offset + 1
        finally:
            await admin.close()
        assert not worker.settings.worker.health_file.exists()
        assert not await worker.redis.exists(f"worker:{worker.context.worker_id}")
    finally:
        service.request_stop()
        if not running.done():
            await asyncio.wait_for(running, 10)


def fast_retry_settings(worker, **options) -> Settings:
    """Keep integration delays short while exercising real consumers and durable dispatch."""
    settings = worker.settings.model_copy(
        update={
            "worker": worker.settings.worker.model_copy(
                update={
                    "retry_base_seconds": 0.01,
                    "retry_cap_seconds": 0.05,
                    "retry_engine_poll_seconds": 0.01,
                    **options,
                }
            )
        }
    )
    worker.executor.settings = settings
    return settings


async def drive_to_terminal(worker, message: TaskMessage, settings: Settings) -> TaskStatus:
    """Run a complete worker/retry engine until an actual terminal outcome commits."""
    await bootstrap_groups(settings)
    service = WorkerService(worker.factory, worker.redis, settings)
    running = asyncio.create_task(service.run())
    try:
        async with producer_context() as producer:
            key = message_key(message.task_id, message.priority)
            await producer.send_and_wait(
                "tasks.pending", key=key, value=message, partition=pending_partition(key)
            )
        async with asyncio.timeout(25):
            while True:
                async with worker.factory() as session:
                    task = await session.get(Task, message.task_id)
                    state = task.status
                if state in {TaskStatus.SUCCEEDED, TaskStatus.DEAD_LETTERED}:
                    return state
                if running.done():
                    running.result()
                    raise RuntimeError("Worker stopped before reaching a terminal outcome")
                await asyncio.sleep(0.02)
    finally:
        service.request_stop()
        await asyncio.wait_for(running, 10)


async def test_flaky_task_retries_until_success(worker, monkeypatch) -> None:
    """Force two failures then success through real Kafka retry topics and the delayed inbox."""
    from services.worker import handlers

    draws = iter([0.1, 0.1, 0.9])
    monkeypatch.setattr(handlers, "random", SimpleNamespace(random=lambda: next(draws)))
    message = await seed(worker, "flaky_task", max_retries=5)
    assert (
        await drive_to_terminal(worker, message, fast_retry_settings(worker))
        == TaskStatus.SUCCEEDED
    )
    async with worker.factory() as session:
        task = await session.get(Task, message.task_id)
        assert task.attempts == 3 and task.result == {"ok": True}
        attempts = list(await session.scalars(select(TaskAttempt).order_by(TaskAttempt.attempt_no)))
        assert all(item.error and item.stack_trace for item in attempts[:2])
        assert attempts[2].error is None
        schedules = list(await session.scalars(select(RetrySchedule)))
        assert len(schedules) == 2
        assert all(row.received_at and row.dispatched_at for row in schedules)
        topics = list(await session.scalars(select(Outbox.topic)))
        assert topics.count("tasks.retry.5s") == topics.count("tasks.retry.30s") == 1
        assert "tasks.dlq" not in topics


async def test_fatal_error_goes_directly_to_dlq(worker) -> None:
    """A real ValidationError ignores the remaining budget and publishes complete history."""
    message = await seed(worker, "bad_input_task", max_retries=10)
    state = await drive_to_terminal(worker, message, fast_retry_settings(worker))
    assert state == TaskStatus.DEAD_LETTERED
    async with worker.factory() as session:
        assert (await session.get(Task, message.task_id)).attempts == 1
        dead_letter = (await session.scalars(select(DeadLetter))).one()
        assert dead_letter.reason == "fatal error"
        assert len(dead_letter.error_history) == 1
        assert "ValidationError" in dead_letter.error_history[0]["error"]
        assert dead_letter.error_history[0]["stack_trace"]
        assert await session.scalar(select(func.count()).select_from(RetrySchedule)) == 0
    async with consumer_context("tasks.dlq", group_id=f"fatal-dlq-{uuid4()}") as consumer:
        async with asyncio.timeout(10):
            while True:
                record = await consumer.getone()
                if record.value.get("task_id") == str(message.task_id):
                    break
        assert record.value["error_history"] == dead_letter.error_history
        assert record.value["reason"] == "fatal error"


@pytest.mark.parametrize("budget", [0, 1, 3])
async def test_max_retries_is_total_attempt_budget(worker, budget: int) -> None:
    """Exhaustion produces exactly max(1, max_retries) attempts and one DLQ event."""
    name = f"always_fails_{uuid4().hex}"

    @register(name)
    async def always_fails(payload, context):
        raise RetryableError("Dependency unavailable")

    message = await seed(worker, name, max_retries=budget)
    assert (
        await drive_to_terminal(worker, message, fast_retry_settings(worker))
        == TaskStatus.DEAD_LETTERED
    )
    expected = max(1, budget)
    async with worker.factory() as session:
        assert (await session.get(Task, message.task_id)).attempts == expected
        letter = (await session.scalars(select(DeadLetter))).one()
        assert letter.reason == "attempt budget exhausted"
        assert [item["attempt_no"] for item in letter.error_history] == list(range(1, expected + 1))
        assert all(item["stack_trace"] for item in letter.error_history)
        assert await session.scalar(select(func.count()).select_from(RetrySchedule)) == expected - 1
        assert (
            await session.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.topic == "tasks.dlq")
            )
            == 1
        )


async def test_poison_crashes_have_independent_claim_limit(worker) -> None:
    """Two claims without completion reach the poison limit despite a budget of ten."""
    settings = fast_retry_settings(
        worker, retry_base_seconds=0, retry_cap_seconds=0, max_crash_claims=2
    )
    engine = RetryEngine(worker.factory, settings, worker.executor)
    message = await seed(worker, "slow_task", {"seconds": 0}, max_retries=10)
    for number in (1, 2):
        token = uuid4().hex
        key = worker.executor.lease_key(message.task_id)
        assert await worker.redis.set(key, token, nx=True, px=10000)
        claimed = await worker.executor._claim(message, token)
        assert claimed.attempt_no == number
        assert await worker.executor.recover_abandoned() == 0
        # Simulate a crashed worker whose lease has expired; no handler completes.
        await worker.redis.delete(key)
        recovered = await asyncio.gather(
            worker.executor.recover_abandoned(), worker.executor.recover_abandoned()
        )
        assert sum(recovered) == 1
        if number == 1:
            async with worker.factory() as session:
                retry = await session.get(Outbox, event_id(message.task_id, 1, "retry"))
                envelope = RetryMessage.model_validate(retry.payload)
            await engine.ingest(envelope)
            assert await engine.dispatch_due() == 1
            async with worker.factory() as session:
                pending = await session.get(Outbox, event_id(message.task_id, 1, "redispatch"))
                message = TaskMessage.model_validate(pending.payload)
    assert await worker.executor.wait_published(
        (event_id(message.task_id, 2, "result"), event_id(message.task_id, 2, "dlq"))
    )
    async with worker.factory() as session:
        task = await session.get(Task, message.task_id)
        assert task.status == TaskStatus.DEAD_LETTERED and task.attempts == 2
        letter = (await session.scalars(select(DeadLetter))).one()
        assert "poison pill" in letter.reason
        assert len(letter.error_history) == 2
        assert all(item["error"].startswith("WorkerCrash:") for item in letter.error_history)


async def test_delay_inbox_is_nonblocking_idempotent_and_concurrency_safe(
    worker, monkeypatch
) -> None:
    """A due retry bypasses a future retry, and racing dispatchers create one event."""
    from services.worker import retry_policy

    settings = fast_retry_settings(worker, retry_base_seconds=60, retry_cap_seconds=60)
    name = f"delay_test_{uuid4().hex}"

    @register(name)
    async def fail(payload, context):
        raise RetryableError("try later")

    messages = [await seed(worker, name), await seed(worker, name)]
    delays = iter([60, 0])
    monkeypatch.setattr(retry_policy, "full_jitter", lambda *args: next(delays))
    for message in messages:
        assert await worker.executor.process(message)
    engine = RetryEngine(worker.factory, settings, worker.executor)
    async with worker.factory() as session:
        rows = [
            await session.get(Outbox, event_id(message.task_id, 1, "retry")) for message in messages
        ]
        envelopes = [RetryMessage.model_validate(row.payload) for row in rows]
    # Receipt never waits for due_at, including an out-of-order duplicate.
    async with asyncio.timeout(1):
        await engine.ingest(envelopes[0])
        await engine.ingest(envelopes[1])
        await engine.ingest(envelopes[0])
    assert sum(await asyncio.gather(engine.dispatch_due(), engine.dispatch_due())) == 1
    await engine.ingest(envelopes[1])
    assert await engine.dispatch_due() == 0
    # An old dispatch must not trigger an early extra attempt while RETRYING/SCHEDULED.
    assert await worker.executor.process(messages[0])
    assert await worker.executor.process(messages[1])
    async with worker.factory() as session:
        first = await session.get(Task, messages[0].task_id)
        second = await session.get(Task, messages[1].task_id)
        assert first.status == TaskStatus.RETRYING and first.attempts == 1
        assert second.status == TaskStatus.SCHEDULED and second.attempts == 1
        assert (await session.get(RetrySchedule, envelopes[0].event_id)).dispatched_at is None
        assert (
            await session.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.topic == "tasks.pending")
            )
            == 1
        )


async def test_dlq_retains_full_large_error_history(worker) -> None:
    """The actual broker accepts a full history larger than its former 1 MiB limit."""
    message = await seed(worker, "bad_input_task", max_retries=100)
    trace = "trace line\n" * 2900
    async with worker.factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        task = await session.get(Task, message.task_id)
        task.status = TaskStatus.SCHEDULED
        task.attempts = 29
        session.add_all(
            [
                TaskAttempt(
                    task_id=task.id,
                    attempt_no=number,
                    worker_id="history-worker",
                    started_at=now,
                    finished_at=now,
                    error="failure" * 1000,
                    stack_trace=trace,
                )
                for number in range(1, 30)
            ]
        )
    message = message.model_copy(update={"attempt_no": 30})
    assert await worker.executor.process(message)
    async with worker.factory() as session:
        letter = (await session.scalars(select(DeadLetter))).one()
        assert len(letter.error_history) == 30
        envelope = await session.get(Outbox, letter.id)
        assert len(serialize_json(envelope.payload)) > 1024 * 1024
        assert envelope.published_at is not None
    async with consumer_context("tasks.dlq", group_id=f"large-dlq-{uuid4()}") as consumer:
        async with asyncio.timeout(10):
            while True:
                record = await consumer.getone()
                if record.value.get("task_id") == str(message.task_id):
                    break
        assert len(record.value["error_history"]) == 30
        assert record.value["error_history"][0]["stack_trace"] == trace


async def test_java_namespace_is_not_executed_by_python(worker) -> None:
    """An actual Java task row remains untouched while the independent Java group owns it."""
    message = await seed(
        worker, "java.data_transform", {"operation": "uppercase", "values": ["test"]}
    )
    assert await worker.executor.process(message)
    async with worker.factory() as session:
        task = await session.get(Task, message.task_id)
        assert task.status == TaskStatus.PENDING and task.attempts == 0
        assert await session.scalar(select(func.count()).select_from(TaskAttempt)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
