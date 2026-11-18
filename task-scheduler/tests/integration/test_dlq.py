"""DLQ audit, queue API, replay fencing, and corrected retries against real services."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.api.main import create_app
from services.dlq.consumer import DLQConsumer
from services.worker.executor import TaskExecutor
from services.worker.registry import HandlerContext, load_builtin_handlers
from shared.config import Settings, WorkerSettings, get_settings
from shared.db import create_session_factory
from shared.kafka import consumer_context, serialize_json
from shared.models import DLQStatus, TaskMessage, TaskStatus
from shared.outbox import OutboxPublisher
from shared.redis import create_redis_client
from shared.tables import DeadLetter, DLQReceipt, Outbox, Task, TaskAttempt

pytestmark = pytest.mark.integration


@dataclass
class Resources:
    """One private schema and dependencies; Kafka messages carry unique task IDs."""

    factory: async_sessionmaker[AsyncSession]
    consumer: DLQConsumer
    client: httpx.AsyncClient
    executor: TaskExecutor
    redis: Redis
    settings: Settings


@pytest.fixture
async def dlq(database, tmp_path) -> AsyncIterator[Resources]:
    """Use real Redis/Kafka but isolate every database write and consumer identity."""
    settings = get_settings().model_copy(
        update={
            "worker": WorkerSettings(
                _env_file=None,
                id=f"dlq-test-{uuid4().hex}",
                group_id=f"dlq-test-{uuid4().hex}",
                result_publish_timeout_seconds=5,
                health_file=tmp_path / "worker.json",
            )
        }
    )
    factory = create_session_factory(database[0])
    redis = create_redis_client(settings.redis)
    publisher = OutboxPublisher(factory, settings)
    relay = asyncio.create_task(publisher.run())
    load_builtin_handlers()
    app = create_app(engine=database[0], settings=settings, start_publisher=False)
    async with httpx.AsyncClient() as http_client, app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            executor = TaskExecutor(
                factory, redis, settings, HandlerContext(settings.worker.id, http_client)
            )
            try:
                yield Resources(
                    factory, DLQConsumer(factory, settings), client, executor, redis, settings
                )
            finally:
                await publisher.shutdown(relay)
                async with factory() as session:
                    tasks = list(await session.scalars(select(Task)))
                keys = [
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


def record(payload: dict | bytes, offset: int = 0) -> SimpleNamespace:
    """Represent exactly the metadata used by raw DLQ ingestion."""
    return SimpleNamespace(
        topic="tasks.dlq",
        partition=0,
        offset=offset,
        value=payload if isinstance(payload, bytes) else serialize_json(payload),
    )


async def seed(
    dlq: Resources, task_type: str = "send_email", *, orphan: bool = False, offset: int = 0
) -> dict:
    """Persist a failed task plus one external incident with full worker history."""
    task_id = uuid4()
    if not orphan:
        async with dlq.factory.begin() as session:
            task = Task(
                id=task_id,
                type=task_type,
                payload={"to": "invalid"},
                attempts=1,
            )
            session.add(task)
            await session.flush()
            for status in (TaskStatus.SCHEDULED, TaskStatus.RUNNING, TaskStatus.FAILED):
                task.status = status
            now = await session.scalar(select(func.clock_timestamp()))
            session.add(
                TaskAttempt(
                    task_id=task_id,
                    generation=0,
                    attempt_no=1,
                    worker_id="worker-123",
                    error="ValidationError: bad recipient 123",
                    started_at=now,
                    finished_at=now,
                )
            )
    payload = {
        "id": str(uuid4()),
        "task_id": str(task_id),
        "type": task_type,
        "generation": 0,
        "attempt_no": 1,
        "reason": "fatal error",
        "worker_id": "worker-123",
        "created_at": datetime.now(UTC).isoformat(),
        "error_history": [
            {
                "attempt_no": 1,
                "worker_id": "worker-123",
                "error": "ValidationError: bad recipient 123",
                "stack_trace": 'File "/app/handler.py", line 123',
                "recorded_at": datetime.now(UTC).isoformat(),
            }
        ],
    }
    await dlq.consumer.ingest(record(payload, offset))
    return payload


async def test_persistence_is_idempotent_and_preserves_diagnosis(dlq: Resources) -> None:
    """Duplicate offsets and republished copies retain one incident and every distinct receipt."""
    payload = await seed(dlq)
    async with dlq.factory.begin() as session:
        incident = await session.get(DeadLetter, UUID(payload["id"]))
        incident.ai_diagnosis = {"summary": "Correct the email recipient"}
    await asyncio.gather(dlq.consumer.ingest(record(payload)), dlq.consumer.ingest(record(payload)))
    await dlq.consumer.ingest(record(payload, 1))
    async with dlq.factory() as session:
        assert await session.scalar(select(func.count()).select_from(DeadLetter)) == 1
        assert await session.scalar(select(func.count()).select_from(DLQReceipt)) == 2
        incident = await session.get(DeadLetter, UUID(payload["id"]))
        assert incident.worker_id == "worker-123" and incident.attempts == 1
        assert incident.category == "fatal" and len(incident.failure_signature) == 64
        assert incident.received_at is not None and incident.ai_diagnosis["summary"]
        assert (
            await session.get(Task, UUID(payload["task_id"]))
        ).status == TaskStatus.DEAD_LETTERED


async def test_malformed_and_orphan_records_are_audited(dlq: Resources) -> None:
    """Missing tasks and malformed JSON cannot wedge a partition ahead of valid messages."""
    await dlq.consumer.ingest(record(b"not json", 0))
    await dlq.consumer.ingest(record(b'{"error":"\\u0000"}', 1))
    orphan = await seed(dlq, orphan=True, offset=2)
    valid = await seed(dlq, offset=3)
    async with dlq.factory() as session:
        assert await session.scalar(select(func.count()).select_from(DLQReceipt)) == 4
        receipt = await session.get(DLQReceipt, ("tasks.dlq", 0, 0))
        assert receipt.payload["raw_base64"] == "bm90IGpzb24="
    assert (await dlq.client.post(f"/dlq/{orphan['task_id']}/retry")).status_code == 409
    assert (await dlq.client.get(f"/dlq/{valid['task_id']}")).status_code == 200


async def test_queue_filters_history_and_metrics(dlq: Resources) -> None:
    """Type/category/status/UTC-date filters and history expose persisted diagnostic data."""
    payload = await seed(dlq)
    await seed(dlq, "generate_report", offset=1)
    task_id = payload["task_id"]
    async with dlq.factory.begin() as session:
        (await session.get(DeadLetter, UUID(payload["id"]))).ai_diagnosis = {
            "summary": "bad recipient"
        }
    response = await dlq.client.get(
        "/dlq",
        params={
            "type": "send_email",
            "category": "fatal",
            "status": "OPEN",
            "date": datetime.now(UTC).date().isoformat(),
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["task_id"] == task_id
    detail = (await dlq.client.get(f"/dlq/{task_id}")).json()
    assert detail["ai_diagnosis"]["summary"] == "bad recipient"
    assert detail["attempt_history"][0]["worker_id"] == "worker-123"
    assert detail["error_history"][0]["stack_trace"]
    tomorrow = (datetime.now(UTC) + timedelta(days=1)).date().isoformat()
    assert (await dlq.client.get("/dlq", params={"date": tomorrow})).json()["total"] == 0
    assert 'dlq_size{type="send_email"} 1.0' in (await dlq.client.get("/metrics")).text
    assert (await dlq.client.delete(f"/dlq/{task_id}")).status_code == 204
    assert (await dlq.client.get(f"/dlq/{task_id}")).status_code == 404
    assert 'dlq_size{type="send_email"} 0.0' in (await dlq.client.get("/metrics")).text
    assert (
        await dlq.client.get(
            "/dlq", params={"date_from": tomorrow + "T00:00:00Z", "date_to": "2020-01-01T00:00:00Z"}
        )
    ).status_code == 422


async def kafka_record(topic: str, task_id: str, generation: int = 0):
    """Read real retained Kafka records using an isolated group and bounded deadline."""
    async with consumer_context(topic, group_id=f"dlq-verify-{uuid4()}") as consumer:
        async with asyncio.timeout(15):
            while True:
                candidate = await consumer.getone()
                if (
                    candidate.value.get("task_id") == task_id
                    and candidate.value.get("generation", 0) == generation
                ):
                    return candidate


async def test_corrected_retry_succeeds_and_fences_old_dispatch(dlq: Resources) -> None:
    """A real fatal handler -> Kafka DLQ -> corrected API retry -> Kafka pending -> success."""
    submitted = await dlq.client.post(
        "/tasks", json={"type": "send_email", "payload": {"to": "invalid"}}
    )
    assert submitted.status_code == 201
    data = submitted.json()
    original = await kafka_record("tasks.pending", data["id"])
    original_message = TaskMessage.model_validate(original.value)
    assert await dlq.executor.process(original_message)
    dead = await kafka_record("tasks.dlq", data["id"])
    await dlq.consumer.ingest(dead)
    responses = await asyncio.gather(
        *(
            dlq.client.post(
                f"/dlq/{data['id']}/retry", json={"payload": {"to": "fixed@example.com"}}
            )
            for _ in range(2)
        )
    )
    assert all(response.status_code == 200 for response in responses)
    assert all(
        response.json()["generation"] == 1
        and response.json()["attempts"] == 0
        and response.json()["status"] == "PENDING"
        for response in responses
    )
    pending = await kafka_record("tasks.pending", data["id"], 1)
    assert await dlq.executor.process(TaskMessage.model_validate(pending.value))
    assert await dlq.executor.process(
        original_message
    )  # Stale input is acknowledged, never executed.
    await dlq.consumer.ingest(record(dead.value, 999999))
    async with dlq.factory() as session:
        task = await session.get(Task, UUID(data["id"]))
        assert task.status == TaskStatus.SUCCEEDED and task.result["to"] == "fixed@example.com"
        assert task.attempts == 1 and task.generation == 1
        attempts = list(await session.scalars(select(TaskAttempt).order_by(TaskAttempt.generation)))
        assert [(item.generation, item.attempt_no) for item in attempts] == [(0, 1), (1, 1)]
        assert attempts[0].error and attempts[1].error is None
        incident = (await session.scalars(select(DeadLetter))).one()
        assert incident.status == DLQStatus.RETRIED
        assert (
            await session.scalar(
                select(func.count())
                .select_from(Outbox)
                .where(
                    Outbox.topic == "tasks.pending", Outbox.payload["generation"].as_integer() == 1
                )
            )
            == 1
        )
    assert (await dlq.client.get("/dlq")).json()["total"] == 0
    detail = (await dlq.client.get(f"/dlq/{data['id']}")).json()
    assert len(detail["attempt_history"]) == 2


async def test_discard_tombstone_survives_republished_incident(dlq: Resources) -> None:
    """Discard is repeatable and a fresh copy of the same generation cannot reopen the queue."""
    payload = await seed(dlq)
    assert (await dlq.client.delete(f"/dlq/{payload['task_id']}")).status_code == 204
    assert (await dlq.client.delete(f"/dlq/{payload['task_id']}")).status_code == 204
    await dlq.consumer.ingest(record(payload, 1))
    payload["id"] = str(uuid4())
    await dlq.consumer.ingest(record(payload, 2))
    assert (await dlq.client.get("/dlq")).json()["total"] == 0
    assert (await dlq.client.post(f"/dlq/{payload['task_id']}/retry")).status_code == 404
    async with dlq.factory() as session:
        assert (await session.get(Task, UUID(payload["task_id"]))).status == TaskStatus.CANCELLED
        assert set(await session.scalars(select(DeadLetter.status))) == {DLQStatus.DISCARDED}


async def test_bulk_retry_is_filtered_bounded_and_reports_orphans(dlq: Resources) -> None:
    """A type-filtered batch changes only eligible tasks and reports missing task rows."""
    first = await seed(dlq, offset=0)
    second = await seed(dlq, offset=1)
    orphan = await seed(dlq, orphan=True, offset=2)
    untouched = await seed(dlq, "generate_report", offset=3)
    response = await dlq.client.post(
        "/dlq/retry-bulk", json={"type": "send_email", "category": "fatal", "limit": 1}
    )
    assert response.status_code == 200, response.text
    assert response.json()["matched"] == 3
    assert response.json()["retried"] + len(response.json()["skipped"]) == 1
    response = await dlq.client.post(
        "/dlq/retry-bulk", json={"type": "send_email", "category": "fatal"}
    )
    assert response.status_code == 200
    assert any(item["task_id"] == orphan["task_id"] for item in response.json()["skipped"])
    async with dlq.factory() as session:
        assert (
            await session.get(Task, UUID(untouched["task_id"]))
        ).status == TaskStatus.DEAD_LETTERED
        assert (await session.get(Task, UUID(first["task_id"]))).generation == 1
        assert (await session.get(Task, UUID(second["task_id"]))).generation == 1


async def test_repeated_failure_keeps_generations_and_bulk_filter_scope(dlq: Resources) -> None:
    """A second failed recovery creates a new incident; old filter matches cannot retry it."""
    original = await seed(dlq)
    task_id = original["task_id"]
    response = await dlq.client.post(f"/dlq/{task_id}/retry")
    assert response.status_code == 200 and response.json()["payload"] == {"to": "invalid"}
    pending = await kafka_record("tasks.pending", task_id, 1)
    assert await dlq.executor.process(TaskMessage.model_validate(pending.value))
    failed = await kafka_record("tasks.dlq", task_id, 1)
    await dlq.consumer.ingest(failed)
    detail = (await dlq.client.get(f"/dlq/{task_id}")).json()
    assert detail["generation"] == 1 and detail["status"] == "OPEN"
    assert len(detail["incidents"]) == 2
    response = await dlq.client.post("/dlq/retry-bulk", json={"status": "RETRIED"})
    assert response.status_code == 200
    assert response.json()["matched"] == 1 and response.json()["retried"] == 0
    assert response.json()["skipped"][0]["task_id"] == task_id
    response = await dlq.client.post(
        f"/dlq/{task_id}/retry", json={"payload": {"to": "fixed@example.com"}}
    )
    assert response.json()["generation"] == 2
    pending = await kafka_record("tasks.pending", task_id, 2)
    assert await dlq.executor.process(TaskMessage.model_validate(pending.value))
    await dlq.consumer.ingest(record(original, 999999))
    detail = (await dlq.client.get(f"/dlq/{task_id}")).json()
    assert detail["task"]["status"] == "SUCCEEDED"
    assert [item["generation"] for item in detail["attempt_history"]] == [0, 1, 2]
    assert all(item["status"] == "RETRIED" for item in detail["incidents"])


async def test_stale_retry_schedule_cannot_dispatch_new_generation(
    dlq: Resources, monkeypatch
) -> None:
    """Equal attempt numbers across executions retain distinct inbox and outcome identities."""
    from services.worker.retry_engine import RetryEngine
    from services.worker.retry_policy import event_id
    from shared.models import RetryMessage
    from shared.tables import RetrySchedule

    monkeypatch.setattr("services.worker.retry_policy.full_jitter", lambda *args: 0.0)
    original = await seed(dlq, "http_call")
    task_id = UUID(original["task_id"])
    async with dlq.factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        session.add(
            RetrySchedule(
                id=event_id(task_id, 1, "retry"),
                task_id=task_id,
                generation=0,
                failed_attempt=1,
                due_at=now,
                received_at=now,
            )
        )
    response = await dlq.client.post(
        f"/dlq/{task_id}/retry", json={"payload": {"url": "http://127.0.0.1:1/"}}
    )
    assert response.status_code == 200
    pending = await kafka_record("tasks.pending", str(task_id), 1)
    assert await dlq.executor.process(TaskMessage.model_validate(pending.value))
    retry = RetryEngine(dlq.factory, dlq.settings, dlq.executor)
    assert await retry.dispatch_due() == 0
    async with dlq.factory() as session:
        assert (await session.get(Task, task_id)).status == TaskStatus.RETRYING
        assert await session.scalar(select(func.count()).select_from(RetrySchedule)) == 2
        payload = await session.scalar(
            select(Outbox.payload).where(Outbox.id == event_id(task_id, 1, "retry", 1))
        )
    await retry.ingest(RetryMessage.model_validate(payload))
    assert await retry.dispatch_due() == 1
    async with dlq.factory() as session:
        task = await session.get(Task, task_id)
        assert task.generation == 1 and task.status == TaskStatus.SCHEDULED


async def test_oversized_retry_rolls_back_the_whole_action(dlq: Resources) -> None:
    """An invalid replacement cannot close the incident, reset history, or enqueue work."""
    original = await seed(dlq)
    response = await dlq.client.post(
        f"/dlq/{original['task_id']}/retry",
        json={
            "payload": {
                "blob": "x" * dlq.settings.api.max_task_message_bytes,
            }
        },
    )
    assert response.status_code == 413
    async with dlq.factory() as session:
        task = await session.get(Task, UUID(original["task_id"]))
        assert (
            task.status == TaskStatus.DEAD_LETTERED and task.generation == 0 and task.attempts == 1
        )
        assert (await session.get(DeadLetter, UUID(original["id"]))).status == DLQStatus.OPEN
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_external_failure_without_attempt_metadata_uses_task_snapshot(dlq: Resources) -> None:
    """Minimal terminal failure records retain authoritative counters and advance FAILED."""
    async with dlq.factory.begin() as session:
        task = Task(type="send_email", attempts=2)
        session.add(task)
        await session.flush()
        task_id = task.id
        for status in (TaskStatus.SCHEDULED, TaskStatus.RUNNING, TaskStatus.FAILED):
            task.status = status
    incident_id = await dlq.consumer.ingest(
        record({"task_id": str(task_id), "reason": "fatal error"})
    )
    async with dlq.factory() as session:
        assert (await session.get(Task, task_id)).status == TaskStatus.DEAD_LETTERED
        incident = await session.get(DeadLetter, incident_id)
        assert incident.attempts == 2 and incident.task_type == "send_email"
