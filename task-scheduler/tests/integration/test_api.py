"""HTTP API, real transactional persistence, and recoverable Kafka publication."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from services.api.main import create_app
from services.api.priority import pending_partition
from shared.db import create_session_factory
from shared.kafka import consumer_context, producer_context
from shared.models import TaskMessage, TaskStatus
from shared.tables import Outbox, Task, TaskAttempt

pytestmark = pytest.mark.integration


@asynccontextmanager
async def open_api(
    engine: AsyncEngine, *, publisher: bool = False
) -> AsyncIterator[tuple[AsyncClient, FastAPI]]:
    """Exercise the real ASGI stack and lifespan with an isolated database."""
    app = create_app(engine=engine, start_publisher=publisher)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client, app


@pytest.fixture
async def api(database: tuple[AsyncEngine, str]) -> AsyncIterator[tuple[AsyncClient, FastAPI]]:
    """Leave publication stopped so durability can be inspected before sending."""
    async with open_api(database[0]) as instance:
        yield instance


async def test_submit_is_durable_and_background_restart_delivers(database) -> None:
    """A new API instance must publish a task committed before its predecessor stopped."""
    engine, _ = database
    factory = create_session_factory(engine)
    async with open_api(engine) as (client, _):
        response = await client.post(
            "/tasks",
            json={"type": "test.submit", "priority": "high", "payload": {"text": "hello 🌏"}},
        )
        assert response.status_code == 201
        task_id = UUID(response.json()["id"])
        async with factory() as session:
            assert (await session.get(Task, task_id)).status == TaskStatus.PENDING
            outbox = (await session.scalars(select(Outbox))).one()
            assert outbox.published_at is None
            assert TaskMessage.model_validate(outbox.payload).task_id == task_id
            key = outbox.key
    async with consumer_context("tasks.pending", group_id=f"api-test-{uuid4()}") as consumer:
        async with open_api(engine, publisher=True):
            async with asyncio.timeout(15):
                while True:
                    record = await consumer.getone()
                    if record.key == key:
                        break
                assert TaskMessage.model_validate(record.value).task_id == task_id
                assert record.partition == pending_partition(key)
                while True:
                    async with factory() as session:
                        published = await session.scalar(select(Outbox.published_at))
                    if published is not None:
                        break
                    await asyncio.sleep(0.02)


async def test_concurrent_idempotent_requests_create_one_event(api, database) -> None:
    """The database unique constraint must arbitrate simultaneous duplicate requests."""
    client, _ = api
    body = {"type": "test.idempotent", "idempotency_key": "same-key", "payload": {"value": 1}}
    responses = await asyncio.gather(*(client.post("/tasks", json=body) for _ in range(8)))
    assert sorted(response.status_code for response in responses) == [200] * 7 + [201]
    assert len({response.json()["id"] for response in responses}) == 1
    repeated = await client.post("/tasks", json={**body, "payload": {"value": 999}})
    assert repeated.status_code == 200
    assert repeated.headers["idempotency-replayed"] == "true"
    assert repeated.json()["payload"] == {"value": 1}
    async with create_session_factory(database[0])() as session:
        assert await session.scalar(select(func.count()).select_from(Task)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_without_key_tasks_are_distinct(api) -> None:
    """A missing idempotency key must not collapse unrelated submissions."""
    client, _ = api
    first = await client.post("/tasks", json={"type": "test"})
    second = await client.post("/tasks", json={"type": "test"})
    assert first.status_code == second.status_code == 201
    assert first.json()["id"] != second.json()["id"]


@pytest.mark.parametrize("state", list(TaskStatus))
async def test_cancel_only_pending_or_scheduled(api, database, state: TaskStatus) -> None:
    """Cancellation must reject all states beyond the API's two permitted ones."""
    client, _ = api
    created = await client.post("/tasks", json={"type": "test.cancel"})
    identifier = created.json()["id"]
    # Seed each valid database state directly; service writes still use lifecycle guards.
    async with create_session_factory(database[0]).begin() as session:
        await session.execute(update(Task).where(Task.id == UUID(identifier)).values(status=state))
    response = await client.post(f"/tasks/{identifier}/cancel")
    allowed = state in {TaskStatus.PENDING, TaskStatus.SCHEDULED}
    assert response.status_code == (200 if allowed else 409)
    task = (await client.get(f"/tasks/{identifier}")).json()
    assert task["status"] == ("CANCELLED" if allowed else state.value)


async def test_history_filtering_and_pagination(api, database) -> None:
    """GET endpoints must expose ordered attempts and consistent exact filters."""
    client, _ = api
    identifiers = []
    for task_type in ["test.a", "test.a", "test.b"]:
        response = await client.post("/tasks", json={"type": task_type})
        identifiers.append(response.json()["id"])
    identifier = UUID(identifiers[0])
    async with create_session_factory(database[0]).begin() as session:
        session.add_all(
            [
                TaskAttempt(task_id=identifier, attempt_no=2, worker_id="worker-2", error="second"),
                TaskAttempt(task_id=identifier, attempt_no=1, worker_id="worker-1", error="first"),
            ]
        )
    detail = (await client.get(f"/tasks/{identifier}")).json()
    assert [attempt["attempt_no"] for attempt in detail["attempt_history"]] == [1, 2]
    page = (
        await client.get("/tasks", params={"type": "test.a", "status": "PENDING", "limit": 1})
    ).json()
    assert page["total"] == 2
    assert len(page["items"]) == 1
    second_page = (
        await client.get("/tasks", params={"type": "test.a", "limit": 1, "offset": 1})
    ).json()
    assert second_page["items"][0]["id"] != page["items"][0]["id"]


async def test_request_ids_errors_docs_and_operations(api) -> None:
    """Normal, validation, missing-route, and conflict responses retain request identity."""
    client, _ = api
    response = await client.post(
        "/tasks",
        json={"type": "bad type", "payload": {"secret": "hidden"}},
        headers={"X-Request-ID": "caller-123"},
    )
    assert response.status_code == 422
    assert response.headers["x-request-id"] == "caller-123"
    assert response.json()["request_id"] == "caller-123"
    assert "hidden" not in response.text
    for path in [f"/tasks/{uuid4()}", "/unknown"]:
        missing = await client.get(path)
        assert missing.status_code == 404
        assert missing.json()["request_id"] == missing.headers["x-request-id"]
    assert (await client.get("/tasks", params={"limit": 101})).status_code == 422
    assert (await client.post(f"/tasks/{uuid4()}/cancel")).status_code == 404
    assert (await client.get("/docs")).status_code == 200
    specification = (await client.get("/openapi.json")).json()
    assert "/tasks" in specification["paths"]
    schema = specification["paths"]["/tasks"]["post"]["responses"]["422"]["content"][
        "application/json"
    ]["schema"]
    assert schema["$ref"].endswith("/ErrorOut")
    metrics = await client.get("/metrics")
    assert metrics.status_code == 200
    assert 'service_up{service="api"} 1.0' in metrics.text
    healthy = await client.get("/health")
    assert healthy.status_code == 200, healthy.text
    assert healthy.json()["checks"]["database"] == "ok"


async def test_task_and_outbox_roll_back_together(api, database, monkeypatch) -> None:
    """An error after task insertion cannot leave a committed task without its event."""
    from services.api import main

    def fail(*args: object) -> str:
        raise RuntimeError("private-error-content")

    monkeypatch.setattr(main, "message_key", fail)
    response = await api[0].post("/tasks", json={"type": "test.rollback"})
    assert response.status_code == 500
    assert response.json()["request_id"] == response.headers["x-request-id"]
    assert "private-error-content" not in response.text
    async with create_session_factory(database[0])() as session:
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_oversized_message_is_rejected_atomically(api, database) -> None:
    """Reject messages that cannot fit the Kafka envelope without retaining a task."""
    client, app = api
    app.state.settings = app.state.settings.model_copy(
        update={"api": app.state.settings.api.model_copy(update={"max_task_message_bytes": 512})}
    )
    response = await client.post(
        "/tasks", json={"type": "test.large", "payload": {"text": "x" * 1000}}
    )
    assert response.status_code == 413
    async with create_session_factory(database[0])() as session:
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_publish_failure_leaves_event_pending_for_retry(api, database) -> None:
    """A failed broker send must not mark an event as published."""
    client, app = api
    assert (await client.post("/tasks", json={"type": "test.retry"})).status_code == 201
    producer = AsyncMock()
    producer.send_and_wait.side_effect = ConnectionError("broker unavailable")
    with pytest.raises(ConnectionError):
        await app.state.publisher.publish_batch(producer)
    async with create_session_factory(database[0])() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is None
    producer.send_and_wait.side_effect = None
    assert await app.state.publisher.publish_batch(producer) == 1
    async with create_session_factory(database[0])() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is not None


async def test_acknowledged_event_is_replayed_after_failed_transaction(api, database) -> None:
    """A crash-equivalent exception after real Kafka acknowledgement produces safe replay."""
    client, app = api
    created = await client.post("/tasks", json={"type": "test.replay", "priority": "low"})
    task_id = created.json()["id"]
    async with producer_context() as producer:

        class CrashAfterAcknowledgement:
            async def send_and_wait(self, *args: object, **kwargs: object) -> None:
                await producer.send_and_wait(*args, **kwargs)
                raise RuntimeError("crashed before recording published_at")

        with pytest.raises(RuntimeError, match="crashed"):
            await app.state.publisher.publish_batch(CrashAfterAcknowledgement())
        async with create_session_factory(database[0])() as session:
            assert (await session.scalars(select(Outbox))).one().published_at is None
        assert await app.state.publisher.publish_batch(producer) == 1
    async with consumer_context("tasks.pending", group_id=f"api-replay-{uuid4()}") as consumer:
        events = []
        async with asyncio.timeout(15):
            while len(events) < 2:
                record = await consumer.getone()
                if str(record.value.get("task_id")) == task_id:
                    events.append(record)
        assert events[0].headers == events[1].headers
        assert events[0].partition == events[1].partition


async def test_two_publishers_skip_locked_rows(api, database) -> None:
    """A second publisher must skip a row held by another publication transaction."""
    client, app = api
    await client.post("/tasks", json={"type": "test.locked"})
    entered = asyncio.Event()
    finish = asyncio.Event()

    async def blocked_send(*args: object, **kwargs: object) -> None:
        entered.set()
        await finish.wait()

    producer = AsyncMock()
    producer.send_and_wait.side_effect = blocked_send
    first = asyncio.create_task(app.state.publisher.publish_batch(producer))
    try:
        async with asyncio.timeout(5):
            await entered.wait()
            assert await app.state.publisher.publish_batch(AsyncMock()) == 0
        finish.set()
        assert await first == 1
    finally:
        finish.set()
        if not first.done():
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)
