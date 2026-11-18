"""HTTP and transactional service behavior with deterministic database/network boundaries."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from fakes import NOW, Factory, Session, attempt, letter, task
from fastapi import HTTPException
from pydantic import ValidationError

from services.api import dlq, main
from services.api.schemas import ScheduleSubmit, TaskSubmit
from shared.models import DLQStatus, TaskOut, TaskStatus
from shared.tables import DelayedTask, Outbox


@pytest.fixture
async def api_unit(monkeypatch):
    """Own an ASGI lifespan without opening any real DB, Redis, or broker connection."""
    session = Session()
    factory = Factory(session)
    engine = MagicMock()
    engine.dispose = AsyncMock()
    connection = Session()
    engine.connect.return_value = connection
    redis = AsyncMock()
    redis.ping.return_value = True
    monkeypatch.setattr(main, "create_session_factory", lambda _: factory)
    monkeypatch.setattr(main, "create_redis_client", lambda _: redis)
    monkeypatch.setattr(main.ServiceMetrics, "render", AsyncMock(return_value=b"# metric\n"))
    broker = AsyncMock()
    broker.describe_topics.return_value = [{"partitions": [{"leader": 0}] * 6}]
    monkeypatch.setattr(main, "AIOKafkaAdminClient", lambda **_: broker)
    app = main.create_app(engine=engine, start_publisher=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://unit") as client,
    ):
        yield SimpleNamespace(client=client, session=session, app=app, redis=redis, broker=broker)


async def test_submission_deduplicates_and_preserves_canonical_trace(api_unit):
    row = task()
    api_unit.session.scalar_values.extend([row, None, row])
    body = {"type": row.type, "idempotency_key": "same"}
    first = await api_unit.client.post("/tasks", json=body)
    repeated = await api_unit.client.post("/tasks", json=body)
    assert first.status_code == 201 and repeated.status_code == 200
    assert repeated.json()["id"] == str(row.id)
    assert repeated.headers["x-trace-id"] == row.trace_id.hex
    assert len(api_unit.session.added) == 1 and isinstance(api_unit.session.added[0], Outbox)
    assert api_unit.session.committed


async def test_delayed_submit_indexes_after_durable_write(api_unit):
    row = task(scheduled_at=NOW + timedelta(hours=1))
    api_unit.session.scalar_values.append(row)
    from redis.exceptions import ConnectionError as RedisConnectionError

    api_unit.redis.zadd.side_effect = RedisConnectionError("unavailable")
    response = await api_unit.client.post("/tasks", json={"type": row.type})
    assert response.status_code == 201 and response.json()["status"] == "SCHEDULED"
    assert isinstance(api_unit.session.added[0], DelayedTask)
    assert api_unit.session.committed


async def test_submit_enforces_wire_limit_and_unresolved_conflict():
    for session, limit, status in [
        (Session(scalar=[task(payload={"text": "x" * 500})]), 10, 413),
        (Session(scalar=[None, None]), 900000, None),
    ]:
        with pytest.raises(HTTPException if status else RuntimeError) as caught:
            await main.submit_task(
                Factory(session), TaskSubmit(type="report"), max_message_bytes=limit
            )
        if status:
            assert caught.value.status_code == status
        assert not session.committed and not session.added


@pytest.mark.parametrize("status", list(TaskStatus))
async def test_cancel_routes_respect_state_fence(api_unit, status):
    row = task(status)
    api_unit.session.scalar_values.append(row)
    response = await api_unit.client.post(f"/tasks/{row.id}/cancel")
    allowed = status in {TaskStatus.PENDING, TaskStatus.SCHEDULED}
    assert response.status_code == (200 if allowed else 409)
    assert row.status == (TaskStatus.CANCELLED if allowed else status)


async def test_task_queries_and_missing_rows(api_unit):
    row = task()
    api_unit.session.scalar_values.extend([1, None])
    api_unit.session.scalars.side_effect = [[row], [attempt(row)]]
    page = await api_unit.client.get(
        "/tasks",
        params={
            "status": "PENDING",
            "type": row.type,
            "schedule_id": str(uuid4()),
            "limit": 1,
            "offset": 0,
        },
    )
    assert page.status_code == 200 and page.json()["total"] == 1
    api_unit.session.get.side_effect = [row, None]
    detail = await api_unit.client.get(f"/tasks/{row.id}")
    assert detail.json()["attempt_history"][0]["worker_id"] == "unit-worker"
    assert (await api_unit.client.get(f"/tasks/{uuid4()}")).status_code == 404
    assert (await api_unit.client.post(f"/tasks/{uuid4()}/cancel")).status_code == 404


async def test_schedule_create_read_pause_resume_and_not_found(api_unit):
    # Service uses real validation, while fake flush supplies database-generated metadata.
    from shared.tables import Schedule

    async def flush():
        for record in api_unit.session.added:
            if isinstance(record, Schedule):
                record.id = uuid4()
                record.created_at = NOW
                record.updated_at = NOW

    api_unit.session.flush.side_effect = flush
    created = await api_unit.client.post(
        "/schedules", json={"type": "generate_report", "cron": "0 0 * * *", "priority": "high"}
    )
    assert created.status_code == 201, created.text
    schedule = api_unit.session.added[-1]
    api_unit.session.get.side_effect = [schedule, None]
    assert (await api_unit.client.get(f"/schedules/{schedule.id}")).status_code == 200
    assert (await api_unit.client.get(f"/schedules/{uuid4()}")).status_code == 404
    api_unit.session.scalar_values.extend([schedule, schedule, None])
    assert (
        await api_unit.client.patch(f"/schedules/{schedule.id}", json={"enabled": False})
    ).json()["enabled"] is False
    assert (
        await api_unit.client.patch(f"/schedules/{schedule.id}", json={"enabled": True})
    ).json()["enabled"] is True
    assert (
        await api_unit.client.patch(f"/schedules/{uuid4()}", json={"enabled": True})
    ).status_code == 404
    assert ScheduleSubmit(type="x", cron="* * * * *", priority="low").priority == 1


async def test_health_metrics_errors_and_request_headers(api_unit):
    response = await api_unit.client.get("/health")
    assert response.status_code == 200
    assert (await api_unit.client.get("/metrics")).status_code == 200
    api_unit.broker.describe_topics.return_value = []
    assert (await api_unit.client.get("/health")).status_code == 503
    assert (await api_unit.client.post("/tasks", json={"type": "!bad"})).status_code == 422
    response = await api_unit.client.get("/missing", headers={"X-Request-ID": "caller"})
    assert response.status_code == 404 and response.headers["x-request-id"] == "caller"
    api_unit.session.scalar_values.append(RuntimeError("private"))
    response = await api_unit.client.get("/tasks")
    assert response.status_code == 500 and "private" not in response.text


@pytest.mark.parametrize(
    "case", ["missing", "orphan", "wrong_incident", "closed", "replayed", "success", "oversized"]
)
async def test_dlq_retry_safety_and_payload_generation(case):
    row = task(TaskStatus.DEAD_LETTERED, attempts=3)
    incident = letter(row)
    expected = None
    limit = 900000
    if case == "closed":
        incident.status = DLQStatus.ESCALATED
    if case == "replayed":
        incident.status = DLQStatus.RETRIED
        row.generation = 1
    if case == "wrong_incident":
        expected = uuid4()
    if case == "oversized":
        limit = 5
    session = Session(
        scalar=[None if case == "orphan" else row, None if case == "missing" else incident]
    )
    if case in {"success", "replayed"}:
        result, created = await dlq.retry_task(
            Factory(session), row.id, {"seconds": 0}, limit, expected
        )
        assert created == (case == "success")
        if created:
            assert (
                result.status == TaskStatus.PENDING
                and result.attempts == 0
                and result.generation == 1
            )
            assert session.added[-1].payload["payload"] == {"seconds": 0}
    else:
        with pytest.raises(HTTPException) as caught:
            await dlq.retry_task(Factory(session), row.id, {}, limit, expected)
        assert caught.value.status_code == ({"missing": 404, "oversized": 413}.get(case, 409))
        assert not session.committed


async def test_dlq_filter_detail_discard_and_bulk(api_unit, monkeypatch):
    row = task(TaskStatus.DEAD_LETTERED)
    incident = letter(row)
    api_unit.session.scalar_values.extend([1, incident, row, incident])
    api_unit.session.scalars.side_effect = [[incident], [incident], [attempt(row)]]
    response = await api_unit.client.get(
        "/dlq",
        params={
            "type": row.type,
            "category": "transient",
            "failure_signature": "a" * 64,
            "date": "2026-11-10",
            "date_from": "2026-01-01T00:00:00Z",
            "date_to": "2027-01-01T00:00:00Z",
        },
    )
    assert response.status_code == 200 and response.json()["total"] == 1
    api_unit.session.get.return_value = row
    assert (await api_unit.client.get(f"/dlq/{row.id}")).json()["task"]["id"] == str(row.id)
    assert (
        await api_unit.client.delete(f"/dlq/{row.id}")
    ).status_code == 204 and row.status == TaskStatus.CANCELLED
    api_unit.session.scalar_values.extend([None, None, None])
    assert (await api_unit.client.get(f"/dlq/{uuid4()}")).status_code == 404
    assert (await api_unit.client.delete(f"/dlq/{uuid4()}")).status_code == 404
    api_unit.session.scalar_values.append(3)
    api_unit.session.execute.side_effect = [
        type("Result", (), {"all": lambda _: [(row.id, uuid4())] * 3})()
    ]
    result = TaskOut.model_validate(task())
    mocked = AsyncMock(side_effect=[(result, True), (result, False), HTTPException(409, "changed")])
    monkeypatch.setattr(dlq, "retry_task", mocked)
    bulk = await api_unit.client.post("/dlq/retry-bulk", json={})
    assert bulk.json()["retried"] == 1 and len(bulk.json()["skipped"]) == 2
    mocked.side_effect = None
    mocked.return_value = (result, True)
    assert (
        await api_unit.client.post(f"/dlq/{row.id}/retry", json={"payload": {}})
    ).status_code == 200
    with pytest.raises(ValidationError):
        dlq.DLQFilters(date_from=NOW, date_to=NOW)
    api_unit.session.scalar_values.append(letter(row, status=DLQStatus.DISCARDED))
    assert await dlq.latest(api_unit.session, row.id) is None
