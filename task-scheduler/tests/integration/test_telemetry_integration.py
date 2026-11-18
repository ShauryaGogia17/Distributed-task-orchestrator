"""Real migrated history, durable counters, and correlation through the shared relay."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import insert, select

from services.api.main import submit_task
from services.api.schemas import TaskSubmit
from shared.config import get_settings
from shared.db import create_session_factory
from shared.metrics import ServiceMetrics
from shared.outbox import OutboxPublisher
from shared.redis import create_redis_client
from shared.tables import AgentAudit, DeadLetter, Outbox, Task

pytestmark = pytest.mark.integration


async def test_trace_survives_idempotency_and_all_outbox_topics(database) -> None:
    """Triggers persist canonical trace independent of producer language and generation."""
    factory = create_session_factory(database[0])
    settings = get_settings()
    trace = uuid4()
    body = TaskSubmit(type="test.trace", idempotency_key="trace", payload={})
    task, _ = await submit_task(factory, body, max_message_bytes=900000, trace_id=trace)
    duplicate, created = await submit_task(
        factory, body, max_message_bytes=900000, trace_id=uuid4()
    )
    assert not created and duplicate.trace_id == trace
    async with factory.begin() as session:
        session.add_all(
            [
                Outbox(
                    topic=topic,
                    key=str(task.id),
                    payload={"task_id": str(task.id), "generation": 7},
                )
                for topic in ("tasks.retry.5s", "tasks.results")
            ]
        )
    producer = AsyncMock()
    assert await OutboxPublisher(factory, settings).publish_batch(producer) == 3
    for call in producer.send_and_wait.await_args_list:
        headers = dict(call.kwargs["headers"])
        assert headers["trace_id"] == trace.hex.encode()
        assert headers["task_id"] == str(task.id).encode()
    async with factory() as session:
        assert set(await session.scalars(select(Outbox.trace_id))) == {trace}


async def test_durable_metrics_match_history_across_instances(database) -> None:
    """Restart/replay cannot inflate outcomes; token usage is per model call, not cluster member."""
    settings = get_settings()
    factory = create_session_factory(database[0])
    client = create_redis_client(settings.redis)
    now = datetime.now(UTC)
    identifier = uuid4()
    try:
        async with factory.begin() as session:
            await session.execute(
                insert(Task).values(
                    id=identifier,
                    type="test.metrics",
                    payload={},
                    status="RUNNING",
                    updated_at=now - timedelta(seconds=600),
                )
            )
            session.add_all(
                [
                    Outbox(
                        topic="tasks.results",
                        key=str(identifier),
                        payload={
                            "task_id": str(identifier),
                            "type": "test.metrics",
                            "status": status,
                            "duration_seconds": duration,
                        },
                    )
                    for status, duration in [("RETRYING", 0.2), ("SUCCEEDED", 1.2)]
                ]
            )
            session.add_all(
                [
                    DeadLetter(
                        task_id=identifier,
                        task_type="test.metrics",
                        reason="timeout",
                        error_history=[],
                        agent_processed_at=now,
                        ai_diagnosis={"category": "transient", "action": "REQUEUE"},
                    )
                    for _ in range(2)
                ]
            )
            session.add(
                AgentAudit(
                    event="LLM_RESPONSE",
                    details={
                        "usage": {"input_tokens": 17, "output_tokens": 9},
                        "charged_tokens": 26,
                        "micro_usd": 100,
                    },
                )
            )
        metrics = ServiceMetrics("test", factory, client, settings)
        text = (await metrics.render()).decode()
        replica = (await ServiceMetrics("replica", factory, client, settings).render()).decode()

        def samples(exposition):
            return {
                (s.name, tuple(sorted(s.labels.items()))): s.value
                for f in text_string_to_metric_families(exposition)
                for s in f.samples
                if not s.name.startswith(("python_", "process_", "service_"))
                and s.name not in {"outbox_lag", "running_tasks_oldest_age_seconds"}
            }

        assert samples(text) == samples(replica)
        assert "tasks_submitted_total 1.0" in text
        assert 'tasks_processed_total{status="SUCCEEDED",type="test.metrics"} 1.0' in text
        assert 'task_latency_seconds_count{type="test.metrics"} 2.0' in text
        assert 'agent_tokens_total{direction="input"} 17.0' in text
        assert 'agent_diagnoses_total{action="REQUEUE",category="transient"} 2.0' in text
        assert "stuck_running_tasks 1.0" in text
    finally:
        await client.aclose()
