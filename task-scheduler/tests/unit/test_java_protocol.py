"""Shared artifacts and namespace filtering protect both independent consumer groups."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from scripts.export_contracts import generated_files
from services.worker.consumer import ConsumerLoop
from services.worker.executor import TaskExecutor
from shared.config import get_settings
from shared.models import TaskMessage


def test_generated_protocol_artifacts_are_current() -> None:
    """Schema, identity/fingerprint vectors, and Redis Lua must agree with their Python source."""
    for path, expected in generated_files().items():
        assert path.read_text() == expected, (
            f"Regenerate {path.name} with scripts/export_contracts.py"
        )


async def test_python_consumer_ignores_java_before_schema_validation() -> None:
    """Java records cannot trigger Python execution or fail its schema checks."""
    executor = SimpleNamespace(process=AsyncMock())
    settings = SimpleNamespace(worker=SimpleNamespace(concurrency=1))
    consumer = ConsumerLoop(executor, settings)
    await consumer.semaphore.acquire()
    assert await consumer.execute_record(
        SimpleNamespace(value=b'{"type":"java.custom","future_field":true}')
    )
    executor.process.assert_not_awaited()


async def test_direct_python_executor_never_claims_java_task() -> None:
    """Even a direct validated dispatch cannot create an attempt or Redis lease for a Java task."""
    factory, redis = AsyncMock(), AsyncMock()
    executor = TaskExecutor(
        factory, redis, get_settings(), SimpleNamespace(worker_id="python-test")
    )
    message = TaskMessage(
        task_id=uuid4(), type="java.data_transform", scheduled_at="2026-11-10T12:00:00Z"
    )
    assert await executor.process(message)
    factory.assert_not_called()
    assert not redis.mock_calls


def test_protocol_counters_fit_shared_postgres_and_java_integer_fields() -> None:
    """Canonical envelopes must not advertise counters a Java/JPA integer cannot represent."""
    import pytest
    from pydantic import ValidationError

    for override in ({"generation": 2147483648}, {"attempt_no": 2147483648}):
        with pytest.raises(ValidationError):
            TaskMessage(
                task_id=uuid4(),
                type="java.data_transform",
                scheduled_at="2026-11-10T12:00:00Z",
                **override,
            )
