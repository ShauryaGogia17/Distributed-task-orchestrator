"""JSON wire compatibility and precise manual offset acknowledgement."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aiokafka.structs import TopicPartition

from shared.kafka import commit_record, deserialize_json, serialize_json
from shared.models import TaskMessage


def test_message_serialization() -> None:
    """Produce JSON that can be validated directly into the versioned wire schema."""
    message = TaskMessage(task_id=uuid4(), type="test", scheduled_at=datetime.now(UTC))
    encoded = serialize_json(message)
    assert TaskMessage.model_validate(deserialize_json(encoded)) == message


@pytest.mark.parametrize("payload", [b"[]", b"null", b'{"x":NaN}', b'{"x":Infinity}', b"bad"])
def test_invalid_json_payload(payload: bytes) -> None:
    """Kafka envelopes must be strict JSON objects."""
    with pytest.raises(ValueError):
        deserialize_json(payload)


def test_nonfinite_values_cannot_be_published() -> None:
    """Match PostgreSQL JSONB's numeric restrictions at the wire boundary."""
    with pytest.raises(ValueError):
        serialize_json({"x": float("inf")})


async def test_commit_only_the_processed_record() -> None:
    """Never acknowledge prefetched messages or a different partition."""
    consumer = AsyncMock()
    record = SimpleNamespace(topic="tasks.pending", partition=2, offset=41)
    await commit_record(consumer, record)
    consumer.commit.assert_awaited_once_with({TopicPartition("tasks.pending", 2): 42})


async def test_commit_failure_is_propagated() -> None:
    """The caller must know a rebalance prevented acknowledgement."""
    consumer = AsyncMock()
    consumer.commit.side_effect = RuntimeError("rebalance")
    with pytest.raises(RuntimeError, match="rebalance"):
        await commit_record(consumer, SimpleNamespace(topic="tasks.pending", partition=0, offset=0))
