"""Priority aliases, deterministic routing, and public validation boundaries."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from services.api.priority import message_key, pending_partition
from services.api.schemas import TaskSubmit


@pytest.mark.parametrize(
    ("name", "numeric", "partitions"),
    [
        ("high", 9, {0, 1}),
        ("normal", 5, {2, 3}),
        ("low", 1, {4, 5}),
    ],
)
def test_priority_alias_routes_to_its_lane(name: str, numeric: int, partitions: set[int]) -> None:
    """Aliases normalize before persistence and UUID hashing stays within the lane."""
    body = TaskSubmit(type="test", priority=name)
    assert body.priority == numeric
    for _ in range(10):
        key = message_key(uuid4(), body.priority)
        assert key.startswith(f"{name}:")
        assert pending_partition(key) in partitions
        assert pending_partition(key) == pending_partition(key)


@pytest.mark.parametrize("priority", [-1, 10, True, "urgent", "9", None])
def test_invalid_api_priority(priority: object) -> None:
    """Reject values outside the declared numeric/alias contract."""
    with pytest.raises(ValidationError):
        TaskSubmit(type="test", priority=priority)


def test_normal_is_default() -> None:
    """The HTTP API defaults to normal, retaining shared numeric schema compatibility."""
    assert TaskSubmit(type="test").priority == 5
