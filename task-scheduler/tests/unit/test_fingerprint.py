"""Error clusters ignore volatile values while retaining the underlying failure shape."""

from uuid import uuid4

import pytest

from services.worker.retry_policy import event_id
from shared.fingerprint import failure_category, failure_signature, normalize_failure
from shared.models import TaskMessage


def test_similar_errors_share_signature() -> None:
    """UUIDs, arbitrary named IDs, timestamps, numbers, and deployment paths are volatile."""
    first = (
        "request_id=req-alpha task 023ca111-f987-4acd-ab42-332abcaf1651 "
        "failed at 2026-11-10T10:01:02.123Z after 17 seconds"
    )
    second = (
        "request_id=req-beta task 123ca111-f987-4acd-ab42-332abcaf1652 "
        "failed at 2026-11-11T13:55:42.999Z after 29 seconds"
    )
    trace1 = (
        'File "/app/worker/handler.py", line 123, in run\nConnectionError: peer 192.168.1.20:12345'
    )
    trace2 = 'File "/home/build/handler.py", line 456, in run\nConnectionError: peer 10.0.0.5:56789'
    assert failure_signature(first, trace1) == failure_signature(second, trace2)
    assert len(failure_signature(first, trace1)) == 64


def test_different_roots_remain_distinct() -> None:
    """A timeout and a validation failure must not collapse into one cluster."""
    assert failure_signature("TimeoutError: connection 123") != failure_signature(
        "ValidationError: connection 456"
    )
    assert normalize_failure("worker_id=worker-alpha 0x1234abcd") == "worker_id=<id> <id>"


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("fatal error", "fatal"),
        ("attempt budget exhausted", "exhausted"),
        ("poison pill: worker crashed", "poison"),
        ("other", "unknown"),
    ],
)
def test_categories(reason: str, expected: str) -> None:
    """Operational categories use stable names for queue filters."""
    assert failure_category(reason) == expected


def test_generation_changes_outcome_identity() -> None:
    """Resetting attempt numbers must not collide with the preceding execution's result."""
    task_id = uuid4()
    assert event_id(task_id, 1, "result") == event_id(task_id, 1, "result", 0)
    assert event_id(task_id, 1, "result", 0) != event_id(task_id, 1, "result", 1)
    assert TaskMessage.model_fields["generation"].default == 0
