"""Schema boundaries and task lifecycle behavior, independent of external services."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from shared.models import (
    DeadLetterOut,
    ErrorRecord,
    InvalidTransition,
    TaskCreate,
    TaskMessage,
    TaskStatus,
    validate_transition,
)
from shared.tables import Task


@pytest.mark.parametrize(
    "fields",
    [
        {"type": ""},
        {"type": "bad type"},
        {"type": "x" * 129},
        {"priority": -1},
        {"priority": 10},
        {"priority": True},
        {"priority": "1"},
        {"max_retries": -1},
        {"max_retries": 101},
        {"idempotency_key": " "},
        {"payload": []},
        {"payload": {"bad": float("nan")}},
        {"payload": {"bad": [float("inf")]}},
        {"scheduled_at": datetime(2026, 1, 1)},
        {"unknown": "rejected"},
    ],
)
def test_invalid_task_definitions(fields: dict) -> None:
    """Reject malformed or unsafe input before accepting a task."""
    with pytest.raises(ValidationError):
        TaskCreate.model_validate({"type": "email.send", **fields})


def test_defaults_are_independent_and_zero_retries_is_valid() -> None:
    """Each task owns its payload and supports disabling retry."""
    first = TaskCreate(type="email.send", max_retries=0)
    second = TaskCreate(type="report.build")
    first.payload["recipient"] = "test@example.invalid"
    assert second.payload == {}
    assert first.max_retries == 0
    assert second.priority == 0


def test_message_json_roundtrip() -> None:
    """UUIDs and aware timestamps must survive an actual JSON wire roundtrip."""
    message = TaskMessage(
        task_id=uuid4(),
        type="email.send",
        scheduled_at=datetime.now(UTC),
        payload={"nested": [1, True, None, {"text": "hello 🌏"}]},
    )
    assert TaskMessage.model_validate_json(message.model_dump_json()) == message


@pytest.mark.parametrize("fields", [{"attempt_no": 0}, {"version": 2}, {"task_id": "bad"}])
def test_invalid_message(fields: dict) -> None:
    """Reject unsupported versions, malformed identifiers, and zero-based attempts."""
    with pytest.raises(ValidationError):
        TaskMessage.model_validate(
            {
                "task_id": uuid4(),
                "type": "test",
                "scheduled_at": datetime.now(UTC),
                **fields,
            }
        )


def test_dead_letter_history_is_validated() -> None:
    """Nested error history preserves timestamps and enforces recovery decisions."""
    error = ErrorRecord(attempt_no=1, worker_id="worker-1", error="timed out")
    dead_letter = DeadLetterOut(
        id=uuid4(),
        task_id=uuid4(),
        reason="retry budget exhausted",
        error_history=[error],
        created_at=datetime.now(UTC),
    )
    assert DeadLetterOut.model_validate_json(dead_letter.model_dump_json()) == dead_letter
    with pytest.raises(ValidationError):
        DeadLetterOut.model_validate({**dead_letter.model_dump(), "resolution": "IGNORE"})


@pytest.mark.parametrize(
    "path",
    [
        "PENDING SCHEDULED RUNNING SUCCEEDED",
        "PENDING SCHEDULED PENDING SCHEDULED RUNNING SUCCEEDED",
        "PENDING SCHEDULED RUNNING RETRYING SCHEDULED RUNNING FAILED DEAD_LETTERED "
        "AI_DIAGNOSED REQUEUED PENDING",
        "PENDING SCHEDULED RUNNING FAILED DEAD_LETTERED AI_DIAGNOSED ESCALATED",
        "PENDING SCHEDULED RUNNING FAILED DEAD_LETTERED REQUEUED PENDING",
        "PENDING CANCELLED",
        "PENDING SCHEDULED CANCELLED",
        "PENDING SCHEDULED RUNNING RETRYING CANCELLED",
        "PENDING SCHEDULED RUNNING RETRYING FAILED DEAD_LETTERED CANCELLED",
        "PENDING SCHEDULED RUNNING FAILED DEAD_LETTERED AI_DIAGNOSED CANCELLED",
    ],
)
def test_valid_lifecycle_paths(path: str) -> None:
    """Validate complete execution, retry, recovery, escalation, and cancellation flows."""
    states = [TaskStatus(state) for state in path.split()]
    task = Task(type="test", payload={}, status=states[0])
    for current, target in zip(states, states[1:], strict=False):
        validate_transition(current, target)
        task.status = target
    assert task.status == states[-1]


@pytest.mark.parametrize(
    ("current", "target"),
    [
        ("PENDING", "RUNNING"),
        ("PENDING", "SUCCEEDED"),
        ("RUNNING", "DEAD_LETTERED"),
        ("FAILED", "RETRYING"),
        ("AI_DIAGNOSED", "RUNNING"),
        ("REQUEUED", "RUNNING"),
        ("SUCCEEDED", "PENDING"),
        ("ESCALATED", "PENDING"),
        ("CANCELLED", "SCHEDULED"),
        ("PENDING", "PENDING"),
        ("RUNNING", "RUNNING"),
    ],
)
def test_illegal_transitions(current: str, target: str) -> None:
    """Illegal skips, terminal-state exits, and duplicate transitions must raise."""
    with pytest.raises(InvalidTransition):
        validate_transition(TaskStatus(current), TaskStatus(target))


def test_orm_rejects_illegal_assignment() -> None:
    """Ordinary ORM assignment must also enforce the state machine."""
    task = Task(type="test", status=TaskStatus.PENDING)
    with pytest.raises(InvalidTransition):
        task.status = TaskStatus.SUCCEEDED
    with pytest.raises(ValueError, match="start in PENDING"):
        Task(type="test", status=TaskStatus.RUNNING)
