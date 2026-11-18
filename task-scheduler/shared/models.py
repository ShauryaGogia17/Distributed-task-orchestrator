"""Validated API/wire schemas and the explicit task lifecycle contract."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    model_validator,
)


class TaskStatus(StrEnum):
    """Durable states of a task, including its dead-letter recovery lifecycle."""

    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"
    DEAD_LETTERED = "DEAD_LETTERED"
    AI_DIAGNOSED = "AI_DIAGNOSED"
    REQUEUED = "REQUEUED"
    ESCALATED = "ESCALATED"
    CANCELLED = "CANCELLED"


class DLQStatus(StrEnum):
    """Queue incident lifecycle, separate from the current task execution status."""

    OPEN = "OPEN"
    RETRIED = "RETRIED"
    DISCARDED = "DISCARDED"
    ESCALATED = "ESCALATED"


TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.SCHEDULED, TaskStatus.CANCELLED}),
    TaskStatus.SCHEDULED: frozenset({TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.CANCELLED}),
    TaskStatus.RUNNING: frozenset({TaskStatus.SUCCEEDED, TaskStatus.RETRYING, TaskStatus.FAILED}),
    TaskStatus.RETRYING: frozenset({TaskStatus.SCHEDULED, TaskStatus.FAILED, TaskStatus.CANCELLED}),
    TaskStatus.FAILED: frozenset({TaskStatus.DEAD_LETTERED}),
    TaskStatus.DEAD_LETTERED: frozenset(
        {TaskStatus.AI_DIAGNOSED, TaskStatus.REQUEUED, TaskStatus.CANCELLED}
    ),
    TaskStatus.AI_DIAGNOSED: frozenset(
        {TaskStatus.REQUEUED, TaskStatus.ESCALATED, TaskStatus.CANCELLED}
    ),
    TaskStatus.REQUEUED: frozenset({TaskStatus.PENDING, TaskStatus.CANCELLED}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.ESCALATED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


class InvalidTransition(ValueError):
    """Raised when an operation attempts an illegal task state transition."""


def validate_transition(current: TaskStatus, target: TaskStatus) -> None:
    """Require a lifecycle edge; self-transitions are also illegal."""
    if target not in TRANSITIONS[current]:
        raise InvalidTransition(f"Illegal task transition: {current.value} -> {target.value}")


TaskType = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True, min_length=1, max_length=128, pattern=r"^[a-zA-Z][a-zA-Z0-9_.-]*$"
    ),
]
IdempotencyKey = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
]
Priority = Annotated[int, Field(strict=True, ge=0, le=9)]
RetryCount = Annotated[int, Field(strict=True, ge=0, le=100)]
AttemptNumber = Annotated[int, Field(strict=True, ge=1, le=2147483647)]


class JSONSchema(BaseModel):
    """Reject unknown input fields and non-finite values unsupported by JSONB."""

    model_config = ConfigDict(extra="forbid", from_attributes=True)

    @model_validator(mode="after")
    def require_finite_json(self) -> JSONSchema:
        """Reject NaN/Infinity before they reach Kafka or PostgreSQL."""
        json.dumps(self.model_dump(mode="python"), default=str, allow_nan=False)
        return self


class TaskCreate(JSONSchema):
    """User-supplied task definition; omitted schedule means immediately eligible."""

    type: TaskType
    payload: dict[str, JsonValue] = Field(default_factory=dict)
    priority: Priority = 0
    max_retries: RetryCount = 3
    idempotency_key: IdempotencyKey | None = None
    scheduled_at: AwareDatetime | None = None


class TaskOut(TaskCreate):
    """Persisted task response, suitable for validation from an ORM task."""

    id: UUID
    trace_id: UUID | None = None
    status: TaskStatus
    attempts: Annotated[int, Field(strict=True, ge=0, le=2147483647)]
    generation: Annotated[int, Field(strict=True, ge=0, le=2147483647)] = 0
    scheduled_at: AwareDatetime
    created_at: AwareDatetime
    updated_at: AwareDatetime
    result: JsonValue | None = None
    last_error: str | None = None
    ai_modified: bool = False
    ai_requeues: int = 0
    schedule_id: UUID | None = None
    scheduled_for: AwareDatetime | None = None


class TaskMessage(JSONSchema):
    """Versioned Kafka dispatch envelope; attempt numbers are one-based."""

    version: Literal[1] = 1
    ai_modified: bool = False
    generation: Annotated[int, Field(strict=True, ge=0, le=2147483647)] = 0
    task_id: UUID
    type: TaskType
    payload: dict[str, JsonValue] = Field(default_factory=dict)
    priority: Priority = 0
    attempt_no: AttemptNumber = 1
    max_retries: RetryCount = 3
    scheduled_at: AwareDatetime
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))


class ErrorRecord(JSONSchema):
    """One failed execution, embedded in a dead letter's JSON error history."""

    attempt_no: AttemptNumber
    generation: Annotated[int, Field(strict=True, ge=0, le=2147483647)] = 0
    worker_id: Annotated[str, StringConstraints(min_length=1, max_length=255)]
    error: Annotated[str, StringConstraints(min_length=1)]
    stack_trace: str | None = None
    recorded_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))


class RetryMessage(JSONSchema):
    """Small durable retry-inbox event; task data remains authoritative in Postgres."""

    version: Literal[1] = 1
    generation: Annotated[int, Field(strict=True, ge=0, le=2147483647)] = 0
    event_id: UUID
    task_id: UUID
    failed_attempt: AttemptNumber
    due_at: AwareDatetime
    delay_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)]


class DeadLetterOut(JSONSchema):
    """A dead-letter event and its optional AI diagnosis and resolution."""

    id: UUID
    task_id: UUID
    reason: str
    error_history: list[ErrorRecord] = Field(default_factory=list)
    ai_diagnosis: dict[str, JsonValue] | None = None
    resolution: Literal["REQUEUED", "ESCALATED", "CANCELLED"] | None = None
    created_at: AwareDatetime
    generation: int = 0
    task_type: str = "unknown"
    worker_id: str | None = None
    attempts: int = 0
    category: str = "unknown"
    status: DLQStatus = DLQStatus.OPEN
    failure_signature: str | None = None
    received_at: AwareDatetime | None = None
