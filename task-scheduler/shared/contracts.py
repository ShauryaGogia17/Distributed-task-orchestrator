"""Canonical cross-language result and DLQ envelopes, alongside shared.models dispatch/retry."""

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, JsonValue

from shared.models import ErrorRecord, JSONSchema, TaskType


class TaskResultMessage(JSONSchema):
    """An immutable attempt outcome; the current task row may already be in a later generation."""

    version: Literal[1] = 1
    generation: int = Field(default=0, ge=0, le=2147483647, strict=True)
    task_id: UUID
    type: TaskType
    status: Literal["SUCCEEDED", "RETRYING", "FAILED"]
    attempt_no: int = Field(ge=1, le=2147483647, strict=True)
    worker_id: str = Field(min_length=1, max_length=255)
    result: JsonValue | None
    error: str | None
    duration_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    finished_at: AwareDatetime


class DeadLetterMessage(JSONSchema):
    """Canonical producer format; raw DLQ audit ingestion remains tolerant of external records."""

    version: Literal[1] = 1
    kind: Literal["task.dead_letter"] = "task.dead_letter"
    generation: int = Field(default=0, ge=0, le=2147483647, strict=True)
    worker_id: str = Field(min_length=1, max_length=255)
    category: Literal["fatal", "exhausted", "poison"]
    id: UUID
    task_id: UUID
    type: TaskType
    attempt_no: int = Field(ge=1, le=2147483647, strict=True)
    status: Literal["DEAD_LETTERED"] = "DEAD_LETTERED"
    reason: str = Field(min_length=1)
    error_history: list[ErrorRecord]
    ai_diagnosis: dict[str, JsonValue] | None
    resolution: Literal["REQUEUED", "ESCALATED", "CANCELLED"] | None
    created_at: AwareDatetime
