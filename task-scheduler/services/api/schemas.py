"""HTTP-specific input aliases and task history/pagination responses."""

from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, Field, field_validator

from shared.models import JSONSchema, Priority, TaskCreate, TaskOut
from shared.scheduling import ScheduleCreate


class TaskSubmit(TaskCreate):
    """Accept named priorities as well as the shared numeric 0–9 representation."""

    priority: Priority = 5

    @field_validator(
        "priority", mode="before", json_schema_input_type=int | Literal["high", "normal", "low"]
    )
    @classmethod
    def normalize_priority(cls, value: object) -> object:
        """Store aliases as high=9, normal=5, low=1 without coercing other input."""
        if isinstance(value, str):
            return {"high": 9, "normal": 5, "low": 1}.get(value, value)
        return value


class AttemptOut(JSONSchema):
    """An execution attempt, ordered by its one-based attempt number."""

    attempt_no: int
    generation: int = 0
    worker_id: str
    started_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    error: str | None = None
    stack_trace: str | None = None


class ScheduleSubmit(ScheduleCreate):
    """Accept the same named priority aliases as ordinary task submission."""

    priority: Priority = 5

    @field_validator(
        "priority", mode="before", json_schema_input_type=int | Literal["high", "normal", "low"]
    )
    @classmethod
    def normalize_priority(cls, value: object) -> object:
        """Reuse the API's high/normal/low normalization contract."""
        return TaskSubmit.normalize_priority(value)


class TaskDetail(TaskOut):
    """A task with its execution history; attempts remains the numeric counter."""

    attempt_history: list[AttemptOut] = Field(default_factory=list)


class TaskPage(JSONSchema):
    """A deterministic page of tasks and the count matching its filters."""

    items: list[TaskOut]
    total: int
    limit: int
    offset: int


class HealthOut(JSONSchema):
    """Readiness of required infrastructure and the publisher background task."""

    status: Literal["ok", "degraded"]
    checks: dict[str, Literal["ok", "fail", "disabled"]]
    checked_at: datetime


class ValidationIssue(JSONSchema):
    """A sanitized validation failure without its submitted input value."""

    loc: list[str | int]
    type: str
    message: str


class ErrorInfo(JSONSchema):
    """Stable error code and public explanation, optionally with field-level details."""

    code: str
    message: str
    details: list[ValidationIssue] | None = None


class ErrorOut(JSONSchema):
    """Document the error envelope actually returned by global exception handlers."""

    error: ErrorInfo
    request_id: str | None
