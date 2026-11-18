"""Timezone-aware cron contracts and deterministic recurring-task identities."""

from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from pydantic import AwareDatetime, Field, JsonValue, field_validator, model_validator

from shared.models import JSONSchema, Priority, RetryCount, TaskType


def next_occurrence(expression: str, timezone: str, after: datetime) -> datetime:
    """Return the next strictly future occurrence in UTC, with bounded cron search."""
    if after.tzinfo is None or after.utcoffset() is None:
        raise ValueError("Cron reference time must include a timezone")
    if len(expression.split()) not in (5, 6):
        raise ValueError("Use five cron fields, or six with seconds at the end")
    try:
        zone = ZoneInfo(timezone)
        return (
            croniter(expression, after.astimezone(zone), max_years_between_matches=8)
            .get_next(datetime)
            .astimezone(UTC)
        )
    except (CroniterBadCronError, CroniterBadDateError, ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("Invalid cron expression or IANA timezone") from exc


def latest_occurrence(expression: str, timezone: str, at: datetime) -> datetime:
    """Include an occurrence exactly at the current instant when coalescing downtime."""
    zone = ZoneInfo(timezone)
    return (
        croniter(
            expression,
            (at + timedelta(microseconds=1)).astimezone(zone),
            max_years_between_matches=8,
        )
        .get_prev(datetime)
        .astimezone(UTC)
    )


def occurrence_task_id(schedule_id: UUID, scheduled_for: datetime) -> UUID:
    """One stable task UUID per schedule and UTC occurrence, independent of leader."""
    return uuid5(schedule_id, scheduled_for.astimezone(UTC).isoformat())


class ScheduleCreate(JSONSchema):
    """Persistent cron definition; omitted policy uses scheduler deployment defaults."""

    name: str | None = Field(default=None, max_length=128)
    cron: str = Field(min_length=1, max_length=128)
    timezone: str = Field(default="UTC", min_length=1, max_length=64)
    type: TaskType
    payload: dict[str, JsonValue] = Field(default_factory=dict)
    priority: Priority = 5
    max_retries: RetryCount = 3
    missed_run_policy: Literal["fire_once", "skip"] | None = None
    enabled: bool = True

    @field_validator("cron")
    @classmethod
    def normalize_cron(cls, value: str) -> str:
        """Persist one whitespace-normalized cron expression."""
        return " ".join(value.split())

    @model_validator(mode="after")
    def validate_calendar(self) -> "ScheduleCreate":
        """Reject invalid dates/timezones before creating a permanently broken job."""
        next_occurrence(self.cron, self.timezone, datetime.now(UTC))
        return self


class ScheduleOut(JSONSchema):
    """Recurring definition and scheduling cursor, validated from its ORM row."""

    id: UUID
    name: str | None
    cron: str
    timezone: str
    type: str
    payload: dict[str, JsonValue]
    priority: int
    max_retries: int
    missed_run_policy: Literal["fire_once", "skip"]
    enabled: bool
    next_run_at: AwareDatetime
    last_run_at: AwareDatetime | None
    created_at: AwareDatetime
    updated_at: AwareDatetime


class SchedulePatch(JSONSchema):
    """Pause/resume materialization; already-created tasks remain independent."""

    enabled: bool
