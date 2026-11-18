"""Cron calendar semantics, validation, and deterministic occurrence identities."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from shared.config import SchedulerSettings
from shared.scheduling import ScheduleCreate, latest_occurrence, next_occurrence, occurrence_task_id


def test_cron_utc_timezone_and_seconds() -> None:
    """Five-field cron uses minutes; optional sixth seconds field follows croniter syntax."""
    start = datetime(2026, 11, 10, 0, 0, tzinfo=UTC)
    assert next_occurrence("*/5 * * * *", "UTC", start) == start.replace(minute=5)
    assert next_occurrence("0 6 * * *", "Asia/Kolkata", start) == start.replace(minute=30)
    assert next_occurrence("* * * * * */2", "UTC", start) == start.replace(second=2)
    assert latest_occurrence("* * * * *", "UTC", start) == start


@pytest.mark.parametrize(
    ("cron", "timezone"),
    [
        ("not cron", "UTC"),
        ("99 * * * *", "UTC"),
        ("0 0 31 2 *", "UTC"),
        ("* * * * *", "Not/AZone"),
        ("* * * * * * *", "UTC"),
    ],
)
def test_invalid_schedule_contract(cron: str, timezone: str) -> None:
    """Invalid calendars and timezones fail before inserting a recurring job."""
    with pytest.raises(ValidationError):
        ScheduleCreate(type="generate_report", cron=cron, timezone=timezone)


def test_calendar_requires_aware_time() -> None:
    """Never silently interpret a naive cron reference using the host timezone."""
    with pytest.raises(ValueError, match="timezone"):
        next_occurrence("* * * * *", "UTC", datetime(2026, 11, 10))


def test_occurrence_identity_is_stable_and_distinct() -> None:
    """Different leaders derive the same UUID for one schedule occurrence."""
    identifier = uuid4()
    first = datetime(2026, 11, 10, tzinfo=UTC)
    assert occurrence_task_id(identifier, first) == occurrence_task_id(identifier, first)
    assert occurrence_task_id(identifier, first) != occurrence_task_id(
        identifier, first.replace(minute=1)
    )
    assert occurrence_task_id(identifier, first) != occurrence_task_id(uuid4(), first)


def test_leader_renewal_config_is_safe() -> None:
    """Reject renewal intervals that leave insufficient room for one delayed heartbeat."""
    with pytest.raises(ValidationError):
        SchedulerSettings(_env_file=None, leader_ttl_seconds=2, renewal_seconds=1)
