"""Scripted I/O boundaries for unit tests; SQL correctness stays in PostgreSQL integration tests."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from shared.models import DLQStatus, TaskStatus
from shared.tables import DeadLetter, Task, TaskAttempt

NOW = datetime.now(UTC)


class Rows:
    """Minimal synchronous SQLAlchemy result view over explicitly supplied test records."""

    def __init__(self, values: Any = ()) -> None:
        self.values = values

    def all(self):
        return list(self.values)

    def scalar_one(self):
        return self.values

    def scalar_one_or_none(self):
        return self.values

    def one(self):
        return self.values

    def first(self):
        return self.values[0] if self.values else None


class Session:
    """Capture writes and expose deterministic I/O outcomes, including rollback evidence."""

    def __init__(self, *, scalar=(), scalars=(), execute=(), get=()) -> None:
        self.scalar_values = deque(scalar)
        self.scalar = AsyncMock(side_effect=self._scalar)
        self.scalars = AsyncMock(side_effect=list(scalars) if scalars else None, return_value=[])
        self.execute = AsyncMock(
            side_effect=[Rows(v) for v in execute] if execute else None, return_value=Rows()
        )
        self.get = AsyncMock(side_effect=list(get) if get else None, return_value=None)
        self.added = []
        self.add = MagicMock(side_effect=self.added.append)
        self.add_all = MagicMock(side_effect=self.added.extend)
        self.flush = AsyncMock()
        self.committed = False
        self.bind = None

    async def _scalar(self, query):
        if "clock_timestamp()" in str(query):
            return NOW
        if not self.scalar_values:
            raise AssertionError(f"Unscripted scalar read: {query}")
        value = self.scalar_values.popleft()
        if isinstance(value, BaseException):
            raise value
        return value

    async def __aenter__(self):
        return self

    async def __aexit__(self, kind, value, traceback):
        self.committed = kind is None


class Factory:
    """Match async_sessionmaker's call and transactional begin entry points."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def __call__(self):
        return self.session

    def begin(self):
        return self.session


def task(status=TaskStatus.PENDING, **updates) -> Task:
    """Represent an already-loaded row without weakening production transition validation."""
    values = dict(
        id=uuid4(),
        trace_id=uuid4(),
        type="generate_report",
        payload={"seconds": 0},
        priority=5,
        attempts=0,
        max_retries=3,
        generation=0,
        ai_modified=False,
        ai_requeues=0,
        scheduled_at=NOW,
        created_at=NOW,
        updated_at=NOW,
        idempotency_key=None,
        result=None,
        last_error=None,
        schedule_id=None,
    )
    values.update(updates)
    row = Task(**values)
    row.__dict__["status"] = status
    return row


def attempt(row: Task, **updates) -> TaskAttempt:
    """Build a persisted attempt with aware timestamps and the matching execution generation."""
    values = dict(
        task_id=row.id,
        generation=row.generation,
        attempt_no=max(1, row.attempts),
        worker_id="unit-worker",
        started_at=NOW,
        finished_at=NOW,
        error="TimeoutError: dependency timeout",
        stack_trace="trace",
    )
    values.update(updates)
    return TaskAttempt(**values)


def letter(row: Task, **updates) -> DeadLetter:
    """Build an OPEN incident retaining the canonical task trace and generation."""
    values = dict(
        id=uuid4(),
        task_id=row.id,
        trace_id=row.trace_id,
        task_type=row.type,
        generation=row.generation,
        reason="timeout",
        error_history=[],
        created_at=NOW,
        received_at=NOW,
        worker_id="unit-worker",
        attempts=row.attempts,
        category="transient",
        status=DLQStatus.OPEN,
        failure_signature="a" * 64,
        agent_eligible=True,
        ai_diagnosis=None,
        resolution=None,
        agent_cluster_id=None,
        agent_processed_at=None,
    )
    values.update(updates)
    return DeadLetter(**values)
