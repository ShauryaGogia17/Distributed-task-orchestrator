"""Error classification, full-jitter backoff, and atomic retry/dead-letter decisions."""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import Settings
from shared.errors import FatalError, RetryableError
from shared.fingerprint import failure_category, failure_signature
from shared.models import ErrorRecord, RetryMessage, TaskStatus
from shared.tables import DeadLetter, Outbox, RetrySchedule, Task, TaskAttempt

RETRY_TOPICS = ("tasks.retry.5s", "tasks.retry.30s", "tasks.retry.5m")


def event_id(task_id: UUID, attempt_no: int, kind: str, generation: int = 0) -> UUID:
    """Stable event IDs make replay and retry ingestion idempotent."""
    suffix = f":generation:{generation}" if generation else ""
    return uuid5(NAMESPACE_URL, f"task-scheduler:{kind}:{task_id}:{attempt_no}{suffix}")


def is_retryable(error: Exception) -> bool:
    """Explicit categories win; 4xx/validation are fatal and 5xx/network are transient.

    Unclassified failures default to retryable, bounded by the task's attempt budget.
    """
    if isinstance(error, FatalError):
        return False
    if isinstance(error, RetryableError):
        return True
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code >= 500
    if isinstance(error, (ValidationError, ValueError, TypeError, LookupError)):
        return False
    return True


def full_jitter(attempt_no: int, base_seconds: float, cap_seconds: float) -> float:
    """Uniform(0, min(cap, base * 2**(attempt-1))), without huge-exponent overflow."""
    if (
        attempt_no < 1
        or not math.isfinite(base_seconds)
        or not math.isfinite(cap_seconds)
        or base_seconds < 0
        or cap_seconds < base_seconds
    ):
        raise ValueError("Require a positive attempt and finite 0 <= base <= cap")
    if base_seconds == 0:
        return 0.0
    exponent = attempt_no - 1
    window = (
        cap_seconds
        if exponent >= math.log2(cap_seconds) - math.log2(base_seconds)
        else math.ldexp(base_seconds, exponent)
    )
    return random.uniform(0, window)


def retry_topic(failed_attempt: int) -> str:
    """Route failure #1/#2/#3+ to nominal 5s/30s/5m buckets; due_at controls timing."""
    if failed_attempt < 1:
        raise ValueError("failed_attempt must be positive")
    return RETRY_TOPICS[min(failed_attempt - 1, 2)]


def result_event(
    task: Task, worker_id: str, finished_at: datetime, *, started_at: datetime | None = None
) -> Outbox:
    """Capture this attempt's outcome, even if a later retry changes the task state."""
    return Outbox(
        id=event_id(task.id, task.attempts, "result", task.generation),
        topic="tasks.results",
        key=str(task.id),
        payload={
            "version": 1,
            "generation": task.generation,
            "task_id": str(task.id),
            "type": task.type,
            "status": task.status.value,
            "attempt_no": task.attempts,
            "worker_id": worker_id,
            "result": task.result,
            "error": task.last_error,
            "finished_at": finished_at.isoformat(),
            "duration_seconds": max(0.0, (finished_at - started_at).total_seconds())
            if started_at
            else None,
        },
    )


class RetryPolicy:
    """Make each attempt outcome and its Kafka events part of the same DB transaction."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def fail(
        self,
        session: AsyncSession,
        task: Task,
        attempt: TaskAttempt,
        error: Exception,
        now: datetime,
        *,
        crashed: bool = False,
    ) -> tuple[UUID, ...]:
        """Record FAILED or RETRYING, retaining all previous failed attempt records."""
        await session.flush()
        history = list(
            await session.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task.id, TaskAttempt.generation == task.generation)
                .order_by(TaskAttempt.attempt_no)
            )
        )
        crash_streak = 0
        for previous in reversed(history):
            if previous.error and previous.error.startswith("WorkerCrash:"):
                crash_streak += 1
            else:
                break
        exhausted = task.attempts >= max(1, task.max_retries)
        poisoned = crashed and crash_streak >= self.settings.worker.max_crash_claims
        fatal = not is_retryable(error)
        task.result = None
        task.last_error = attempt.error
        if fatal or exhausted or poisoned:
            task.status = TaskStatus.FAILED
            reason = (
                "poison pill: repeated worker crashes"
                if poisoned
                else "fatal error"
                if fatal
                else "attempt budget exhausted"
            )
            identifier = event_id(task.id, task.attempts, "dlq", task.generation)
            records = [
                ErrorRecord(
                    attempt_no=item.attempt_no,
                    generation=task.generation,
                    worker_id=item.worker_id,
                    error=item.error,
                    stack_trace=item.stack_trace,
                    recorded_at=item.finished_at or now,
                ).model_dump(mode="json")
                for item in history
                if item.error
            ]
            session.add(
                DeadLetter(
                    id=identifier,
                    task_id=task.id,
                    reason=reason,
                    error_history=records,
                    created_at=now,
                    generation=task.generation,
                    task_type=task.type,
                    worker_id=attempt.worker_id,
                    attempts=task.attempts,
                    category=failure_category(reason),
                    failure_signature=failure_signature(
                        attempt.error or reason, attempt.stack_trace
                    ),
                )
            )
            decision = Outbox(
                id=identifier,
                topic="tasks.dlq",
                key=str(task.id),
                payload={
                    "version": 1,
                    "kind": "task.dead_letter",
                    "generation": task.generation,
                    "worker_id": attempt.worker_id,
                    "category": failure_category(reason),
                    "id": str(identifier),
                    "task_id": str(task.id),
                    "type": task.type,
                    "attempt_no": task.attempts,
                    "status": "DEAD_LETTERED",
                    "reason": reason,
                    "error_history": records,
                    "ai_diagnosis": None,
                    "resolution": None,
                    "created_at": now.isoformat(),
                },
            )
        else:
            task.status = TaskStatus.RETRYING
            delay = full_jitter(
                task.attempts,
                self.settings.worker.retry_base_seconds,
                self.settings.worker.retry_cap_seconds,
            )
            due_at = now + timedelta(seconds=delay)
            task.scheduled_at = due_at
            identifier = event_id(task.id, task.attempts, "retry", task.generation)
            session.add(
                RetrySchedule(
                    id=identifier,
                    task_id=task.id,
                    generation=task.generation,
                    failed_attempt=task.attempts,
                    due_at=due_at,
                )
            )
            message = RetryMessage(
                event_id=identifier,
                generation=task.generation,
                task_id=task.id,
                failed_attempt=task.attempts,
                due_at=due_at,
                delay_seconds=delay,
            )
            decision = Outbox(
                id=identifier,
                topic=retry_topic(task.attempts),
                key=str(task.id),
                payload=message.model_dump(mode="json"),
            )
        outcome = result_event(task, attempt.worker_id, now, started_at=attempt.started_at)
        session.add_all([outcome, decision])
        return outcome.id, decision.id
