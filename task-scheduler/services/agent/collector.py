"""Bounded, read-only collector tools scoped to the current representative and cluster."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

from pydantic import Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.agent.guardrails import Redactor, bounded
from shared.config import Settings
from shared.db import database_now
from shared.models import JSONSchema, TaskStatus
from shared.tables import DeadLetter, Task, TaskAttempt


class ToolArguments(JSONSchema):
    """Optional selectors cannot expand access beyond the current task type and incident."""

    task_id: UUID | None = None
    task_type: str | None = Field(default=None, max_length=128)
    failure_signature: str | None = None


TOOLS = [
    {"name": name, "description": description, "input_schema": ToolArguments.model_json_schema()}
    for name, description in (
        (
            "get_task_history",
            "Read the current representative task and its last execution attempts.",
        ),
        ("get_similar_failures", "Read recent failures of this task type with the same signature."),
        (
            "get_task_type_stats",
            "Read recent success/failure counts and success rate of this type.",
        ),
        (
            "get_worker_logs",
            "Read persisted worker attempt errors/traces; not arbitrary host files.",
        ),
    )
]


class Collector:
    """Query PostgreSQL evidence, never run shell commands or accept arbitrary log paths."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        redactor: Redactor,
        incident_id: UUID,
    ) -> None:
        self.factory, self.settings, self.redactor, self.incident_id = (
            factory,
            settings,
            redactor,
            incident_id,
        )
        self.task_id: UUID | None = None
        self.task_type = ""
        self.signature = ""

    async def initialize(self) -> None:
        """Resolve immutable incident scope before evaluating tool input."""
        async with self.factory() as session:
            incident = await session.get(DeadLetter, self.incident_id)
            if incident is None:
                raise LookupError("Representative incident no longer exists")
            self.task_id, self.task_type, self.signature = (
                incident.task_id,
                incident.task_type,
                incident.failure_signature or "",
            )

    async def history(self) -> dict[str, Any]:
        """Collect a bounded payload and the latest N attempts, including stack traces."""
        async with self.factory() as session:
            task = await session.get(Task, self.task_id)
            incident = await session.get(DeadLetter, self.incident_id)
            attempts = list(
                await session.scalars(
                    select(TaskAttempt)
                    .where(TaskAttempt.task_id == self.task_id)
                    .order_by(TaskAttempt.generation.desc(), TaskAttempt.attempt_no.desc())
                    .limit(self.settings.agent.last_errors)
                )
            )
            errors = [
                {
                    "generation": item.generation,
                    "attempt_no": item.attempt_no,
                    "worker_id": item.worker_id,
                    "error": (item.error or "")[-2000:],
                    "stack_trace": (item.stack_trace or "")[-4000:],
                }
                for item in reversed(attempts)
            ]
            if not errors and incident is not None:
                errors = [
                    {
                        "error": str(item.get("error", ""))[-2000:],
                        "stack_trace": str(item.get("stack_trace") or "")[-4000:],
                        "worker_id": item.get("worker_id"),
                    }
                    for item in incident.error_history[-self.settings.agent.last_errors :]
                ]
            if task is None:
                return {"task": None, "errors": self.redactor.redact(errors)}
            payload = bounded(
                self.redactor.redact(task.payload), self.settings.agent.context_max_bytes // 5
            )
            return {
                "task": {
                    "id": str(task.id),
                    "trace_id": task.trace_id.hex,
                    "type": task.type,
                    "generation": task.generation,
                    "status": task.status.value,
                    "payload": payload,
                    "attempts": task.attempts,
                },
                "errors": self.redactor.redact(errors),
            }

    async def similar(self) -> dict[str, Any]:
        """Limit similar evidence to this signature/type and the configured recent window."""
        async with self.factory() as session:
            now = await database_now(session)
            rows = list(
                await session.scalars(
                    select(DeadLetter)
                    .where(
                        DeadLetter.failure_signature == self.signature,
                        DeadLetter.task_type == self.task_type,
                        DeadLetter.created_at
                        >= now - timedelta(seconds=self.settings.agent.cluster_window_seconds),
                    )
                    .order_by(DeadLetter.created_at.desc())
                    .limit(10)
                )
            )
            return {
                "failures": self.redactor.redact(
                    [
                        {
                            "task_id": str(row.task_id),
                            "reason": row.reason,
                            "last_error": str(row.error_history[-1].get("error", ""))[-1000:]
                            if row.error_history
                            else "",
                            "created_at": row.created_at.isoformat(),
                        }
                        for row in rows
                    ]
                )
            }

    async def stats(self) -> dict[str, Any]:
        """Compute type success rate over terminal tasks created in the last 24 hours."""
        async with self.factory() as session:
            now = await database_now(session)
            successes, terminal = (
                await session.execute(
                    select(
                        func.count().filter(Task.status == TaskStatus.SUCCEEDED),
                        func.count().filter(
                            Task.status.in_(
                                [
                                    TaskStatus.SUCCEEDED,
                                    TaskStatus.FAILED,
                                    TaskStatus.DEAD_LETTERED,
                                    TaskStatus.AI_DIAGNOSED,
                                    TaskStatus.ESCALATED,
                                ]
                            )
                        ),
                    ).where(
                        Task.type == self.task_type, Task.created_at >= now - timedelta(hours=24)
                    )
                )
            ).one()
            return {
                "task_type": self.task_type,
                "successes": successes,
                "terminal_tasks": terminal,
                "success_rate": successes / terminal if terminal else None,
                "window_hours": 24,
            }

    async def gather(self) -> dict[str, Any]:
        """Gather collector evidence once, and retain only sanitized bounded values."""
        await self.initialize()
        history = await self.history()
        return {
            **history,
            "similar_failures": await self.similar(),
            "type_stats": await self.stats(),
        }

    async def call(self, name: str, raw: object) -> dict[str, Any]:
        """Validate arguments and reject arbitrary task IDs/types/signatures or unknown tools."""
        arguments = ToolArguments.model_validate(raw)
        if (
            (arguments.task_id is not None and arguments.task_id != self.task_id)
            or (arguments.task_type is not None and arguments.task_type != self.task_type)
            or (
                arguments.failure_signature is not None
                and arguments.failure_signature != self.signature
            )
        ):
            raise ValueError("Tool selector is outside the representative scope")
        if name == "get_task_history":
            result = await self.history()
        elif name == "get_similar_failures":
            result = await self.similar()
        elif name == "get_task_type_stats":
            result = await self.stats()
        elif name == "get_worker_logs":
            result = {"source": "persisted_task_attempts", "logs": (await self.history())["errors"]}
        else:
            raise ValueError("Unknown read-only diagnosis tool")
        return bounded(self.redactor.redact(result), self.settings.agent.context_max_bytes // 4)

    @staticmethod
    def encode(value: Any) -> str:
        """Serialize already sanitized evidence for message content."""
        return json.dumps(value, ensure_ascii=False)
