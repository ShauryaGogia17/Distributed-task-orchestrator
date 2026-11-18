"""Structured JSON logging with async-safe, explicitly scoped execution context."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import TextIO
from uuid import UUID

_task_id: ContextVar[str | None] = ContextVar("log_task_id", default=None)
_worker_id: ContextVar[str | None] = ContextVar("log_worker_id", default=None)
_trace_id: ContextVar[str | None] = ContextVar("log_trace_id", default=None)
_request_id: ContextVar[str | None] = ContextVar("log_request_id", default=None)


@contextmanager
def log_context(
    *,
    task_id: UUID | str | None = None,
    worker_id: str | None = None,
    request_id: str | None = None,
    trace_id: UUID | str | None = None,
) -> Iterator[None]:
    """Bind provided IDs for this scope, preserving nested context and async isolation."""
    tokens = []
    if task_id is not None:
        tokens.append((_task_id, _task_id.set(str(task_id))))
    if worker_id is not None:
        tokens.append((_worker_id, _worker_id.set(worker_id)))
    if trace_id is not None:
        from shared.tracing import trace_id as normalize_trace

        tokens.append((_trace_id, _trace_id.set(normalize_trace(trace_id))))
    if request_id is not None:
        tokens.append((_request_id, _request_id.set(request_id)))
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


def current_trace_id() -> str | None:
    """Read the active trace for direct Kafka publications outside the outbox relay."""
    return _trace_id.get()


class JSONFormatter(logging.Formatter):
    """Emit one JSON object per record, including escaped exception tracebacks."""

    def format(self, record: logging.LogRecord) -> str:
        """Read context while formatting so third-party loggers need no adapters."""
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "task_id": getattr(record, "task_id", _task_id.get()),
            "worker_id": getattr(record, "worker_id", _worker_id.get()),
            "request_id": getattr(record, "request_id", _request_id.get()),
            "trace_id": getattr(record, "trace_id", _trace_id.get()),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def configure_logging(*, level: str = "INFO", stream: TextIO | None = None) -> None:
    """Configure the process root logger once at service entry, never during import."""
    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
