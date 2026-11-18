"""Explicit async handler registration with no client creation on import."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx
from pydantic import JsonValue


@dataclass(frozen=True)
class HandlerContext:
    """Per-service dependencies available to registered asynchronous handlers."""

    worker_id: str
    http_client: httpx.AsyncClient
    http_timeout_seconds: float = 5
    http_max_response_bytes: int = 65536


Handler = Callable[[dict[str, JsonValue], HandlerContext], Awaitable[JsonValue]]
_handlers: dict[str, Handler] = {}


def register(task_type: str) -> Callable[[Handler], Handler]:
    """Register one async function; reject duplicate names and synchronous functions."""
    if not task_type.strip():
        raise ValueError("Handler type must not be empty")

    def decorate(handler: Handler) -> Handler:
        if not inspect.iscoroutinefunction(handler):
            raise TypeError("Task handlers must be async functions")
        if task_type in _handlers:
            raise ValueError(f"Handler already registered: {task_type}")
        _handlers[task_type] = handler
        return handler

    return decorate


def get_handler(task_type: str) -> Handler:
    """Resolve a handler or raise a descriptive error recorded by the executor."""
    try:
        return _handlers[task_type]
    except KeyError as exc:
        raise LookupError(f"Unknown task type: {task_type}") from exc


def registered_types() -> tuple[str, ...]:
    """Expose registered names for startup logging and inspection."""
    return tuple(sorted(_handlers))


def load_builtin_handlers() -> None:
    """Load sample handlers once through Python's normal import cache."""
    from services.worker import handlers  # noqa: F401
