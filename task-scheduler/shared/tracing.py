"""Stable task traces across HTTP, transactional outboxes, Kafka, and structured logs."""

import re
from collections.abc import Mapping
from uuid import UUID, uuid4


def trace_id(value: UUID | str | bytes | None = None) -> str:
    """Normalize a UUID/32-hex trace to nonzero W3C-compatible lowercase hexadecimal."""
    try:
        if isinstance(value, bytes):
            value = value.decode("ascii")
        parsed = value if isinstance(value, UUID) else UUID(str(value))
        if parsed.int:
            return parsed.hex
    except (ValueError, TypeError, UnicodeError):
        pass
    return uuid4().hex


def request_trace(headers: Mapping[str, str]) -> str:
    """Accept X-Trace-ID or the trace ID from a valid W3C traceparent; otherwise start a trace."""
    explicit = headers.get("x-trace-id")
    if explicit:
        return trace_id(explicit)
    parent = headers.get("traceparent", "")
    if re.fullmatch(r"[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", parent):
        return trace_id(parent.split("-")[1])
    return trace_id()


def headers_for(task_id: UUID | str | None, trace: UUID | str) -> list[tuple[str, bytes]]:
    """Kafka correlation uses UTF-8 task UUID and ASCII 32-hex trace, never metric labels."""
    headers = [("trace_id", trace_id(trace).encode("ascii"))]
    if task_id is not None:
        headers.append(("task_id", str(task_id).encode("utf-8")))
    return headers


def record_trace(headers: list[tuple[str, bytes]] | None) -> str:
    """Tolerate old records with no correlation headers without losing the task."""
    values = dict(headers or [])
    return trace_id(values.get("trace_id"))
