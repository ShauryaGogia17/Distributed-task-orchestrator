"""Runnable sample handlers; only http_call performs external HTTP requests."""

from __future__ import annotations

import asyncio
import random
from typing import Literal
from uuid import uuid4

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl, JsonValue, ValidationError

from services.worker.registry import HandlerContext, register
from shared.errors import RetryableError


class Payload(BaseModel):
    """Reject unexpected fields and non-finite numeric inputs."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class EmailPayload(Payload):
    """A simulated email; no credentials or delivery provider are needed."""

    to: str = Field(
        pattern=r"^[^\s@]+@[^\s@]+\.[^\s@]+$", validation_alias=AliasChoices("to", "recipient")
    )
    subject: str = Field(default="Task scheduler notification", max_length=256)
    body: str = Field(default="", max_length=10000)


@register("send_email")
async def send_email(payload: dict[str, JsonValue], context: HandlerContext) -> JsonValue:
    """Simulate delivery and return a generated message identity."""
    email = EmailPayload.model_validate(payload)
    await asyncio.sleep(0.05)
    return {"simulated": True, "to": email.to, "message_id": str(uuid4())}


class ReportPayload(Payload):
    """A numeric report with a configurable asynchronous computation delay."""

    title: str = Field(default="report", max_length=256)
    data: list[float] = Field(default_factory=list, max_length=10000)
    seconds: float = Field(default=0.1, ge=0, le=3600)


@register("generate_report")
async def generate_report(payload: dict[str, JsonValue], context: HandlerContext) -> JsonValue:
    """Sleep asynchronously, then return a small aggregate report."""
    report = ReportPayload.model_validate(payload)
    await asyncio.sleep(report.seconds)
    total = sum(report.data)
    return {
        "title": report.title,
        "count": len(report.data),
        "sum": total,
        "mean": total / len(report.data) if report.data else None,
    }


class HTTPPayload(Payload):
    """An HTTP request bounded by the worker's configured timeout."""

    url: HttpUrl
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] = "GET"
    json_body: dict[str, JsonValue] | None = Field(default=None, alias="json")
    timeout_seconds: float | None = Field(default=None, gt=0)


@register("http_call")
async def http_call(payload: dict[str, JsonValue], context: HandlerContext) -> JsonValue:
    """Make a real HTTP request and reject oversized response bodies."""
    request = HTTPPayload.model_validate(payload)
    timeout = min(
        request.timeout_seconds or context.http_timeout_seconds, context.http_timeout_seconds
    )
    async with context.http_client.stream(
        request.method,
        str(request.url),
        json=request.json_body,
        timeout=timeout,
    ) as response:
        response.raise_for_status()
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > context.http_max_response_bytes:
                raise ValueError("HTTP response exceeds WORKER_HTTP_MAX_RESPONSE_BYTES")
        return {
            "status_code": response.status_code,
            "body": bytes(body).decode("utf-8", errors="replace"),
        }


@register("flaky_task")
async def flaky_task(payload: dict[str, JsonValue], context: HandlerContext) -> JsonValue:
    """Fail with probability 0.5 on each execution, otherwise return success."""
    if random.random() < 0.5:
        raise RetryableError("Simulated transient failure")
    return {"ok": True}


@register("bad_input_task")
async def bad_input_task(payload: dict[str, JsonValue], context: HandlerContext) -> JsonValue:
    """Always raise a real Pydantic ValidationError for diagnosis demonstrations."""
    raise ValidationError.from_exception_data(
        "BadInputTask",
        [
            {
                "type": "value_error",
                "loc": ("payload",),
                "input": payload,
                "ctx": {"error": ValueError("Simulated invalid task input")},
            }
        ],
    )


class SlowPayload(Payload):
    """A configurable asynchronous sleep bounded by the outer execution timeout."""

    seconds: float = Field(
        default=1, ge=0, le=3600, validation_alias=AliasChoices("seconds", "sleep_seconds")
    )


@register("slow_task")
async def slow_task(payload: dict[str, JsonValue], context: HandlerContext) -> JsonValue:
    """Sleep without blocking the event loop and report the requested duration."""
    seconds = SlowPayload.model_validate(payload).seconds
    await asyncio.sleep(seconds)
    return {"slept_seconds": seconds}
