"""Consistent, sanitized error responses and request identity propagation."""

from __future__ import annotations

import logging
import re
import time
from uuid import uuid4

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from shared.logging import log_context
from shared.tracing import request_trace

logger = logging.getLogger(__name__)


def error_response(
    request: Request, status: int, code: str, message: str, **extra: object
) -> JSONResponse:
    """Keep exception text and submitted payloads out of public error messages."""
    return JSONResponse(
        status_code=status,
        content={
            "error": {"code": code, "message": message, **extra},
            "request_id": getattr(request.state, "request_id", None),
        },
    )


async def http_error(request: Request, exc: Exception) -> JSONResponse:
    """Normalize expected HTTP errors, including framework 404/405 responses."""
    assert isinstance(exc, HTTPException)
    response = error_response(request, exc.status_code, f"http_{exc.status_code}", str(exc.detail))
    if exc.headers:
        response.headers.update(exc.headers)
    return response


async def validation_error(request: Request, exc: Exception) -> JSONResponse:
    """Return field locations and messages without echoing potentially secret input."""
    assert isinstance(exc, RequestValidationError)
    details = [
        {"loc": list(error["loc"]), "type": error["type"], "message": error["msg"]}
        for error in exc.errors()
    ]
    return error_response(
        request, 422, "validation_error", "Request validation failed", details=details
    )


async def database_error(request: Request, exc: Exception) -> JSONResponse:
    """Report a retryable database outage without exposing SQL or connection details."""
    logger.error("Database request failed", exc_info=exc)
    return error_response(request, 503, "database_unavailable", "Database temporarily unavailable")


async def internal_error(request: Request, exc: Exception) -> JSONResponse:
    """Log unexpected failures and return a generic response with its request ID."""
    logger.error("Unhandled request failure", exc_info=exc)
    return error_response(request, 500, "internal_error", "Internal server error")


class RequestIDMiddleware:
    """Pure ASGI context scope that also attaches request IDs to error responses."""

    def __init__(self, app: ASGIApp) -> None:
        """Wrap the application without modifying global logging configuration."""
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Validate caller-provided IDs, bind context, and emit one completion log."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        supplied = request.headers.get("x-request-id", "")
        request_id = supplied if re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", supplied) else str(uuid4())
        scope.setdefault("state", {})["request_id"] = request_id
        trace = request_trace(request.headers)
        scope["state"]["trace_id"] = trace
        started = time.monotonic()
        response_started = False
        status = 500

        async def send_with_id(message: Message) -> None:
            nonlocal response_started, status
            if message["type"] == "http.response.start":
                response_started = True
                status = message["status"]
                headers = [
                    (key, value)
                    for key, value in message.get("headers", [])
                    if key.lower() != b"x-request-id"
                ]
                if not any(k.lower() == b"x-trace-id" for k, _ in headers):
                    headers.append((b"x-trace-id", trace.encode("ascii")))
                message["headers"] = [*headers, (b"x-request-id", request_id.encode("ascii"))]
            await send(message)

        with log_context(request_id=request_id, trace_id=trace):
            try:
                await self.app(scope, receive, send_with_id)
            except Exception as exc:
                if response_started:
                    raise
                response = await internal_error(request, exc)
                await response(scope, receive, send_with_id)
            finally:
                logger.info(
                    "%s %s status=%d duration_ms=%.2f",
                    scope["method"],
                    scope["path"],
                    status,
                    (time.monotonic() - started) * 1000,
                )
