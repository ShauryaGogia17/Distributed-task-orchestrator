"""Handler registration, sample behavior, and bounded HTTP error handling."""

from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from services.worker import registry
from services.worker.registry import HandlerContext, get_handler, load_builtin_handlers, register
from shared.errors import RetryableError


@pytest.fixture
def handlers(monkeypatch):
    """Keep test registrations isolated while retaining built-in handlers."""
    load_builtin_handlers()
    monkeypatch.setattr(registry, "_handlers", dict(registry._handlers))


def test_registry_rejects_duplicates_and_sync_functions(handlers) -> None:
    """A registered handler is returned unchanged; duplicate and sync handlers fail."""
    name = f"test_{uuid4().hex}"

    @register(name)
    async def handler(payload, context):
        return {"ok": True}

    assert get_handler(name) is handler
    with pytest.raises(ValueError, match="already registered"):
        register(name)(handler)
    with pytest.raises(TypeError, match="async"):
        register("sync_test")(lambda payload, context: {})
    with pytest.raises(LookupError, match="Unknown task type"):
        get_handler("not_registered")


async def test_sample_email_report_slow_and_invalid_input(handlers) -> None:
    """Successful examples return JSON, while bad_input_task always raises ValidationError."""
    async with httpx.AsyncClient() as client:
        context = HandlerContext("test-worker", client)
        email = await get_handler("send_email")({"to": "demo@example.invalid"}, context)
        assert email["simulated"] is True
        report = await get_handler("generate_report")({"data": [1, 2, 3], "seconds": 0}, context)
        assert report["sum"] == 6
        assert report["mean"] == 2
        assert await get_handler("slow_task")({"seconds": 0}, context) == {"slept_seconds": 0}
        with pytest.raises(ValidationError):
            await get_handler("bad_input_task")({}, context)


async def test_flaky_task_has_both_outcomes(handlers) -> None:
    """Force both sides of the 50% branch without nondeterministic tests."""
    async with httpx.AsyncClient() as client:
        context = HandlerContext("test-worker", client)
        with patch("services.worker.handlers.random.random", return_value=0.1):
            with pytest.raises(RetryableError, match="transient"):
                await get_handler("flaky_task")({}, context)
        with patch("services.worker.handlers.random.random", return_value=0.9):
            assert await get_handler("flaky_task")({}, context) == {"ok": True}


async def test_http_errors_and_response_limit(handlers) -> None:
    """The HTTP handler propagates timeouts and rejects oversized returned data."""

    async def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("test timeout")

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
        with pytest.raises(httpx.ReadTimeout):
            await get_handler("http_call")(
                {"url": "https://example.invalid"}, HandlerContext("test", client)
            )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"too long"))
    ) as client:
        with pytest.raises(ValueError, match="response exceeds"):
            await get_handler("http_call")(
                {"url": "https://example.invalid"},
                HandlerContext("test", client, http_max_response_bytes=3),
            )
