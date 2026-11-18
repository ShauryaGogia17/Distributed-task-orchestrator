"""Retry classification, full-jitter bounds, routing, and budget configuration."""

from unittest.mock import patch

import httpx
import pytest
from pydantic import ValidationError

from services.worker.retry_policy import full_jitter, is_retryable, retry_topic
from shared.config import WorkerSettings
from shared.errors import FatalError, RetryableError
from shared.models import TaskCreate


@pytest.mark.parametrize(
    ("error", "retryable"),
    [
        (RetryableError("transient"), True),
        (FatalError("permanent"), False),
        (TimeoutError(), True),
        (ConnectionError(), True),
        (httpx.ConnectError("offline"), True),
        (httpx.ReadTimeout("slow"), True),
        (ValueError("bad value"), False),
        (TypeError("wrong type"), False),
        (LookupError("unknown handler"), False),
        (RuntimeError("unclassified"), True),
    ],
)
def test_error_classification(error: Exception, retryable: bool) -> None:
    """Known permanent errors stop immediately; other errors have bounded retries."""
    assert is_retryable(error) is retryable


@pytest.mark.parametrize("status", [400, 401, 404, 408, 429, 500, 502, 503, 504])
def test_http_status_classification(status: int) -> None:
    """All 4xx (including 429) are fatal per the phase's requested default mapping."""
    request = httpx.Request("GET", "https://example.invalid")
    response = httpx.Response(status, request=request)
    error = httpx.HTTPStatusError("HTTP failure", request=request, response=response)
    assert is_retryable(error) is (status >= 500)


def test_validation_error_is_fatal() -> None:
    """Use an actual Pydantic validation failure rather than an imitation exception."""
    with pytest.raises(ValidationError) as caught:
        TaskCreate(type="")
    assert not is_retryable(caught.value)


@pytest.mark.parametrize(("attempt", "window"), [(1, 5), (2, 10), (3, 20), (7, 300), (100000, 300)])
def test_exponential_window_and_cap(attempt: int, window: float) -> None:
    """Jitter spans zero through the capped exponential bound without overflow."""
    with patch(
        "services.worker.retry_policy.random.uniform", side_effect=lambda low, high: high / 2
    ) as uniform:
        assert full_jitter(attempt, 5, 300) == window / 2
        uniform.assert_called_once_with(0, window)


def test_full_jitter_endpoints_and_disabled_delay() -> None:
    """Both jitter extremes are valid; zero base provides deterministic immediate tests."""
    with patch("services.worker.retry_policy.random.uniform", return_value=0):
        assert full_jitter(1, 5, 300) == 0
    with patch("services.worker.retry_policy.random.uniform", side_effect=lambda low, high: high):
        assert full_jitter(20, 5, 300) == 300
    assert full_jitter(999, 0, 0) == 0


@pytest.mark.parametrize(
    ("attempt", "base", "cap"), [(0, 5, 300), (1, -1, 10), (1, 10, 5), (1, 1, float("inf"))]
)
def test_invalid_backoff_inputs(attempt: int, base: float, cap: float) -> None:
    """Invalid inputs must fail instead of creating negative or infinite schedules."""
    with pytest.raises(ValueError):
        full_jitter(attempt, base, cap)


@pytest.mark.parametrize(
    ("attempt", "topic"),
    [(1, "tasks.retry.5s"), (2, "tasks.retry.30s"), (3, "tasks.retry.5m"), (20, "tasks.retry.5m")],
)
def test_attempt_routing(attempt: int, topic: str) -> None:
    """Topic selection depends on the failed attempt, separately from jitter timing."""
    assert retry_topic(attempt) == topic


def test_retry_config_rejects_inverted_bounds() -> None:
    """Reject bad deployment settings before starting any consumers."""
    with pytest.raises(ValidationError):
        WorkerSettings(_env_file=None, retry_base_seconds=10, retry_cap_seconds=5)
