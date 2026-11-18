"""Failure isolation, topic verification, timeouts, and safe CLI output."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from scripts import check_infra as infra
from shared.config import KafkaSettings, LLMSettings, PostgresSettings, RedisSettings, Settings
from shared.topics import expected_topics


@pytest.fixture
def settings() -> Settings:
    """Create deterministic test settings without reading the developer .env."""
    return Settings(
        _env_file=None,
        kafka=KafkaSettings(_env_file=None),
        redis=RedisSettings(_env_file=None),
        postgres=PostgresSettings(_env_file=None, password="test-only"),
        llm=LLMSettings(_env_file=None, provider="mock", api_key=""),
    )


async def test_failure_does_not_hide_other_services(settings, monkeypatch) -> None:
    """Report all checks even if one raises an exception containing credentials."""
    monkeypatch.setattr(infra, "check_kafka", AsyncMock(side_effect=OSError("secret-password")))
    monkeypatch.setattr(infra, "check_redis", AsyncMock(return_value="PONG"))
    monkeypatch.setattr(infra, "check_postgres", AsyncMock(return_value="SELECT 1"))
    results = await infra.check_infrastructure(settings)
    assert [result.ok for result in results] == [False, True, True]
    assert "secret-password" not in results[0].detail


async def test_timeout_is_a_failure() -> None:
    """A stalled service cannot make the checker wait indefinitely."""

    async def stalled() -> str:
        await asyncio.Event().wait()
        return "unreachable"

    result = await infra.run_check("Kafka", stalled, deadline_seconds=0.01)
    assert not result.ok
    assert "TimeoutError" in result.detail


@pytest.mark.parametrize("failure", [None, "missing", "partitions", "leader"])
async def test_kafka_contract(settings, monkeypatch, failure: str | None) -> None:
    """Reject absent topics, wrong partitions, and unavailable partition leaders."""
    metadata = [
        {
            "topic": name,
            "error_code": 0,
            "partitions": [{"leader": 1, "error_code": 0} for _ in range(count)],
        }
        for name, count in expected_topics().items()
    ]
    if failure == "missing":
        metadata[0]["error_code"] = 3
    elif failure == "partitions":
        metadata[0]["partitions"].pop()
    elif failure == "leader":
        metadata[0]["partitions"][0]["leader"] = -1
    client = AsyncMock()
    client.describe_topics.return_value = metadata
    monkeypatch.setattr(infra, "AIOKafkaAdminClient", lambda **kwargs: client)
    if failure:
        with pytest.raises(infra.InfrastructureMismatch):
            await infra.check_kafka(settings)
    else:
        assert "all 6 topics ready" in await infra.check_kafka(settings)
    client.close.assert_awaited_once()


@pytest.mark.parametrize("success", [True, False])
def test_cli_exit_status(settings, monkeypatch, capsys, success: bool) -> None:
    """Automation must receive a nonzero exit code if any service failed."""
    monkeypatch.setattr(infra, "get_settings", lambda: settings)
    results = [infra.CheckResult("Kafka", success, "test outcome")]
    monkeypatch.setattr(infra, "check_infrastructure", AsyncMock(return_value=results))
    assert infra.main() == (0 if success else 1)
    assert ("[OK]" if success else "[FAIL]") in capsys.readouterr().out
