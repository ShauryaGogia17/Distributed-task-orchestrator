"""Configuration validation and credential-handling regressions."""

import pytest
from pydantic import ValidationError

from shared.config import KafkaSettings, LLMSettings, PostgresSettings, RedisSettings


def test_environment_overrides_dotenv(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A host/container environment override must take priority over a shared .env."""
    env_file = tmp_path / ".env"
    env_file.write_text("KAFKA_BOOTSTRAP_SERVERS=from-file:9092\nUNRELATED=value\n")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    assert KafkaSettings(_env_file=env_file).bootstrap_servers == "kafka:19092"


def test_password_is_required_and_hidden(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing passwords fail early and representations never expose the secret."""
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    with pytest.raises(ValidationError):
        PostgresSettings(_env_file=None)
    with pytest.raises(ValidationError):
        PostgresSettings(_env_file=None, password="")
    settings = PostgresSettings(_env_file=None, password="test-only:p@ss/word")
    assert "test-only:p@ss/word" not in repr(settings)
    assert settings.sqlalchemy_url.password == "test-only:p@ss/word"
    assert "test-only:p@ss/word" not in str(settings.sqlalchemy_url)


def test_llm_provider_validation() -> None:
    """Mock needs no key, while selecting Anthropic requires one."""
    assert LLMSettings(_env_file=None, provider="mock", api_key="").provider == "mock"
    with pytest.raises(ValidationError):
        LLMSettings(_env_file=None, provider="anthropic", api_key="")
    assert LLMSettings(_env_file=None, provider="anthropic", api_key="test-only").provider


@pytest.mark.parametrize("port", [0, 65536])
def test_invalid_port_is_rejected(port: int) -> None:
    """Malformed connection settings should fail before a network operation."""
    with pytest.raises(ValidationError):
        RedisSettings(_env_file=None, port=port)


@pytest.mark.parametrize("servers", ["", " ", "localhost:9092,"])
def test_empty_broker_list_is_rejected(servers: str) -> None:
    """Reject empty endpoints in comma-separated bootstrap servers."""
    with pytest.raises(ValidationError):
        KafkaSettings(_env_file=None, bootstrap_servers=servers)
