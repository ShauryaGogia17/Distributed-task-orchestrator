"""Validated, environment-driven infrastructure settings; no import-time I/O."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


class EnvironmentSettings(BaseSettings):
    """Load the project .env while ignoring unrelated Compose variables."""

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")


class KafkaSettings(EnvironmentSettings):
    """Kafka client settings for host or Docker-network listeners."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "task-scheduler"
    request_timeout_ms: int = Field(default=10_000, ge=1000)
    max_message_bytes: int = Field(default=33554432, ge=1048576, le=67108864)

    @field_validator("bootstrap_servers")
    @classmethod
    def validate_servers(cls, value: str) -> str:
        """Reject an empty broker list before attempting any network operations."""
        if not value.strip() or any(not server.strip() for server in value.split(",")):
            raise ValueError("provide a nonempty comma-separated broker list")
        return value


class RedisSettings(EnvironmentSettings):
    """Redis database and bounded socket timeouts."""

    model_config = SettingsConfigDict(env_prefix="REDIS_")
    host: str = "localhost"
    port: int = Field(default=6379, ge=1, le=65535)
    db: int = Field(default=0, ge=0)
    socket_timeout_seconds: float = Field(default=5, gt=0)


class PostgresSettings(EnvironmentSettings):
    """PostgreSQL credentials and a safely escaped SQLAlchemy connection URL."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_")
    host: str = "localhost"
    port: int = Field(default=5432, ge=1, le=65535)
    db: str = "task_scheduler"
    user: str = "task_scheduler"
    password: SecretStr = Field(min_length=1)
    connect_timeout_seconds: int = Field(default=5, ge=1)

    @property
    def sqlalchemy_url(self) -> URL:
        """Return a psycopg SQLAlchemy URL without string-interpolating credentials."""
        return URL.create(
            "postgresql+psycopg",
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.db,
        )


class LLMSettings(EnvironmentSettings):
    """Configure a bounded LLM adapter; mock is usable without an API key."""

    model_config = SettingsConfigDict(env_prefix="LLM_")
    provider: Literal["mock", "anthropic"] = "mock"
    model: str = "claude-sonnet-4-5"
    api_key: SecretStr = SecretStr("")
    timeout_seconds: float = Field(default=30, gt=0)
    max_tokens: int = Field(default=2048, gt=0, le=8192)
    max_attempts: int = Field(default=3, ge=1, le=5)
    retry_cap_seconds: float = Field(default=5, ge=0, le=30)
    input_usd_per_million: float = Field(default=3, ge=0, allow_inf_nan=False)
    output_usd_per_million: float = Field(default=15, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def require_anthropic_key(self) -> "LLMSettings":
        """Fail locally when the real provider is selected without credentials."""
        if self.provider == "anthropic" and not self.api_key.get_secret_value().strip():
            raise ValueError("LLM_API_KEY is required for the anthropic provider")
        return self


class APISettings(EnvironmentSettings):
    """HTTP service and bounded outbox publication loop settings."""

    model_config = SettingsConfigDict(env_prefix="API_")
    host: str = "127.0.0.1"
    port: int = Field(default=8002, ge=1, le=65535)
    max_task_message_bytes: int = Field(default=900_000, ge=1, le=900_000)
    outbox_enabled: bool = True
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_poll_seconds: float = Field(default=0.5, gt=0)
    outbox_retry_seconds: float = Field(default=1, gt=0)
    outbox_publish_timeout_seconds: float = Field(default=10, gt=0)
    outbox_shutdown_timeout_seconds: float = Field(default=30, gt=0)
    health_timeout_seconds: float = Field(default=3, gt=0)


class WorkerSettings(EnvironmentSettings):
    """Bounded execution, Kafka membership, leases, and worker liveness."""

    model_config = SettingsConfigDict(env_prefix="WORKER_")
    id: str | None = None
    group_id: str = Field(default="task-workers", min_length=1)
    concurrency: int = Field(default=4, ge=1, le=128)
    task_timeout_seconds: float = Field(default=30, gt=0)
    visibility_timeout_seconds: float = Field(default=60, ge=0.3)
    heartbeat_interval_seconds: float = Field(default=5, gt=0)
    heartbeat_ttl_seconds: int = Field(default=20, ge=1)
    completion_ttl_seconds: int = Field(default=86400, ge=1)
    result_publish_timeout_seconds: float = Field(default=30, gt=0)
    retry_poll_seconds: float = Field(default=1, gt=0)
    retry_base_seconds: float = Field(default=5, ge=0, allow_inf_nan=False)
    retry_cap_seconds: float = Field(default=300, ge=0, allow_inf_nan=False)
    retry_engine_poll_seconds: float = Field(default=0.25, gt=0)
    retry_batch_size: int = Field(default=100, ge=1, le=1000)
    max_crash_claims: int = Field(default=3, ge=1, le=100)
    shutdown_timeout_seconds: float = Field(default=90, gt=0)
    rebalance_timeout_seconds: float = Field(default=90, gt=0)
    max_poll_interval_ms: int = Field(default=300000, ge=1000)
    http_timeout_seconds: float = Field(default=5, gt=0)
    http_max_response_bytes: int = Field(default=65536, ge=1, le=500000)
    metrics_enabled: bool = True
    metrics_host: str = "0.0.0.0"
    metrics_port: int = Field(default=8003, ge=0, le=65535)
    health_file: Path = Path("/tmp/task-scheduler-worker.json")

    @model_validator(mode="after")
    def validate_heartbeat(self) -> "WorkerSettings":
        """A heartbeat must refresh before its TTL expires."""
        if self.heartbeat_interval_seconds >= self.heartbeat_ttl_seconds:
            raise ValueError("worker heartbeat interval must be shorter than its TTL")
        if self.retry_cap_seconds < self.retry_base_seconds:
            raise ValueError("worker retry cap must be at least the retry base")
        return self


class SchedulerSettings(EnvironmentSettings):
    """Leader election, bounded due polling, and recurring-run policy."""

    model_config = SettingsConfigDict(env_prefix="SCHEDULER_")
    id: str | None = None
    host: str = "127.0.0.1"
    port: int = Field(default=8004, ge=1, le=65535)
    queue_key: str = Field(default="scheduled_tasks", min_length=1)
    leader_key: str = Field(default="scheduler:leader", min_length=1)
    poll_seconds: float = Field(default=1, gt=0)
    election_poll_seconds: float = Field(default=0.25, gt=0)
    leader_ttl_seconds: float = Field(default=5, ge=0.3)
    renewal_seconds: float = Field(default=1, gt=0)
    batch_size: int = Field(default=100, ge=1, le=1000)
    missed_run_policy: Literal["fire_once", "skip"] = "fire_once"
    misfire_grace_seconds: float = Field(default=2, ge=0)

    @model_validator(mode="after")
    def validate_renewal(self) -> "SchedulerSettings":
        """Renew often enough to tolerate one delayed heartbeat."""
        if self.renewal_seconds * 2 >= self.leader_ttl_seconds:
            raise ValueError("scheduler renewal interval must be less than half the leader TTL")
        if self.queue_key == self.leader_key:
            raise ValueError("scheduler queue and leader keys must differ")
        return self


class DLQSettings(EnvironmentSettings):
    """Manual-commit ingestion and bounded human-initiated bulk retry."""

    model_config = SettingsConfigDict(env_prefix="DLQ_")
    host: str = "127.0.0.1"
    port: int = Field(default=8005, ge=1, le=65535)
    group_id: str = Field(default="task-dlq", min_length=1)
    batch_size: int = Field(default=10, ge=1, le=100)
    retry_seconds: float = Field(default=1, gt=0)
    bulk_limit: int = Field(default=100, ge=1, le=500)


class CircuitSettings(EnvironmentSettings):
    """One atomic, token-fenced half-open probe per task type across workers."""

    model_config = SettingsConfigDict(env_prefix="CIRCUIT_")
    key_prefix: str = Field(default="task-scheduler:circuit:", min_length=1)
    cooldown_seconds: float = Field(default=30, ge=0.1)


class AgentSettings(EnvironmentSettings):
    """Bounded clustering, LLM egress, policy, and shared hourly spend controls."""

    model_config = SettingsConfigDict(env_prefix="AGENT_")
    enabled: bool = True
    kill_switch: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=8006, ge=1, le=65535)
    poll_seconds: float = Field(default=0.5, gt=0)
    batch_size: int = Field(default=50, ge=1, le=500)
    cluster_window_seconds: int = Field(default=60, ge=1)
    cluster_debounce_seconds: float = Field(default=2, ge=0)
    circuit_failure_threshold: int = Field(default=3, ge=2)
    confidence_threshold: float = Field(default=0.85, ge=0, le=1)
    max_auto_requeues: int = Field(default=1, ge=0, le=10)
    min_backoff_seconds: float = Field(default=30, ge=0)
    max_backoff_seconds: float = Field(default=3600, ge=0)
    max_steps: int = Field(default=4, ge=1, le=10)
    diagnosis_timeout_seconds: float = Field(default=60, gt=0)
    lease_seconds: float = Field(default=180, gt=0)
    last_errors: int = Field(default=3, ge=1, le=10)
    context_max_bytes: int = Field(default=64000, ge=4096, le=256000)
    hourly_token_budget: int = Field(default=1000000, ge=0)
    hourly_cost_budget_usd: float = Field(default=5, ge=0, allow_inf_nan=False)
    budget_key_prefix: str = Field(default="task-scheduler:agent-budget:", min_length=1)
    prompt_version: Literal["v1"] = "v1"

    @model_validator(mode="after")
    def validate_bounds(self) -> "AgentSettings":
        """Leave enough lease slack to fence timed-out or crashed diagnosis owners."""
        if self.lease_seconds <= self.diagnosis_timeout_seconds + 10:
            raise ValueError("agent lease must exceed diagnosis timeout by more than ten seconds")
        if self.max_backoff_seconds < self.min_backoff_seconds:
            raise ValueError("agent max backoff must be at least its minimum")
        return self


class JavaWorkerSettings(EnvironmentSettings):
    """Host-facing Java worker endpoints and an isolated Java 17 Maven build toolchain."""

    model_config = SettingsConfigDict(env_prefix="JAVA_WORKER_")
    host: str = "127.0.0.1"
    port: int = Field(default=8007, ge=1, le=65535)
    group_id: str = Field(default="task-workers-java", min_length=1)
    concurrency: int = Field(default=2, ge=1, le=16)
    maven_image: str = "maven:3.9.11-eclipse-temurin-17"


class MetricsSettings(EnvironmentSettings):
    """Bounded global-history snapshots and local observability endpoints."""

    model_config = SettingsConfigDict(env_prefix="METRICS_")
    scrape_timeout_seconds: float = Field(default=5, gt=0, le=30)
    stuck_running_seconds: int = Field(default=300, ge=1)


class ObservabilitySettings(EnvironmentSettings):
    """Host ports; Grafana admin credentials are generated into the ignored .env file."""

    model_config = SettingsConfigDict(env_prefix="OBSERVABILITY_")
    prometheus_port: int = Field(default=9090, ge=1, le=65535)
    grafana_port: int = Field(default=3001, ge=1, le=65535)


class Settings(EnvironmentSettings):
    """Aggregate independently prefixed service settings."""

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    api: APISettings = Field(default_factory=APISettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    dlq: DLQSettings = Field(default_factory=DLQSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    circuit: CircuitSettings = Field(default_factory=CircuitSettings)
    java_worker: JavaWorkerSettings = Field(default_factory=JavaWorkerSettings)
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    infra_check_timeout_seconds: float = Field(default=20, gt=0)


@lru_cache
def get_settings() -> Settings:
    """Load and cache settings on demand; use cache_clear after environment changes."""
    return Settings()
