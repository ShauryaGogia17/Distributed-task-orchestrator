"""SQLAlchemy 2.0 mappings for durable tasks, execution history, and outbox."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, validates

from shared.models import TaskStatus, validate_transition


class Base(DeclarativeBase):
    """Metadata with stable constraint names for future Alembic revisions."""

    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class Task(Base):
    """A task's current state; attempt history is retained separately."""

    __tablename__ = "tasks"
    # Fetch UPDATE-generated timestamps in the same statement, avoiding async lazy I/O.
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        CheckConstraint("priority BETWEEN 0 AND 9", name="priority_range"),
        CheckConstraint("attempts >= 0", name="attempts_nonnegative"),
        CheckConstraint("generation >= 0", name="generation_nonnegative"),
        CheckConstraint("max_retries BETWEEN 0 AND 100", name="max_retries_range"),
        Index("ix_tasks_status_scheduled_at_priority", "status", "scheduled_at", "priority"),
        UniqueConstraint("schedule_id", "scheduled_for"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    trace_id: Mapped[UUID] = mapped_column(default=uuid4, server_default=func.gen_random_uuid())
    type: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, default=dict, server_default="{}")
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, native_enum=False, create_constraint=True, name="task_status", length=32),
        default=TaskStatus.PENDING,
        server_default=TaskStatus.PENDING.value,
    )
    priority: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    ai_modified: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    ai_requeues: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_retries: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True)
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    result: Mapped[JsonValue | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text)
    schedule_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("schedules.id", ondelete="SET NULL")
    )
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    @validates("status")
    def validate_status(self, key: str, value: TaskStatus) -> TaskStatus:
        """Guard ORM attribute changes; use transition_task for concurrent updates."""
        target = TaskStatus(value)
        current = self.__dict__.get("status")
        if current is not None:
            validate_transition(TaskStatus(current), target)
        elif target != TaskStatus.PENDING:
            raise ValueError("New tasks must start in PENDING")
        return target


class TaskAttempt(Base):
    """A uniquely numbered execution attempt for a task."""

    __tablename__ = "task_attempts"
    __table_args__ = (
        CheckConstraint("attempt_no >= 1", name="attempt_no_positive"),
        CheckConstraint("generation >= 0", name="generation_nonnegative"),
        CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="time_order"),
    )

    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True
    )
    generation: Mapped[int] = mapped_column(
        Integer, primary_key=True, default=0, server_default="0"
    )
    attempt_no: Mapped[int] = mapped_column(Integer, primary_key=True)
    worker_id: Mapped[str] = mapped_column(String(255))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)
    stack_trace: Mapped[str | None] = mapped_column(Text)


class DeadLetter(Base):
    """An individual dead-letter event; a requeued task may have multiple events."""

    __tablename__ = "dead_letters"
    __table_args__ = (
        CheckConstraint(
            "resolution IS NULL OR resolution IN ('REQUEUED', 'ESCALATED', 'CANCELLED')",
            name="resolution_values",
        ),
        Index("ix_dead_letters_status_created_at", "status", "created_at"),
        Index("ix_dead_letters_failure_signature", "failure_signature"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    trace_id: Mapped[UUID] = mapped_column(default=uuid4, server_default=func.gen_random_uuid())
    # DLQ audit records outlive removed tasks and accept retained/orphan Kafka events.
    task_id: Mapped[UUID] = mapped_column(index=True)
    reason: Mapped[str] = mapped_column(Text)
    error_history: Mapped[list[dict[str, JsonValue]]] = mapped_column(
        JSONB, default=list, server_default="[]"
    )
    ai_diagnosis: Mapped[dict[str, JsonValue] | None] = mapped_column(JSONB(none_as_null=True))
    resolution: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    task_type: Mapped[str] = mapped_column(String(128), default="unknown", server_default="unknown")
    worker_id: Mapped[str | None] = mapped_column(String(255))
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    category: Mapped[str] = mapped_column(String(32), default="unknown", server_default="unknown")
    status: Mapped[str] = mapped_column(String(16), default="OPEN", server_default="OPEN")
    failure_signature: Mapped[str | None] = mapped_column(String(64))
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    agent_eligible: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    agent_cluster_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("agent_clusters.id", ondelete="SET NULL"), index=True
    )
    agent_processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AgentCluster(Base):
    """One leased diagnosis per task type, signature, and UTC time bucket."""

    __tablename__ = "agent_clusters"
    __table_args__ = (UniqueConstraint("task_type", "failure_signature", "window_start"),)
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    task_type: Mapped[str] = mapped_column(String(128))
    failure_signature: Mapped[str] = mapped_column(String(64))
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ready_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(16), default="NEW", server_default="NEW")
    representative_id: Mapped[UUID | None] = mapped_column()
    representative_payload: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSONB(none_as_null=True)
    )
    lease_token: Mapped[UUID | None] = mapped_column()
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    diagnosis: Mapped[dict[str, JsonValue] | None] = mapped_column(JSONB(none_as_null=True))
    guard_reason: Mapped[str | None] = mapped_column(Text)
    usage: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, default=dict, server_default="{}")
    circuit_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AgentAudit(Base):
    """Append-only redacted evidence for every collection, LLM turn, and policy decision."""

    __tablename__ = "agent_audit"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    cluster_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("agent_clusters.id", ondelete="SET NULL"), index=True
    )
    dead_letter_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("dead_letters.id", ondelete="SET NULL"), index=True
    )
    event: Mapped[str] = mapped_column(String(64))
    details: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DLQReceipt(Base):
    """One durable receipt per Kafka source offset, including malformed wire messages."""

    __tablename__ = "dlq_receipts"
    topic: Mapped[str] = mapped_column(String(249), primary_key=True)
    partition: Mapped[int] = mapped_column(Integer, primary_key=True)
    offset: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    dead_letter_id: Mapped[UUID] = mapped_column(
        ForeignKey("dead_letters.id", ondelete="CASCADE"), index=True
    )
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Outbox(Base):
    """An event persisted in the same transaction as its task state change."""

    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished_created_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    trace_id: Mapped[UUID] = mapped_column(default=uuid4, server_default=func.gen_random_uuid())
    topic: Mapped[str] = mapped_column(String(249))
    key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RetrySchedule(Base):
    """Durable delayed inbox, marked received before its Kafka offset is committed."""

    __tablename__ = "retry_schedules"
    __table_args__ = (
        UniqueConstraint("task_id", "generation", "failed_attempt"),
        CheckConstraint("failed_attempt >= 1", name="failed_attempt_positive"),
        Index(
            "ix_retry_schedules_due_at",
            "due_at",
            postgresql_where=text("dispatched_at IS NULL AND received_at IS NOT NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    task_id: Mapped[UUID] = mapped_column(ForeignKey("tasks.id", ondelete="CASCADE"))
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    failed_attempt: Mapped[int] = mapped_column(Integer)
    due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Schedule(Base):
    """A recurring task definition and its next unprocessed cron occurrence."""

    __tablename__ = "schedules"
    __table_args__ = (
        CheckConstraint("priority BETWEEN 0 AND 9", name="priority_range"),
        CheckConstraint("max_retries BETWEEN 0 AND 100", name="max_retries_range"),
        CheckConstraint(
            "missed_run_policy IN ('fire_once', 'skip')", name="missed_run_policy_values"
        ),
        Index("ix_schedules_next_run_at", "next_run_at", postgresql_where=text("enabled")),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str | None] = mapped_column(String(128))
    cron: Mapped[str] = mapped_column(String(128))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC", server_default="UTC")
    type: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, default=dict, server_default="{}")
    priority: Mapped[int] = mapped_column(Integer, default=5, server_default="5")
    max_retries: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    missed_run_policy: Mapped[str] = mapped_column(
        String(16), default="fire_once", server_default="fire_once"
    )
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    __mapper_args__ = {"eager_defaults": True}


class DelayedTask(Base):
    """Durable recovery ledger for entries atomically removed from the Redis due queue."""

    __tablename__ = "delayed_tasks"
    __table_args__ = (
        Index(
            "ix_delayed_tasks_release_at",
            "release_at",
            postgresql_where=text("dispatched_at IS NULL"),
        ),
    )

    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True
    )
    release_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
