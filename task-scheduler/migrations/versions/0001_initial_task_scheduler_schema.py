"""initial task scheduler schema

Revision ID: 0001
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this schema revision."""
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("topic", sa.String(length=249), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(
        "ix_outbox_unpublished_created_at",
        "outbox",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("type", sa.String(length=128), nullable=False),
        sa.Column(
            "payload", postgresql.JSONB(astext_type=sa.Text()), server_default="{}", nullable=False
        ),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "SCHEDULED",
                "RUNNING",
                "SUCCEEDED",
                "RETRYING",
                "FAILED",
                "DEAD_LETTERED",
                "AI_DIAGNOSED",
                "REQUEUED",
                "ESCALATED",
                "CANCELLED",
                name="task_status",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            server_default="PENDING",
            nullable=False,
        ),
        sa.Column("priority", sa.Integer(), server_default="0", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_retries", sa.Integer(), server_default="3", nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column(
            "scheduled_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "result", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint("attempts >= 0", name=op.f("ck_tasks_attempts_nonnegative")),
        sa.CheckConstraint(
            "max_retries BETWEEN 0 AND 100", name=op.f("ck_tasks_max_retries_range")
        ),
        sa.CheckConstraint("priority BETWEEN 0 AND 9", name=op.f("ck_tasks_priority_range")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tasks")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_tasks_idempotency_key")),
    )
    op.create_index(
        "ix_tasks_status_scheduled_at_priority",
        "tasks",
        ["status", "scheduled_at", "priority"],
        unique=False,
    )
    op.create_table(
        "dead_letters",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "error_history",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default="[]",
            nullable=False,
        ),
        sa.Column(
            "ai_diagnosis",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("resolution", sa.String(length=32), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "resolution IS NULL OR resolution IN ('REQUEUED', 'ESCALATED', 'CANCELLED')",
            name=op.f("ck_dead_letters_resolution_values"),
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name=op.f("fk_dead_letters_task_id_tasks"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dead_letters")),
    )
    op.create_index(op.f("ix_dead_letters_task_id"), "dead_letters", ["task_id"], unique=False)
    op.create_table(
        "task_attempts",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("stack_trace", sa.Text(), nullable=True),
        sa.CheckConstraint("attempt_no >= 1", name=op.f("ck_task_attempts_attempt_no_positive")),
        sa.CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at",
            name=op.f("ck_task_attempts_time_order"),
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name=op.f("fk_task_attempts_task_id_tasks"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("task_id", "attempt_no", name=op.f("pk_task_attempts")),
    )
    op.execute("""
        CREATE FUNCTION task_scheduler_touch_updated_at() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            NEW.updated_at = statement_timestamp();
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER tasks_touch_updated_at BEFORE UPDATE ON tasks
        FOR EACH ROW EXECUTE FUNCTION task_scheduler_touch_updated_at()
    """)


def downgrade() -> None:
    """Reverse this schema revision."""
    op.execute("DROP TRIGGER tasks_touch_updated_at ON tasks")
    op.execute("DROP FUNCTION task_scheduler_touch_updated_at()")
    op.drop_table("task_attempts")
    op.drop_index(op.f("ix_dead_letters_task_id"), table_name="dead_letters")
    op.drop_table("dead_letters")
    op.drop_index("ix_tasks_status_scheduled_at_priority", table_name="tasks")
    op.drop_table("tasks")
    op.drop_index(
        "ix_outbox_unpublished_created_at",
        table_name="outbox",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox")
