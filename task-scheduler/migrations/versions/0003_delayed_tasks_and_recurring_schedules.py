"""delayed tasks and recurring schedules

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this schema revision."""
    op.create_table(
        "schedules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=True),
        sa.Column("cron", sa.String(length=128), nullable=False),
        sa.Column("timezone", sa.String(length=64), server_default="UTC", nullable=False),
        sa.Column("type", sa.String(length=128), nullable=False),
        sa.Column(
            "payload", postgresql.JSONB(astext_type=sa.Text()), server_default="{}", nullable=False
        ),
        sa.Column("priority", sa.Integer(), server_default="5", nullable=False),
        sa.Column("max_retries", sa.Integer(), server_default="3", nullable=False),
        sa.Column(
            "missed_run_policy", sa.String(length=16), server_default="fire_once", nullable=False
        ),
        sa.Column("enabled", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint(
            "missed_run_policy IN ('fire_once', 'skip')",
            name=op.f("ck_schedules_missed_run_policy_values"),
        ),
        sa.CheckConstraint(
            "max_retries BETWEEN 0 AND 100", name=op.f("ck_schedules_max_retries_range")
        ),
        sa.CheckConstraint("priority BETWEEN 0 AND 9", name=op.f("ck_schedules_priority_range")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_schedules")),
    )
    op.create_index(
        "ix_schedules_next_run_at",
        "schedules",
        ["next_run_at"],
        unique=False,
        postgresql_where=sa.text("enabled"),
    )
    op.create_table(
        "delayed_tasks",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("release_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name=op.f("fk_delayed_tasks_task_id_tasks"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("task_id", name=op.f("pk_delayed_tasks")),
    )
    op.create_index(
        "ix_delayed_tasks_release_at",
        "delayed_tasks",
        ["release_at"],
        unique=False,
        postgresql_where=sa.text("dispatched_at IS NULL"),
    )
    op.add_column("tasks", sa.Column("schedule_id", sa.Uuid(), nullable=True))
    op.add_column("tasks", sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True))
    op.create_unique_constraint(
        op.f("uq_tasks_schedule_id"), "tasks", ["schedule_id", "scheduled_for"]
    )
    op.create_foreign_key(
        op.f("fk_tasks_schedule_id_schedules"),
        "tasks",
        "schedules",
        ["schedule_id"],
        ["id"],
        ondelete="SET NULL",
    )
    # Index already-committed future tasks, preserving their durable dispatch intent.
    op.execute("""
        INSERT INTO delayed_tasks (task_id, release_at)
        SELECT id, scheduled_at FROM tasks
        WHERE status = 'PENDING' AND attempts = 0 AND scheduled_at > clock_timestamp()
    """)
    op.execute("""
        DELETE FROM outbox WHERE topic = 'tasks.pending' AND published_at IS NULL
        AND payload->>'task_id' IN (SELECT task_id::text FROM delayed_tasks)
    """)
    op.execute("""
        UPDATE tasks SET status = 'SCHEDULED'
        WHERE id IN (SELECT task_id FROM delayed_tasks)
    """)


def downgrade() -> None:
    """Reverse this schema revision."""
    # Restore the earlier worker-deferred delivery path before removing the ledger.
    op.execute("""
        INSERT INTO outbox (id, topic, key, payload)
        SELECT gen_random_uuid(), 'tasks.pending',
               (CASE WHEN t.priority >= 7 THEN 'high' WHEN t.priority >= 3 THEN 'normal'
                     ELSE 'low' END) || ':' || t.id::text,
               jsonb_build_object('version', 1, 'task_id', t.id::text, 'type', t.type,
                                  'payload', t.payload, 'priority', t.priority,
                                  'attempt_no', t.attempts + 1, 'max_retries', t.max_retries,
                                  'scheduled_at', t.scheduled_at, 'created_at', t.created_at)
        FROM tasks t JOIN delayed_tasks d ON d.task_id = t.id
        WHERE d.dispatched_at IS NULL AND t.status = 'SCHEDULED'
    """)
    op.execute("""
        UPDATE tasks SET status = 'PENDING'
        WHERE id IN (SELECT task_id FROM delayed_tasks WHERE dispatched_at IS NULL)
          AND status = 'SCHEDULED'
    """)
    op.drop_constraint(op.f("fk_tasks_schedule_id_schedules"), "tasks", type_="foreignkey")
    op.drop_constraint(op.f("uq_tasks_schedule_id"), "tasks", type_="unique")
    op.drop_column("tasks", "scheduled_for")
    op.drop_column("tasks", "schedule_id")
    op.drop_index(
        "ix_delayed_tasks_release_at",
        table_name="delayed_tasks",
        postgresql_where=sa.text("dispatched_at IS NULL"),
    )
    op.drop_table("delayed_tasks")
    op.drop_index(
        "ix_schedules_next_run_at", table_name="schedules", postgresql_where=sa.text("enabled")
    )
    op.drop_table("schedules")
