"""DLQ audit and execution generations

Revision ID: 0004
Revises: 0003
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this schema revision."""
    op.create_table(
        "dlq_receipts",
        sa.Column("topic", sa.String(length=249), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column("dead_letter_id", sa.Uuid(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["dead_letter_id"],
            ["dead_letters.id"],
            name=op.f("fk_dlq_receipts_dead_letter_id_dead_letters"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("topic", "partition", "offset", name=op.f("pk_dlq_receipts")),
    )
    op.create_index(
        op.f("ix_dlq_receipts_dead_letter_id"), "dlq_receipts", ["dead_letter_id"], unique=False
    )
    op.add_column(
        "dead_letters", sa.Column("generation", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column(
        "dead_letters",
        sa.Column("task_type", sa.String(length=128), server_default="unknown", nullable=False),
    )
    op.add_column("dead_letters", sa.Column("worker_id", sa.String(length=255), nullable=True))
    op.add_column(
        "dead_letters", sa.Column("attempts", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column(
        "dead_letters",
        sa.Column("category", sa.String(length=32), server_default="unknown", nullable=False),
    )
    op.add_column(
        "dead_letters",
        sa.Column("status", sa.String(length=16), server_default="OPEN", nullable=False),
    )
    op.add_column(
        "dead_letters", sa.Column("failure_signature", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "dead_letters", sa.Column("received_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index(
        "ix_dead_letters_failure_signature", "dead_letters", ["failure_signature"], unique=False
    )
    op.create_index(
        "ix_dead_letters_status_created_at", "dead_letters", ["status", "created_at"], unique=False
    )
    op.drop_constraint(op.f("fk_dead_letters_task_id_tasks"), "dead_letters", type_="foreignkey")
    op.add_column(
        "retry_schedules", sa.Column("generation", sa.Integer(), server_default="0", nullable=False)
    )
    op.drop_constraint(op.f("uq_retry_schedules_task_id"), "retry_schedules", type_="unique")
    op.create_unique_constraint(
        op.f("uq_retry_schedules_task_id"),
        "retry_schedules",
        ["task_id", "generation", "failed_attempt"],
    )
    op.add_column(
        "task_attempts", sa.Column("generation", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column(
        "tasks", sa.Column("generation", sa.Integer(), server_default="0", nullable=False)
    )
    # Alembic does not infer changes to an existing composite primary key.
    op.drop_constraint("pk_task_attempts", "task_attempts", type_="primary")
    op.create_primary_key(
        "pk_task_attempts", "task_attempts", ["task_id", "generation", "attempt_no"]
    )
    op.create_check_constraint(op.f("ck_tasks_generation_nonnegative"), "tasks", "generation >= 0")
    op.create_check_constraint(
        op.f("ck_task_attempts_generation_nonnegative"), "task_attempts", "generation >= 0"
    )
    op.execute("""
        UPDATE dead_letters d SET
            task_type = COALESCE((SELECT type FROM tasks WHERE id = d.task_id), 'unknown'),
            worker_id = d.error_history->-1->>'worker_id',
            attempts = COALESCE((d.error_history->-1->>'attempt_no')::integer, 0),
            category = CASE WHEN lower(reason) LIKE '%poison%' THEN 'poison'
                            WHEN lower(reason) LIKE '%fatal%' THEN 'fatal'
                            WHEN lower(reason) LIKE '%exhaust%' THEN 'exhausted' ELSE 'unknown' END,
            status = CASE resolution WHEN 'REQUEUED' THEN 'RETRIED'
                                     WHEN 'CANCELLED' THEN 'DISCARDED'
                                     WHEN 'ESCALATED' THEN 'ESCALATED' ELSE 'OPEN' END
    """)


def downgrade() -> None:
    """Reverse this schema revision."""
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM tasks WHERE generation > 0)
             OR EXISTS (SELECT 1 FROM task_attempts WHERE generation > 0) THEN
            RAISE EXCEPTION 'Cannot downgrade execution generations after manual retries';
          END IF;
          IF EXISTS (SELECT 1 FROM dead_letters d WHERE NOT EXISTS
                     (SELECT 1 FROM tasks t WHERE t.id = d.task_id)) THEN
            RAISE EXCEPTION 'Cannot downgrade orphan DLQ audit records to a task foreign key';
          END IF;
        END $$
    """)
    op.drop_constraint(op.f("ck_tasks_generation_nonnegative"), "tasks", type_="check")
    op.drop_constraint(
        op.f("ck_task_attempts_generation_nonnegative"), "task_attempts", type_="check"
    )
    op.drop_constraint("pk_task_attempts", "task_attempts", type_="primary")
    op.create_primary_key("pk_task_attempts", "task_attempts", ["task_id", "attempt_no"])
    op.drop_column("tasks", "generation")
    op.drop_column("task_attempts", "generation")
    op.drop_constraint(op.f("uq_retry_schedules_task_id"), "retry_schedules", type_="unique")
    op.create_unique_constraint(
        op.f("uq_retry_schedules_task_id"),
        "retry_schedules",
        ["task_id", "failed_attempt"],
        postgresql_nulls_not_distinct=False,
    )
    op.drop_column("retry_schedules", "generation")
    op.create_foreign_key(
        op.f("fk_dead_letters_task_id_tasks"),
        "dead_letters",
        "tasks",
        ["task_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_index("ix_dead_letters_status_created_at", table_name="dead_letters")
    op.drop_index("ix_dead_letters_failure_signature", table_name="dead_letters")
    op.drop_column("dead_letters", "received_at")
    op.drop_column("dead_letters", "failure_signature")
    op.drop_column("dead_letters", "status")
    op.drop_column("dead_letters", "category")
    op.drop_column("dead_letters", "attempts")
    op.drop_column("dead_letters", "worker_id")
    op.drop_column("dead_letters", "task_type")
    op.drop_column("dead_letters", "generation")
    op.drop_index(op.f("ix_dlq_receipts_dead_letter_id"), table_name="dlq_receipts")
    op.drop_table("dlq_receipts")
