"""Create the durable delayed retry inbox.

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Create the inbox with idempotent retry identity and a due-work partial index."""
    op.create_table(
        "retry_schedules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("failed_attempt", sa.Integer(), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_retry_schedules")),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            ondelete="CASCADE",
            name=op.f("fk_retry_schedules_task_id_tasks"),
        ),
        sa.UniqueConstraint("task_id", "failed_attempt", name=op.f("uq_retry_schedules_task_id")),
        sa.CheckConstraint(
            "failed_attempt >= 1", name=op.f("ck_retry_schedules_failed_attempt_positive")
        ),
    )
    op.create_index(
        "ix_retry_schedules_due_at",
        "retry_schedules",
        ["due_at"],
        postgresql_where=sa.text("dispatched_at IS NULL AND received_at IS NOT NULL"),
    )


def downgrade() -> None:
    """Remove only this revision's inbox and index."""
    op.drop_index("ix_retry_schedules_due_at", table_name="retry_schedules")
    op.drop_table("retry_schedules")
