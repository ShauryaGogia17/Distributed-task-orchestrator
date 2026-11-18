"""durable agent clusters audit and AI recovery metadata

Revision ID: 0005
Revises: 0004
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this schema revision."""
    op.create_table(
        "agent_clusters",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_type", sa.String(length=128), nullable=False),
        sa.Column("failure_signature", sa.String(length=64), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="NEW", nullable=False),
        sa.Column("representative_id", sa.Uuid(), nullable=True),
        sa.Column(
            "representative_payload",
            postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "diagnosis", postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True
        ),
        sa.Column("guard_reason", sa.Text(), nullable=True),
        sa.Column(
            "usage", postgresql.JSONB(astext_type=sa.Text()), server_default="{}", nullable=False
        ),
        sa.Column("circuit_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agent_clusters")),
        sa.UniqueConstraint(
            "task_type",
            "failure_signature",
            "window_start",
            name=op.f("uq_agent_clusters_task_type"),
        ),
    )
    op.create_index(
        op.f("ix_agent_clusters_ready_at"), "agent_clusters", ["ready_at"], unique=False
    )
    op.create_table(
        "agent_audit",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("cluster_id", sa.Uuid(), nullable=True),
        sa.Column("dead_letter_id", sa.Uuid(), nullable=True),
        sa.Column("event", sa.String(length=64), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["agent_clusters.id"],
            name=op.f("fk_agent_audit_cluster_id_agent_clusters"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["dead_letter_id"],
            ["dead_letters.id"],
            name=op.f("fk_agent_audit_dead_letter_id_dead_letters"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agent_audit")),
    )
    op.create_index(op.f("ix_agent_audit_cluster_id"), "agent_audit", ["cluster_id"], unique=False)
    op.create_index(
        op.f("ix_agent_audit_dead_letter_id"), "agent_audit", ["dead_letter_id"], unique=False
    )
    op.add_column(
        "dead_letters",
        sa.Column("agent_eligible", sa.Boolean(), server_default="false", nullable=False),
    )
    # Only new incidents enter the agent automatically; historical audits remain untouched.
    op.alter_column("dead_letters", "agent_eligible", server_default=sa.text("true"))
    op.add_column("dead_letters", sa.Column("agent_cluster_id", sa.Uuid(), nullable=True))
    op.add_column(
        "dead_letters", sa.Column("agent_processed_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index(
        op.f("ix_dead_letters_agent_cluster_id"), "dead_letters", ["agent_cluster_id"], unique=False
    )
    op.create_foreign_key(
        op.f("fk_dead_letters_agent_cluster_id_agent_clusters"),
        "dead_letters",
        "agent_clusters",
        ["agent_cluster_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column(
        "tasks", sa.Column("ai_modified", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column(
        "tasks", sa.Column("ai_requeues", sa.Integer(), server_default="0", nullable=False)
    )


def downgrade() -> None:
    """Reverse this schema revision."""
    op.drop_column("tasks", "ai_requeues")
    op.drop_column("tasks", "ai_modified")
    op.drop_constraint(
        op.f("fk_dead_letters_agent_cluster_id_agent_clusters"), "dead_letters", type_="foreignkey"
    )
    op.drop_index(op.f("ix_dead_letters_agent_cluster_id"), table_name="dead_letters")
    op.drop_column("dead_letters", "agent_processed_at")
    op.drop_column("dead_letters", "agent_cluster_id")
    op.drop_column("dead_letters", "agent_eligible")
    op.drop_index(op.f("ix_agent_audit_dead_letter_id"), table_name="agent_audit")
    op.drop_index(op.f("ix_agent_audit_cluster_id"), table_name="agent_audit")
    op.drop_table("agent_audit")
    op.drop_index(op.f("ix_agent_clusters_ready_at"), table_name="agent_clusters")
    op.drop_table("agent_clusters")
